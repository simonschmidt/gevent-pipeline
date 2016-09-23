from gevent_pipeline import ClosableQueue

import gevent
from gevent import queue

import functools
import pytest
import random


def repeat(n=10):
    """Used to run racy tests multiple times"""
    def decorator(f):
        @functools.wraps(f)
        def inner(*args, **kwargs):
            for _ in range(n):
                last = f(*args, **kwargs)
            return last
        return inner
    return decorator


def test_cq_cant_close_twice():
    cq = ClosableQueue(fuzz=0.01)
    cq.put(1)
    cq.close()

    with pytest.raises(RuntimeError):
        cq.close()


def test_cq_stopiteration():
    cq = ClosableQueue(fuzz=0.01)
    cq.close()
    assert cq.get() == StopIteration
    assert cq.get() == StopIteration


def test_cq_cant_put():
    cq = ClosableQueue(fuzz=0.01)
    cq.close()

    with pytest.raises(RuntimeError):
        cq.put(1)


def test_cq_gets_remaining():
    cq = ClosableQueue(fuzz=0.01)

    for _ in range(5):
        cq.put(1)

    cq.close()

    assert 5 == sum(i for i in cq)


@repeat()
def test_cq_getter_unstuck():
    cq = ClosableQueue(fuzz=0.01)
    trigger_close = queue.Queue()

    def blocked_reader():
        """
        Read from the queue when there is nothing to get,
        shouldn't be stuck forever if queue closed
        """
        trigger_close.put_nowait(1)
        cq.get()

    w = gevent.spawn(blocked_reader)

    trigger_close.get()
    cq.close()

    w.join()


@repeat(n=100)
def test_cq_racy_get():
    """
    Tries to achieve following race condition:

    1. (getter) calls .get, takes branch `not self._closed`
    2. (closer) sets `self._closed = True`
    3. (closer) gets length of self.getters
    4. (getter) becomes a self.getter
    5. - stuck forever
    """

    cq = ClosableQueue(fuzz=0.001)

    def closer():
        cq.close()

    def getter():
        cq.get()

    gevent.joinall([
        gevent.spawn(getter),
        gevent.spawn(closer)])


@repeat(n=100)
def test_cq_racy_put():
    """
    Tries to achive following race condition:

    1. (putter) Sees is still open
    2. (closer) sets _closed to True
    3. (closer) starts sending stopiteration
    4. (putter) Finally puts
    5.  - getter misses out on value
    """

    cq = ClosableQueue(fuzz=0.001)
    q_got = queue.Queue()
    q_put_result = queue.Queue()

    def closer():
        cq.close()

    def getter():
        q_got.put(cq.get())

    def putter():
        try:
            cq.put(1)
            q_put_result.put_nowait(True)
        except RuntimeError:
            q_put_result.put(False)

    gevent.joinall([
        gevent.spawn(getter),
        gevent.spawn(putter),
        gevent.spawn(closer)])

    # Valid results:
    #  putter successfully ran AND getter got 1
    #  putter failed AND getter got StopIteration
    got = q_got.get_nowait()
    put_ok = q_put_result.get_nowait()

    # The commented out asserts here were used to ensure both branches were hit
    if put_ok:
        # assert False
        assert got == 1
    else:
        # assert False
        assert got is StopIteration


@repeat(n=100)
def test_cq_maxsize_racy():
    """
    Tries to break a queue with maxsize
    """

    maxsize = random.randint(1, 3)
    n_getters = random.randint(50, 100)
    n_putters = random.randint(50, 100)

    cq = ClosableQueue(fuzz=0.001, maxsize=maxsize)
    q_got = queue.Queue()
    q_put_result = queue.Queue()

    def closer():
        gevent.sleep(random.uniform(0, 0.01))
        cq.close()

    def getter():
        gevent.sleep(random.uniform(0, 0.01))
        try:
            q_got.put_nowait(cq.get())
        except Exception as exc:
            q_got.put_nowait(exc)

    def putter():
        gevent.sleep(random.uniform(0, 0.01))
        try:
            cq.put_nowait(1)
            q_put_result.put(True)
        except Exception:
            q_put_result.put(False)

    workers = set()

    for _ in range(n_putters):
        workers.add(gevent.spawn(putter))

    for _ in range(n_getters):
        workers.add(gevent.spawn(getter))

    workers.add(gevent.spawn(closer))
    gevent.wait(workers)

    q_put_result.put(StopIteration)
    n_ok_put = sum(1 for b in q_put_result if b)

    # Items never claimed by any getter
    n_left_on_queue = sum(1 for _ in cq)
    n_ungettable = max(0, n_ok_put - n_getters)
    assert n_left_on_queue == n_ungettable

    # Items claimed by some getter
    # Can't loop due to StopIteration behaviour
    n_got = 0
    n_stop_iter = 0
    q_got.put('last')
    while True:
        value = q_got.get()
        if value == 'last':
            break
        elif value == StopIteration:
            n_stop_iter = n_stop_iter + 1
        elif value == 1:
            n_got = n_got + 1
        else:
            raise value

    # Got as many items on q_got as there were getters spawned
    assert n_stop_iter + n_got == n_getters

    # No items lost
    n_ok_get = n_left_on_queue + n_got
    assert n_ok_get == n_ok_put
