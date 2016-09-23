from gevent_pipeline import ClosableQueue

import gevent
from gevent import queue

import functools
import pytest


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

    def closer():
        for _ in trigger_close:
            cq.close()

    w = gevent.spawn(blocked_reader)
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
