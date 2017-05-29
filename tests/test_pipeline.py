from gevent_pipeline import Pipeline, ClosableQueue, worker, forward_input

import gevent
from gevent import queue

import functools
import itertools
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


def test_worker_raises():
    @worker()
    def f(x):
        if x == 'raise':
            raise ValueError()
        return x

    q_in = ClosableQueue(fuzz=0.01)
    q_out = ClosableQueue(fuzz=0.01)
    q_done = queue.Queue()

    q_in.put(0)
    q_in.put('raise')
    q_in.put(1)

    with pytest.raises(ValueError):
        f(q_in, q_out, q_done)

    # Should have signaled done
    q_done.get_nowait()

    assert q_out.get_nowait() == 0

    # Should not have continued
    with pytest.raises(queue.Empty):
        q_out.get_nowait()


def test_worker_in_out():
    @worker()
    def f(x):
        return x*x

    q_in = ClosableQueue(fuzz=0.01)
    q_out = ClosableQueue(fuzz=0.01)
    q_done = queue.Queue()

    for i in range(4):
        q_in.put(i)
    q_in.put(StopIteration)

    f(q_in, q_out, q_done)

    # Should have signaled done
    q_done.get_nowait()

    q_out.put(StopIteration)
    assert 3*3 + 2*2 + 1 == sum(i for i in q_out)


@repeat(10)
def test_pipeline():

    @worker()
    def a(x):
        return x * 2

    @worker()
    def b(x):
        return x + 1

    def load(q_in):
        for i in range(10):
            q_in.put(i)
        q_in.close()

    q_in = ClosableQueue()
    q_out = ClosableQueue()
    gevent.spawn(load, q_in)

    (Pipeline(q_in)
        .chain_workers(a, n_workers=3)
        .chain_workers(b, n_workers=3, q_out=q_out))

    assert 100 == sum(i for i in q_out)


def test_pipeline_fold():
    def add(x, y):
        gevent.sleep(random.uniform(0, 0.001))
        return x + y

    x = (Pipeline()
         .from_iter(range(0))
         .fold(add, x0=7))
    assert x == 7

    x = (Pipeline()
         .from_iter(range(10))
         .fold(add, x0=7, n_workers=8))
    assert x == 52


def test_pipeline_fromto_iter():
    def doubler(x):
        gevent.sleep(random.uniform(0, 0.001))
        return x*x

    p = Pipeline()\
        .from_iter(range(10))\
        .map(doubler, n_workers=10)

    l = sorted(p)
    assert l == [i*i for i in range(10)]

    p.join()


def test_pipeline_sloppy_map():
    @worker(exception_handler=forward_input)
    def f(x):
        if x & 1:
            raise ValueError("oh no!")
        else:
            return 2 * x

    p = (Pipeline()
         .from_iter(range(100))
         .chain_workers(f, n_workers=10))

    s_odd = sum(range(1, 100, 2))
    s_even = sum(2*i for i in range(0, 100, 2))
    assert sum(p) == s_odd + s_even


def test_pipeline_filter():
    bad_values = set((34, 'abc', False))
    good_values = set((None, 31))

    def f(x):
        return x not in bad_values

    p = (Pipeline()
         .from_iter(itertools.chain(bad_values, good_values))
         .filter(f))

    result = set(p)
    assert result == good_values
