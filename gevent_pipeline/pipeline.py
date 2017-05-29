import gevent
from gevent import queue
from functools import partial
from collections import namedtuple

from .closablequeue import ClosableQueue

WorkerExceptionContext = namedtuple('WorkerExceptionContext', ('input', 'exception', 'q_out'))


def forward_input(wec):
    """
    Exception handler that forwards input
    """
    wec.q_out.put(wec.input)


def ignore(wec):
    """
    Exception handler that silently discards error
    """
    pass


def raise_(wec):
    """
    Exception handler that raises the exception
    """
    raise


def sorter(q_in, q_out, q_done, key=None, reverse=False):
    """
    Worker that sorts incoming data

    Warning:
        Will completely exhaust input queue before forwarding to output
    """
    for value in sorted(q_in, key=key, reverse=reverse):
        q_out.put(value)

    # Signal done
    q_done.put(None)


def filterer(condition, q_in, q_out, q_done):
    """
    Puts items from q_in to q_out if `condition(item)`
    """
    for item in q_in:
        if condition(item):
            q_out.put(item)

    q_done.put(None)


def worker(exception_handler=raise_, discard_none=False):
    """
    Wraps a function to become a suitable worker for Pipeline

    Will feed the function input from q_in and pass output to q_out

    Arguments:
        exception_handler(callable):
            Called when an exception occurs during function evaluation
        discard_none(bool): Do not forward None results to output queue

    """

    def inner(f, q_in, q_out=None):
        for input_ in q_in:
            try:
                out = f(input_)
            except Exception as exc:
                wec = WorkerExceptionContext(input_, exc, q_out)
                exception_handler(wec)
                continue

            if discard_none and out is None:
                continue

            if q_out is not None:
                q_out.put(out)

    def decorator(f):
        def outer(q_in, q_out=None, q_done=None):
            try:
                inner(f, q_in, q_out=q_out)
            finally:
                if q_done is not None:
                    q_done.put(None)

        outer.__name__ = f.__name__
        outer.__doc__ = "[Worker wrapped]\n{}".format(f.__doc__)
        outer.f = f
        return outer

    return decorator


_unset = object()


class Pipeline:
    """
    Chain together operations with multiple workers for each layer.

    Example:

    >>> import operator
    >>> def only_odd(x):
    ...     return x & 1
    ...
    >>> def double(x):
    ...     return 2 * x
    ...
    >>> Pipeline()\\
    ...         .from_iter(range(100))\\
    ...         .filter(only_odd, n_workers=10)\\
    ...         .map(double, n_workers=8)\\
    ...         .fold(operator.add, x0=0, n_workers=5)
    5000

    The above is functionally equivalent to:

    >>> @worker(discard_none=True)
    ... def only_odd(x):
    ...     '''Forward only odd numbers to the next layer'''
    ...     if x & 1:
    ...        return x
    ...
    >>> @worker()
    ... def double(x):
    ...     return 2 * x
    ...
    >>> def load_numbers(q_in, q_out, q_done):
    ...     for i in range(100):
    ...         q_out.put(i)
    ...     q_done.put(None)
    ...
    >>> q_out = ClosableQueue()
    >>> p = Pipeline()\\
    ...     .chain_workers(load_numbers)\\
    ...     .chain_workers(only_odd, n_workers=10)\\
    ...     .chain_workers(double, n_workers=8, q_out=q_out)
    >>> sum(i for i in q_out)
    5000


    There is no guarantee of order:

    >>> def f(x):
    ...     gevent.sleep(random.uniform(0, 0.001))
    ...     return x
    >>> p = Pipeline().from_iter(range(10)).map(f, n_workers=5)
    >>> list(p)  # doctest: +SKIP
    [2, 1, 4, 0, 3, 5, 8, 6, 7, 9]
    """

    def __init__(self, q_in=None):
        """
        q_in: First input queue
        """
        self.q_out_prev = q_in
        self._greenlets = []

    def __iter__(self):
        q_out = self.q_out_prev

        # Have any future workers fail early
        self.q_out_prev = None

        if q_out is None:
            raise RuntimeError("Tried iterating over output, but the last output queue is None")

        yield from q_out

    @staticmethod
    def _queue_closer(q_out, q_start, q_done):
        """
        Close q_out after q_start is exhausted
        Calls get on q_done for each item from q_start to keep track

        Used to close queue after all workers are done
        """
        for _ in q_start:
            q_done.get()

        if q_out is not None:
            q_out.close()

    def _spawn(self, f, *args, **kwargs):
        w = gevent.spawn(f, *args, **kwargs)
        self._greenlets.append(w)

    def chain_workers(self, f, n_workers=1, q_out=_unset, maxsize=_unset):
        """
        Chain another set of workers

        Worker function conventions (see worker decorator for automation):
        1. takes three queues as arguments: q_in, q_out and q_done
        2. gets from q_in until it is exhausted (i.e. .get returns StopIteration)
        3. puts result(s) to q_out
        4. puts anything to q_done to indicate end of results

        The output queue from one function will is given as input queue to the next.

        Once the workers for a layer have exhausted the input queue the output queue is closed

        For each added function a new output queue is created.


        Arguments:
            f: Worker function
            n_workers: Number of greenlets to spawn
            q_out: Specify manual output queue, if unset creates new
            maxsize: maxsize of created output queue (default 2 * n_workers)
        """
        if q_out is _unset:
            maxsize = maxsize if maxsize is not _unset else 2 * n_workers
            q_out = ClosableQueue(maxsize=maxsize)
        elif maxsize is not _unset:
            raise ValueError("`maxsize` cannot be set together with `q_out`")

        q_start = queue.Queue()
        q_done = queue.Queue()

        self._spawn(Pipeline._queue_closer, q_out, q_start, q_done)

        for _ in range(n_workers):
            q_start.put(None)
            self._spawn(f, self.q_out_prev, q_out, q_done)

        q_start.put(StopIteration)

        self.q_out_prev = q_out

        return self

    def from_iter(self, iter_, *args, **kwargs):
        """
        Provide next layer with values from iterator

        Returns: self
        """
        def loader(q_in, q_out, q_done):
            if q_in is not None:
                raise ValueError("from_iter makes no sense when there is an input queue")

            try:
                for value in iter_:
                    q_out.put(value)
            finally:
                q_done.put(None)

        return self.chain_workers(loader, *args, **kwargs)

    def filter(self, f, *args, **kwargs):
        """
        Add a filter step, if f(x) is trueish then x will be passed along
        otherwise discarded.

        Returns: self
        """
        return self.chain_workers(partial(filterer, f), *args, **kwargs)

    def map(self, f, *args, **kwargs):
        """
        Like chain_worker but decorates f with worker() first

        Returns: self
        """

        g = worker()(f)
        return self.chain_workers(g, *args, **kwargs)

    def fold(self, f, x0, *args, **kwargs):
        """
        Reduce pipe to a single value.
        Will block until done

        Returns: folded value
        """
        def g(q_in, q_out, q_done):
            # 0-length protection
            result = q_in.get()

            if result is StopIteration:
                q_done.put(None)
                return

            try:
                for v in q_in:
                    result = f(result, v)
                q_out.put(result)
            finally:
                q_done.put(None)

        # Logic:
        # Start reducing with desired number of workers
        # have them dump result on intermediate queue.
        # Run a final pass with single worker to combine

        n_workers = kwargs.pop('n_workers', 1)
        q_out = kwargs.pop('q_out', ClosableQueue())
        q_interim = ClosableQueue()

        # Make sure final pass has at least x0
        q_interim.put(x0)

        # Main workload
        self.chain_workers(g, *args, q_out=q_interim, n_workers=n_workers, **kwargs)

        # Final reduction
        self.chain_workers(g, *args, q_out=q_out, n_workers=1, **kwargs)

        result = q_out.get()
        if q_out.get() is not StopIteration:
            raise RuntimeError("Unexpected data on fold output channel")
        return result

    def sort(self, key=None, reverse=False, maxsize=_unset):
        """
        Sort before passing on to the next stage

        Warning:
            Waits for all previous work to complete before feeding it to the next stage.

        Example:
            >>> import random
            >>> values = [random.randint(-100, 100) for _ in range(100)]
            >>> result = list(Pipeline().from_iter(values)
            ...                         .sort(key=lambda x: abs(x))
            ...                         .map(lambda x: x*x))
            >>> assert result == sorted(x*x for x in values)

        Arguments:
            key: Same as builtin `sorted`
            reverse: Same as builtin `sorted`
            maxsize: Size of output queue
        """
        self.chain_workers(
            partial(sorter, key=key, reverse=reverse),
            n_workers=1,
            maxsize=maxsize)
        return self

    def join(self):
        """
        Wait for the greenlets to finish
        Wrapper around gevent.joinall
        """

        done = gevent.joinall(self._greenlets)

        # TODO pass argumetns to .joinall and remove greenlets in done from _greenlets
        self._greenlets = []

        return done
