import random
import gevent
from gevent import queue


class ClosableQueue(queue.Queue):
    """
    Acts like `gevent.queue.Queue` but in addition has a `.close()` method which invokes following behavior:

    * Calling `.put(item)` becomes an error
    * Successive calls to `.get()` will return whatever remains in the queue
      and after that StopIteration is returned for every subsequent call

    Example:
        >>> q = ClosableQueue()
        >>> q.put('hello')
        >>> q.close()
        >>> q.get()
        'hello'
        >>> q.get() is StopIteration
        True
    """
    _closed = False

    def __init__(self, *args, fuzz=None, **kwargs):
        self._fuzz_factor = fuzz
        super().__init__(*args, **kwargs)

    def _fuzz(self):
        """
        Wait random amount of time
        used in hopes of increasing chance to break tests
        """
        if self._fuzz_factor is not None:
            gevent.sleep(random.uniform(0, self._fuzz_factor))

    @property
    def closed(self):
        return self._closed

    def close(self, once=True):
        """
        Close the queue

        Arguments:
            once(bool): Raise error if closing closed queue
        """
        self._fuzz()
        if once and self._closed:
            raise RuntimeError("Tried closing already closed queue")

        self._closed = True

        # Unstick all waiting getters, no need to see if there are any putters
        # as there is no harm in a few extra StopIterations.
        #
        # Has assumptions:
        # 1. No interruption between setting _closed and obtaining getters
        #     - OK! Just simple objects, no place where gevent switches
        # 2. No interruption in .get between _closed check and beieng added to self.getters
        #     - OK! Either nonblocking or __get_or_peek which appends to getters before waiting
        # 3. That any .put always ends up before StopIteration
        #     - Ok! Either put resolves without blocking or wakes getters only or is appended
        #       to putters before blocking. So there is no time for _closed to change between
        #       self.put checking it and the put ending up in an ok situation.
        for _ in range(len(self.getters)):
            self._fuzz()
            super().put(StopIteration)

    def put(self, item, *args, **kwargs):
        kwargs.get('block', True) and self._fuzz()

        if self._closed:
            raise RuntimeError("Cannot put to a closed queue")

        return super().put(item, *args, **kwargs)

    def get(self, *args, **kwargs):
        kwargs.get('block', True) and self._fuzz()

        if not self._closed:
            return super().get(*args, **kwargs)

        try:
            # Note: cannot use super().get_nowait here as that will just call this function again
            return super().get(block=False)
        except Exception:
            return StopIteration
