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

    def _unstick(self):
        """
        Unstick all blocking getters
        """
        if not self._closed:
            raise RuntimeError("Don't start unsticking before closing")

        gevent.sleep()
        while len(self.getters) > len(self.putters):
            gevent.sleep()
            try:
                super().put(StopIteration, block=False)
            except queue.Full:
                pass

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
        gevent.spawn(self._unstick)

    def put(self, item, *args, **kwargs):
        if self._closed:
            raise RuntimeError("Cannot put to a closed queue")

        result = super().put(item, *args, **kwargs)
        kwargs.get('block', True) and self._fuzz()
        return result

    def get(self, *args, **kwargs):
        if not self._closed:
            item = super().get(*args, **kwargs)
            kwargs.get('block', True) and self._fuzz()
            return item

        try:
            # Note: cannot use super().get_nowait here as that will just call this function again
            return super().get(block=False)
        except queue.Empty:
            return StopIteration
