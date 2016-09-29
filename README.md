Multi-worker pipelines with gevent.

### Installation

```bash
pip install gevent-pipeline
```

## Example

```python

import gevent
import random

from itertools import repeat
from gevent_pipeline import Pipeline

def sample(b):
    r = random.uniform(0, b)
    gevent.sleep(r)
    return r

(Pipeline()
    .from_iter(repeat(1, times=200))
    .map(sample, n_workers=100)
    .filter(lambda x: x < 0.5)
    .fold(max, x0=0, n_workers=50))
```


## Pipeline

Chain together operations with multiple workers for each layer.

Example:

```python
>>> import operator
>>> def only_odd(x):
...     return x & 1
...
>>> def double(x):
...     return 2 * x
...
>>> (Pipeline()
...         .from_iter(range(100))
...         .filter(only_odd, n_workers=10)
...         .map(double, n_workers=8)
...         .fold(operator.add, x0=0, n_workers=5))
5000
```

The above is functionally equivalent to:

```python
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
>>> p = (Pipeline()
...     .chain_workers(load_numbers)
...     .chain_workers(only_odd, n_workers=10)
...     .chain_workers(double, n_workers=8, q_out=q_out))
>>> sum(i for i in q_out)
5000
```

There is no guarantee of order:

```python
>>> def f(x):
...     gevent.sleep(random.uniform(0, 0.001))
...     return x
>>> p = Pipeline().from_iter(range(10)).map(f, n_workers=5)
>>> list(p)
[2, 1, 4, 0, 3, 5, 8, 6, 7, 9]
```


### Exceptions in workers

There is a predefined `forward_input` exception handler,
in the event the function raises an exception the handler
takes the input to the function and passes it along as if it was the output.


```python
from gevent_pipeline import Pipeline, worker, forward_input

@worker(exception_handler=forward_input)
def f(x):
    if x & 1:
        raise ValueError("oh no!")
        # Will be treated as if it were:
        # return x
    else:
        return 2 * x

p = (Pipeline()
     .from_iter(range(100))
     .chain_workers(f, n_workers=10))

s_odd = sum(range(1, 100, 2))
s_even = sum(2*i for i in range(0, 100, 2))
assert sum(p) == s_odd + s_even
```


## ClosableQueue

Acts like `gevent.queue.Queue` but in addition has a `.close()` method which invokes following behavior:

* Calling `.put(item)` becomes an error
* Successive calls to `.get()` will return whatever remains in the queue
  and after that StopIteration is returned for every subsequent call

```python
>>> from gevent_pipeline import ClosableQueue
>>> q = ClosableQueue()
>>> q.put('hello')
>>> q.close()
>>> q.get()
'hello'
>>> q.get() is StopIteration
True
```

