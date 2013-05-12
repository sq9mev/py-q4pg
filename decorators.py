from functools import wraps, partial

import exceptions
import taskregistry


def task(method=None, queue=None, name=None):
    if queue is None:
        raise ConfigurationError('Queue instance should be provided')

    if method is None:
        return partial(task, queue=queue, name=name)

    taskregistry.tasks.register(method, name)

    def delay(*args, **kwargs):
        data = {
                'task_name': name,
                'args': args,
                'kwargs': kwargs
                }
        return queue.message_queue.enqueue(queue.tag, data)

    @wraps(method)
    def f(*args, **kwrgs):
        m=method(*args, **kwargs)
        return m
    f.delay = delay
    return f




