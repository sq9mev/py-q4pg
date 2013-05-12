from time import sleep

from taskqueue import TaskQueue
import taskregistry
from decorators import task


dsn='postgresql://bociek:qwerty@localhost:5432/q4pg'
tq = TaskQueue(dsn, 'default_tag', taskregistry.tasks)


@task(queue=tq, name="add")
def add(a, b):
    sleep(1)
    ret = a + b
    print ret
    return ret

@task(queue=tq, name="sub")
def sub(a, b):
    sleep(1)
    ret = a - b
    print ret
    return ret

