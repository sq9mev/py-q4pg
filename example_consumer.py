import taskregistry
from example_tasks import tq, add, sub
import logging



if __name__ == "__main__":
    logger=logging.getLogger()
    logger.setLevel(logging.DEBUG)


    for (k, v) in taskregistry.tasks.get_all().items():
        print '%s: %s' % (k, v)
    tq.run()
