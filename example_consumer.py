import logging
from psycopg2 import ProgrammingError
import taskregistry
from example_tasks import tq, add, sub



if __name__ == "__main__":
    logger=logging.getLogger()
    logger.setLevel(logging.DEBUG)


    for (k, v) in taskregistry.tasks.get_all().items():
        print '%s: %s' % (k, v)

    try:
        tq.message_queue.create_table()
        logging.info('Table has been created!')
    except ProgrammingError, ex:
        pass

    tq.run()
