from time import  sleep
from psycopg2 import ProgrammingError
import logging
import sys

from example_tasks import add, sub

if __name__ == "__main__":
    logger=logging.getLogger()
    logger.setLevel(logging.DEBUG)

    while True:
        try:
            print 'id: %s' % add.delay(1,1)
            print 'id: %s' % sub.delay(1,1)
            sleep(1)
        except ProgrammingError, ex:
            logging.error(ex)
            sys.exit('Run example_consumer first, it will create queue table if needed!')
        except KeyboardInterrupt:
            sys.exit('No more tasks is beeing produced!')

