from time import  sleep
from example_tasks import add, sub


while True:
    print 'id: %s' % add.delay(1,1)
    print 'id: %s' % sub.delay(1,1)
    sleep(1)
