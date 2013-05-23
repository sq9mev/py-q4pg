import q4pg
import os, sys
import multiprocessing as mp
import logging

def process_result(result):
    print "result"
    print result
    pass


class TaskQueue(object):

    def __init__(self, dsn, tag, task_registry, workers=None, excepted_times_to_ignore=3):
        self.dsn = dsn
        self.tag = tag
        self.message_queue = q4pg.QueueManager(
            dsn=self.dsn,
            excepted_times_to_ignore = excepted_times_to_ignore,
        )
        self.task_registry = task_registry
        self.workers = workers or mp.cpu_count()
        self.pool = mp.Pool(self.workers)


    def run(self):
        logging.info('%d producer[s]  running, PID: %s' % (self.workers, os.getpid()))
        for task in self.message_queue.listen_item(self.tag):
            id, tag, data, created_at, except_times, state = task
            data = self.message_queue.deserializer(data)
            td = self.task_registry.get(data['task_name'])
            _module = __import__(td['c_module'], fromlist=['add', 'sub'])
            print _module.__dict__['add']

            try:
                logging.debug('got task [%d]: %s' % (id, data))
                self.pool.apply_async(_module.__dict__['add'], data['args'], data['kwargs'],
                        process_result)
            except KeyboardInterrupt:
                logging.info('good bye cruel world')
                sys.exit()
            except Exception, ex:
                raise ex
                logging.info('Got exception from task [%d]: %s' % (id, ex))

