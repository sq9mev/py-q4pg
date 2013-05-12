import q4pg
import os, sys
import logging

class TaskQueue(object):

    def __init__(self, dsn, tag, task_registry, excepted_times_to_ignore=3):
        self.dsn = dsn
        self.tag = tag
        self.message_queue = q4pg.QueueManager(
            dsn=self.dsn,
            excepted_times_to_ignore = excepted_times_to_ignore,
        )
        self.task_registry = task_registry

    def run(self):
        logging.info('producer running, PID: %s' % os.getpid())
        for task in self.message_queue.listen_item(self.tag):
            id, tag, data, created_at, except_times = task
            data = self.message_queue.deserializer(data)
            method = self.task_registry.get(data['task_name'])
            try:
                logging.debug('got task [%d]: %s' % (id, data))
                method(*data['args'], **data['kwargs'])
            except KeyboardInterrupt:
                logging.info('good bye cruel world')
                sys.exit()
            except Exception, ex:
                logging.info('Got exception from task [%d]: %s' % (id, ex))

