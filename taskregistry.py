import json
import exceptions


class TaskRegistry(object):
    def __init__(self):
        self._tasks = {}

    def register(self, callable, name=None):
        name = name or '%s.%s' % (callable.__module__, callable.__name__)
        if name in self._tasks:
            raise AlreadyRegisteredError
        self._tasks.update({name: callable})
            
    def get(self, name):
        return self._tasks[name]
        
    def get_all(self):
        return dict(self._tasks.items()) # copy


tasks = TaskRegistry()

