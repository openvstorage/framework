import os
import imp
import json
import logging
import inspect
from ovs.extensions.generic.configuration import Configuration

class Migrator(object):
    def __init__(self):
        pass

    @staticmethod
    def migrate():
        """ Executes all migrations. It keeps track of an internal "migration version" which is always increasing by one """

        loglevel = logging.root.manager.disable  # Workaround for disabling Arakoon logging
        logging.disable('WARNING')

        data = json.loads(Configuration.get('ovs.core.plugin_versions'))
        migrators = []
        path = os.path.join(os.path.dirname(__file__), 'migration')
        for filename in os.listdir(path):
            if os.path.isfile(os.path.join(path, filename)) and filename.endswith('.py'):
                name = filename.replace('.py', '')
                module = imp.load_source(name, os.path.join(path, filename))
                for member in inspect.getmembers(module):
                    if inspect.isclass(member[1]) and member[1].__module__ == name and 'object' in [base.__name__ for base in member[1].__bases__]:
                        migrators.append((member[1].identifier, member[1].migrate))

        end_version = 0
        for identifier, method in migrators:
            base_version = data[identifier] if identifier in data else 0
            version = method(base_version)
            if version > end_version:
                end_version = version
            data[identifier] = end_version

        Configuration.set('ovs.core.plugin_versions', json.dumps(data))

        logging.disable(loglevel)  # Restore workaround
