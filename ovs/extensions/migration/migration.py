# Copyright 2014 CloudFounders NV
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

#!/usr/bin/env python

"""
Migration module
"""
import os
import imp
import inspect
import ConfigParser
from ovs.log.logHandler import LogHandler

logger = LogHandler('extensions', name='migrations')
logger.logger.propagate = False  # No need to propagate this


class Migration(object):
    """
    Handles all migrations between versions
    """

    @staticmethod
    def migrate():
        """
        Executes all migrations. It keeps track of an internal "migration version" which is
        a always increasing one digit version for now.
        """

        def execute(function, start, end):
            """
            Executes a single migration, syncing versions
            """
            version = function(start)
            if version > end:
                end = version
            logger.debug('Migrated %s.%s from %s to %s' % (function.__module__, function.__name__, start, end))
            return end

        cfg_filename = '/opt/OpenvStorage/config/main.cfg'
        parser = ConfigParser.RawConfigParser()
        parser.read(cfg_filename)

        # Load mapping
        migrators = []
        path = os.path.join(os.path.dirname(__file__), 'migrators')
        for filename in os.listdir(path):
            if os.path.isfile(os.path.join(path, filename)) and filename.endswith('.py'):
                name = filename.replace('.py', '')
                module = imp.load_source(name, os.path.join(path, filename))
                for member in inspect.getmembers(module):
                    if inspect.isclass(member[1]) \
                            and member[1].__module__ == name \
                            and 'object' in [base.__name__ for base in member[1].__bases__]:
                        migrators.append((member[1].identifier, member[1].migrate))
        for identifier, method in migrators:
            base_version = int(parser.get('migration', identifier) if parser.has_option('migration', identifier) else 0)
            new_version = execute(method, base_version, 0)
            parser.set('migration', identifier, str(new_version))
        logger.debug('Migrations completed')

        with open(cfg_filename, 'wb') as configfile:
            parser.write(configfile)


if __name__ == '__main__':
    Migration.migrate()
