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
import ConfigParser
from model import Model
from brander import Brander
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

        filename = '/opt/OpenvStorage/config/main.cfg'
        parser = ConfigParser.RawConfigParser()
        parser.read(filename)

        base_version = int(parser.get('migration', 'version'))
        new_version = 0

        new_version = execute(Model.migrate, base_version, new_version)
        new_version = execute(Brander.migrate, base_version, new_version)
        logger.debug('Migration from %s to %s completed' % (base_version, new_version))

        parser.set('migration', 'version', str(new_version))

        with open(filename, 'wb') as configfile:
            parser.write(configfile)


if __name__ == '__main__':
    Migration.migrate()
