#!/usr/bin/env python

"""
Migration module
"""
import ConfigParser
from model import Model
from brander import Brander


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
            print 'Migrated %s.%s from %s to %s' % (function.__module__, function.__name__, start, end)
            return end

        filename = '/opt/OpenvStorage/config/version.cfg'
        parser = ConfigParser.RawConfigParser()
        parser.read(filename)

        base_version = int(parser.get('migration', 'version'))
        new_version = 0

        new_version = execute(Model.migrate, base_version, new_version)
        new_version = execute(Brander.migrate, base_version, new_version)
        print 'Migration from %s to %s completed' % (base_version, new_version)

        parser.set('migration', 'version', str(new_version))

        with open(filename, 'wb') as configfile:
            parser.write(configfile)


if __name__ == '__main__':
    Migration.migrate()
