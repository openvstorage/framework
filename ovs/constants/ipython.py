# Copyright (C) 2019 iNuron NV
#
# This file is part of Open vStorage Open Source Edition (OSE),
# as available from
#
#      http://www.openvstorage.org and
#      http://www.openvstorage.com.
#
# This file is free software; you can redistribute it and/or modify it
# under the terms of the GNU Affero General Public License v3 (GNU AGPLv3)
# as published by the Free Software Foundation, in version 3 as it comes
# in the LICENSE.txt file of the Open vStorage OSE distribution.
#
# Open vStorage is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY of any kind.

COMMAND_PROFILE_LOCATE = ['ipython', 'locate', 'profile']
COMMAND_PROFILE_CREATE = ['ipython', 'profile', 'create']

CONFIG_FILE_NAME = 'ipython_config.py'
LOGGING_EXEC_LINES = ['import logging.config',
                      'from ovs.extensions.log import get_log_config_shells',
                      'logging.config.dictConfig(get_log_config_shells())']
LOGGING_EXEC_LINES_CONFIG = 'c.InteractiveShellApp.exec_lines = {}\n'.format(LOGGING_EXEC_LINES)
