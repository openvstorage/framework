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
import os
from ovs_extensions.constants.logging import LOG_PATH, EXTENSIONS_LOGGER_NAME

OVS_LOGGER = 'ovs'
API_LOGGER = 'api'

# These loggers should ideally always be configured separately or let them propagate to a root logger
CORE_LOGGERS = [OVS_LOGGER, API_LOGGER, EXTENSIONS_LOGGER_NAME]

# API
OAUTH_LOGGER = '{0}.oauth2'.format(API_LOGGER)

# OVS
UPDATE_LOGGER = '{0}.update'.format(OVS_LOGGER)

CELERY_LOGGER = '{0}.celery'.format(OVS_LOGGER)
CELERY_RUN_LOGGER = '{0}.run'.format(CELERY_LOGGER)
CELERY_BEAT_LOGGER = '{0}.beat'.format(CELERY_LOGGER)

RABBITMQ_LOGGER = '{0}.rabbitmq'.format(OVS_LOGGER)

VOLUMEDRIVER_LOGGER = '{0}.volumedriver'.format(OVS_LOGGER)
VOLUMEDRIVER_EVENT_LOGGER = '{0}.events'.format(VOLUMEDRIVER_LOGGER)

SUPPORT_AGENT_LOGGER = '{0}.support_agent'.format(OVS_LOGGER)

UNITTEST_LOGGER = '{0}.unittest'.format(OVS_LOGGER)

WATCHER_LOGGER = '{0}.watcher'.format(OVS_LOGGER)

LIB_LOGGER = '{0}.lib'.format(OVS_LOGGER)
EXTENSIONS_LOGGER = '{}.extensions'.format(OVS_LOGGER)
DAL_LOGGER = '{0}.dal'.format(OVS_LOGGER)

# File pairs
LOGGER_FILE_MAP = {'': os.path.join(LOG_PATH, 'root_logger.log'),
                   API_LOGGER: os.path.join(LOG_PATH, 'api.log'),
                   DAL_LOGGER: os.path.join(LOG_PATH, 'dal.log'),
                   LIB_LOGGER: os.path.join(LOG_PATH, 'lib.log'),
                   OVS_LOGGER: os.path.join(LOG_PATH, 'ovs.log'),
                   UPDATE_LOGGER: os.path.join(LOG_PATH, 'update.log'),
                   CELERY_LOGGER: os.path.join(LOG_PATH, 'celery.log'),
                   UNITTEST_LOGGER: os.path.join(LOG_PATH, 'unittest.log'),
                   RABBITMQ_LOGGER: os.path.join(LOG_PATH, 'rabbitmq.log'),
                   WATCHER_LOGGER: os.path.join(LOG_PATH, 'ovs_watcher.log'),
                   SUPPORT_AGENT_LOGGER: os.path.join(LOG_PATH, 'support_agent.log'),
                   EXTENSIONS_LOGGER_NAME: os.path.join(LOG_PATH, 'ovs_extensions.log'),
                   VOLUMEDRIVER_LOGGER: os.path.join(LOG_PATH, 'framework_volumedriver.log')}

VOLUMEDRIVER_CLIENT_LOG_PATH = os.path.join(LOG_PATH, 'storagerouterclient.log')

OVS_SHELL_LOG_PATH = os.path.join(LOG_PATH, 'ovs_shell.log')
