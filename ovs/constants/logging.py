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

OVS_LOGGER = 'ovs'
API_LOGGER = 'webapps'

UPDATE_LOGGER = '{}.update'.format(OVS_LOGGER)

# Celery
CELERY_LOGGER = '{}.celery'.format(OVS_LOGGER)
CELERY_RUN_LOGGER = '{}.run'.format(CELERY_LOGGER)
CELERY_BEAT_LOGGER = '{}.beat'.format(CELERY_LOGGER)

# RabbitMQ
RABBITMQ_LOGGER = '{}.rabbitmq'.format(OVS_LOGGER)

# Volumedriver
VOLUMEDRIVER_LOGGER = '{}.volumedriver'.format(OVS_LOGGER)
VOLUMEDRIVER_EVENT_LOGGER = '{}.events'.format(VOLUMEDRIVER_LOGGER)

# Support agent
SUPPORT_AGENT_LOGGER = '{}.support_agent'.format(OVS_LOGGER)

# Unittest
UNITTEST_LOGGER = '{}.unittest'.format(OVS_LOGGER)

# Watcher
WATCHER_LOGGER = '{}.watcher'.format(OVS_LOGGER)

# API
OAUTH_LOGGER = '{}.oauth2'.format(API_LOGGER)
