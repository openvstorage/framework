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

"""
Log configuration module
"""
import logging
import requests
import logging.config
from ovs.constants.logging import OVS_LOGGER
from ovs_extensions.log import get_ovs_formatter_config, EXTENSIONS_LOGGER_NAME
from ovs_extensions.db.arakoon.pyrakoon.pyrakoon.compat import ArakoonException
from ovs.extensions.generic.configuration import Configuration, NotFoundException
from ovs.extensions.storageserver.storagedriver import LOG_LEVEL_MAPPING
from ovs.extensions.generic.logger import Logger
try:
    from requests.packages.urllib3 import disable_warnings
except ImportError:
    try:
        reload(requests)  # Required for 2.6 > 2.7 upgrade (new requests.packages module)
    except ImportError:
        pass  # So, this reload fails because of some FileNodeWarning that can't be found. But it did reload. Yay.
    from requests.packages.urllib3 import disable_warnings
from requests.packages.urllib3.exceptions import InsecurePlatformWarning
from requests.packages.urllib3.exceptions import InsecureRequestWarning
from requests.packages.urllib3.exceptions import SNIMissingWarning
from volumedriver.storagerouter import storagerouterclient


# Configures the root logger
DEFAULT_LOGGER_CONFIG = {'handlers': ['default'],
                         'level': 'INFO',
                         'propagate': True}

DEFAULT_LOG_HANDLER_CONFIG = {'default': {'level': 'INFO',
                                          'class': 'logging.StreamHandler',
                                          'formatter': 'ovs'}}

DEFAULT_LOG_CONFIG = {'version': 1,
                      'disable_existing_loggers': False,
                      'formatters': {
                          'standard': {'format': '%(asctime)s [%(levelname)s] %(name)s: %(message)s'},
                          'ovs': get_ovs_formatter_config()
                      },
                      'handlers': DEFAULT_LOG_HANDLER_CONFIG,
                      'loggers': {OVS_LOGGER: DEFAULT_LOGGER_CONFIG,
                                  EXTENSIONS_LOGGER_NAME: DEFAULT_LOGGER_CONFIG}}


def get_logging_info():
    """
    Retrieve logging information from the Configuration management
    :return: Dictionary containing logging information
    :rtype: dict
    """
    try:
        return Configuration.get('/ovs/framework/logging')
    except (IOError, NotFoundException, ArakoonException):
        return {}


def configure_logging():
    """
    Configure the OpenvStorage logging
    - Based
    """
    # @todo configure ovs logger - see current logger implementation for level/file
    logging.config.dictConfig(DEFAULT_LOG_CONFIG)

    ovs_logger = logging.getLogger(OVS_LOGGER)

    # Disable requests warnings
    disable_warnings(InsecurePlatformWarning)
    disable_warnings(InsecureRequestWarning)
    disable_warnings(SNIMissingWarning)

    # Set api-related loggers to warning
    logging.getLogger('urllib3').setLevel(logging.WARNING)
    logging.getLogger('requests').setLevel(logging.WARNING)

    # Setup Volumedriver logging
    _log_level = LOG_LEVEL_MAPPING[ovs_logger.getEffectiveLevel()]
    # noinspection PyCallByClass,PyTypeChecker
    storagerouterclient.Logger.setupLogging(Logger.load_path('storagerouterclient'), _log_level)
    # noinspection PyArgumentList
    storagerouterclient.Logger.enableLogging()


