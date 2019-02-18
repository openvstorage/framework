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
import copy
import logging
import requests
import logging.config
from ovs.constants.logging import LOGGER_FILE_MAP, LOGGER_FILE_MAP_ALWAYS_FILE, CORE_LOGGERS, VOLUMEDRIVER_CLIENT_LOG_PATH, OVS_SHELL_LOG_PATH
from ovs_extensions.constants.logging import TARGET_TYPES, TARGET_TYPE_REDIS, TARGET_TYPE_CONSOLE, TARGET_TYPE_FILE
from ovs_extensions.log import OVS_FORMATTER_CONFIG, LogFormatter, LOG_FORMAT
from ovs_extensions.db.arakoon.pyrakoon.pyrakoon.compat import ArakoonException
from ovs.extensions.generic.configuration import Configuration, NotFoundException
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


# Handlers
OVS_REDIS_HANDLER = 'ovs_redis'
OVS_STREAM_HANDLER = 'ovs_console'
OVS_FILE_HANDLER = 'ovs_file_{0}'  # Format is the name of the associated logger
OVS_FORMATTER_NAME = 'ovs'

# Configures the logger
LIBRARY_LOG_CONFIGS = {'urllib3': {'level': 'WARNING'},
                       'paramiko': {'level': 'WARNING'},
                       'requests': {'level': 'WARNING'}}

# Root logger. It only serves for leveling handling
LOGGER_ROOT_CONFIG_BASE = {'handlers': [OVS_STREAM_HANDLER],
                           'level': 'INFO'}

LOG_HANDLERS_BASE = {OVS_STREAM_HANDLER: {'class': 'logging.StreamHandler',
                                                   'formatter': OVS_FORMATTER_NAME}}

LOG_CONFIG_BASE = {'version': 1,
                   'disable_existing_loggers': False,
                   'formatters': {
                       'standard': {'format': '%(asctime)s [%(levelname)s] %(name)s: %(message)s'},
                       OVS_FORMATTER_NAME: OVS_FORMATTER_CONFIG},
                   'handlers': LOG_HANDLERS_BASE,
                   'loggers': {'': LOGGER_ROOT_CONFIG_BASE}}

LOG_CONFIG_BASE['loggers'].update(LIBRARY_LOG_CONFIGS)


class RedisLogInfoContainer(object):
    """
    Contains redis logging configuration
    """

    def __init__(self, queue='/ovs/logging', host='localhost', port=6379):
        # type: (str, str, int) -> None
        """
        Initialize the redis container
        :param queue: Optional for redis logging: set the logging queue
        :type queue: str
        :param host: Optional host for redis logging
        :type host: str
        :param port: Optional port for redis logging
        :type port: int
        """
        self.queue = queue
        self.host = host
        self.port = port

    def get_log_queue(self, source):
        # type: (str) -> str
        """
        Retrieve the log queue to use for the given source
        :param source: Source to use. Usually a logger name
        :type source: str
        :return:
        """
        if '{0}' in self.queue:
            return self.queue.format(source)
        return self.queue


class LogInfoContainer(object):
    """
    Configurable LogInfo. Level and target type can be overruled by the user
    """
    def __init__(self, level=logging.DEBUG, type=TARGET_TYPE_CONSOLE, **kwargs):
        # type: (Union[str, int], str, **any) -> None
        """
        Initialize a LogInfoContainer
        :param level: Logging level to configure
        :type level: Union[str, int]
        :param type: Type of logging. Determines the handler to be used
        :type type: str
        """
        if type not in TARGET_TYPES:
            raise ValueError('Unsupported logging type \'{0}\'. Supported types are \'{1}\''.format(type, ', '.join(TARGET_TYPES)))

        if isinstance(level, str):
            level = logging.getLevelName(level.upper())
        self.level = level
        self.log_type = type
        # Redis
        self.redis_log_container = RedisLogInfoContainer(**kwargs)

    @property
    def log_level(self):
        return getattr(logging, self.level)


def get_logging_info():
    # type: () -> LogInfoContainer
    """
    Retrieve logging information from the Configuration management
    :return: Dictionary containing logging information
    :rtype: dict
    """
    try:
        configuration = Configuration.get('/ovs/framework/logging')
    except (IOError, NotFoundException, ArakoonException):
        configuration = {}
    return LogInfoContainer(**configuration)


def get_file_logging_config(log_info, logger_names=None):
    # type: (LogInfoContainer, Optional[List[str]]) -> Tuple[dict, dict]
    """
    Retrieve loghandlers and loggers configuration for logging to file
    :param log_info: Logging information retrieved
    :type log_info: LogInfoContainer
    :param logger_names: Logger to configure the handlers/loggers for
    :type logger_names: List[str]
    :return: The handler configs and logger configs
    ;:rtype: Tuple[dict, dict]
    """
    _ = log_info

    handlers = {}
    loggers = {}
    # Multiple file handlers need to be created as each file handler logs to a different file
    for logger_name, log_file_path in LOGGER_FILE_MAP.iteritems():
        if logger_names and logger_name not in logger_names:
            continue
        log_handler_name = OVS_FILE_HANDLER.format(logger_name)
        handlers[log_handler_name] = {'class': 'logging.FileHandler',
                                      'formatter': OVS_FORMATTER_NAME,
                                      'filename': log_file_path}
        # Non root loggers will propagate and creat duplicate logs within the root loggers file
        # Currently the case as the console logging should both output to file and the console for the special loggers
        if logger_name in LOGGER_FILE_MAP_ALWAYS_FILE:
            logger_config = {'handlers': [log_handler_name], 'propagate': True, 'level': logging.DEBUG}
        else:
            logger_config = {'handlers': [log_handler_name], 'propagate': True}

        loggers[logger_name] = logger_config

    return handlers, loggers


def get_redis_logging_config(log_info):
    # type: (LogInfoContainer) -> Tuple[dict, dict]
    """
    Retrieve loghandlers and loggers configuration for Redis
    :param log_info: Logging information retrieved
    :type log_info: LogInfoContainer
    :return: The handler configs and logger configs
    ;:rtype: Tuple[dict, dict]
    """
    from redis import Redis
    from ovs_extensions.log.redis_logging import RedisListHandler

    handlers = {}
    loggers = {}

    # @todo the source aspect was lost. Redis is not used for logging at the moment but it might be needed in the future
    logging_queue = log_info.redis_log_container.get_log_queue('')
    handlers[OVS_REDIS_HANDLER] = {'class': RedisListHandler.__module__ + '.' + RedisListHandler.__name__,
                                   'formatter': OVS_FORMATTER_NAME,
                                   'queue': logging_queue,
                                   'client': Redis(host=log_info.redis_log_container.host,
                                                   port=log_info.redis_log_container.port)}
    logger_config = {'handlers': [OVS_REDIS_HANDLER], 'propagate': True, 'level': log_info.level}

    # Reconfigure the loggers
    for logger_name in CORE_LOGGERS:
        loggers[logger_name] = logger_config

    return handlers, loggers


def get_log_config():
    # type: () -> dict
    """
    Retrieve the log config as the user configured it in the config management
    Generating the config completely instead of reconfiguring the loggers after the default config was applied
    Reconfiguration would require handlers to be removed and would be more of a hassle
    Note: loggers pass log records both to own handlers and to parent logger objects when propagating
    See: https://docs.python.org/2/howto/logging.html#logging-flow
    :return: The log config
    :rtype: dict
    """
    # @todo configure ovs logger - see current logger implementation for level/file
    log_info = get_logging_info()

    handlers = {}
    loggers = {}

    # Set the root logger level.
    logger_root_config = LOGGER_ROOT_CONFIG_BASE.copy()
    logger_root_config['level'] = log_info.level

    loggers[''] = logger_root_config

    new_loggers = new_handlers = {}
    if log_info.log_type == TARGET_TYPE_FILE:
        new_handlers, new_loggers = get_file_logging_config(log_info)
    elif log_info.log_type == TARGET_TYPE_REDIS:
        new_handlers, new_loggers = get_redis_logging_config(log_info)
    elif log_info.log_type == TARGET_TYPE_CONSOLE:
        # Always make sure that the update logger logs to a file
        new_handlers, new_loggers = get_file_logging_config(log_info, LOGGER_FILE_MAP_ALWAYS_FILE.keys())

    handlers.update(new_handlers)
    loggers.update(new_loggers)

    logger_config = copy.deepcopy(LOG_CONFIG_BASE)
    logger_config['loggers'].update(loggers)
    logger_config['handlers'].update(handlers)

    return logger_config


def get_log_config_shells(log_path=OVS_SHELL_LOG_PATH):
    # type: (str) -> dict
    """
    Get the logging config for ovs shells
    Configures the root logger to log to both stdout and the given file
    :param log_path: Log file to log to
    :type log_path: str
    :return: The logging config
    :rtype: dict
    """
    log_info = get_logging_info()

    logger_root_config = copy.deepcopy(LOGGER_ROOT_CONFIG_BASE)
    logger_root_config['level'] = log_info.level

    # Add the file handler
    handler_name = 'ovs_shell'
    handler_config = {'class': 'logging.FileHandler', 'formatter': OVS_FORMATTER_NAME, 'filename': log_path}
    handlers = {handler_name: handler_config}

    logger_root_config['handlers'].append(handler_name)

    logger_config = copy.deepcopy(LOG_CONFIG_BASE)
    logger_config['loggers'][''] = logger_root_config
    logger_config['handlers'].update(handlers)

    return logger_config


def get_ovs_streamhandler():
    # type: () -> logging.StreamHandler
    """
    Build an ovs streamhandler. Used for logging to console in process bound code
    :return: A streamhandler
    """
    formatter = LogFormatter(LOG_FORMAT)
    handler = logging.StreamHandler()
    handler.setFormatter(formatter)
    return handler


def configure_logging(configure_volumedriver_logging=True):
    # type: (bool) -> None
    """
    Configure the OpenvStorage logging
    """
    log_config = get_log_config()
    logging.config.dictConfig(log_config)

    # Disable requests warnings
    disable_warnings(InsecurePlatformWarning)
    disable_warnings(InsecureRequestWarning)
    disable_warnings(SNIMissingWarning)

    if configure_volumedriver_logging:
        configure_volumedriver_logger()


def configure_volumedriver_logger(log_path=VOLUMEDRIVER_CLIENT_LOG_PATH, level=None):
    # type: (str, int) -> None
    """
    Configures the logger of the volumedriver client
    Takes on the level of the root logger if no level is given
    :param log_path: Log sink path for the client
    :type log_path: str
    :param level: Log level to use (effective level)
    :type level: int
    :return: None
    """
    # Import here to avoid having the wrong singleton (unittest mode will load mocks)
    from ovs.extensions.storageserver.storagedriver import LOG_LEVEL_MAPPING
    from volumedriver.storagerouter import storagerouterclient

    if level is None:
        root_logger = logging.getLogger('')
        level = root_logger.getEffectiveLevel()

    # Setup Volumedriver logging
    log_level = LOG_LEVEL_MAPPING[level]
    # noinspection PyCallByClass,PyTypeChecker
    storagerouterclient.Logger.setupLogging(log_path, log_level)
    # noinspection PyArgumentList
    storagerouterclient.Logger.enableLogging()
