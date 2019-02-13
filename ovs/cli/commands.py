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
import copy
import click
import logging
import logging.config
from ovs_extensions.cli import OVSCommand as _OVSCommand
from ovs.constants.logging import LOG_PATH
from ovs_extensions.generic.unittests import enable_unittest_mode, disable_unittest_mode

logger = logging.getLogger(__name__)


def configure_cli_logging(unittest_mode=False):
    # type: (bool) -> None
    """
    Configure the logging when running the ovs cli
    Makes everything log to 'ovs_cli.log' when not running unittests
    """
    # Avoid side effects from importing. Keep the volumedriver singletons mocked when unittesting
    from ovs.extensions.log import LOG_CONFIG_BASE, OVS_FORMATTER_NAME, configure_volumedriver_logger, get_logging_info

    cli_handler_name = 'cli_handler'

    log_info = get_logging_info()
    logger_root_config = {'handlers': [cli_handler_name],
                          'level': log_info.level}
    # Configure the root logger
    LOG_CONFIG_BASE['loggers'][''] = logger_root_config
    # Setup the handler. Re-uses the ovs formatter
    cli_log_path = None
    handler_config = {'formatter': OVS_FORMATTER_NAME}
    if unittest_mode:
        # Attach a nullhandler. Suppress all logging
        handler_config.update({'class': 'logging.NullHandler'})
    else:
        cli_log_path = os.path.join(LOG_PATH, 'ovs_cli.log')
        handler_config.update({'class': 'logging.FileHandler',
                               'filename': os.path.join(LOG_PATH, 'ovs_cli.log')})

    logger_config = copy.deepcopy(LOG_CONFIG_BASE)
    logger_config['loggers'][''] = logger_root_config
    logger_config['handlers'] = {cli_handler_name: handler_config}

    logging.config.dictConfig(logger_config)

    if cli_log_path:
        configure_volumedriver_logger(cli_log_path)


class UnittestCommand(_OVSCommand):

    """
    Command used to run the unittests with
    """

    def invoke(self, ctx):
        # type: (click.Context) -> None
        """
        Invoke the command
        """
        try:
            enable_unittest_mode()
            configure_cli_logging(True)
            # Log the start of the command with the current time
            logger.info('Starting command: {0}'.format(ctx.command_path))
            super(UnittestCommand, self).invoke(ctx)
        finally:
            disable_unittest_mode()


class OVSCommand(_OVSCommand):

    """
    Command used to run ovs commands with
    """

    def invoke(self, ctx):
        # type: (click.Context) -> None
        """
        Invoke the command
        """
        configure_cli_logging()
        # Log the start of the command with the current time
        logger.info('Starting command: {0}'.format(ctx.command_path))

        super(OVSCommand, self).invoke(ctx)
