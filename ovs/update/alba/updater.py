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

from ovs.extensions.generic.system import System
from ovs.extensions.storage.persistentfactory import PersistentFactory
from ovs_extensions.update.alba_component_update import AlbaComponentUpdater as _AlbacomponentUpdater
from ovs.extensions.generic.logger import Logger


class AlbaComponentUpdater(_AlbacomponentUpdater):
    """
    Implementation of abstract class to update alba
    """
    logger = Logger('update-alba')

    @staticmethod
    def get_persistent_client():
        """
        Retrieve a persistent client which needs
        Needs to be implemented by the callee
        """
        return PersistentFactory.get_client()

    @classmethod
    def get_node_id(cls):
        """
        use a factory to provide the machine id
        :return:
        """
        return System.get_my_machine_id()
