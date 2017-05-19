# Copyright (C) 2017 iNuron NV
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
Generic module for managing configuration somewhere
"""
from ovs_extensions.db.arakoon.arakooninstaller import ArakoonClusterConfig as _ArakoonClusterConfig, ArakoonInstaller as _ArakoonInstaller
from ovs.extensions.generic.configuration import Configuration


class ArakoonClusterConfig(_ArakoonClusterConfig):
    """
    Extends the 'default' ArakoonClusterConfig
    """

    def __init__(self, cluster_id, load_config=True, source_ip=None, plugins=None):
        """
        Initializes an empty Cluster Config
        """
        super(ArakoonClusterConfig, self).__init__(cluster_id=cluster_id,
                                                   configuration=Configuration,
                                                   load_config=load_config,
                                                   source_ip=source_ip,
                                                   plugins=plugins)

    @classmethod
    def _get_configuration(cls):
        return Configuration


class ArakoonInstaller(_ArakoonInstaller):
    """
    Class to dynamically install/(re)configure Arakoon cluster
    """

    def __init__(self, cluster_name):
        """
        ArakoonInstaller constructor
        """
        super(ArakoonInstaller, self).__init__(cluster_name=cluster_name,
                                               configuration=Configuration)

    @classmethod
    def _get_configuration(cls):
        return Configuration
