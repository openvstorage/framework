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
Package Factory module
"""
from ovs_extensions.db.arakoon.pyrakoon.pyrakoon.compat import ArakoonException
from ovs.extensions.generic.configuration import Configuration
from ovs.extensions.generic.logger import Logger
from ovs_extensions.packages.packagefactory import PackageFactory as _PackageFactory


class PackageFactory(_PackageFactory):
    """
    Factory class returning specialized classes
    """
    _logger = Logger('package_factory')

    universal_packages = ['arakoon', 'openvstorage', 'openvstorage-backend', 'openvstorage-sdm']
    ose_only_packages = ['alba', 'volumedriver-no-dedup-base', 'volumedriver-no-dedup-server']
    ee_only_packages = ['alba-ee', 'volumedriver-ee-base', 'volumedriver-ee-server']

    universal_binaries = ['arakoon']
    ose_only_binaries = ['alba', 'volumedriver-no-dedup-server']
    ee_only_binaries = ['alba-ee', 'volumedriver-ee-server']

    @classmethod
    def _get_packages(cls):
        package_names = cls.ose_only_packages + cls.ee_only_packages
        binaries = cls.ose_only_binaries + cls.ee_only_binaries
        try:
            if Configuration.exists('/ovs/framework/edition'):
                edition = Configuration.get('/ovs/framework/edition')
                if edition == 'community':
                    package_names = cls.ose_only_packages
                    binaries = cls.ose_only_binaries
                elif edition == 'enterprise':
                    package_names = cls.ee_only_packages
                    binaries = cls.ee_only_binaries
                else:
                    raise ValueError('Edition could not be found in configuration')
        except ArakoonException:
            cls._logger.exception('Unable to connect to the configuration Arakoon. Returning all packages/binaries')
        return {'names': package_names + cls.universal_packages,
                'binaries': binaries + cls.universal_binaries}

    @classmethod
    def _get_versions(cls):
        return {'alba': 'alba version --terse',
                'arakoon': "arakoon --version | grep version: | awk '{print $2}'",
                'storagedriver': "volumedriver_fs --version | grep version: | awk '{print $2}'"}
