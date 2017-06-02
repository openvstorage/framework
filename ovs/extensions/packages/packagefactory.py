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
from ovs_extensions.packages.packagefactory import PackageFactory as _PackageFactory


class PackageFactory(_PackageFactory):
    """
    Factory class returning specialized classes
    """

    @classmethod
    def _get_packages(cls):
        return {'names': ['alba', 'alba-ee', 'arakoon',
                          'openvstorage', 'openvstorage-backend', 'openvstorage-sdm',
                          'volumedriver-no-dedup-base', 'volumedriver-no-dedup-server',
                          'volumedriver-ee-base', 'volumedriver-ee-server'],
                'binaries': ['alba', 'alba-ee', 'arakoon',
                             'volumedriver-no-dedup-server', 'volumedriver-ee-server']}

    @classmethod
    def _get_versions(cls):
        return {'alba': 'alba version --terse',
                'arakoon': "arakoon --version | grep version: | awk '{print $2}'",
                'storagedriver': "volumedriver_fs --version | grep version: | awk '{print $2}'"}
