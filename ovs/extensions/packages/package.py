# Copyright (C) 2016 iNuron NV
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
import os
from subprocess import check_output
from ovs.extensions.packages.debian import DebianPackage
from ovs.extensions.packages.rpm import RpmPackage


class PackageManager(object):
    """
    Factory class returning specialized classes
    """
    ImplementationClass = None
    OVS_PACKAGE_NAMES = ['alba', 'alba-ee', 'arakoon',
                         'openvstorage', 'openvstorage-backend', 'openvstorage-sdm',
                         'volumedriver-no-dedup-base', 'volumedriver-no-dedup-server',
                         'volumedriver-ee-base', 'volumedriver-ee-server']
    OVS_PACKAGES_WITH_BINARIES = ['alba', 'alba-ee', 'arakoon', 'volumedriver-no-dedup-server', 'volumedriver-ee-server']

    GET_VERSION_ALBA = 'alba version --terse'
    GET_VERSION_ARAKOON = "arakoon --version | grep version: | awk '{print $2}'"
    GET_VERSION_STORAGEDRIVER = "volumedriver_fs --version | grep version: | awk '{print $2}'"

    class MetaClass(type):
        """
        Metaclass
        """

        def __getattr__(cls, item):
            """
            Returns the appropriate class
            """
            _ = cls
            if PackageManager.ImplementationClass is None:
                distributor = None
                check_lsb = check_output('which lsb_release 2>&1 || true', shell=True).strip()
                if "no lsb_release in" in check_lsb:
                    if os.path.exists('/etc/centos-release'):
                        distributor = 'CentOS'
                else:
                    distributor = check_output('lsb_release -i', shell=True)
                    distributor = distributor.replace('Distributor ID:', '').strip()
                # All *Package classes used in below code should share the exact same interface!
                if distributor in ['Ubuntu']:
                    PackageManager.ImplementationClass = DebianPackage
                elif distributor in ['CentOS']:
                    PackageManager.ImplementationClass = RpmPackage
                else:
                    raise RuntimeError('There is no handler for Distributor ID: {0}'.format(distributor))
                PackageManager.ImplementationClass.OVS_PACKAGE_NAMES = PackageManager.OVS_PACKAGE_NAMES
                PackageManager.ImplementationClass.OVS_PACKAGES_WITH_BINARIES = PackageManager.OVS_PACKAGES_WITH_BINARIES

                PackageManager.ImplementationClass.GET_VERSION_ALBA = PackageManager.GET_VERSION_ALBA
                PackageManager.ImplementationClass.GET_VERSION_ARAKOON = PackageManager.GET_VERSION_ARAKOON
                PackageManager.ImplementationClass.GET_VERSION_STORAGEDRIVER = PackageManager.GET_VERSION_STORAGEDRIVER
            return getattr(PackageManager.ImplementationClass, item)

    __metaclass__ = MetaClass
