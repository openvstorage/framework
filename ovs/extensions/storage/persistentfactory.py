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
Overrides persistent factory.
"""
from ovs.extensions.generic.configuration import Configuration
from ovs_extensions.storage.persistentfactory import PersistentFactory as _PersistentFactory


class PersistentFactory(_PersistentFactory):
    """
    The PersistentFactory will generate certain default clients.
    """

    @classmethod
    def _get_store_info(cls):
        client_type = Configuration.get('/ovs/framework/stores|persistent')
        if client_type not in ['pyrakoon', 'arakoon']:
            raise RuntimeError('Configured client type {0} is not implemented'.format(client_type))
        cluster = Configuration.get('/ovs/framework/arakoon_clusters|ovsdb')
        contents = Configuration.get('/ovs/arakoon/{0}/config'.format(cluster), raw=True)
        return {'cluster': cluster,
                'configuration': contents}

    @classmethod
    def _get_client_type(cls):
        return Configuration.get('/ovs/framework/stores|persistent')
