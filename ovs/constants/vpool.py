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

VPOOL_UPDATE_KEY = os.path.join(os.path.sep, 'ovs', 'volumedriver', 'update', 'storagerouter')

STORAGEDRIVER_SERVICE_BASE = 'ovs-volumedriver'

PACKAGES_OSE = ['volumedriver-no-dedup-base', 'volumedriver-no-dedup-server']
PACKAGES_EE = ['volumedriver-ee-base', 'volumedriver-ee-server']

VOLUMEDRIVER_BIN_PATH = os.path.join(os.path.sep, 'usr', 'bin', 'volumedriver_fs.sh')
VOLUMEDRIVER_CMD_NAME = 'volumedriver_fs'
