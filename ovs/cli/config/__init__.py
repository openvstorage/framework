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

from .config import edit, list, list_recursive, get
from ovs_extensions.cli import OVSGroup

config_group = OVSGroup('config', help='Use OVS config management')
config_group.add_command(edit)
config_group.add_command(list)
config_group.add_command(list_recursive)
config_group.add_command(get)
