# Copyright (C) 2018 iNuron NV
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
VDisk Constants module. Contains constants related to vdisks
"""

# General
LOCK_NAMESPACE = 'ovs_locks'

# Scrub related
SCRUB_VDISK_LOCK = '{0}_{{0}}'.format(LOCK_NAMESPACE)  # Second format is the vdisk guid

# Snapshot related
# Note: the scheduled task will always skip the first 24 hours before enforcing the policy
SNAPSHOT_POLICY_DEFAULT = [{'nr_of_snapshots': 24, 'nr_of_days': 1},  # One per hour
                           # one per day for rest of the week and opt for a consistent snapshot for the first day
                           {'nr_of_snapshots': 6, 'nr_of_days': 6, 'consistency_first': True, 'consistency_first_on': [1]},
                           # One per week for the rest of the month
                           {'nr_of_snapshots': 3, 'nr_of_days': 21}]
