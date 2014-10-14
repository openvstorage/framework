# Copyright 2014 CloudFounders NV
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""
Flexible partition layout test module

- verify if detected disk configuration is correct
- verify if generated default layout is correct / useable without changes

Example detected disk configuration:
  disk_config =
      {'sda': {'boot_device': True, 'model': 'Virtual_disk', 'size': 17179869184.0, 'software_raid': False, 'type': 'disk'},
       'sdb': {'boot_device': False, 'model': 'Virtual_disk', 'size': 109279444992.0, 'software_raid': False, 'type': 'disk'},
       'sdc': {'boot_device': False, 'model': 'Virtual_disk', 'size': 109279444992.0, 'software_raid': False, 'type': 'disk'}}

Disks that should be excluded:
- boot device
- disk with 1 or more partitions being part of a software raid

"""

import unittest

from ovs.lib.setup import SetupController
from ovs.extensions.generic.sshclient import SSHClient
from datadiff import diff
from datadiff.tools import assert_equal

import sys
import pexpect

class PartitionLayout(unittest.TestCase):
    full_map = {
        '12-8': {'/mnt/cache8': {'sip': '100', 'DIR_ONLY': False}, '/mnt/bfs': {'sip': '100', 'DIR_ONLY': False},
                 '/mnt/cache1': {'sip': '75', 'DIR_ONLY': False}, '/mnt/cache2': {'sip': '75', 'DIR_ONLY': False},
                 '/mnt/cache3': {'sip': '100', 'DIR_ONLY': False}, '/mnt/cache4': {'sip': '100', 'DIR_ONLY': False},
                 '/mnt/cache5': {'sip': '100', 'DIR_ONLY': False}, '/mnt/cache6': {'sip': '100', 'DIR_ONLY': False},
                 '/mnt/cache7': {'sip': '100', 'DIR_ONLY': False}, '/mnt/db': {'sip': '25', 'DIR_ONLY': False},
                 '/mnt/md': {'sip': '25', 'DIR_ONLY': False}, '/var/tmp': {'sip': '100', 'DIR_ONLY': False}},
        '12-9': {'/mnt/cache8': {'sip': '100', 'DIR_ONLY': False}, '/mnt/cache9': {'sip': '100', 'DIR_ONLY': False},
                 '/mnt/bfs': {'sip': '100', 'DIR_ONLY': False}, '/mnt/cache1': {'sip': '75', 'DIR_ONLY': False},
                 '/mnt/cache2': {'sip': '75', 'DIR_ONLY': False}, '/mnt/cache3': {'sip': '100', 'DIR_ONLY': False},
                 '/mnt/cache4': {'sip': '100', 'DIR_ONLY': False}, '/mnt/cache5': {'sip': '100', 'DIR_ONLY': False},
                 '/mnt/cache6': {'sip': '100', 'DIR_ONLY': False}, '/mnt/cache7': {'sip': '100', 'DIR_ONLY': False},
                 '/mnt/db': {'sip': '25', 'DIR_ONLY': False}, '/mnt/md': {'sip': '25', 'DIR_ONLY': False},
                 '/var/tmp': {'sip': '100', 'DIR_ONLY': False}},
        '4-5': {'/var/tmp': {'sip': '100', 'DIR_ONLY': False}, '/mnt/db': {'sip': '25', 'DIR_ONLY': False},
                '/mnt/md': {'sip': '25', 'DIR_ONLY': False}, '/mnt/bfs': {'sip': '100', 'DIR_ONLY': False},
                '/mnt/cache1': {'sip': '75', 'DIR_ONLY': False}, '/mnt/cache2': {'sip': '75', 'DIR_ONLY': False},
                '/mnt/cache3': {'sip': '100', 'DIR_ONLY': False}, '/mnt/cache4': {'sip': '100', 'DIR_ONLY': False},
                '/mnt/cache5': {'sip': '100', 'DIR_ONLY': False}},
        '1-11': {'/mnt/cache8': {'sip': '100', 'DIR_ONLY': False}, '/mnt/cache9': {'sip': '100', 'DIR_ONLY': False},
                 '/mnt/bfs': {'sip': '80', 'DIR_ONLY': False}, '/mnt/cache1': {'sip': '75', 'DIR_ONLY': False},
                 '/mnt/cache2': {'sip': '75', 'DIR_ONLY': False}, '/mnt/cache3': {'sip': '100', 'DIR_ONLY': False},
                 '/mnt/cache4': {'sip': '100', 'DIR_ONLY': False}, '/mnt/cache5': {'sip': '100', 'DIR_ONLY': False},
                 '/mnt/cache6': {'sip': '100', 'DIR_ONLY': False}, '/mnt/cache7': {'sip': '100', 'DIR_ONLY': False},
                 '/mnt/db': {'sip': '25', 'DIR_ONLY': False}, '/mnt/md': {'sip': '25', 'DIR_ONLY': False},
                 '/var/tmp': {'sip': '20', 'DIR_ONLY': False}, '/mnt/cache10': {'sip': '100', 'DIR_ONLY': False},
                 '/mnt/cache11': {'sip': '100', 'DIR_ONLY': False}},
        '1-10': {'/mnt/cache8': {'sip': '100', 'DIR_ONLY': False}, '/mnt/cache9': {'sip': '100', 'DIR_ONLY': False},
                 '/mnt/bfs': {'sip': '80', 'DIR_ONLY': False}, '/mnt/cache1': {'sip': '75', 'DIR_ONLY': False},
                 '/mnt/cache2': {'sip': '75', 'DIR_ONLY': False}, '/mnt/cache3': {'sip': '100', 'DIR_ONLY': False},
                 '/mnt/cache4': {'sip': '100', 'DIR_ONLY': False}, '/mnt/cache5': {'sip': '100', 'DIR_ONLY': False},
                 '/mnt/cache6': {'sip': '100', 'DIR_ONLY': False}, '/mnt/cache7': {'sip': '100', 'DIR_ONLY': False},
                 '/mnt/db': {'sip': '25', 'DIR_ONLY': False}, '/mnt/md': {'sip': '25', 'DIR_ONLY': False},
                 '/var/tmp': {'sip': '20', 'DIR_ONLY': False}, '/mnt/cache10': {'sip': '100', 'DIR_ONLY': False}},
        '1-13': {'/mnt/cache8': {'sip': '100', 'DIR_ONLY': False}, '/mnt/cache9': {'sip': '100', 'DIR_ONLY': False},
                 '/mnt/bfs': {'sip': '80', 'DIR_ONLY': False}, '/mnt/cache1': {'sip': '75', 'DIR_ONLY': False},
                 '/mnt/cache2': {'sip': '75', 'DIR_ONLY': False}, '/mnt/cache3': {'sip': '100', 'DIR_ONLY': False},
                 '/mnt/cache4': {'sip': '100', 'DIR_ONLY': False}, '/mnt/cache5': {'sip': '100', 'DIR_ONLY': False},
                 '/mnt/cache6': {'sip': '100', 'DIR_ONLY': False}, '/mnt/cache7': {'sip': '100', 'DIR_ONLY': False},
                 '/var/tmp': {'sip': '20', 'DIR_ONLY': False}, '/mnt/db': {'sip': '25', 'DIR_ONLY': False},
                 '/mnt/md': {'sip': '25', 'DIR_ONLY': False}, '/mnt/cache12': {'sip': '100', 'DIR_ONLY': False},
                 '/mnt/cache13': {'sip': '100', 'DIR_ONLY': False}, '/mnt/cache10': {'sip': '100', 'DIR_ONLY': False},
                 '/mnt/cache11': {'sip': '100', 'DIR_ONLY': False}},
        '1-12': {'/mnt/cache8': {'sip': '100', 'DIR_ONLY': False}, '/mnt/cache9': {'sip': '100', 'DIR_ONLY': False},
                 '/mnt/bfs': {'sip': '80', 'DIR_ONLY': False}, '/mnt/cache1': {'sip': '75', 'DIR_ONLY': False},
                 '/mnt/cache2': {'sip': '75', 'DIR_ONLY': False}, '/mnt/cache3': {'sip': '100', 'DIR_ONLY': False},
                 '/mnt/cache4': {'sip': '100', 'DIR_ONLY': False}, '/mnt/cache5': {'sip': '100', 'DIR_ONLY': False},
                 '/mnt/cache6': {'sip': '100', 'DIR_ONLY': False}, '/mnt/cache7': {'sip': '100', 'DIR_ONLY': False},
                 '/var/tmp': {'sip': '20', 'DIR_ONLY': False}, '/mnt/db': {'sip': '25', 'DIR_ONLY': False},
                 '/mnt/md': {'sip': '25', 'DIR_ONLY': False}, '/mnt/cache12': {'sip': '100', 'DIR_ONLY': False},
                 '/mnt/cache10': {'sip': '100', 'DIR_ONLY': False}, '/mnt/cache11': {'sip': '100', 'DIR_ONLY': False}},
        '12-2': {'/mnt/bfs': {'sip': '100', 'DIR_ONLY': False}, '/mnt/cache1': {'sip': '75', 'DIR_ONLY': False},
                 '/mnt/cache2': {'sip': '75', 'DIR_ONLY': False}, '/mnt/db': {'sip': '25', 'DIR_ONLY': False},
                 '/mnt/md': {'sip': '25', 'DIR_ONLY': False}, '/var/tmp': {'sip': '100', 'DIR_ONLY': False}},
        '5-12': {'/mnt/cache8': {'sip': '100', 'DIR_ONLY': False}, '/mnt/cache9': {'sip': '100', 'DIR_ONLY': False},
                 '/mnt/bfs': {'sip': '100', 'DIR_ONLY': False}, '/mnt/cache1': {'sip': '75', 'DIR_ONLY': False},
                 '/mnt/cache2': {'sip': '75', 'DIR_ONLY': False}, '/mnt/cache3': {'sip': '100', 'DIR_ONLY': False},
                 '/mnt/cache4': {'sip': '100', 'DIR_ONLY': False}, '/mnt/cache5': {'sip': '100', 'DIR_ONLY': False},
                 '/mnt/cache6': {'sip': '100', 'DIR_ONLY': False}, '/mnt/cache7': {'sip': '100', 'DIR_ONLY': False},
                 '/var/tmp': {'sip': '100', 'DIR_ONLY': False}, '/mnt/db': {'sip': '25', 'DIR_ONLY': False},
                 '/mnt/md': {'sip': '25', 'DIR_ONLY': False}, '/mnt/cache12': {'sip': '100', 'DIR_ONLY': False},
                 '/mnt/cache10': {'sip': '100', 'DIR_ONLY': False}, '/mnt/cache11': {'sip': '100', 'DIR_ONLY': False}},
        '12-3': {'/mnt/bfs': {'sip': '100', 'DIR_ONLY': False}, '/mnt/cache1': {'sip': '75', 'DIR_ONLY': False},
                 '/mnt/cache2': {'sip': '75', 'DIR_ONLY': False}, '/mnt/cache3': {'sip': '100', 'DIR_ONLY': False},
                 '/mnt/db': {'sip': '25', 'DIR_ONLY': False}, '/mnt/md': {'sip': '25', 'DIR_ONLY': False},
                 '/var/tmp': {'sip': '100', 'DIR_ONLY': False}},
        '0-11': {'/mnt/cache8': {'sip': '100', 'DIR_ONLY': False}, '/mnt/cache9': {'sip': '100', 'DIR_ONLY': False},
                 '/mnt/bfs': {'sip': 'NA', 'DIR_ONLY': True}, '/mnt/cache1': {'sip': '75', 'DIR_ONLY': False},
                 '/mnt/cache2': {'sip': '75', 'DIR_ONLY': False}, '/mnt/cache3': {'sip': '100', 'DIR_ONLY': False},
                 '/mnt/cache4': {'sip': '100', 'DIR_ONLY': False}, '/mnt/cache5': {'sip': '100', 'DIR_ONLY': False},
                 '/mnt/cache6': {'sip': '100', 'DIR_ONLY': False}, '/mnt/cache7': {'sip': '100', 'DIR_ONLY': False},
                 '/mnt/db': {'sip': '25', 'DIR_ONLY': False}, '/mnt/md': {'sip': '25', 'DIR_ONLY': False},
                 '/var/tmp': {'sip': 'NA', 'DIR_ONLY': True}, '/mnt/cache10': {'sip': '100', 'DIR_ONLY': False},
                 '/mnt/cache11': {'sip': '100', 'DIR_ONLY': False}},
        '4-0': {'/mnt/bfs': {'sip': '100', 'DIR_ONLY': False}, '/mnt/cache1': {'sip': 'NA', 'DIR_ONLY': True},
                '/var/tmp': {'sip': '100', 'DIR_ONLY': False}, '/mnt/db': {'sip': 'NA', 'DIR_ONLY': True},
                '/mnt/md': {'sip': 'NA', 'DIR_ONLY': True}},
        '9-0': {'/mnt/bfs': {'sip': '100', 'DIR_ONLY': False}, '/mnt/cache1': {'sip': 'NA', 'DIR_ONLY': True},
                '/var/tmp': {'sip': '100', 'DIR_ONLY': False}, '/mnt/db': {'sip': 'NA', 'DIR_ONLY': True},
                '/mnt/md': {'sip': 'NA', 'DIR_ONLY': True}},
        '7-13': {'/mnt/cache8': {'sip': '100', 'DIR_ONLY': False}, '/mnt/cache9': {'sip': '100', 'DIR_ONLY': False},
                 '/mnt/bfs': {'sip': '100', 'DIR_ONLY': False}, '/mnt/cache1': {'sip': '75', 'DIR_ONLY': False},
                 '/mnt/cache2': {'sip': '75', 'DIR_ONLY': False}, '/mnt/cache3': {'sip': '100', 'DIR_ONLY': False},
                 '/mnt/cache4': {'sip': '100', 'DIR_ONLY': False}, '/mnt/cache5': {'sip': '100', 'DIR_ONLY': False},
                 '/mnt/cache6': {'sip': '100', 'DIR_ONLY': False}, '/mnt/cache7': {'sip': '100', 'DIR_ONLY': False},
                 '/var/tmp': {'sip': '100', 'DIR_ONLY': False}, '/mnt/db': {'sip': '25', 'DIR_ONLY': False},
                 '/mnt/md': {'sip': '25', 'DIR_ONLY': False}, '/mnt/cache12': {'sip': '100', 'DIR_ONLY': False},
                 '/mnt/cache13': {'sip': '100', 'DIR_ONLY': False}, '/mnt/cache10': {'sip': '100', 'DIR_ONLY': False},
                 '/mnt/cache11': {'sip': '100', 'DIR_ONLY': False}},
        '7-12': {'/mnt/cache8': {'sip': '100', 'DIR_ONLY': False}, '/mnt/cache9': {'sip': '100', 'DIR_ONLY': False},
                 '/mnt/bfs': {'sip': '100', 'DIR_ONLY': False}, '/mnt/cache1': {'sip': '75', 'DIR_ONLY': False},
                 '/mnt/cache2': {'sip': '75', 'DIR_ONLY': False}, '/mnt/cache3': {'sip': '100', 'DIR_ONLY': False},
                 '/mnt/cache4': {'sip': '100', 'DIR_ONLY': False}, '/mnt/cache5': {'sip': '100', 'DIR_ONLY': False},
                 '/mnt/cache6': {'sip': '100', 'DIR_ONLY': False}, '/mnt/cache7': {'sip': '100', 'DIR_ONLY': False},
                 '/var/tmp': {'sip': '100', 'DIR_ONLY': False}, '/mnt/db': {'sip': '25', 'DIR_ONLY': False},
                 '/mnt/md': {'sip': '25', 'DIR_ONLY': False}, '/mnt/cache12': {'sip': '100', 'DIR_ONLY': False},
                 '/mnt/cache10': {'sip': '100', 'DIR_ONLY': False}, '/mnt/cache11': {'sip': '100', 'DIR_ONLY': False}},
        '7-11': {'/mnt/cache8': {'sip': '100', 'DIR_ONLY': False}, '/mnt/cache9': {'sip': '100', 'DIR_ONLY': False},
                 '/mnt/bfs': {'sip': '100', 'DIR_ONLY': False}, '/mnt/cache1': {'sip': '75', 'DIR_ONLY': False},
                 '/mnt/cache2': {'sip': '75', 'DIR_ONLY': False}, '/mnt/cache3': {'sip': '100', 'DIR_ONLY': False},
                 '/mnt/cache4': {'sip': '100', 'DIR_ONLY': False}, '/mnt/cache5': {'sip': '100', 'DIR_ONLY': False},
                 '/mnt/cache6': {'sip': '100', 'DIR_ONLY': False}, '/mnt/cache7': {'sip': '100', 'DIR_ONLY': False},
                 '/mnt/db': {'sip': '25', 'DIR_ONLY': False}, '/mnt/md': {'sip': '25', 'DIR_ONLY': False},
                 '/var/tmp': {'sip': '100', 'DIR_ONLY': False}, '/mnt/cache10': {'sip': '100', 'DIR_ONLY': False},
                 '/mnt/cache11': {'sip': '100', 'DIR_ONLY': False}},
        '7-10': {'/mnt/cache8': {'sip': '100', 'DIR_ONLY': False}, '/mnt/cache9': {'sip': '100', 'DIR_ONLY': False},
                 '/mnt/bfs': {'sip': '100', 'DIR_ONLY': False}, '/mnt/cache1': {'sip': '75', 'DIR_ONLY': False},
                 '/mnt/cache2': {'sip': '75', 'DIR_ONLY': False}, '/mnt/cache3': {'sip': '100', 'DIR_ONLY': False},
                 '/mnt/cache4': {'sip': '100', 'DIR_ONLY': False}, '/mnt/cache5': {'sip': '100', 'DIR_ONLY': False},
                 '/mnt/cache6': {'sip': '100', 'DIR_ONLY': False}, '/mnt/cache7': {'sip': '100', 'DIR_ONLY': False},
                 '/mnt/db': {'sip': '25', 'DIR_ONLY': False}, '/mnt/md': {'sip': '25', 'DIR_ONLY': False},
                 '/var/tmp': {'sip': '100', 'DIR_ONLY': False}, '/mnt/cache10': {'sip': '100', 'DIR_ONLY': False}},
        '11-5': {'/var/tmp': {'sip': '100', 'DIR_ONLY': False}, '/mnt/db': {'sip': '25', 'DIR_ONLY': False},
                 '/mnt/md': {'sip': '25', 'DIR_ONLY': False}, '/mnt/bfs': {'sip': '100', 'DIR_ONLY': False},
                 '/mnt/cache1': {'sip': '75', 'DIR_ONLY': False}, '/mnt/cache2': {'sip': '75', 'DIR_ONLY': False},
                 '/mnt/cache3': {'sip': '100', 'DIR_ONLY': False}, '/mnt/cache4': {'sip': '100', 'DIR_ONLY': False},
                 '/mnt/cache5': {'sip': '100', 'DIR_ONLY': False}},
        '11-4': {'/var/tmp': {'sip': '100', 'DIR_ONLY': False}, '/mnt/db': {'sip': '25', 'DIR_ONLY': False},
                 '/mnt/md': {'sip': '25', 'DIR_ONLY': False}, '/mnt/bfs': {'sip': '100', 'DIR_ONLY': False},
                 '/mnt/cache1': {'sip': '75', 'DIR_ONLY': False}, '/mnt/cache2': {'sip': '75', 'DIR_ONLY': False},
                 '/mnt/cache3': {'sip': '100', 'DIR_ONLY': False}, '/mnt/cache4': {'sip': '100', 'DIR_ONLY': False}},
        '11-7': {'/mnt/bfs': {'sip': '100', 'DIR_ONLY': False}, '/mnt/cache1': {'sip': '75', 'DIR_ONLY': False},
                 '/mnt/cache2': {'sip': '75', 'DIR_ONLY': False}, '/mnt/cache3': {'sip': '100', 'DIR_ONLY': False},
                 '/mnt/cache4': {'sip': '100', 'DIR_ONLY': False}, '/mnt/cache5': {'sip': '100', 'DIR_ONLY': False},
                 '/mnt/cache6': {'sip': '100', 'DIR_ONLY': False}, '/mnt/cache7': {'sip': '100', 'DIR_ONLY': False},
                 '/mnt/db': {'sip': '25', 'DIR_ONLY': False}, '/mnt/md': {'sip': '25', 'DIR_ONLY': False},
                 '/var/tmp': {'sip': '100', 'DIR_ONLY': False}},
        '7-8': {'/mnt/cache8': {'sip': '100', 'DIR_ONLY': False}, '/mnt/bfs': {'sip': '100', 'DIR_ONLY': False},
                '/mnt/cache1': {'sip': '75', 'DIR_ONLY': False}, '/mnt/cache2': {'sip': '75', 'DIR_ONLY': False},
                '/mnt/cache3': {'sip': '100', 'DIR_ONLY': False}, '/mnt/cache4': {'sip': '100', 'DIR_ONLY': False},
                '/mnt/cache5': {'sip': '100', 'DIR_ONLY': False}, '/mnt/cache6': {'sip': '100', 'DIR_ONLY': False},
                '/mnt/cache7': {'sip': '100', 'DIR_ONLY': False}, '/mnt/db': {'sip': '25', 'DIR_ONLY': False},
                '/mnt/md': {'sip': '25', 'DIR_ONLY': False}, '/var/tmp': {'sip': '100', 'DIR_ONLY': False}},
        '11-1': {'/mnt/bfs': {'sip': '100', 'DIR_ONLY': False}, '/mnt/cache1': {'sip': '50', 'DIR_ONLY': False},
                 '/var/tmp': {'sip': '100', 'DIR_ONLY': False}, '/mnt/db': {'sip': '25', 'DIR_ONLY': False},
                 '/mnt/md': {'sip': '25', 'DIR_ONLY': False}},
        '11-0': {'/mnt/bfs': {'sip': '100', 'DIR_ONLY': False}, '/mnt/cache1': {'sip': 'NA', 'DIR_ONLY': True},
                 '/var/tmp': {'sip': '100', 'DIR_ONLY': False}, '/mnt/db': {'sip': 'NA', 'DIR_ONLY': True},
                 '/mnt/md': {'sip': 'NA', 'DIR_ONLY': True}},
        '11-3': {'/mnt/bfs': {'sip': '100', 'DIR_ONLY': False}, '/mnt/cache1': {'sip': '75', 'DIR_ONLY': False},
                 '/mnt/cache2': {'sip': '75', 'DIR_ONLY': False}, '/mnt/cache3': {'sip': '100', 'DIR_ONLY': False},
                 '/mnt/db': {'sip': '25', 'DIR_ONLY': False}, '/mnt/md': {'sip': '25', 'DIR_ONLY': False},
                 '/var/tmp': {'sip': '100', 'DIR_ONLY': False}},
        '11-2': {'/mnt/bfs': {'sip': '100', 'DIR_ONLY': False}, '/mnt/cache1': {'sip': '75', 'DIR_ONLY': False},
                 '/mnt/cache2': {'sip': '75', 'DIR_ONLY': False}, '/mnt/db': {'sip': '25', 'DIR_ONLY': False},
                 '/mnt/md': {'sip': '25', 'DIR_ONLY': False}, '/var/tmp': {'sip': '100', 'DIR_ONLY': False}},
        '12-6': {'/var/tmp': {'sip': '100', 'DIR_ONLY': False}, '/mnt/db': {'sip': '25', 'DIR_ONLY': False},
                 '/mnt/md': {'sip': '25', 'DIR_ONLY': False}, '/mnt/bfs': {'sip': '100', 'DIR_ONLY': False},
                 '/mnt/cache1': {'sip': '75', 'DIR_ONLY': False}, '/mnt/cache2': {'sip': '75', 'DIR_ONLY': False},
                 '/mnt/cache3': {'sip': '100', 'DIR_ONLY': False}, '/mnt/cache4': {'sip': '100', 'DIR_ONLY': False},
                 '/mnt/cache5': {'sip': '100', 'DIR_ONLY': False}, '/mnt/cache6': {'sip': '100', 'DIR_ONLY': False}},
        '0-12': {'/mnt/cache8': {'sip': '100', 'DIR_ONLY': False}, '/mnt/cache9': {'sip': '100', 'DIR_ONLY': False},
                 '/mnt/bfs': {'sip': 'NA', 'DIR_ONLY': True}, '/mnt/cache1': {'sip': '75', 'DIR_ONLY': False},
                 '/mnt/cache2': {'sip': '75', 'DIR_ONLY': False}, '/mnt/cache3': {'sip': '100', 'DIR_ONLY': False},
                 '/mnt/cache4': {'sip': '100', 'DIR_ONLY': False}, '/mnt/cache5': {'sip': '100', 'DIR_ONLY': False},
                 '/mnt/cache6': {'sip': '100', 'DIR_ONLY': False}, '/mnt/cache7': {'sip': '100', 'DIR_ONLY': False},
                 '/var/tmp': {'sip': 'NA', 'DIR_ONLY': True}, '/mnt/db': {'sip': '25', 'DIR_ONLY': False},
                 '/mnt/md': {'sip': '25', 'DIR_ONLY': False}, '/mnt/cache12': {'sip': '100', 'DIR_ONLY': False},
                 '/mnt/cache10': {'sip': '100', 'DIR_ONLY': False}, '/mnt/cache11': {'sip': '100', 'DIR_ONLY': False}},
        '12-4': {'/var/tmp': {'sip': '100', 'DIR_ONLY': False}, '/mnt/db': {'sip': '25', 'DIR_ONLY': False},
                 '/mnt/md': {'sip': '25', 'DIR_ONLY': False}, '/mnt/bfs': {'sip': '100', 'DIR_ONLY': False},
                 '/mnt/cache1': {'sip': '75', 'DIR_ONLY': False}, '/mnt/cache2': {'sip': '75', 'DIR_ONLY': False},
                 '/mnt/cache3': {'sip': '100', 'DIR_ONLY': False}, '/mnt/cache4': {'sip': '100', 'DIR_ONLY': False}},
        '12-5': {'/var/tmp': {'sip': '100', 'DIR_ONLY': False}, '/mnt/db': {'sip': '25', 'DIR_ONLY': False},
                 '/mnt/md': {'sip': '25', 'DIR_ONLY': False}, '/mnt/bfs': {'sip': '100', 'DIR_ONLY': False},
                 '/mnt/cache1': {'sip': '75', 'DIR_ONLY': False}, '/mnt/cache2': {'sip': '75', 'DIR_ONLY': False},
                 '/mnt/cache3': {'sip': '100', 'DIR_ONLY': False}, '/mnt/cache4': {'sip': '100', 'DIR_ONLY': False},
                 '/mnt/cache5': {'sip': '100', 'DIR_ONLY': False}},
        '11-9': {'/mnt/cache8': {'sip': '100', 'DIR_ONLY': False}, '/mnt/cache9': {'sip': '100', 'DIR_ONLY': False},
                 '/mnt/bfs': {'sip': '100', 'DIR_ONLY': False}, '/mnt/cache1': {'sip': '75', 'DIR_ONLY': False},
                 '/mnt/cache2': {'sip': '75', 'DIR_ONLY': False}, '/mnt/cache3': {'sip': '100', 'DIR_ONLY': False},
                 '/mnt/cache4': {'sip': '100', 'DIR_ONLY': False}, '/mnt/cache5': {'sip': '100', 'DIR_ONLY': False},
                 '/mnt/cache6': {'sip': '100', 'DIR_ONLY': False}, '/mnt/cache7': {'sip': '100', 'DIR_ONLY': False},
                 '/mnt/db': {'sip': '25', 'DIR_ONLY': False}, '/mnt/md': {'sip': '25', 'DIR_ONLY': False},
                 '/var/tmp': {'sip': '100', 'DIR_ONLY': False}},
        '11-8': {'/mnt/cache8': {'sip': '100', 'DIR_ONLY': False}, '/mnt/bfs': {'sip': '100', 'DIR_ONLY': False},
                 '/mnt/cache1': {'sip': '75', 'DIR_ONLY': False}, '/mnt/cache2': {'sip': '75', 'DIR_ONLY': False},
                 '/mnt/cache3': {'sip': '100', 'DIR_ONLY': False}, '/mnt/cache4': {'sip': '100', 'DIR_ONLY': False},
                 '/mnt/cache5': {'sip': '100', 'DIR_ONLY': False}, '/mnt/cache6': {'sip': '100', 'DIR_ONLY': False},
                 '/mnt/cache7': {'sip': '100', 'DIR_ONLY': False}, '/mnt/db': {'sip': '25', 'DIR_ONLY': False},
                 '/mnt/md': {'sip': '25', 'DIR_ONLY': False}, '/var/tmp': {'sip': '100', 'DIR_ONLY': False}},
        '12-0': {'/mnt/bfs': {'sip': '100', 'DIR_ONLY': False}, '/mnt/cache1': {'sip': 'NA', 'DIR_ONLY': True},
                 '/var/tmp': {'sip': '100', 'DIR_ONLY': False}, '/mnt/db': {'sip': 'NA', 'DIR_ONLY': True},
                 '/mnt/md': {'sip': 'NA', 'DIR_ONLY': True}},
        '12-1': {'/mnt/bfs': {'sip': '100', 'DIR_ONLY': False}, '/mnt/cache1': {'sip': '50', 'DIR_ONLY': False},
                 '/var/tmp': {'sip': '100', 'DIR_ONLY': False}, '/mnt/db': {'sip': '25', 'DIR_ONLY': False},
                 '/mnt/md': {'sip': '25', 'DIR_ONLY': False}},
        '4-6': {'/var/tmp': {'sip': '100', 'DIR_ONLY': False}, '/mnt/db': {'sip': '25', 'DIR_ONLY': False},
                '/mnt/md': {'sip': '25', 'DIR_ONLY': False}, '/mnt/bfs': {'sip': '100', 'DIR_ONLY': False},
                '/mnt/cache1': {'sip': '75', 'DIR_ONLY': False}, '/mnt/cache2': {'sip': '75', 'DIR_ONLY': False},
                '/mnt/cache3': {'sip': '100', 'DIR_ONLY': False}, '/mnt/cache4': {'sip': '100', 'DIR_ONLY': False},
                '/mnt/cache5': {'sip': '100', 'DIR_ONLY': False}, '/mnt/cache6': {'sip': '100', 'DIR_ONLY': False}},
        '4-7': {'/mnt/bfs': {'sip': '100', 'DIR_ONLY': False}, '/mnt/cache1': {'sip': '75', 'DIR_ONLY': False},
                '/mnt/cache2': {'sip': '75', 'DIR_ONLY': False}, '/mnt/cache3': {'sip': '100', 'DIR_ONLY': False},
                '/mnt/cache4': {'sip': '100', 'DIR_ONLY': False}, '/mnt/cache5': {'sip': '100', 'DIR_ONLY': False},
                '/mnt/cache6': {'sip': '100', 'DIR_ONLY': False}, '/mnt/cache7': {'sip': '100', 'DIR_ONLY': False},
                '/mnt/db': {'sip': '25', 'DIR_ONLY': False}, '/mnt/md': {'sip': '25', 'DIR_ONLY': False},
                '/var/tmp': {'sip': '100', 'DIR_ONLY': False}},
        '7-9': {'/mnt/cache8': {'sip': '100', 'DIR_ONLY': False}, '/mnt/cache9': {'sip': '100', 'DIR_ONLY': False},
                '/mnt/bfs': {'sip': '100', 'DIR_ONLY': False}, '/mnt/cache1': {'sip': '75', 'DIR_ONLY': False},
                '/mnt/cache2': {'sip': '75', 'DIR_ONLY': False}, '/mnt/cache3': {'sip': '100', 'DIR_ONLY': False},
                '/mnt/cache4': {'sip': '100', 'DIR_ONLY': False}, '/mnt/cache5': {'sip': '100', 'DIR_ONLY': False},
                '/mnt/cache6': {'sip': '100', 'DIR_ONLY': False}, '/mnt/cache7': {'sip': '100', 'DIR_ONLY': False},
                '/mnt/db': {'sip': '25', 'DIR_ONLY': False}, '/mnt/md': {'sip': '25', 'DIR_ONLY': False},
                '/var/tmp': {'sip': '100', 'DIR_ONLY': False}},
        '0-4': {'/var/tmp': {'sip': 'NA', 'DIR_ONLY': True}, '/mnt/db': {'sip': '25', 'DIR_ONLY': False},
                '/mnt/md': {'sip': '25', 'DIR_ONLY': False}, '/mnt/bfs': {'sip': 'NA', 'DIR_ONLY': True},
                '/mnt/cache1': {'sip': '75', 'DIR_ONLY': False}, '/mnt/cache2': {'sip': '75', 'DIR_ONLY': False},
                '/mnt/cache3': {'sip': '100', 'DIR_ONLY': False}, '/mnt/cache4': {'sip': '100', 'DIR_ONLY': False}},
        '7-3': {'/mnt/bfs': {'sip': '100', 'DIR_ONLY': False}, '/mnt/cache1': {'sip': '75', 'DIR_ONLY': False},
                '/mnt/cache2': {'sip': '75', 'DIR_ONLY': False}, '/mnt/cache3': {'sip': '100', 'DIR_ONLY': False},
                '/mnt/db': {'sip': '25', 'DIR_ONLY': False}, '/mnt/md': {'sip': '25', 'DIR_ONLY': False},
                '/var/tmp': {'sip': '100', 'DIR_ONLY': False}},
        '7-2': {'/mnt/bfs': {'sip': '100', 'DIR_ONLY': False}, '/mnt/cache1': {'sip': '75', 'DIR_ONLY': False},
                '/mnt/cache2': {'sip': '75', 'DIR_ONLY': False}, '/mnt/db': {'sip': '25', 'DIR_ONLY': False},
                '/mnt/md': {'sip': '25', 'DIR_ONLY': False}, '/var/tmp': {'sip': '100', 'DIR_ONLY': False}},
        '4-12': {'/mnt/cache8': {'sip': '100', 'DIR_ONLY': False}, '/mnt/cache9': {'sip': '100', 'DIR_ONLY': False},
                 '/mnt/bfs': {'sip': '100', 'DIR_ONLY': False}, '/mnt/cache1': {'sip': '75', 'DIR_ONLY': False},
                 '/mnt/cache2': {'sip': '75', 'DIR_ONLY': False}, '/mnt/cache3': {'sip': '100', 'DIR_ONLY': False},
                 '/mnt/cache4': {'sip': '100', 'DIR_ONLY': False}, '/mnt/cache5': {'sip': '100', 'DIR_ONLY': False},
                 '/mnt/cache6': {'sip': '100', 'DIR_ONLY': False}, '/mnt/cache7': {'sip': '100', 'DIR_ONLY': False},
                 '/var/tmp': {'sip': '100', 'DIR_ONLY': False}, '/mnt/db': {'sip': '25', 'DIR_ONLY': False},
                 '/mnt/md': {'sip': '25', 'DIR_ONLY': False}, '/mnt/cache12': {'sip': '100', 'DIR_ONLY': False},
                 '/mnt/cache10': {'sip': '100', 'DIR_ONLY': False}, '/mnt/cache11': {'sip': '100', 'DIR_ONLY': False}},
        '4-13': {'/mnt/cache8': {'sip': '100', 'DIR_ONLY': False}, '/mnt/cache9': {'sip': '100', 'DIR_ONLY': False},
                 '/mnt/bfs': {'sip': '100', 'DIR_ONLY': False}, '/mnt/cache1': {'sip': '75', 'DIR_ONLY': False},
                 '/mnt/cache2': {'sip': '75', 'DIR_ONLY': False}, '/mnt/cache3': {'sip': '100', 'DIR_ONLY': False},
                 '/mnt/cache4': {'sip': '100', 'DIR_ONLY': False}, '/mnt/cache5': {'sip': '100', 'DIR_ONLY': False},
                 '/mnt/cache6': {'sip': '100', 'DIR_ONLY': False}, '/mnt/cache7': {'sip': '100', 'DIR_ONLY': False},
                 '/var/tmp': {'sip': '100', 'DIR_ONLY': False}, '/mnt/db': {'sip': '25', 'DIR_ONLY': False},
                 '/mnt/md': {'sip': '25', 'DIR_ONLY': False}, '/mnt/cache12': {'sip': '100', 'DIR_ONLY': False},
                 '/mnt/cache13': {'sip': '100', 'DIR_ONLY': False}, '/mnt/cache10': {'sip': '100', 'DIR_ONLY': False},
                 '/mnt/cache11': {'sip': '100', 'DIR_ONLY': False}},
        '4-10': {'/mnt/cache8': {'sip': '100', 'DIR_ONLY': False}, '/mnt/cache9': {'sip': '100', 'DIR_ONLY': False},
                 '/mnt/bfs': {'sip': '100', 'DIR_ONLY': False}, '/mnt/cache1': {'sip': '75', 'DIR_ONLY': False},
                 '/mnt/cache2': {'sip': '75', 'DIR_ONLY': False}, '/mnt/cache3': {'sip': '100', 'DIR_ONLY': False},
                 '/mnt/cache4': {'sip': '100', 'DIR_ONLY': False}, '/mnt/cache5': {'sip': '100', 'DIR_ONLY': False},
                 '/mnt/cache6': {'sip': '100', 'DIR_ONLY': False}, '/mnt/cache7': {'sip': '100', 'DIR_ONLY': False},
                 '/mnt/db': {'sip': '25', 'DIR_ONLY': False}, '/mnt/md': {'sip': '25', 'DIR_ONLY': False},
                 '/var/tmp': {'sip': '100', 'DIR_ONLY': False}, '/mnt/cache10': {'sip': '100', 'DIR_ONLY': False}},
        '4-11': {'/mnt/cache8': {'sip': '100', 'DIR_ONLY': False}, '/mnt/cache9': {'sip': '100', 'DIR_ONLY': False},
                 '/mnt/bfs': {'sip': '100', 'DIR_ONLY': False}, '/mnt/cache1': {'sip': '75', 'DIR_ONLY': False},
                 '/mnt/cache2': {'sip': '75', 'DIR_ONLY': False}, '/mnt/cache3': {'sip': '100', 'DIR_ONLY': False},
                 '/mnt/cache4': {'sip': '100', 'DIR_ONLY': False}, '/mnt/cache5': {'sip': '100', 'DIR_ONLY': False},
                 '/mnt/cache6': {'sip': '100', 'DIR_ONLY': False}, '/mnt/cache7': {'sip': '100', 'DIR_ONLY': False},
                 '/mnt/db': {'sip': '25', 'DIR_ONLY': False}, '/mnt/md': {'sip': '25', 'DIR_ONLY': False},
                 '/var/tmp': {'sip': '100', 'DIR_ONLY': False}, '/mnt/cache10': {'sip': '100', 'DIR_ONLY': False},
                 '/mnt/cache11': {'sip': '100', 'DIR_ONLY': False}},
        '5-3': {'/mnt/bfs': {'sip': '100', 'DIR_ONLY': False}, '/mnt/cache1': {'sip': '75', 'DIR_ONLY': False},
                '/mnt/cache2': {'sip': '75', 'DIR_ONLY': False}, '/mnt/cache3': {'sip': '100', 'DIR_ONLY': False},
                '/mnt/db': {'sip': '25', 'DIR_ONLY': False}, '/mnt/md': {'sip': '25', 'DIR_ONLY': False},
                '/var/tmp': {'sip': '100', 'DIR_ONLY': False}},
        '5-11': {'/mnt/cache8': {'sip': '100', 'DIR_ONLY': False}, '/mnt/cache9': {'sip': '100', 'DIR_ONLY': False},
                 '/mnt/bfs': {'sip': '100', 'DIR_ONLY': False}, '/mnt/cache1': {'sip': '75', 'DIR_ONLY': False},
                 '/mnt/cache2': {'sip': '75', 'DIR_ONLY': False}, '/mnt/cache3': {'sip': '100', 'DIR_ONLY': False},
                 '/mnt/cache4': {'sip': '100', 'DIR_ONLY': False}, '/mnt/cache5': {'sip': '100', 'DIR_ONLY': False},
                 '/mnt/cache6': {'sip': '100', 'DIR_ONLY': False}, '/mnt/cache7': {'sip': '100', 'DIR_ONLY': False},
                 '/mnt/db': {'sip': '25', 'DIR_ONLY': False}, '/mnt/md': {'sip': '25', 'DIR_ONLY': False},
                 '/var/tmp': {'sip': '100', 'DIR_ONLY': False}, '/mnt/cache10': {'sip': '100', 'DIR_ONLY': False},
                 '/mnt/cache11': {'sip': '100', 'DIR_ONLY': False}},
        '12-11': {'/mnt/cache8': {'sip': '100', 'DIR_ONLY': False}, '/mnt/cache9': {'sip': '100', 'DIR_ONLY': False},
                  '/mnt/bfs': {'sip': '100', 'DIR_ONLY': False}, '/mnt/cache1': {'sip': '75', 'DIR_ONLY': False},
                  '/mnt/cache2': {'sip': '75', 'DIR_ONLY': False}, '/mnt/cache3': {'sip': '100', 'DIR_ONLY': False},
                  '/mnt/cache4': {'sip': '100', 'DIR_ONLY': False}, '/mnt/cache5': {'sip': '100', 'DIR_ONLY': False},
                  '/mnt/cache6': {'sip': '100', 'DIR_ONLY': False}, '/mnt/cache7': {'sip': '100', 'DIR_ONLY': False},
                  '/mnt/db': {'sip': '25', 'DIR_ONLY': False}, '/mnt/md': {'sip': '25', 'DIR_ONLY': False},
                  '/var/tmp': {'sip': '100', 'DIR_ONLY': False}, '/mnt/cache10': {'sip': '100', 'DIR_ONLY': False},
                  '/mnt/cache11': {'sip': '100', 'DIR_ONLY': False}},
        '1-0': {'/mnt/bfs': {'sip': '80', 'DIR_ONLY': False}, '/mnt/cache1': {'sip': 'NA', 'DIR_ONLY': True},
                '/var/tmp': {'sip': '20', 'DIR_ONLY': False}, '/mnt/db': {'sip': 'NA', 'DIR_ONLY': True},
                '/mnt/md': {'sip': 'NA', 'DIR_ONLY': True}},
        '4-4': {'/var/tmp': {'sip': '100', 'DIR_ONLY': False}, '/mnt/db': {'sip': '25', 'DIR_ONLY': False},
                '/mnt/md': {'sip': '25', 'DIR_ONLY': False}, '/mnt/bfs': {'sip': '100', 'DIR_ONLY': False},
                '/mnt/cache1': {'sip': '75', 'DIR_ONLY': False}, '/mnt/cache2': {'sip': '75', 'DIR_ONLY': False},
                '/mnt/cache3': {'sip': '100', 'DIR_ONLY': False}, '/mnt/cache4': {'sip': '100', 'DIR_ONLY': False}},
        '11-11': {'/mnt/cache8': {'sip': '100', 'DIR_ONLY': False}, '/mnt/cache9': {'sip': '100', 'DIR_ONLY': False},
                  '/mnt/bfs': {'sip': '100', 'DIR_ONLY': False}, '/mnt/cache1': {'sip': '75', 'DIR_ONLY': False},
                  '/mnt/cache2': {'sip': '75', 'DIR_ONLY': False}, '/mnt/cache3': {'sip': '100', 'DIR_ONLY': False},
                  '/mnt/cache4': {'sip': '100', 'DIR_ONLY': False}, '/mnt/cache5': {'sip': '100', 'DIR_ONLY': False},
                  '/mnt/cache6': {'sip': '100', 'DIR_ONLY': False}, '/mnt/cache7': {'sip': '100', 'DIR_ONLY': False},
                  '/mnt/db': {'sip': '25', 'DIR_ONLY': False}, '/mnt/md': {'sip': '25', 'DIR_ONLY': False},
                  '/var/tmp': {'sip': '100', 'DIR_ONLY': False}, '/mnt/cache10': {'sip': '100', 'DIR_ONLY': False},
                  '/mnt/cache11': {'sip': '100', 'DIR_ONLY': False}},
        '11-10': {'/mnt/cache8': {'sip': '100', 'DIR_ONLY': False}, '/mnt/cache9': {'sip': '100', 'DIR_ONLY': False},
                  '/mnt/bfs': {'sip': '100', 'DIR_ONLY': False}, '/mnt/cache1': {'sip': '75', 'DIR_ONLY': False},
                  '/mnt/cache2': {'sip': '75', 'DIR_ONLY': False}, '/mnt/cache3': {'sip': '100', 'DIR_ONLY': False},
                  '/mnt/cache4': {'sip': '100', 'DIR_ONLY': False}, '/mnt/cache5': {'sip': '100', 'DIR_ONLY': False},
                  '/mnt/cache6': {'sip': '100', 'DIR_ONLY': False}, '/mnt/cache7': {'sip': '100', 'DIR_ONLY': False},
                  '/mnt/db': {'sip': '25', 'DIR_ONLY': False}, '/mnt/md': {'sip': '25', 'DIR_ONLY': False},
                  '/var/tmp': {'sip': '100', 'DIR_ONLY': False}, '/mnt/cache10': {'sip': '100', 'DIR_ONLY': False}},
        '11-13': {'/mnt/cache8': {'sip': '100', 'DIR_ONLY': False}, '/mnt/cache9': {'sip': '100', 'DIR_ONLY': False},
                  '/mnt/bfs': {'sip': '100', 'DIR_ONLY': False}, '/mnt/cache1': {'sip': '75', 'DIR_ONLY': False},
                  '/mnt/cache2': {'sip': '75', 'DIR_ONLY': False}, '/mnt/cache3': {'sip': '100', 'DIR_ONLY': False},
                  '/mnt/cache4': {'sip': '100', 'DIR_ONLY': False}, '/mnt/cache5': {'sip': '100', 'DIR_ONLY': False},
                  '/mnt/cache6': {'sip': '100', 'DIR_ONLY': False}, '/mnt/cache7': {'sip': '100', 'DIR_ONLY': False},
                  '/var/tmp': {'sip': '100', 'DIR_ONLY': False}, '/mnt/db': {'sip': '25', 'DIR_ONLY': False},
                  '/mnt/md': {'sip': '25', 'DIR_ONLY': False}, '/mnt/cache12': {'sip': '100', 'DIR_ONLY': False},
                  '/mnt/cache13': {'sip': '100', 'DIR_ONLY': False}, '/mnt/cache10': {'sip': '100', 'DIR_ONLY': False},
                  '/mnt/cache11': {'sip': '100', 'DIR_ONLY': False}},
        '11-12': {'/mnt/cache8': {'sip': '100', 'DIR_ONLY': False}, '/mnt/cache9': {'sip': '100', 'DIR_ONLY': False},
                  '/mnt/bfs': {'sip': '100', 'DIR_ONLY': False}, '/mnt/cache1': {'sip': '75', 'DIR_ONLY': False},
                  '/mnt/cache2': {'sip': '75', 'DIR_ONLY': False}, '/mnt/cache3': {'sip': '100', 'DIR_ONLY': False},
                  '/mnt/cache4': {'sip': '100', 'DIR_ONLY': False}, '/mnt/cache5': {'sip': '100', 'DIR_ONLY': False},
                  '/mnt/cache6': {'sip': '100', 'DIR_ONLY': False}, '/mnt/cache7': {'sip': '100', 'DIR_ONLY': False},
                  '/var/tmp': {'sip': '100', 'DIR_ONLY': False}, '/mnt/db': {'sip': '25', 'DIR_ONLY': False},
                  '/mnt/md': {'sip': '25', 'DIR_ONLY': False}, '/mnt/cache12': {'sip': '100', 'DIR_ONLY': False},
                  '/mnt/cache10': {'sip': '100', 'DIR_ONLY': False}, '/mnt/cache11': {'sip': '100', 'DIR_ONLY': False}},
        '12-12': {'/mnt/cache8': {'sip': '100', 'DIR_ONLY': False}, '/mnt/cache9': {'sip': '100', 'DIR_ONLY': False},
                  '/mnt/bfs': {'sip': '100', 'DIR_ONLY': False}, '/mnt/cache1': {'sip': '75', 'DIR_ONLY': False},
                  '/mnt/cache2': {'sip': '75', 'DIR_ONLY': False}, '/mnt/cache3': {'sip': '100', 'DIR_ONLY': False},
                  '/mnt/cache4': {'sip': '100', 'DIR_ONLY': False}, '/mnt/cache5': {'sip': '100', 'DIR_ONLY': False},
                  '/mnt/cache6': {'sip': '100', 'DIR_ONLY': False}, '/mnt/cache7': {'sip': '100', 'DIR_ONLY': False},
                  '/var/tmp': {'sip': '100', 'DIR_ONLY': False}, '/mnt/db': {'sip': '25', 'DIR_ONLY': False},
                  '/mnt/md': {'sip': '25', 'DIR_ONLY': False}, '/mnt/cache12': {'sip': '100', 'DIR_ONLY': False},
                  '/mnt/cache10': {'sip': '100', 'DIR_ONLY': False}, '/mnt/cache11': {'sip': '100', 'DIR_ONLY': False}},
        '6-8': {'/mnt/cache8': {'sip': '100', 'DIR_ONLY': False}, '/mnt/bfs': {'sip': '100', 'DIR_ONLY': False},
                '/mnt/cache1': {'sip': '75', 'DIR_ONLY': False}, '/mnt/cache2': {'sip': '75', 'DIR_ONLY': False},
                '/mnt/cache3': {'sip': '100', 'DIR_ONLY': False}, '/mnt/cache4': {'sip': '100', 'DIR_ONLY': False},
                '/mnt/cache5': {'sip': '100', 'DIR_ONLY': False}, '/mnt/cache6': {'sip': '100', 'DIR_ONLY': False},
                '/mnt/cache7': {'sip': '100', 'DIR_ONLY': False}, '/mnt/db': {'sip': '25', 'DIR_ONLY': False},
                '/mnt/md': {'sip': '25', 'DIR_ONLY': False}, '/var/tmp': {'sip': '100', 'DIR_ONLY': False}},
        '6-9': {'/mnt/cache8': {'sip': '100', 'DIR_ONLY': False}, '/mnt/cache9': {'sip': '100', 'DIR_ONLY': False},
                '/mnt/bfs': {'sip': '100', 'DIR_ONLY': False}, '/mnt/cache1': {'sip': '75', 'DIR_ONLY': False},
                '/mnt/cache2': {'sip': '75', 'DIR_ONLY': False}, '/mnt/cache3': {'sip': '100', 'DIR_ONLY': False},
                '/mnt/cache4': {'sip': '100', 'DIR_ONLY': False}, '/mnt/cache5': {'sip': '100', 'DIR_ONLY': False},
                '/mnt/cache6': {'sip': '100', 'DIR_ONLY': False}, '/mnt/cache7': {'sip': '100', 'DIR_ONLY': False},
                '/mnt/db': {'sip': '25', 'DIR_ONLY': False}, '/mnt/md': {'sip': '25', 'DIR_ONLY': False},
                '/var/tmp': {'sip': '100', 'DIR_ONLY': False}},
        '6-6': {'/var/tmp': {'sip': '100', 'DIR_ONLY': False}, '/mnt/db': {'sip': '25', 'DIR_ONLY': False},
                '/mnt/md': {'sip': '25', 'DIR_ONLY': False}, '/mnt/bfs': {'sip': '100', 'DIR_ONLY': False},
                '/mnt/cache1': {'sip': '75', 'DIR_ONLY': False}, '/mnt/cache2': {'sip': '75', 'DIR_ONLY': False},
                '/mnt/cache3': {'sip': '100', 'DIR_ONLY': False}, '/mnt/cache4': {'sip': '100', 'DIR_ONLY': False},
                '/mnt/cache5': {'sip': '100', 'DIR_ONLY': False}, '/mnt/cache6': {'sip': '100', 'DIR_ONLY': False}},
        '6-7': {'/mnt/bfs': {'sip': '100', 'DIR_ONLY': False}, '/mnt/cache1': {'sip': '75', 'DIR_ONLY': False},
                '/mnt/cache2': {'sip': '75', 'DIR_ONLY': False}, '/mnt/cache3': {'sip': '100', 'DIR_ONLY': False},
                '/mnt/cache4': {'sip': '100', 'DIR_ONLY': False}, '/mnt/cache5': {'sip': '100', 'DIR_ONLY': False},
                '/mnt/cache6': {'sip': '100', 'DIR_ONLY': False}, '/mnt/cache7': {'sip': '100', 'DIR_ONLY': False},
                '/mnt/db': {'sip': '25', 'DIR_ONLY': False}, '/mnt/md': {'sip': '25', 'DIR_ONLY': False},
                '/var/tmp': {'sip': '100', 'DIR_ONLY': False}},
        '6-4': {'/var/tmp': {'sip': '100', 'DIR_ONLY': False}, '/mnt/db': {'sip': '25', 'DIR_ONLY': False},
                '/mnt/md': {'sip': '25', 'DIR_ONLY': False}, '/mnt/bfs': {'sip': '100', 'DIR_ONLY': False},
                '/mnt/cache1': {'sip': '75', 'DIR_ONLY': False}, '/mnt/cache2': {'sip': '75', 'DIR_ONLY': False},
                '/mnt/cache3': {'sip': '100', 'DIR_ONLY': False}, '/mnt/cache4': {'sip': '100', 'DIR_ONLY': False}},
        '1-6': {'/var/tmp': {'sip': '20', 'DIR_ONLY': False}, '/mnt/db': {'sip': '25', 'DIR_ONLY': False},
                '/mnt/md': {'sip': '25', 'DIR_ONLY': False}, '/mnt/bfs': {'sip': '80', 'DIR_ONLY': False},
                '/mnt/cache1': {'sip': '75', 'DIR_ONLY': False}, '/mnt/cache2': {'sip': '75', 'DIR_ONLY': False},
                '/mnt/cache3': {'sip': '100', 'DIR_ONLY': False}, '/mnt/cache4': {'sip': '100', 'DIR_ONLY': False},
                '/mnt/cache5': {'sip': '100', 'DIR_ONLY': False}, '/mnt/cache6': {'sip': '100', 'DIR_ONLY': False}},
        '6-2': {'/mnt/bfs': {'sip': '100', 'DIR_ONLY': False}, '/mnt/cache1': {'sip': '75', 'DIR_ONLY': False},
                '/mnt/cache2': {'sip': '75', 'DIR_ONLY': False}, '/mnt/db': {'sip': '25', 'DIR_ONLY': False},
                '/mnt/md': {'sip': '25', 'DIR_ONLY': False}, '/var/tmp': {'sip': '100', 'DIR_ONLY': False}},
        '9-10': {'/mnt/cache8': {'sip': '100', 'DIR_ONLY': False}, '/mnt/cache9': {'sip': '100', 'DIR_ONLY': False},
                 '/mnt/bfs': {'sip': '100', 'DIR_ONLY': False}, '/mnt/cache1': {'sip': '75', 'DIR_ONLY': False},
                 '/mnt/cache2': {'sip': '75', 'DIR_ONLY': False}, '/mnt/cache3': {'sip': '100', 'DIR_ONLY': False},
                 '/mnt/cache4': {'sip': '100', 'DIR_ONLY': False}, '/mnt/cache5': {'sip': '100', 'DIR_ONLY': False},
                 '/mnt/cache6': {'sip': '100', 'DIR_ONLY': False}, '/mnt/cache7': {'sip': '100', 'DIR_ONLY': False},
                 '/mnt/db': {'sip': '25', 'DIR_ONLY': False}, '/mnt/md': {'sip': '25', 'DIR_ONLY': False},
                 '/var/tmp': {'sip': '100', 'DIR_ONLY': False}, '/mnt/cache10': {'sip': '100', 'DIR_ONLY': False}},
        '9-13': {'/mnt/cache8': {'sip': '100', 'DIR_ONLY': False}, '/mnt/cache9': {'sip': '100', 'DIR_ONLY': False},
                 '/mnt/bfs': {'sip': '100', 'DIR_ONLY': False}, '/mnt/cache1': {'sip': '75', 'DIR_ONLY': False},
                 '/mnt/cache2': {'sip': '75', 'DIR_ONLY': False}, '/mnt/cache3': {'sip': '100', 'DIR_ONLY': False},
                 '/mnt/cache4': {'sip': '100', 'DIR_ONLY': False}, '/mnt/cache5': {'sip': '100', 'DIR_ONLY': False},
                 '/mnt/cache6': {'sip': '100', 'DIR_ONLY': False}, '/mnt/cache7': {'sip': '100', 'DIR_ONLY': False},
                 '/var/tmp': {'sip': '100', 'DIR_ONLY': False}, '/mnt/db': {'sip': '25', 'DIR_ONLY': False},
                 '/mnt/md': {'sip': '25', 'DIR_ONLY': False}, '/mnt/cache12': {'sip': '100', 'DIR_ONLY': False},
                 '/mnt/cache13': {'sip': '100', 'DIR_ONLY': False}, '/mnt/cache10': {'sip': '100', 'DIR_ONLY': False},
                 '/mnt/cache11': {'sip': '100', 'DIR_ONLY': False}},
        '9-12': {'/mnt/cache8': {'sip': '100', 'DIR_ONLY': False}, '/mnt/cache9': {'sip': '100', 'DIR_ONLY': False},
                 '/mnt/bfs': {'sip': '100', 'DIR_ONLY': False}, '/mnt/cache1': {'sip': '75', 'DIR_ONLY': False},
                 '/mnt/cache2': {'sip': '75', 'DIR_ONLY': False}, '/mnt/cache3': {'sip': '100', 'DIR_ONLY': False},
                 '/mnt/cache4': {'sip': '100', 'DIR_ONLY': False}, '/mnt/cache5': {'sip': '100', 'DIR_ONLY': False},
                 '/mnt/cache6': {'sip': '100', 'DIR_ONLY': False}, '/mnt/cache7': {'sip': '100', 'DIR_ONLY': False},
                 '/var/tmp': {'sip': '100', 'DIR_ONLY': False}, '/mnt/db': {'sip': '25', 'DIR_ONLY': False},
                 '/mnt/md': {'sip': '25', 'DIR_ONLY': False}, '/mnt/cache12': {'sip': '100', 'DIR_ONLY': False},
                 '/mnt/cache10': {'sip': '100', 'DIR_ONLY': False}, '/mnt/cache11': {'sip': '100', 'DIR_ONLY': False}},
        '5-8': {'/mnt/cache8': {'sip': '100', 'DIR_ONLY': False}, '/mnt/bfs': {'sip': '100', 'DIR_ONLY': False},
                '/mnt/cache1': {'sip': '75', 'DIR_ONLY': False}, '/mnt/cache2': {'sip': '75', 'DIR_ONLY': False},
                '/mnt/cache3': {'sip': '100', 'DIR_ONLY': False}, '/mnt/cache4': {'sip': '100', 'DIR_ONLY': False},
                '/mnt/cache5': {'sip': '100', 'DIR_ONLY': False}, '/mnt/cache6': {'sip': '100', 'DIR_ONLY': False},
                '/mnt/cache7': {'sip': '100', 'DIR_ONLY': False}, '/mnt/db': {'sip': '25', 'DIR_ONLY': False},
                '/mnt/md': {'sip': '25', 'DIR_ONLY': False}, '/var/tmp': {'sip': '100', 'DIR_ONLY': False}},
        '9-4': {'/var/tmp': {'sip': '100', 'DIR_ONLY': False}, '/mnt/db': {'sip': '25', 'DIR_ONLY': False},
                '/mnt/md': {'sip': '25', 'DIR_ONLY': False}, '/mnt/bfs': {'sip': '100', 'DIR_ONLY': False},
                '/mnt/cache1': {'sip': '75', 'DIR_ONLY': False}, '/mnt/cache2': {'sip': '75', 'DIR_ONLY': False},
                '/mnt/cache3': {'sip': '100', 'DIR_ONLY': False}, '/mnt/cache4': {'sip': '100', 'DIR_ONLY': False}},
        '0-10': {'/mnt/cache8': {'sip': '100', 'DIR_ONLY': False}, '/mnt/cache9': {'sip': '100', 'DIR_ONLY': False},
                 '/mnt/bfs': {'sip': 'NA', 'DIR_ONLY': True}, '/mnt/cache1': {'sip': '75', 'DIR_ONLY': False},
                 '/mnt/cache2': {'sip': '75', 'DIR_ONLY': False}, '/mnt/cache3': {'sip': '100', 'DIR_ONLY': False},
                 '/mnt/cache4': {'sip': '100', 'DIR_ONLY': False}, '/mnt/cache5': {'sip': '100', 'DIR_ONLY': False},
                 '/mnt/cache6': {'sip': '100', 'DIR_ONLY': False}, '/mnt/cache7': {'sip': '100', 'DIR_ONLY': False},
                 '/mnt/db': {'sip': '25', 'DIR_ONLY': False}, '/mnt/md': {'sip': '25', 'DIR_ONLY': False},
                 '/var/tmp': {'sip': 'NA', 'DIR_ONLY': True}, '/mnt/cache10': {'sip': '100', 'DIR_ONLY': False}},
        '4-2': {'/mnt/bfs': {'sip': '100', 'DIR_ONLY': False}, '/mnt/cache1': {'sip': '75', 'DIR_ONLY': False},
                '/mnt/cache2': {'sip': '75', 'DIR_ONLY': False}, '/mnt/db': {'sip': '25', 'DIR_ONLY': False},
                '/mnt/md': {'sip': '25', 'DIR_ONLY': False}, '/var/tmp': {'sip': '100', 'DIR_ONLY': False}},
        '12-13': {'/mnt/cache8': {'sip': '100', 'DIR_ONLY': False}, '/mnt/cache9': {'sip': '100', 'DIR_ONLY': False},
                  '/mnt/bfs': {'sip': '100', 'DIR_ONLY': False}, '/mnt/cache1': {'sip': '75', 'DIR_ONLY': False},
                  '/mnt/cache2': {'sip': '75', 'DIR_ONLY': False}, '/mnt/cache3': {'sip': '100', 'DIR_ONLY': False},
                  '/mnt/cache4': {'sip': '100', 'DIR_ONLY': False}, '/mnt/cache5': {'sip': '100', 'DIR_ONLY': False},
                  '/mnt/cache6': {'sip': '100', 'DIR_ONLY': False}, '/mnt/cache7': {'sip': '100', 'DIR_ONLY': False},
                  '/var/tmp': {'sip': '100', 'DIR_ONLY': False}, '/mnt/db': {'sip': '25', 'DIR_ONLY': False},
                  '/mnt/md': {'sip': '25', 'DIR_ONLY': False}, '/mnt/cache12': {'sip': '100', 'DIR_ONLY': False},
                  '/mnt/cache13': {'sip': '100', 'DIR_ONLY': False}, '/mnt/cache10': {'sip': '100', 'DIR_ONLY': False},
                  '/mnt/cache11': {'sip': '100', 'DIR_ONLY': False}},
        '8-8': {'/mnt/cache8': {'sip': '100', 'DIR_ONLY': False}, '/mnt/bfs': {'sip': '100', 'DIR_ONLY': False},
                '/mnt/cache1': {'sip': '75', 'DIR_ONLY': False}, '/mnt/cache2': {'sip': '75', 'DIR_ONLY': False},
                '/mnt/cache3': {'sip': '100', 'DIR_ONLY': False}, '/mnt/cache4': {'sip': '100', 'DIR_ONLY': False},
                '/mnt/cache5': {'sip': '100', 'DIR_ONLY': False}, '/mnt/cache6': {'sip': '100', 'DIR_ONLY': False},
                '/mnt/cache7': {'sip': '100', 'DIR_ONLY': False}, '/mnt/db': {'sip': '25', 'DIR_ONLY': False},
                '/mnt/md': {'sip': '25', 'DIR_ONLY': False}, '/var/tmp': {'sip': '100', 'DIR_ONLY': False}},
        '8-9': {'/mnt/cache8': {'sip': '100', 'DIR_ONLY': False}, '/mnt/cache9': {'sip': '100', 'DIR_ONLY': False},
                '/mnt/bfs': {'sip': '100', 'DIR_ONLY': False}, '/mnt/cache1': {'sip': '75', 'DIR_ONLY': False},
                '/mnt/cache2': {'sip': '75', 'DIR_ONLY': False}, '/mnt/cache3': {'sip': '100', 'DIR_ONLY': False},
                '/mnt/cache4': {'sip': '100', 'DIR_ONLY': False}, '/mnt/cache5': {'sip': '100', 'DIR_ONLY': False},
                '/mnt/cache6': {'sip': '100', 'DIR_ONLY': False}, '/mnt/cache7': {'sip': '100', 'DIR_ONLY': False},
                '/mnt/db': {'sip': '25', 'DIR_ONLY': False}, '/mnt/md': {'sip': '25', 'DIR_ONLY': False},
                '/var/tmp': {'sip': '100', 'DIR_ONLY': False}},
        '8-4': {'/var/tmp': {'sip': '100', 'DIR_ONLY': False}, '/mnt/db': {'sip': '25', 'DIR_ONLY': False},
                '/mnt/md': {'sip': '25', 'DIR_ONLY': False}, '/mnt/bfs': {'sip': '100', 'DIR_ONLY': False},
                '/mnt/cache1': {'sip': '75', 'DIR_ONLY': False}, '/mnt/cache2': {'sip': '75', 'DIR_ONLY': False},
                '/mnt/cache3': {'sip': '100', 'DIR_ONLY': False}, '/mnt/cache4': {'sip': '100', 'DIR_ONLY': False}},
        '8-5': {'/var/tmp': {'sip': '100', 'DIR_ONLY': False}, '/mnt/db': {'sip': '25', 'DIR_ONLY': False},
                '/mnt/md': {'sip': '25', 'DIR_ONLY': False}, '/mnt/bfs': {'sip': '100', 'DIR_ONLY': False},
                '/mnt/cache1': {'sip': '75', 'DIR_ONLY': False}, '/mnt/cache2': {'sip': '75', 'DIR_ONLY': False},
                '/mnt/cache3': {'sip': '100', 'DIR_ONLY': False}, '/mnt/cache4': {'sip': '100', 'DIR_ONLY': False},
                '/mnt/cache5': {'sip': '100', 'DIR_ONLY': False}},
        '8-6': {'/var/tmp': {'sip': '100', 'DIR_ONLY': False}, '/mnt/db': {'sip': '25', 'DIR_ONLY': False},
                '/mnt/md': {'sip': '25', 'DIR_ONLY': False}, '/mnt/bfs': {'sip': '100', 'DIR_ONLY': False},
                '/mnt/cache1': {'sip': '75', 'DIR_ONLY': False}, '/mnt/cache2': {'sip': '75', 'DIR_ONLY': False},
                '/mnt/cache3': {'sip': '100', 'DIR_ONLY': False}, '/mnt/cache4': {'sip': '100', 'DIR_ONLY': False},
                '/mnt/cache5': {'sip': '100', 'DIR_ONLY': False}, '/mnt/cache6': {'sip': '100', 'DIR_ONLY': False}},
        '8-7': {'/mnt/bfs': {'sip': '100', 'DIR_ONLY': False}, '/mnt/cache1': {'sip': '75', 'DIR_ONLY': False},
                '/mnt/cache2': {'sip': '75', 'DIR_ONLY': False}, '/mnt/cache3': {'sip': '100', 'DIR_ONLY': False},
                '/mnt/cache4': {'sip': '100', 'DIR_ONLY': False}, '/mnt/cache5': {'sip': '100', 'DIR_ONLY': False},
                '/mnt/cache6': {'sip': '100', 'DIR_ONLY': False}, '/mnt/cache7': {'sip': '100', 'DIR_ONLY': False},
                '/mnt/db': {'sip': '25', 'DIR_ONLY': False}, '/mnt/md': {'sip': '25', 'DIR_ONLY': False},
                '/var/tmp': {'sip': '100', 'DIR_ONLY': False}},
        '8-0': {'/mnt/bfs': {'sip': '100', 'DIR_ONLY': False}, '/mnt/cache1': {'sip': 'NA', 'DIR_ONLY': True},
                '/var/tmp': {'sip': '100', 'DIR_ONLY': False}, '/mnt/db': {'sip': 'NA', 'DIR_ONLY': True},
                '/mnt/md': {'sip': 'NA', 'DIR_ONLY': True}},
        '8-1': {'/mnt/bfs': {'sip': '100', 'DIR_ONLY': False}, '/mnt/cache1': {'sip': '50', 'DIR_ONLY': False},
                '/var/tmp': {'sip': '100', 'DIR_ONLY': False}, '/mnt/db': {'sip': '25', 'DIR_ONLY': False},
                '/mnt/md': {'sip': '25', 'DIR_ONLY': False}},
        '8-2': {'/mnt/bfs': {'sip': '100', 'DIR_ONLY': False}, '/mnt/cache1': {'sip': '75', 'DIR_ONLY': False},
                '/mnt/cache2': {'sip': '75', 'DIR_ONLY': False}, '/mnt/db': {'sip': '25', 'DIR_ONLY': False},
                '/mnt/md': {'sip': '25', 'DIR_ONLY': False}, '/var/tmp': {'sip': '100', 'DIR_ONLY': False}},
        '8-3': {'/mnt/bfs': {'sip': '100', 'DIR_ONLY': False}, '/mnt/cache1': {'sip': '75', 'DIR_ONLY': False},
                '/mnt/cache2': {'sip': '75', 'DIR_ONLY': False}, '/mnt/cache3': {'sip': '100', 'DIR_ONLY': False},
                '/mnt/db': {'sip': '25', 'DIR_ONLY': False}, '/mnt/md': {'sip': '25', 'DIR_ONLY': False},
                '/var/tmp': {'sip': '100', 'DIR_ONLY': False}},
        '4-3': {'/mnt/bfs': {'sip': '100', 'DIR_ONLY': False}, '/mnt/cache1': {'sip': '75', 'DIR_ONLY': False},
                '/mnt/cache2': {'sip': '75', 'DIR_ONLY': False}, '/mnt/cache3': {'sip': '100', 'DIR_ONLY': False},
                '/mnt/db': {'sip': '25', 'DIR_ONLY': False}, '/mnt/md': {'sip': '25', 'DIR_ONLY': False},
                '/var/tmp': {'sip': '100', 'DIR_ONLY': False}},
        '6-5': {'/var/tmp': {'sip': '100', 'DIR_ONLY': False}, '/mnt/db': {'sip': '25', 'DIR_ONLY': False},
                '/mnt/md': {'sip': '25', 'DIR_ONLY': False}, '/mnt/bfs': {'sip': '100', 'DIR_ONLY': False},
                '/mnt/cache1': {'sip': '75', 'DIR_ONLY': False}, '/mnt/cache2': {'sip': '75', 'DIR_ONLY': False},
                '/mnt/cache3': {'sip': '100', 'DIR_ONLY': False}, '/mnt/cache4': {'sip': '100', 'DIR_ONLY': False},
                '/mnt/cache5': {'sip': '100', 'DIR_ONLY': False}},
        '9-11': {'/mnt/cache8': {'sip': '100', 'DIR_ONLY': False}, '/mnt/cache9': {'sip': '100', 'DIR_ONLY': False},
                 '/mnt/bfs': {'sip': '100', 'DIR_ONLY': False}, '/mnt/cache1': {'sip': '75', 'DIR_ONLY': False},
                 '/mnt/cache2': {'sip': '75', 'DIR_ONLY': False}, '/mnt/cache3': {'sip': '100', 'DIR_ONLY': False},
                 '/mnt/cache4': {'sip': '100', 'DIR_ONLY': False}, '/mnt/cache5': {'sip': '100', 'DIR_ONLY': False},
                 '/mnt/cache6': {'sip': '100', 'DIR_ONLY': False}, '/mnt/cache7': {'sip': '100', 'DIR_ONLY': False},
                 '/mnt/db': {'sip': '25', 'DIR_ONLY': False}, '/mnt/md': {'sip': '25', 'DIR_ONLY': False},
                 '/var/tmp': {'sip': '100', 'DIR_ONLY': False}, '/mnt/cache10': {'sip': '100', 'DIR_ONLY': False},
                 '/mnt/cache11': {'sip': '100', 'DIR_ONLY': False}},
        '6-3': {'/mnt/bfs': {'sip': '100', 'DIR_ONLY': False}, '/mnt/cache1': {'sip': '75', 'DIR_ONLY': False},
                '/mnt/cache2': {'sip': '75', 'DIR_ONLY': False}, '/mnt/cache3': {'sip': '100', 'DIR_ONLY': False},
                '/mnt/db': {'sip': '25', 'DIR_ONLY': False}, '/mnt/md': {'sip': '25', 'DIR_ONLY': False},
                '/var/tmp': {'sip': '100', 'DIR_ONLY': False}},
        '3-13': {'/mnt/cache8': {'sip': '100', 'DIR_ONLY': False}, '/mnt/cache9': {'sip': '100', 'DIR_ONLY': False},
                 '/mnt/bfs': {'sip': '100', 'DIR_ONLY': False}, '/mnt/cache1': {'sip': '75', 'DIR_ONLY': False},
                 '/mnt/cache2': {'sip': '75', 'DIR_ONLY': False}, '/mnt/cache3': {'sip': '100', 'DIR_ONLY': False},
                 '/mnt/cache4': {'sip': '100', 'DIR_ONLY': False}, '/mnt/cache5': {'sip': '100', 'DIR_ONLY': False},
                 '/mnt/cache6': {'sip': '100', 'DIR_ONLY': False}, '/mnt/cache7': {'sip': '100', 'DIR_ONLY': False},
                 '/var/tmp': {'sip': '100', 'DIR_ONLY': False}, '/mnt/db': {'sip': '25', 'DIR_ONLY': False},
                 '/mnt/md': {'sip': '25', 'DIR_ONLY': False}, '/mnt/cache12': {'sip': '100', 'DIR_ONLY': False},
                 '/mnt/cache13': {'sip': '100', 'DIR_ONLY': False}, '/mnt/cache10': {'sip': '100', 'DIR_ONLY': False},
                 '/mnt/cache11': {'sip': '100', 'DIR_ONLY': False}},
        '3-12': {'/mnt/cache8': {'sip': '100', 'DIR_ONLY': False}, '/mnt/cache9': {'sip': '100', 'DIR_ONLY': False},
                 '/mnt/bfs': {'sip': '100', 'DIR_ONLY': False}, '/mnt/cache1': {'sip': '75', 'DIR_ONLY': False},
                 '/mnt/cache2': {'sip': '75', 'DIR_ONLY': False}, '/mnt/cache3': {'sip': '100', 'DIR_ONLY': False},
                 '/mnt/cache4': {'sip': '100', 'DIR_ONLY': False}, '/mnt/cache5': {'sip': '100', 'DIR_ONLY': False},
                 '/mnt/cache6': {'sip': '100', 'DIR_ONLY': False}, '/mnt/cache7': {'sip': '100', 'DIR_ONLY': False},
                 '/var/tmp': {'sip': '100', 'DIR_ONLY': False}, '/mnt/db': {'sip': '25', 'DIR_ONLY': False},
                 '/mnt/md': {'sip': '25', 'DIR_ONLY': False}, '/mnt/cache12': {'sip': '100', 'DIR_ONLY': False},
                 '/mnt/cache10': {'sip': '100', 'DIR_ONLY': False}, '/mnt/cache11': {'sip': '100', 'DIR_ONLY': False}},
        '3-11': {'/mnt/cache8': {'sip': '100', 'DIR_ONLY': False}, '/mnt/cache9': {'sip': '100', 'DIR_ONLY': False},
                 '/mnt/bfs': {'sip': '100', 'DIR_ONLY': False}, '/mnt/cache1': {'sip': '75', 'DIR_ONLY': False},
                 '/mnt/cache2': {'sip': '75', 'DIR_ONLY': False}, '/mnt/cache3': {'sip': '100', 'DIR_ONLY': False},
                 '/mnt/cache4': {'sip': '100', 'DIR_ONLY': False}, '/mnt/cache5': {'sip': '100', 'DIR_ONLY': False},
                 '/mnt/cache6': {'sip': '100', 'DIR_ONLY': False}, '/mnt/cache7': {'sip': '100', 'DIR_ONLY': False},
                 '/mnt/db': {'sip': '25', 'DIR_ONLY': False}, '/mnt/md': {'sip': '25', 'DIR_ONLY': False},
                 '/var/tmp': {'sip': '100', 'DIR_ONLY': False}, '/mnt/cache10': {'sip': '100', 'DIR_ONLY': False},
                 '/mnt/cache11': {'sip': '100', 'DIR_ONLY': False}},
        '3-10': {'/mnt/cache8': {'sip': '100', 'DIR_ONLY': False}, '/mnt/cache9': {'sip': '100', 'DIR_ONLY': False},
                 '/mnt/bfs': {'sip': '100', 'DIR_ONLY': False}, '/mnt/cache1': {'sip': '75', 'DIR_ONLY': False},
                 '/mnt/cache2': {'sip': '75', 'DIR_ONLY': False}, '/mnt/cache3': {'sip': '100', 'DIR_ONLY': False},
                 '/mnt/cache4': {'sip': '100', 'DIR_ONLY': False}, '/mnt/cache5': {'sip': '100', 'DIR_ONLY': False},
                 '/mnt/cache6': {'sip': '100', 'DIR_ONLY': False}, '/mnt/cache7': {'sip': '100', 'DIR_ONLY': False},
                 '/mnt/db': {'sip': '25', 'DIR_ONLY': False}, '/mnt/md': {'sip': '25', 'DIR_ONLY': False},
                 '/var/tmp': {'sip': '100', 'DIR_ONLY': False}, '/mnt/cache10': {'sip': '100', 'DIR_ONLY': False}},
        '6-0': {'/mnt/bfs': {'sip': '100', 'DIR_ONLY': False}, '/mnt/cache1': {'sip': 'NA', 'DIR_ONLY': True},
                '/var/tmp': {'sip': '100', 'DIR_ONLY': False}, '/mnt/db': {'sip': 'NA', 'DIR_ONLY': True},
                '/mnt/md': {'sip': 'NA', 'DIR_ONLY': True}},
        '6-1': {'/mnt/bfs': {'sip': '100', 'DIR_ONLY': False}, '/mnt/cache1': {'sip': '50', 'DIR_ONLY': False},
                '/var/tmp': {'sip': '100', 'DIR_ONLY': False}, '/mnt/db': {'sip': '25', 'DIR_ONLY': False},
                '/mnt/md': {'sip': '25', 'DIR_ONLY': False}},
        '2-2': {'/mnt/bfs': {'sip': '100', 'DIR_ONLY': False}, '/mnt/cache1': {'sip': '75', 'DIR_ONLY': False},
                '/mnt/cache2': {'sip': '75', 'DIR_ONLY': False}, '/mnt/db': {'sip': '25', 'DIR_ONLY': False},
                '/mnt/md': {'sip': '25', 'DIR_ONLY': False}, '/var/tmp': {'sip': '100', 'DIR_ONLY': False}},
        '5-10': {'/mnt/cache8': {'sip': '100', 'DIR_ONLY': False}, '/mnt/cache9': {'sip': '100', 'DIR_ONLY': False},
                 '/mnt/bfs': {'sip': '100', 'DIR_ONLY': False}, '/mnt/cache1': {'sip': '75', 'DIR_ONLY': False},
                 '/mnt/cache2': {'sip': '75', 'DIR_ONLY': False}, '/mnt/cache3': {'sip': '100', 'DIR_ONLY': False},
                 '/mnt/cache4': {'sip': '100', 'DIR_ONLY': False}, '/mnt/cache5': {'sip': '100', 'DIR_ONLY': False},
                 '/mnt/cache6': {'sip': '100', 'DIR_ONLY': False}, '/mnt/cache7': {'sip': '100', 'DIR_ONLY': False},
                 '/mnt/db': {'sip': '25', 'DIR_ONLY': False}, '/mnt/md': {'sip': '25', 'DIR_ONLY': False},
                 '/var/tmp': {'sip': '100', 'DIR_ONLY': False}, '/mnt/cache10': {'sip': '100', 'DIR_ONLY': False}},
        '2-0': {'/mnt/bfs': {'sip': '100', 'DIR_ONLY': False}, '/mnt/cache1': {'sip': 'NA', 'DIR_ONLY': True},
                '/var/tmp': {'sip': '100', 'DIR_ONLY': False}, '/mnt/db': {'sip': 'NA', 'DIR_ONLY': True},
                '/mnt/md': {'sip': 'NA', 'DIR_ONLY': True}},
        '2-1': {'/mnt/bfs': {'sip': '100', 'DIR_ONLY': False}, '/mnt/cache1': {'sip': '50', 'DIR_ONLY': False},
                '/var/tmp': {'sip': '100', 'DIR_ONLY': False}, '/mnt/db': {'sip': '25', 'DIR_ONLY': False},
                '/mnt/md': {'sip': '25', 'DIR_ONLY': False}},
        '2-6': {'/var/tmp': {'sip': '100', 'DIR_ONLY': False}, '/mnt/db': {'sip': '25', 'DIR_ONLY': False},
                '/mnt/md': {'sip': '25', 'DIR_ONLY': False}, '/mnt/bfs': {'sip': '100', 'DIR_ONLY': False},
                '/mnt/cache1': {'sip': '75', 'DIR_ONLY': False}, '/mnt/cache2': {'sip': '75', 'DIR_ONLY': False},
                '/mnt/cache3': {'sip': '100', 'DIR_ONLY': False}, '/mnt/cache4': {'sip': '100', 'DIR_ONLY': False},
                '/mnt/cache5': {'sip': '100', 'DIR_ONLY': False}, '/mnt/cache6': {'sip': '100', 'DIR_ONLY': False}},
        '2-7': {'/mnt/bfs': {'sip': '100', 'DIR_ONLY': False}, '/mnt/cache1': {'sip': '75', 'DIR_ONLY': False},
                '/mnt/cache2': {'sip': '75', 'DIR_ONLY': False}, '/mnt/cache3': {'sip': '100', 'DIR_ONLY': False},
                '/mnt/cache4': {'sip': '100', 'DIR_ONLY': False}, '/mnt/cache5': {'sip': '100', 'DIR_ONLY': False},
                '/mnt/cache6': {'sip': '100', 'DIR_ONLY': False}, '/mnt/cache7': {'sip': '100', 'DIR_ONLY': False},
                '/mnt/db': {'sip': '25', 'DIR_ONLY': False}, '/mnt/md': {'sip': '25', 'DIR_ONLY': False},
                '/var/tmp': {'sip': '100', 'DIR_ONLY': False}},
        '2-4': {'/var/tmp': {'sip': '100', 'DIR_ONLY': False}, '/mnt/db': {'sip': '25', 'DIR_ONLY': False},
                '/mnt/md': {'sip': '25', 'DIR_ONLY': False}, '/mnt/bfs': {'sip': '100', 'DIR_ONLY': False},
                '/mnt/cache1': {'sip': '75', 'DIR_ONLY': False}, '/mnt/cache2': {'sip': '75', 'DIR_ONLY': False},
                '/mnt/cache3': {'sip': '100', 'DIR_ONLY': False}, '/mnt/cache4': {'sip': '100', 'DIR_ONLY': False}},
        '2-5': {'/var/tmp': {'sip': '100', 'DIR_ONLY': False}, '/mnt/db': {'sip': '25', 'DIR_ONLY': False},
                '/mnt/md': {'sip': '25', 'DIR_ONLY': False}, '/mnt/bfs': {'sip': '100', 'DIR_ONLY': False},
                '/mnt/cache1': {'sip': '75', 'DIR_ONLY': False}, '/mnt/cache2': {'sip': '75', 'DIR_ONLY': False},
                '/mnt/cache3': {'sip': '100', 'DIR_ONLY': False}, '/mnt/cache4': {'sip': '100', 'DIR_ONLY': False},
                '/mnt/cache5': {'sip': '100', 'DIR_ONLY': False}},
        '2-8': {'/mnt/cache8': {'sip': '100', 'DIR_ONLY': False}, '/mnt/bfs': {'sip': '100', 'DIR_ONLY': False},
                '/mnt/cache1': {'sip': '75', 'DIR_ONLY': False}, '/mnt/cache2': {'sip': '75', 'DIR_ONLY': False},
                '/mnt/cache3': {'sip': '100', 'DIR_ONLY': False}, '/mnt/cache4': {'sip': '100', 'DIR_ONLY': False},
                '/mnt/cache5': {'sip': '100', 'DIR_ONLY': False}, '/mnt/cache6': {'sip': '100', 'DIR_ONLY': False},
                '/mnt/cache7': {'sip': '100', 'DIR_ONLY': False}, '/mnt/db': {'sip': '25', 'DIR_ONLY': False},
                '/mnt/md': {'sip': '25', 'DIR_ONLY': False}, '/var/tmp': {'sip': '100', 'DIR_ONLY': False}},
        '2-9': {'/mnt/cache8': {'sip': '100', 'DIR_ONLY': False}, '/mnt/cache9': {'sip': '100', 'DIR_ONLY': False},
                '/mnt/bfs': {'sip': '100', 'DIR_ONLY': False}, '/mnt/cache1': {'sip': '75', 'DIR_ONLY': False},
                '/mnt/cache2': {'sip': '75', 'DIR_ONLY': False}, '/mnt/cache3': {'sip': '100', 'DIR_ONLY': False},
                '/mnt/cache4': {'sip': '100', 'DIR_ONLY': False}, '/mnt/cache5': {'sip': '100', 'DIR_ONLY': False},
                '/mnt/cache6': {'sip': '100', 'DIR_ONLY': False}, '/mnt/cache7': {'sip': '100', 'DIR_ONLY': False},
                '/mnt/db': {'sip': '25', 'DIR_ONLY': False}, '/mnt/md': {'sip': '25', 'DIR_ONLY': False},
                '/var/tmp': {'sip': '100', 'DIR_ONLY': False}},
        '0-8': {'/mnt/cache8': {'sip': '100', 'DIR_ONLY': False}, '/mnt/bfs': {'sip': 'NA', 'DIR_ONLY': True},
                '/mnt/cache1': {'sip': '75', 'DIR_ONLY': False}, '/mnt/cache2': {'sip': '75', 'DIR_ONLY': False},
                '/mnt/cache3': {'sip': '100', 'DIR_ONLY': False}, '/mnt/cache4': {'sip': '100', 'DIR_ONLY': False},
                '/mnt/cache5': {'sip': '100', 'DIR_ONLY': False}, '/mnt/cache6': {'sip': '100', 'DIR_ONLY': False},
                '/mnt/cache7': {'sip': '100', 'DIR_ONLY': False}, '/mnt/db': {'sip': '25', 'DIR_ONLY': False},
                '/mnt/md': {'sip': '25', 'DIR_ONLY': False}, '/var/tmp': {'sip': 'NA', 'DIR_ONLY': True}},
        '0-9': {'/mnt/cache8': {'sip': '100', 'DIR_ONLY': False}, '/mnt/cache9': {'sip': '100', 'DIR_ONLY': False},
                '/mnt/bfs': {'sip': 'NA', 'DIR_ONLY': True}, '/mnt/cache1': {'sip': '75', 'DIR_ONLY': False},
                '/mnt/cache2': {'sip': '75', 'DIR_ONLY': False}, '/mnt/cache3': {'sip': '100', 'DIR_ONLY': False},
                '/mnt/cache4': {'sip': '100', 'DIR_ONLY': False}, '/mnt/cache5': {'sip': '100', 'DIR_ONLY': False},
                '/mnt/cache6': {'sip': '100', 'DIR_ONLY': False}, '/mnt/cache7': {'sip': '100', 'DIR_ONLY': False},
                '/mnt/db': {'sip': '25', 'DIR_ONLY': False}, '/mnt/md': {'sip': '25', 'DIR_ONLY': False},
                '/var/tmp': {'sip': 'NA', 'DIR_ONLY': True}},
        '13-3': {'/mnt/bfs': {'sip': '100', 'DIR_ONLY': False}, '/mnt/cache1': {'sip': '75', 'DIR_ONLY': False},
                 '/mnt/cache2': {'sip': '75', 'DIR_ONLY': False}, '/mnt/cache3': {'sip': '100', 'DIR_ONLY': False},
                 '/mnt/db': {'sip': '25', 'DIR_ONLY': False}, '/mnt/md': {'sip': '25', 'DIR_ONLY': False},
                 '/var/tmp': {'sip': '100', 'DIR_ONLY': False}},
        '13-2': {'/mnt/bfs': {'sip': '100', 'DIR_ONLY': False}, '/mnt/cache1': {'sip': '75', 'DIR_ONLY': False},
                 '/mnt/cache2': {'sip': '75', 'DIR_ONLY': False}, '/mnt/db': {'sip': '25', 'DIR_ONLY': False},
                 '/mnt/md': {'sip': '25', 'DIR_ONLY': False}, '/var/tmp': {'sip': '100', 'DIR_ONLY': False}},
        '13-1': {'/mnt/bfs': {'sip': '100', 'DIR_ONLY': False}, '/mnt/cache1': {'sip': '50', 'DIR_ONLY': False},
                 '/var/tmp': {'sip': '100', 'DIR_ONLY': False}, '/mnt/db': {'sip': '25', 'DIR_ONLY': False},
                 '/mnt/md': {'sip': '25', 'DIR_ONLY': False}},
        '13-0': {'/mnt/bfs': {'sip': '100', 'DIR_ONLY': False}, '/mnt/cache1': {'sip': 'NA', 'DIR_ONLY': True},
                 '/var/tmp': {'sip': '100', 'DIR_ONLY': False}, '/mnt/db': {'sip': 'NA', 'DIR_ONLY': True},
                 '/mnt/md': {'sip': 'NA', 'DIR_ONLY': True}},
        '13-7': {'/mnt/bfs': {'sip': '100', 'DIR_ONLY': False}, '/mnt/cache1': {'sip': '75', 'DIR_ONLY': False},
                 '/mnt/cache2': {'sip': '75', 'DIR_ONLY': False}, '/mnt/cache3': {'sip': '100', 'DIR_ONLY': False},
                 '/mnt/cache4': {'sip': '100', 'DIR_ONLY': False}, '/mnt/cache5': {'sip': '100', 'DIR_ONLY': False},
                 '/mnt/cache6': {'sip': '100', 'DIR_ONLY': False}, '/mnt/cache7': {'sip': '100', 'DIR_ONLY': False},
                 '/mnt/db': {'sip': '25', 'DIR_ONLY': False}, '/mnt/md': {'sip': '25', 'DIR_ONLY': False},
                 '/var/tmp': {'sip': '100', 'DIR_ONLY': False}},
        '13-6': {'/var/tmp': {'sip': '100', 'DIR_ONLY': False}, '/mnt/db': {'sip': '25', 'DIR_ONLY': False},
                 '/mnt/md': {'sip': '25', 'DIR_ONLY': False}, '/mnt/bfs': {'sip': '100', 'DIR_ONLY': False},
                 '/mnt/cache1': {'sip': '75', 'DIR_ONLY': False}, '/mnt/cache2': {'sip': '75', 'DIR_ONLY': False},
                 '/mnt/cache3': {'sip': '100', 'DIR_ONLY': False}, '/mnt/cache4': {'sip': '100', 'DIR_ONLY': False},
                 '/mnt/cache5': {'sip': '100', 'DIR_ONLY': False}, '/mnt/cache6': {'sip': '100', 'DIR_ONLY': False}},
        '13-5': {'/var/tmp': {'sip': '100', 'DIR_ONLY': False}, '/mnt/db': {'sip': '25', 'DIR_ONLY': False},
                 '/mnt/md': {'sip': '25', 'DIR_ONLY': False}, '/mnt/bfs': {'sip': '100', 'DIR_ONLY': False},
                 '/mnt/cache1': {'sip': '75', 'DIR_ONLY': False}, '/mnt/cache2': {'sip': '75', 'DIR_ONLY': False},
                 '/mnt/cache3': {'sip': '100', 'DIR_ONLY': False}, '/mnt/cache4': {'sip': '100', 'DIR_ONLY': False},
                 '/mnt/cache5': {'sip': '100', 'DIR_ONLY': False}},
        '13-4': {'/var/tmp': {'sip': '100', 'DIR_ONLY': False}, '/mnt/db': {'sip': '25', 'DIR_ONLY': False},
                 '/mnt/md': {'sip': '25', 'DIR_ONLY': False}, '/mnt/bfs': {'sip': '100', 'DIR_ONLY': False},
                 '/mnt/cache1': {'sip': '75', 'DIR_ONLY': False}, '/mnt/cache2': {'sip': '75', 'DIR_ONLY': False},
                 '/mnt/cache3': {'sip': '100', 'DIR_ONLY': False}, '/mnt/cache4': {'sip': '100', 'DIR_ONLY': False}},
        '12-10': {'/mnt/cache8': {'sip': '100', 'DIR_ONLY': False}, '/mnt/cache9': {'sip': '100', 'DIR_ONLY': False},
                  '/mnt/bfs': {'sip': '100', 'DIR_ONLY': False}, '/mnt/cache1': {'sip': '75', 'DIR_ONLY': False},
                  '/mnt/cache2': {'sip': '75', 'DIR_ONLY': False}, '/mnt/cache3': {'sip': '100', 'DIR_ONLY': False},
                  '/mnt/cache4': {'sip': '100', 'DIR_ONLY': False}, '/mnt/cache5': {'sip': '100', 'DIR_ONLY': False},
                  '/mnt/cache6': {'sip': '100', 'DIR_ONLY': False}, '/mnt/cache7': {'sip': '100', 'DIR_ONLY': False},
                  '/mnt/db': {'sip': '25', 'DIR_ONLY': False}, '/mnt/md': {'sip': '25', 'DIR_ONLY': False},
                  '/var/tmp': {'sip': '100', 'DIR_ONLY': False}, '/mnt/cache10': {'sip': '100', 'DIR_ONLY': False}},
        '13-9': {'/mnt/cache8': {'sip': '100', 'DIR_ONLY': False}, '/mnt/cache9': {'sip': '100', 'DIR_ONLY': False},
                 '/mnt/bfs': {'sip': '100', 'DIR_ONLY': False}, '/mnt/cache1': {'sip': '75', 'DIR_ONLY': False},
                 '/mnt/cache2': {'sip': '75', 'DIR_ONLY': False}, '/mnt/cache3': {'sip': '100', 'DIR_ONLY': False},
                 '/mnt/cache4': {'sip': '100', 'DIR_ONLY': False}, '/mnt/cache5': {'sip': '100', 'DIR_ONLY': False},
                 '/mnt/cache6': {'sip': '100', 'DIR_ONLY': False}, '/mnt/cache7': {'sip': '100', 'DIR_ONLY': False},
                 '/mnt/db': {'sip': '25', 'DIR_ONLY': False}, '/mnt/md': {'sip': '25', 'DIR_ONLY': False},
                 '/var/tmp': {'sip': '100', 'DIR_ONLY': False}},
        '13-8': {'/mnt/cache8': {'sip': '100', 'DIR_ONLY': False}, '/mnt/bfs': {'sip': '100', 'DIR_ONLY': False},
                 '/mnt/cache1': {'sip': '75', 'DIR_ONLY': False}, '/mnt/cache2': {'sip': '75', 'DIR_ONLY': False},
                 '/mnt/cache3': {'sip': '100', 'DIR_ONLY': False}, '/mnt/cache4': {'sip': '100', 'DIR_ONLY': False},
                 '/mnt/cache5': {'sip': '100', 'DIR_ONLY': False}, '/mnt/cache6': {'sip': '100', 'DIR_ONLY': False},
                 '/mnt/cache7': {'sip': '100', 'DIR_ONLY': False}, '/mnt/db': {'sip': '25', 'DIR_ONLY': False},
                 '/mnt/md': {'sip': '25', 'DIR_ONLY': False}, '/var/tmp': {'sip': '100', 'DIR_ONLY': False}},
        '0-5': {'/var/tmp': {'sip': 'NA', 'DIR_ONLY': True}, '/mnt/db': {'sip': '25', 'DIR_ONLY': False},
                '/mnt/md': {'sip': '25', 'DIR_ONLY': False}, '/mnt/bfs': {'sip': 'NA', 'DIR_ONLY': True},
                '/mnt/cache1': {'sip': '75', 'DIR_ONLY': False}, '/mnt/cache2': {'sip': '75', 'DIR_ONLY': False},
                '/mnt/cache3': {'sip': '100', 'DIR_ONLY': False}, '/mnt/cache4': {'sip': '100', 'DIR_ONLY': False},
                '/mnt/cache5': {'sip': '100', 'DIR_ONLY': False}},
        '9-8': {'/mnt/cache8': {'sip': '100', 'DIR_ONLY': False}, '/mnt/bfs': {'sip': '100', 'DIR_ONLY': False},
                '/mnt/cache1': {'sip': '75', 'DIR_ONLY': False}, '/mnt/cache2': {'sip': '75', 'DIR_ONLY': False},
                '/mnt/cache3': {'sip': '100', 'DIR_ONLY': False}, '/mnt/cache4': {'sip': '100', 'DIR_ONLY': False},
                '/mnt/cache5': {'sip': '100', 'DIR_ONLY': False}, '/mnt/cache6': {'sip': '100', 'DIR_ONLY': False},
                '/mnt/cache7': {'sip': '100', 'DIR_ONLY': False}, '/mnt/db': {'sip': '25', 'DIR_ONLY': False},
                '/mnt/md': {'sip': '25', 'DIR_ONLY': False}, '/var/tmp': {'sip': '100', 'DIR_ONLY': False}},
        '9-1': {'/mnt/bfs': {'sip': '100', 'DIR_ONLY': False}, '/mnt/cache1': {'sip': '50', 'DIR_ONLY': False},
                '/var/tmp': {'sip': '100', 'DIR_ONLY': False}, '/mnt/db': {'sip': '25', 'DIR_ONLY': False},
                '/mnt/md': {'sip': '25', 'DIR_ONLY': False}},
        '4-1': {'/mnt/bfs': {'sip': '100', 'DIR_ONLY': False}, '/mnt/cache1': {'sip': '50', 'DIR_ONLY': False},
                '/var/tmp': {'sip': '100', 'DIR_ONLY': False}, '/mnt/db': {'sip': '25', 'DIR_ONLY': False},
                '/mnt/md': {'sip': '25', 'DIR_ONLY': False}},
        '9-3': {'/mnt/bfs': {'sip': '100', 'DIR_ONLY': False}, '/mnt/cache1': {'sip': '75', 'DIR_ONLY': False},
                '/mnt/cache2': {'sip': '75', 'DIR_ONLY': False}, '/mnt/cache3': {'sip': '100', 'DIR_ONLY': False},
                '/mnt/db': {'sip': '25', 'DIR_ONLY': False}, '/mnt/md': {'sip': '25', 'DIR_ONLY': False},
                '/var/tmp': {'sip': '100', 'DIR_ONLY': False}},
        '9-2': {'/mnt/bfs': {'sip': '100', 'DIR_ONLY': False}, '/mnt/cache1': {'sip': '75', 'DIR_ONLY': False},
                '/mnt/cache2': {'sip': '75', 'DIR_ONLY': False}, '/mnt/db': {'sip': '25', 'DIR_ONLY': False},
                '/mnt/md': {'sip': '25', 'DIR_ONLY': False}, '/var/tmp': {'sip': '100', 'DIR_ONLY': False}},
        '9-5': {'/var/tmp': {'sip': '100', 'DIR_ONLY': False}, '/mnt/db': {'sip': '25', 'DIR_ONLY': False},
                '/mnt/md': {'sip': '25', 'DIR_ONLY': False}, '/mnt/bfs': {'sip': '100', 'DIR_ONLY': False},
                '/mnt/cache1': {'sip': '75', 'DIR_ONLY': False}, '/mnt/cache2': {'sip': '75', 'DIR_ONLY': False},
                '/mnt/cache3': {'sip': '100', 'DIR_ONLY': False}, '/mnt/cache4': {'sip': '100', 'DIR_ONLY': False},
                '/mnt/cache5': {'sip': '100', 'DIR_ONLY': False}},
        '0-13': {'/mnt/cache8': {'sip': '100', 'DIR_ONLY': False}, '/mnt/cache9': {'sip': '100', 'DIR_ONLY': False},
                 '/mnt/bfs': {'sip': 'NA', 'DIR_ONLY': True}, '/mnt/cache1': {'sip': '75', 'DIR_ONLY': False},
                 '/mnt/cache2': {'sip': '75', 'DIR_ONLY': False}, '/mnt/cache3': {'sip': '100', 'DIR_ONLY': False},
                 '/mnt/cache4': {'sip': '100', 'DIR_ONLY': False}, '/mnt/cache5': {'sip': '100', 'DIR_ONLY': False},
                 '/mnt/cache6': {'sip': '100', 'DIR_ONLY': False}, '/mnt/cache7': {'sip': '100', 'DIR_ONLY': False},
                 '/var/tmp': {'sip': 'NA', 'DIR_ONLY': True}, '/mnt/db': {'sip': '25', 'DIR_ONLY': False},
                 '/mnt/md': {'sip': '25', 'DIR_ONLY': False}, '/mnt/cache12': {'sip': '100', 'DIR_ONLY': False},
                 '/mnt/cache13': {'sip': '100', 'DIR_ONLY': False}, '/mnt/cache10': {'sip': '100', 'DIR_ONLY': False},
                 '/mnt/cache11': {'sip': '100', 'DIR_ONLY': False}},
        '9-7': {'/mnt/bfs': {'sip': '100', 'DIR_ONLY': False}, '/mnt/cache1': {'sip': '75', 'DIR_ONLY': False},
                '/mnt/cache2': {'sip': '75', 'DIR_ONLY': False}, '/mnt/cache3': {'sip': '100', 'DIR_ONLY': False},
                '/mnt/cache4': {'sip': '100', 'DIR_ONLY': False}, '/mnt/cache5': {'sip': '100', 'DIR_ONLY': False},
                '/mnt/cache6': {'sip': '100', 'DIR_ONLY': False}, '/mnt/cache7': {'sip': '100', 'DIR_ONLY': False},
                '/mnt/db': {'sip': '25', 'DIR_ONLY': False}, '/mnt/md': {'sip': '25', 'DIR_ONLY': False},
                '/var/tmp': {'sip': '100', 'DIR_ONLY': False}},
        '9-6': {'/var/tmp': {'sip': '100', 'DIR_ONLY': False}, '/mnt/db': {'sip': '25', 'DIR_ONLY': False},
                '/mnt/md': {'sip': '25', 'DIR_ONLY': False}, '/mnt/bfs': {'sip': '100', 'DIR_ONLY': False},
                '/mnt/cache1': {'sip': '75', 'DIR_ONLY': False}, '/mnt/cache2': {'sip': '75', 'DIR_ONLY': False},
                '/mnt/cache3': {'sip': '100', 'DIR_ONLY': False}, '/mnt/cache4': {'sip': '100', 'DIR_ONLY': False},
                '/mnt/cache5': {'sip': '100', 'DIR_ONLY': False}, '/mnt/cache6': {'sip': '100', 'DIR_ONLY': False}},
        '4-8': {'/mnt/cache8': {'sip': '100', 'DIR_ONLY': False}, '/mnt/bfs': {'sip': '100', 'DIR_ONLY': False},
                '/mnt/cache1': {'sip': '75', 'DIR_ONLY': False}, '/mnt/cache2': {'sip': '75', 'DIR_ONLY': False},
                '/mnt/cache3': {'sip': '100', 'DIR_ONLY': False}, '/mnt/cache4': {'sip': '100', 'DIR_ONLY': False},
                '/mnt/cache5': {'sip': '100', 'DIR_ONLY': False}, '/mnt/cache6': {'sip': '100', 'DIR_ONLY': False},
                '/mnt/cache7': {'sip': '100', 'DIR_ONLY': False}, '/mnt/db': {'sip': '25', 'DIR_ONLY': False},
                '/mnt/md': {'sip': '25', 'DIR_ONLY': False}, '/var/tmp': {'sip': '100', 'DIR_ONLY': False}},
        '4-9': {'/mnt/cache8': {'sip': '100', 'DIR_ONLY': False}, '/mnt/cache9': {'sip': '100', 'DIR_ONLY': False},
                '/mnt/bfs': {'sip': '100', 'DIR_ONLY': False}, '/mnt/cache1': {'sip': '75', 'DIR_ONLY': False},
                '/mnt/cache2': {'sip': '75', 'DIR_ONLY': False}, '/mnt/cache3': {'sip': '100', 'DIR_ONLY': False},
                '/mnt/cache4': {'sip': '100', 'DIR_ONLY': False}, '/mnt/cache5': {'sip': '100', 'DIR_ONLY': False},
                '/mnt/cache6': {'sip': '100', 'DIR_ONLY': False}, '/mnt/cache7': {'sip': '100', 'DIR_ONLY': False},
                '/mnt/db': {'sip': '25', 'DIR_ONLY': False}, '/mnt/md': {'sip': '25', 'DIR_ONLY': False},
                '/var/tmp': {'sip': '100', 'DIR_ONLY': False}},
        '7-1': {'/mnt/bfs': {'sip': '100', 'DIR_ONLY': False}, '/mnt/cache1': {'sip': '50', 'DIR_ONLY': False},
                '/var/tmp': {'sip': '100', 'DIR_ONLY': False}, '/mnt/db': {'sip': '25', 'DIR_ONLY': False},
                '/mnt/md': {'sip': '25', 'DIR_ONLY': False}},
        '7-0': {'/mnt/bfs': {'sip': '100', 'DIR_ONLY': False}, '/mnt/cache1': {'sip': 'NA', 'DIR_ONLY': True},
                '/var/tmp': {'sip': '100', 'DIR_ONLY': False}, '/mnt/db': {'sip': 'NA', 'DIR_ONLY': True},
                '/mnt/md': {'sip': 'NA', 'DIR_ONLY': True}},
        '7-7': {'/mnt/bfs': {'sip': '100', 'DIR_ONLY': False}, '/mnt/cache1': {'sip': '75', 'DIR_ONLY': False},
                '/mnt/cache2': {'sip': '75', 'DIR_ONLY': False}, '/mnt/cache3': {'sip': '100', 'DIR_ONLY': False},
                '/mnt/cache4': {'sip': '100', 'DIR_ONLY': False}, '/mnt/cache5': {'sip': '100', 'DIR_ONLY': False},
                '/mnt/cache6': {'sip': '100', 'DIR_ONLY': False}, '/mnt/cache7': {'sip': '100', 'DIR_ONLY': False},
                '/mnt/db': {'sip': '25', 'DIR_ONLY': False}, '/mnt/md': {'sip': '25', 'DIR_ONLY': False},
                '/var/tmp': {'sip': '100', 'DIR_ONLY': False}},
        '7-6': {'/var/tmp': {'sip': '100', 'DIR_ONLY': False}, '/mnt/db': {'sip': '25', 'DIR_ONLY': False},
                '/mnt/md': {'sip': '25', 'DIR_ONLY': False}, '/mnt/bfs': {'sip': '100', 'DIR_ONLY': False},
                '/mnt/cache1': {'sip': '75', 'DIR_ONLY': False}, '/mnt/cache2': {'sip': '75', 'DIR_ONLY': False},
                '/mnt/cache3': {'sip': '100', 'DIR_ONLY': False}, '/mnt/cache4': {'sip': '100', 'DIR_ONLY': False},
                '/mnt/cache5': {'sip': '100', 'DIR_ONLY': False}, '/mnt/cache6': {'sip': '100', 'DIR_ONLY': False}},
        '7-5': {'/var/tmp': {'sip': '100', 'DIR_ONLY': False}, '/mnt/db': {'sip': '25', 'DIR_ONLY': False},
                '/mnt/md': {'sip': '25', 'DIR_ONLY': False}, '/mnt/bfs': {'sip': '100', 'DIR_ONLY': False},
                '/mnt/cache1': {'sip': '75', 'DIR_ONLY': False}, '/mnt/cache2': {'sip': '75', 'DIR_ONLY': False},
                '/mnt/cache3': {'sip': '100', 'DIR_ONLY': False}, '/mnt/cache4': {'sip': '100', 'DIR_ONLY': False},
                '/mnt/cache5': {'sip': '100', 'DIR_ONLY': False}},
        '7-4': {'/var/tmp': {'sip': '100', 'DIR_ONLY': False}, '/mnt/db': {'sip': '25', 'DIR_ONLY': False},
                '/mnt/md': {'sip': '25', 'DIR_ONLY': False}, '/mnt/bfs': {'sip': '100', 'DIR_ONLY': False},
                '/mnt/cache1': {'sip': '75', 'DIR_ONLY': False}, '/mnt/cache2': {'sip': '75', 'DIR_ONLY': False},
                '/mnt/cache3': {'sip': '100', 'DIR_ONLY': False}, '/mnt/cache4': {'sip': '100', 'DIR_ONLY': False}},
        '10-8': {'/mnt/cache8': {'sip': '100', 'DIR_ONLY': False}, '/mnt/bfs': {'sip': '100', 'DIR_ONLY': False},
                 '/mnt/cache1': {'sip': '75', 'DIR_ONLY': False}, '/mnt/cache2': {'sip': '75', 'DIR_ONLY': False},
                 '/mnt/cache3': {'sip': '100', 'DIR_ONLY': False}, '/mnt/cache4': {'sip': '100', 'DIR_ONLY': False},
                 '/mnt/cache5': {'sip': '100', 'DIR_ONLY': False}, '/mnt/cache6': {'sip': '100', 'DIR_ONLY': False},
                 '/mnt/cache7': {'sip': '100', 'DIR_ONLY': False}, '/mnt/db': {'sip': '25', 'DIR_ONLY': False},
                 '/mnt/md': {'sip': '25', 'DIR_ONLY': False}, '/var/tmp': {'sip': '100', 'DIR_ONLY': False}},
        '10-9': {'/mnt/cache8': {'sip': '100', 'DIR_ONLY': False}, '/mnt/cache9': {'sip': '100', 'DIR_ONLY': False},
                 '/mnt/bfs': {'sip': '100', 'DIR_ONLY': False}, '/mnt/cache1': {'sip': '75', 'DIR_ONLY': False},
                 '/mnt/cache2': {'sip': '75', 'DIR_ONLY': False}, '/mnt/cache3': {'sip': '100', 'DIR_ONLY': False},
                 '/mnt/cache4': {'sip': '100', 'DIR_ONLY': False}, '/mnt/cache5': {'sip': '100', 'DIR_ONLY': False},
                 '/mnt/cache6': {'sip': '100', 'DIR_ONLY': False}, '/mnt/cache7': {'sip': '100', 'DIR_ONLY': False},
                 '/mnt/db': {'sip': '25', 'DIR_ONLY': False}, '/mnt/md': {'sip': '25', 'DIR_ONLY': False},
                 '/var/tmp': {'sip': '100', 'DIR_ONLY': False}},
        '10-0': {'/mnt/bfs': {'sip': '100', 'DIR_ONLY': False}, '/mnt/cache1': {'sip': 'NA', 'DIR_ONLY': True},
                 '/var/tmp': {'sip': '100', 'DIR_ONLY': False}, '/mnt/db': {'sip': 'NA', 'DIR_ONLY': True},
                 '/mnt/md': {'sip': 'NA', 'DIR_ONLY': True}},
        '10-1': {'/mnt/bfs': {'sip': '100', 'DIR_ONLY': False}, '/mnt/cache1': {'sip': '50', 'DIR_ONLY': False},
                 '/var/tmp': {'sip': '100', 'DIR_ONLY': False}, '/mnt/db': {'sip': '25', 'DIR_ONLY': False},
                 '/mnt/md': {'sip': '25', 'DIR_ONLY': False}},
        '10-2': {'/mnt/bfs': {'sip': '100', 'DIR_ONLY': False}, '/mnt/cache1': {'sip': '75', 'DIR_ONLY': False},
                 '/mnt/cache2': {'sip': '75', 'DIR_ONLY': False}, '/mnt/db': {'sip': '25', 'DIR_ONLY': False},
                 '/mnt/md': {'sip': '25', 'DIR_ONLY': False}, '/var/tmp': {'sip': '100', 'DIR_ONLY': False}},
        '10-3': {'/mnt/bfs': {'sip': '100', 'DIR_ONLY': False}, '/mnt/cache1': {'sip': '75', 'DIR_ONLY': False},
                 '/mnt/cache2': {'sip': '75', 'DIR_ONLY': False}, '/mnt/cache3': {'sip': '100', 'DIR_ONLY': False},
                 '/mnt/db': {'sip': '25', 'DIR_ONLY': False}, '/mnt/md': {'sip': '25', 'DIR_ONLY': False},
                 '/var/tmp': {'sip': '100', 'DIR_ONLY': False}},
        '10-4': {'/var/tmp': {'sip': '100', 'DIR_ONLY': False}, '/mnt/db': {'sip': '25', 'DIR_ONLY': False},
                 '/mnt/md': {'sip': '25', 'DIR_ONLY': False}, '/mnt/bfs': {'sip': '100', 'DIR_ONLY': False},
                 '/mnt/cache1': {'sip': '75', 'DIR_ONLY': False}, '/mnt/cache2': {'sip': '75', 'DIR_ONLY': False},
                 '/mnt/cache3': {'sip': '100', 'DIR_ONLY': False}, '/mnt/cache4': {'sip': '100', 'DIR_ONLY': False}},
        '10-5': {'/var/tmp': {'sip': '100', 'DIR_ONLY': False}, '/mnt/db': {'sip': '25', 'DIR_ONLY': False},
                 '/mnt/md': {'sip': '25', 'DIR_ONLY': False}, '/mnt/bfs': {'sip': '100', 'DIR_ONLY': False},
                 '/mnt/cache1': {'sip': '75', 'DIR_ONLY': False}, '/mnt/cache2': {'sip': '75', 'DIR_ONLY': False},
                 '/mnt/cache3': {'sip': '100', 'DIR_ONLY': False}, '/mnt/cache4': {'sip': '100', 'DIR_ONLY': False},
                 '/mnt/cache5': {'sip': '100', 'DIR_ONLY': False}},
        '10-6': {'/var/tmp': {'sip': '100', 'DIR_ONLY': False}, '/mnt/db': {'sip': '25', 'DIR_ONLY': False},
                 '/mnt/md': {'sip': '25', 'DIR_ONLY': False}, '/mnt/bfs': {'sip': '100', 'DIR_ONLY': False},
                 '/mnt/cache1': {'sip': '75', 'DIR_ONLY': False}, '/mnt/cache2': {'sip': '75', 'DIR_ONLY': False},
                 '/mnt/cache3': {'sip': '100', 'DIR_ONLY': False}, '/mnt/cache4': {'sip': '100', 'DIR_ONLY': False},
                 '/mnt/cache5': {'sip': '100', 'DIR_ONLY': False}, '/mnt/cache6': {'sip': '100', 'DIR_ONLY': False}},
        '10-7': {'/mnt/bfs': {'sip': '100', 'DIR_ONLY': False}, '/mnt/cache1': {'sip': '75', 'DIR_ONLY': False},
                 '/mnt/cache2': {'sip': '75', 'DIR_ONLY': False}, '/mnt/cache3': {'sip': '100', 'DIR_ONLY': False},
                 '/mnt/cache4': {'sip': '100', 'DIR_ONLY': False}, '/mnt/cache5': {'sip': '100', 'DIR_ONLY': False},
                 '/mnt/cache6': {'sip': '100', 'DIR_ONLY': False}, '/mnt/cache7': {'sip': '100', 'DIR_ONLY': False},
                 '/mnt/db': {'sip': '25', 'DIR_ONLY': False}, '/mnt/md': {'sip': '25', 'DIR_ONLY': False},
                 '/var/tmp': {'sip': '100', 'DIR_ONLY': False}},
        '10-12': {'/mnt/cache8': {'sip': '100', 'DIR_ONLY': False}, '/mnt/cache9': {'sip': '100', 'DIR_ONLY': False},
                  '/mnt/bfs': {'sip': '100', 'DIR_ONLY': False}, '/mnt/cache1': {'sip': '75', 'DIR_ONLY': False},
                  '/mnt/cache2': {'sip': '75', 'DIR_ONLY': False}, '/mnt/cache3': {'sip': '100', 'DIR_ONLY': False},
                  '/mnt/cache4': {'sip': '100', 'DIR_ONLY': False}, '/mnt/cache5': {'sip': '100', 'DIR_ONLY': False},
                  '/mnt/cache6': {'sip': '100', 'DIR_ONLY': False}, '/mnt/cache7': {'sip': '100', 'DIR_ONLY': False},
                  '/var/tmp': {'sip': '100', 'DIR_ONLY': False}, '/mnt/db': {'sip': '25', 'DIR_ONLY': False},
                  '/mnt/md': {'sip': '25', 'DIR_ONLY': False}, '/mnt/cache12': {'sip': '100', 'DIR_ONLY': False},
                  '/mnt/cache10': {'sip': '100', 'DIR_ONLY': False}, '/mnt/cache11': {'sip': '100', 'DIR_ONLY': False}},
        '10-13': {'/mnt/cache8': {'sip': '100', 'DIR_ONLY': False}, '/mnt/cache9': {'sip': '100', 'DIR_ONLY': False},
                  '/mnt/bfs': {'sip': '100', 'DIR_ONLY': False}, '/mnt/cache1': {'sip': '75', 'DIR_ONLY': False},
                  '/mnt/cache2': {'sip': '75', 'DIR_ONLY': False}, '/mnt/cache3': {'sip': '100', 'DIR_ONLY': False},
                  '/mnt/cache4': {'sip': '100', 'DIR_ONLY': False}, '/mnt/cache5': {'sip': '100', 'DIR_ONLY': False},
                  '/mnt/cache6': {'sip': '100', 'DIR_ONLY': False}, '/mnt/cache7': {'sip': '100', 'DIR_ONLY': False},
                  '/var/tmp': {'sip': '100', 'DIR_ONLY': False}, '/mnt/db': {'sip': '25', 'DIR_ONLY': False},
                  '/mnt/md': {'sip': '25', 'DIR_ONLY': False}, '/mnt/cache12': {'sip': '100', 'DIR_ONLY': False},
                  '/mnt/cache13': {'sip': '100', 'DIR_ONLY': False}, '/mnt/cache10': {'sip': '100', 'DIR_ONLY': False},
                  '/mnt/cache11': {'sip': '100', 'DIR_ONLY': False}},
        '10-10': {'/mnt/cache8': {'sip': '100', 'DIR_ONLY': False}, '/mnt/cache9': {'sip': '100', 'DIR_ONLY': False},
                  '/mnt/bfs': {'sip': '100', 'DIR_ONLY': False}, '/mnt/cache1': {'sip': '75', 'DIR_ONLY': False},
                  '/mnt/cache2': {'sip': '75', 'DIR_ONLY': False}, '/mnt/cache3': {'sip': '100', 'DIR_ONLY': False},
                  '/mnt/cache4': {'sip': '100', 'DIR_ONLY': False}, '/mnt/cache5': {'sip': '100', 'DIR_ONLY': False},
                  '/mnt/cache6': {'sip': '100', 'DIR_ONLY': False}, '/mnt/cache7': {'sip': '100', 'DIR_ONLY': False},
                  '/mnt/db': {'sip': '25', 'DIR_ONLY': False}, '/mnt/md': {'sip': '25', 'DIR_ONLY': False},
                  '/var/tmp': {'sip': '100', 'DIR_ONLY': False}, '/mnt/cache10': {'sip': '100', 'DIR_ONLY': False}},
        '10-11': {'/mnt/cache8': {'sip': '100', 'DIR_ONLY': False}, '/mnt/cache9': {'sip': '100', 'DIR_ONLY': False},
                  '/mnt/bfs': {'sip': '100', 'DIR_ONLY': False}, '/mnt/cache1': {'sip': '75', 'DIR_ONLY': False},
                  '/mnt/cache2': {'sip': '75', 'DIR_ONLY': False}, '/mnt/cache3': {'sip': '100', 'DIR_ONLY': False},
                  '/mnt/cache4': {'sip': '100', 'DIR_ONLY': False}, '/mnt/cache5': {'sip': '100', 'DIR_ONLY': False},
                  '/mnt/cache6': {'sip': '100', 'DIR_ONLY': False}, '/mnt/cache7': {'sip': '100', 'DIR_ONLY': False},
                  '/mnt/db': {'sip': '25', 'DIR_ONLY': False}, '/mnt/md': {'sip': '25', 'DIR_ONLY': False},
                  '/var/tmp': {'sip': '100', 'DIR_ONLY': False}, '/mnt/cache10': {'sip': '100', 'DIR_ONLY': False},
                  '/mnt/cache11': {'sip': '100', 'DIR_ONLY': False}},
        '2-3': {'/mnt/bfs': {'sip': '100', 'DIR_ONLY': False}, '/mnt/cache1': {'sip': '75', 'DIR_ONLY': False},
                '/mnt/cache2': {'sip': '75', 'DIR_ONLY': False}, '/mnt/cache3': {'sip': '100', 'DIR_ONLY': False},
                '/mnt/db': {'sip': '25', 'DIR_ONLY': False}, '/mnt/md': {'sip': '25', 'DIR_ONLY': False},
                '/var/tmp': {'sip': '100', 'DIR_ONLY': False}},
        '5-9': {'/mnt/cache8': {'sip': '100', 'DIR_ONLY': False}, '/mnt/cache9': {'sip': '100', 'DIR_ONLY': False},
                '/mnt/bfs': {'sip': '100', 'DIR_ONLY': False}, '/mnt/cache1': {'sip': '75', 'DIR_ONLY': False},
                '/mnt/cache2': {'sip': '75', 'DIR_ONLY': False}, '/mnt/cache3': {'sip': '100', 'DIR_ONLY': False},
                '/mnt/cache4': {'sip': '100', 'DIR_ONLY': False}, '/mnt/cache5': {'sip': '100', 'DIR_ONLY': False},
                '/mnt/cache6': {'sip': '100', 'DIR_ONLY': False}, '/mnt/cache7': {'sip': '100', 'DIR_ONLY': False},
                '/mnt/db': {'sip': '25', 'DIR_ONLY': False}, '/mnt/md': {'sip': '25', 'DIR_ONLY': False},
                '/var/tmp': {'sip': '100', 'DIR_ONLY': False}},
        '0-6': {'/var/tmp': {'sip': 'NA', 'DIR_ONLY': True}, '/mnt/db': {'sip': '25', 'DIR_ONLY': False},
                '/mnt/md': {'sip': '25', 'DIR_ONLY': False}, '/mnt/bfs': {'sip': 'NA', 'DIR_ONLY': True},
                '/mnt/cache1': {'sip': '75', 'DIR_ONLY': False}, '/mnt/cache2': {'sip': '75', 'DIR_ONLY': False},
                '/mnt/cache3': {'sip': '100', 'DIR_ONLY': False}, '/mnt/cache4': {'sip': '100', 'DIR_ONLY': False},
                '/mnt/cache5': {'sip': '100', 'DIR_ONLY': False}, '/mnt/cache6': {'sip': '100', 'DIR_ONLY': False}},
        '5-2': {'/mnt/bfs': {'sip': '100', 'DIR_ONLY': False}, '/mnt/cache1': {'sip': '75', 'DIR_ONLY': False},
                '/mnt/cache2': {'sip': '75', 'DIR_ONLY': False}, '/mnt/db': {'sip': '25', 'DIR_ONLY': False},
                '/mnt/md': {'sip': '25', 'DIR_ONLY': False}, '/var/tmp': {'sip': '100', 'DIR_ONLY': False}},
        '0-7': {'/mnt/bfs': {'sip': 'NA', 'DIR_ONLY': True}, '/mnt/cache1': {'sip': '75', 'DIR_ONLY': False},
                '/mnt/cache2': {'sip': '75', 'DIR_ONLY': False}, '/mnt/cache3': {'sip': '100', 'DIR_ONLY': False},
                '/mnt/cache4': {'sip': '100', 'DIR_ONLY': False}, '/mnt/cache5': {'sip': '100', 'DIR_ONLY': False},
                '/mnt/cache6': {'sip': '100', 'DIR_ONLY': False}, '/mnt/cache7': {'sip': '100', 'DIR_ONLY': False},
                '/mnt/db': {'sip': '25', 'DIR_ONLY': False}, '/mnt/md': {'sip': '25', 'DIR_ONLY': False},
                '/var/tmp': {'sip': 'NA', 'DIR_ONLY': True}},
        '5-13': {'/mnt/cache8': {'sip': '100', 'DIR_ONLY': False}, '/mnt/cache9': {'sip': '100', 'DIR_ONLY': False},
                 '/mnt/bfs': {'sip': '100', 'DIR_ONLY': False}, '/mnt/cache1': {'sip': '75', 'DIR_ONLY': False},
                 '/mnt/cache2': {'sip': '75', 'DIR_ONLY': False}, '/mnt/cache3': {'sip': '100', 'DIR_ONLY': False},
                 '/mnt/cache4': {'sip': '100', 'DIR_ONLY': False}, '/mnt/cache5': {'sip': '100', 'DIR_ONLY': False},
                 '/mnt/cache6': {'sip': '100', 'DIR_ONLY': False}, '/mnt/cache7': {'sip': '100', 'DIR_ONLY': False},
                 '/var/tmp': {'sip': '100', 'DIR_ONLY': False}, '/mnt/db': {'sip': '25', 'DIR_ONLY': False},
                 '/mnt/md': {'sip': '25', 'DIR_ONLY': False}, '/mnt/cache12': {'sip': '100', 'DIR_ONLY': False},
                 '/mnt/cache13': {'sip': '100', 'DIR_ONLY': False}, '/mnt/cache10': {'sip': '100', 'DIR_ONLY': False},
                 '/mnt/cache11': {'sip': '100', 'DIR_ONLY': False}},
        '12-7': {'/mnt/bfs': {'sip': '100', 'DIR_ONLY': False}, '/mnt/cache1': {'sip': '75', 'DIR_ONLY': False},
                 '/mnt/cache2': {'sip': '75', 'DIR_ONLY': False}, '/mnt/cache3': {'sip': '100', 'DIR_ONLY': False},
                 '/mnt/cache4': {'sip': '100', 'DIR_ONLY': False}, '/mnt/cache5': {'sip': '100', 'DIR_ONLY': False},
                 '/mnt/cache6': {'sip': '100', 'DIR_ONLY': False}, '/mnt/cache7': {'sip': '100', 'DIR_ONLY': False},
                 '/mnt/db': {'sip': '25', 'DIR_ONLY': False}, '/mnt/md': {'sip': '25', 'DIR_ONLY': False},
                 '/var/tmp': {'sip': '100', 'DIR_ONLY': False}},
        '0-0': {'/mnt/bfs': {'sip': 'NA', 'DIR_ONLY': True}, '/mnt/cache1': {'sip': 'NA', 'DIR_ONLY': True},
                '/var/tmp': {'sip': 'NA', 'DIR_ONLY': True}, '/mnt/db': {'sip': 'NA', 'DIR_ONLY': True},
                '/mnt/md': {'sip': 'NA', 'DIR_ONLY': True}},
        '0-1': {'/mnt/bfs': {'sip': 'NA', 'DIR_ONLY': True}, '/mnt/cache1': {'sip': '50', 'DIR_ONLY': False},
                '/var/tmp': {'sip': 'NA', 'DIR_ONLY': True}, '/mnt/db': {'sip': '25', 'DIR_ONLY': False},
                '/mnt/md': {'sip': '25', 'DIR_ONLY': False}},
        '6-10': {'/mnt/cache8': {'sip': '100', 'DIR_ONLY': False}, '/mnt/cache9': {'sip': '100', 'DIR_ONLY': False},
                 '/mnt/bfs': {'sip': '100', 'DIR_ONLY': False}, '/mnt/cache1': {'sip': '75', 'DIR_ONLY': False},
                 '/mnt/cache2': {'sip': '75', 'DIR_ONLY': False}, '/mnt/cache3': {'sip': '100', 'DIR_ONLY': False},
                 '/mnt/cache4': {'sip': '100', 'DIR_ONLY': False}, '/mnt/cache5': {'sip': '100', 'DIR_ONLY': False},
                 '/mnt/cache6': {'sip': '100', 'DIR_ONLY': False}, '/mnt/cache7': {'sip': '100', 'DIR_ONLY': False},
                 '/mnt/db': {'sip': '25', 'DIR_ONLY': False}, '/mnt/md': {'sip': '25', 'DIR_ONLY': False},
                 '/var/tmp': {'sip': '100', 'DIR_ONLY': False}, '/mnt/cache10': {'sip': '100', 'DIR_ONLY': False}},
        '6-11': {'/mnt/cache8': {'sip': '100', 'DIR_ONLY': False}, '/mnt/cache9': {'sip': '100', 'DIR_ONLY': False},
                 '/mnt/bfs': {'sip': '100', 'DIR_ONLY': False}, '/mnt/cache1': {'sip': '75', 'DIR_ONLY': False},
                 '/mnt/cache2': {'sip': '75', 'DIR_ONLY': False}, '/mnt/cache3': {'sip': '100', 'DIR_ONLY': False},
                 '/mnt/cache4': {'sip': '100', 'DIR_ONLY': False}, '/mnt/cache5': {'sip': '100', 'DIR_ONLY': False},
                 '/mnt/cache6': {'sip': '100', 'DIR_ONLY': False}, '/mnt/cache7': {'sip': '100', 'DIR_ONLY': False},
                 '/mnt/db': {'sip': '25', 'DIR_ONLY': False}, '/mnt/md': {'sip': '25', 'DIR_ONLY': False},
                 '/var/tmp': {'sip': '100', 'DIR_ONLY': False}, '/mnt/cache10': {'sip': '100', 'DIR_ONLY': False},
                 '/mnt/cache11': {'sip': '100', 'DIR_ONLY': False}},
        '6-12': {'/mnt/cache8': {'sip': '100', 'DIR_ONLY': False}, '/mnt/cache9': {'sip': '100', 'DIR_ONLY': False},
                 '/mnt/bfs': {'sip': '100', 'DIR_ONLY': False}, '/mnt/cache1': {'sip': '75', 'DIR_ONLY': False},
                 '/mnt/cache2': {'sip': '75', 'DIR_ONLY': False}, '/mnt/cache3': {'sip': '100', 'DIR_ONLY': False},
                 '/mnt/cache4': {'sip': '100', 'DIR_ONLY': False}, '/mnt/cache5': {'sip': '100', 'DIR_ONLY': False},
                 '/mnt/cache6': {'sip': '100', 'DIR_ONLY': False}, '/mnt/cache7': {'sip': '100', 'DIR_ONLY': False},
                 '/var/tmp': {'sip': '100', 'DIR_ONLY': False}, '/mnt/db': {'sip': '25', 'DIR_ONLY': False},
                 '/mnt/md': {'sip': '25', 'DIR_ONLY': False}, '/mnt/cache12': {'sip': '100', 'DIR_ONLY': False},
                 '/mnt/cache10': {'sip': '100', 'DIR_ONLY': False}, '/mnt/cache11': {'sip': '100', 'DIR_ONLY': False}},
        '6-13': {'/mnt/cache8': {'sip': '100', 'DIR_ONLY': False}, '/mnt/cache9': {'sip': '100', 'DIR_ONLY': False},
                 '/mnt/bfs': {'sip': '100', 'DIR_ONLY': False}, '/mnt/cache1': {'sip': '75', 'DIR_ONLY': False},
                 '/mnt/cache2': {'sip': '75', 'DIR_ONLY': False}, '/mnt/cache3': {'sip': '100', 'DIR_ONLY': False},
                 '/mnt/cache4': {'sip': '100', 'DIR_ONLY': False}, '/mnt/cache5': {'sip': '100', 'DIR_ONLY': False},
                 '/mnt/cache6': {'sip': '100', 'DIR_ONLY': False}, '/mnt/cache7': {'sip': '100', 'DIR_ONLY': False},
                 '/var/tmp': {'sip': '100', 'DIR_ONLY': False}, '/mnt/db': {'sip': '25', 'DIR_ONLY': False},
                 '/mnt/md': {'sip': '25', 'DIR_ONLY': False}, '/mnt/cache12': {'sip': '100', 'DIR_ONLY': False},
                 '/mnt/cache13': {'sip': '100', 'DIR_ONLY': False}, '/mnt/cache10': {'sip': '100', 'DIR_ONLY': False},
                 '/mnt/cache11': {'sip': '100', 'DIR_ONLY': False}},
        '0-2': {'/mnt/bfs': {'sip': 'NA', 'DIR_ONLY': True}, '/mnt/cache1': {'sip': '75', 'DIR_ONLY': False},
                '/mnt/cache2': {'sip': '75', 'DIR_ONLY': False}, '/mnt/db': {'sip': '25', 'DIR_ONLY': False},
                '/mnt/md': {'sip': '25', 'DIR_ONLY': False}, '/var/tmp': {'sip': 'NA', 'DIR_ONLY': True}},
        '0-3': {'/mnt/bfs': {'sip': 'NA', 'DIR_ONLY': True}, '/mnt/cache1': {'sip': '75', 'DIR_ONLY': False},
                '/mnt/cache2': {'sip': '75', 'DIR_ONLY': False}, '/mnt/cache3': {'sip': '100', 'DIR_ONLY': False},
                '/mnt/db': {'sip': '25', 'DIR_ONLY': False}, '/mnt/md': {'sip': '25', 'DIR_ONLY': False},
                '/var/tmp': {'sip': 'NA', 'DIR_ONLY': True}},
        '9-9': {'/mnt/cache8': {'sip': '100', 'DIR_ONLY': False}, '/mnt/cache9': {'sip': '100', 'DIR_ONLY': False},
                '/mnt/bfs': {'sip': '100', 'DIR_ONLY': False}, '/mnt/cache1': {'sip': '75', 'DIR_ONLY': False},
                '/mnt/cache2': {'sip': '75', 'DIR_ONLY': False}, '/mnt/cache3': {'sip': '100', 'DIR_ONLY': False},
                '/mnt/cache4': {'sip': '100', 'DIR_ONLY': False}, '/mnt/cache5': {'sip': '100', 'DIR_ONLY': False},
                '/mnt/cache6': {'sip': '100', 'DIR_ONLY': False}, '/mnt/cache7': {'sip': '100', 'DIR_ONLY': False},
                '/mnt/db': {'sip': '25', 'DIR_ONLY': False}, '/mnt/md': {'sip': '25', 'DIR_ONLY': False},
                '/var/tmp': {'sip': '100', 'DIR_ONLY': False}},
        '13-13': {'/mnt/cache8': {'sip': '100', 'DIR_ONLY': False}, '/mnt/cache9': {'sip': '100', 'DIR_ONLY': False},
                  '/mnt/bfs': {'sip': '100', 'DIR_ONLY': False}, '/mnt/cache1': {'sip': '75', 'DIR_ONLY': False},
                  '/mnt/cache2': {'sip': '75', 'DIR_ONLY': False}, '/mnt/cache3': {'sip': '100', 'DIR_ONLY': False},
                  '/mnt/cache4': {'sip': '100', 'DIR_ONLY': False}, '/mnt/cache5': {'sip': '100', 'DIR_ONLY': False},
                  '/mnt/cache6': {'sip': '100', 'DIR_ONLY': False}, '/mnt/cache7': {'sip': '100', 'DIR_ONLY': False},
                  '/var/tmp': {'sip': '100', 'DIR_ONLY': False}, '/mnt/db': {'sip': '25', 'DIR_ONLY': False},
                  '/mnt/md': {'sip': '25', 'DIR_ONLY': False}, '/mnt/cache12': {'sip': '100', 'DIR_ONLY': False},
                  '/mnt/cache13': {'sip': '100', 'DIR_ONLY': False}, '/mnt/cache10': {'sip': '100', 'DIR_ONLY': False},
                  '/mnt/cache11': {'sip': '100', 'DIR_ONLY': False}},
        '13-12': {'/mnt/cache8': {'sip': '100', 'DIR_ONLY': False}, '/mnt/cache9': {'sip': '100', 'DIR_ONLY': False},
                  '/mnt/bfs': {'sip': '100', 'DIR_ONLY': False}, '/mnt/cache1': {'sip': '75', 'DIR_ONLY': False},
                  '/mnt/cache2': {'sip': '75', 'DIR_ONLY': False}, '/mnt/cache3': {'sip': '100', 'DIR_ONLY': False},
                  '/mnt/cache4': {'sip': '100', 'DIR_ONLY': False}, '/mnt/cache5': {'sip': '100', 'DIR_ONLY': False},
                  '/mnt/cache6': {'sip': '100', 'DIR_ONLY': False}, '/mnt/cache7': {'sip': '100', 'DIR_ONLY': False},
                  '/var/tmp': {'sip': '100', 'DIR_ONLY': False}, '/mnt/db': {'sip': '25', 'DIR_ONLY': False},
                  '/mnt/md': {'sip': '25', 'DIR_ONLY': False}, '/mnt/cache12': {'sip': '100', 'DIR_ONLY': False},
                  '/mnt/cache10': {'sip': '100', 'DIR_ONLY': False}, '/mnt/cache11': {'sip': '100', 'DIR_ONLY': False}},
        '13-11': {'/mnt/cache8': {'sip': '100', 'DIR_ONLY': False}, '/mnt/cache9': {'sip': '100', 'DIR_ONLY': False},
                  '/mnt/bfs': {'sip': '100', 'DIR_ONLY': False}, '/mnt/cache1': {'sip': '75', 'DIR_ONLY': False},
                  '/mnt/cache2': {'sip': '75', 'DIR_ONLY': False}, '/mnt/cache3': {'sip': '100', 'DIR_ONLY': False},
                  '/mnt/cache4': {'sip': '100', 'DIR_ONLY': False}, '/mnt/cache5': {'sip': '100', 'DIR_ONLY': False},
                  '/mnt/cache6': {'sip': '100', 'DIR_ONLY': False}, '/mnt/cache7': {'sip': '100', 'DIR_ONLY': False},
                  '/mnt/db': {'sip': '25', 'DIR_ONLY': False}, '/mnt/md': {'sip': '25', 'DIR_ONLY': False},
                  '/var/tmp': {'sip': '100', 'DIR_ONLY': False}, '/mnt/cache10': {'sip': '100', 'DIR_ONLY': False},
                  '/mnt/cache11': {'sip': '100', 'DIR_ONLY': False}},
        '13-10': {'/mnt/cache8': {'sip': '100', 'DIR_ONLY': False}, '/mnt/cache9': {'sip': '100', 'DIR_ONLY': False},
                  '/mnt/bfs': {'sip': '100', 'DIR_ONLY': False}, '/mnt/cache1': {'sip': '75', 'DIR_ONLY': False},
                  '/mnt/cache2': {'sip': '75', 'DIR_ONLY': False}, '/mnt/cache3': {'sip': '100', 'DIR_ONLY': False},
                  '/mnt/cache4': {'sip': '100', 'DIR_ONLY': False}, '/mnt/cache5': {'sip': '100', 'DIR_ONLY': False},
                  '/mnt/cache6': {'sip': '100', 'DIR_ONLY': False}, '/mnt/cache7': {'sip': '100', 'DIR_ONLY': False},
                  '/mnt/db': {'sip': '25', 'DIR_ONLY': False}, '/mnt/md': {'sip': '25', 'DIR_ONLY': False},
                  '/var/tmp': {'sip': '100', 'DIR_ONLY': False}, '/mnt/cache10': {'sip': '100', 'DIR_ONLY': False}},
        '1-9': {'/mnt/cache8': {'sip': '100', 'DIR_ONLY': False}, '/mnt/cache9': {'sip': '100', 'DIR_ONLY': False},
                '/mnt/bfs': {'sip': '80', 'DIR_ONLY': False}, '/mnt/cache1': {'sip': '75', 'DIR_ONLY': False},
                '/mnt/cache2': {'sip': '75', 'DIR_ONLY': False}, '/mnt/cache3': {'sip': '100', 'DIR_ONLY': False},
                '/mnt/cache4': {'sip': '100', 'DIR_ONLY': False}, '/mnt/cache5': {'sip': '100', 'DIR_ONLY': False},
                '/mnt/cache6': {'sip': '100', 'DIR_ONLY': False}, '/mnt/cache7': {'sip': '100', 'DIR_ONLY': False},
                '/mnt/db': {'sip': '25', 'DIR_ONLY': False}, '/mnt/md': {'sip': '25', 'DIR_ONLY': False},
                '/var/tmp': {'sip': '20', 'DIR_ONLY': False}},
        '1-8': {'/mnt/cache8': {'sip': '100', 'DIR_ONLY': False}, '/mnt/bfs': {'sip': '80', 'DIR_ONLY': False},
                '/mnt/cache1': {'sip': '75', 'DIR_ONLY': False}, '/mnt/cache2': {'sip': '75', 'DIR_ONLY': False},
                '/mnt/cache3': {'sip': '100', 'DIR_ONLY': False}, '/mnt/cache4': {'sip': '100', 'DIR_ONLY': False},
                '/mnt/cache5': {'sip': '100', 'DIR_ONLY': False}, '/mnt/cache6': {'sip': '100', 'DIR_ONLY': False},
                '/mnt/cache7': {'sip': '100', 'DIR_ONLY': False}, '/mnt/db': {'sip': '25', 'DIR_ONLY': False},
                '/mnt/md': {'sip': '25', 'DIR_ONLY': False}, '/var/tmp': {'sip': '20', 'DIR_ONLY': False}},
        '1-1': {'/mnt/bfs': {'sip': '80', 'DIR_ONLY': False}, '/mnt/cache1': {'sip': '50', 'DIR_ONLY': False},
                '/var/tmp': {'sip': '20', 'DIR_ONLY': False}, '/mnt/db': {'sip': '25', 'DIR_ONLY': False},
                '/mnt/md': {'sip': '25', 'DIR_ONLY': False}},
        '8-12': {'/mnt/cache8': {'sip': '100', 'DIR_ONLY': False}, '/mnt/cache9': {'sip': '100', 'DIR_ONLY': False},
                 '/mnt/bfs': {'sip': '100', 'DIR_ONLY': False}, '/mnt/cache1': {'sip': '75', 'DIR_ONLY': False},
                 '/mnt/cache2': {'sip': '75', 'DIR_ONLY': False}, '/mnt/cache3': {'sip': '100', 'DIR_ONLY': False},
                 '/mnt/cache4': {'sip': '100', 'DIR_ONLY': False}, '/mnt/cache5': {'sip': '100', 'DIR_ONLY': False},
                 '/mnt/cache6': {'sip': '100', 'DIR_ONLY': False}, '/mnt/cache7': {'sip': '100', 'DIR_ONLY': False},
                 '/var/tmp': {'sip': '100', 'DIR_ONLY': False}, '/mnt/db': {'sip': '25', 'DIR_ONLY': False},
                 '/mnt/md': {'sip': '25', 'DIR_ONLY': False}, '/mnt/cache12': {'sip': '100', 'DIR_ONLY': False},
                 '/mnt/cache10': {'sip': '100', 'DIR_ONLY': False}, '/mnt/cache11': {'sip': '100', 'DIR_ONLY': False}},
        '1-3': {'/mnt/bfs': {'sip': '80', 'DIR_ONLY': False}, '/mnt/cache1': {'sip': '75', 'DIR_ONLY': False},
                '/mnt/cache2': {'sip': '75', 'DIR_ONLY': False}, '/mnt/cache3': {'sip': '100', 'DIR_ONLY': False},
                '/mnt/db': {'sip': '25', 'DIR_ONLY': False}, '/mnt/md': {'sip': '25', 'DIR_ONLY': False},
                '/var/tmp': {'sip': '20', 'DIR_ONLY': False}},
        '1-2': {'/mnt/bfs': {'sip': '80', 'DIR_ONLY': False}, '/mnt/cache1': {'sip': '75', 'DIR_ONLY': False},
                '/mnt/cache2': {'sip': '75', 'DIR_ONLY': False}, '/mnt/db': {'sip': '25', 'DIR_ONLY': False},
                '/mnt/md': {'sip': '25', 'DIR_ONLY': False}, '/var/tmp': {'sip': '20', 'DIR_ONLY': False}},
        '1-5': {'/var/tmp': {'sip': '20', 'DIR_ONLY': False}, '/mnt/db': {'sip': '25', 'DIR_ONLY': False},
                '/mnt/md': {'sip': '25', 'DIR_ONLY': False}, '/mnt/bfs': {'sip': '80', 'DIR_ONLY': False},
                '/mnt/cache1': {'sip': '75', 'DIR_ONLY': False}, '/mnt/cache2': {'sip': '75', 'DIR_ONLY': False},
                '/mnt/cache3': {'sip': '100', 'DIR_ONLY': False}, '/mnt/cache4': {'sip': '100', 'DIR_ONLY': False},
                '/mnt/cache5': {'sip': '100', 'DIR_ONLY': False}},
        '1-4': {'/var/tmp': {'sip': '20', 'DIR_ONLY': False}, '/mnt/db': {'sip': '25', 'DIR_ONLY': False},
                '/mnt/md': {'sip': '25', 'DIR_ONLY': False}, '/mnt/bfs': {'sip': '80', 'DIR_ONLY': False},
                '/mnt/cache1': {'sip': '75', 'DIR_ONLY': False}, '/mnt/cache2': {'sip': '75', 'DIR_ONLY': False},
                '/mnt/cache3': {'sip': '100', 'DIR_ONLY': False}, '/mnt/cache4': {'sip': '100', 'DIR_ONLY': False}},
        '1-7': {'/mnt/bfs': {'sip': '80', 'DIR_ONLY': False}, '/mnt/cache1': {'sip': '75', 'DIR_ONLY': False},
                '/mnt/cache2': {'sip': '75', 'DIR_ONLY': False}, '/mnt/cache3': {'sip': '100', 'DIR_ONLY': False},
                '/mnt/cache4': {'sip': '100', 'DIR_ONLY': False}, '/mnt/cache5': {'sip': '100', 'DIR_ONLY': False},
                '/mnt/cache6': {'sip': '100', 'DIR_ONLY': False}, '/mnt/cache7': {'sip': '100', 'DIR_ONLY': False},
                '/mnt/db': {'sip': '25', 'DIR_ONLY': False}, '/mnt/md': {'sip': '25', 'DIR_ONLY': False},
                '/var/tmp': {'sip': '20', 'DIR_ONLY': False}},
        '8-13': {'/mnt/cache8': {'sip': '100', 'DIR_ONLY': False}, '/mnt/cache9': {'sip': '100', 'DIR_ONLY': False},
                 '/mnt/bfs': {'sip': '100', 'DIR_ONLY': False}, '/mnt/cache1': {'sip': '75', 'DIR_ONLY': False},
                 '/mnt/cache2': {'sip': '75', 'DIR_ONLY': False}, '/mnt/cache3': {'sip': '100', 'DIR_ONLY': False},
                 '/mnt/cache4': {'sip': '100', 'DIR_ONLY': False}, '/mnt/cache5': {'sip': '100', 'DIR_ONLY': False},
                 '/mnt/cache6': {'sip': '100', 'DIR_ONLY': False}, '/mnt/cache7': {'sip': '100', 'DIR_ONLY': False},
                 '/var/tmp': {'sip': '100', 'DIR_ONLY': False}, '/mnt/db': {'sip': '25', 'DIR_ONLY': False},
                 '/mnt/md': {'sip': '25', 'DIR_ONLY': False}, '/mnt/cache12': {'sip': '100', 'DIR_ONLY': False},
                 '/mnt/cache13': {'sip': '100', 'DIR_ONLY': False}, '/mnt/cache10': {'sip': '100', 'DIR_ONLY': False},
                 '/mnt/cache11': {'sip': '100', 'DIR_ONLY': False}},
        '8-10': {'/mnt/cache8': {'sip': '100', 'DIR_ONLY': False}, '/mnt/cache9': {'sip': '100', 'DIR_ONLY': False},
                 '/mnt/bfs': {'sip': '100', 'DIR_ONLY': False}, '/mnt/cache1': {'sip': '75', 'DIR_ONLY': False},
                 '/mnt/cache2': {'sip': '75', 'DIR_ONLY': False}, '/mnt/cache3': {'sip': '100', 'DIR_ONLY': False},
                 '/mnt/cache4': {'sip': '100', 'DIR_ONLY': False}, '/mnt/cache5': {'sip': '100', 'DIR_ONLY': False},
                 '/mnt/cache6': {'sip': '100', 'DIR_ONLY': False}, '/mnt/cache7': {'sip': '100', 'DIR_ONLY': False},
                 '/mnt/db': {'sip': '25', 'DIR_ONLY': False}, '/mnt/md': {'sip': '25', 'DIR_ONLY': False},
                 '/var/tmp': {'sip': '100', 'DIR_ONLY': False}, '/mnt/cache10': {'sip': '100', 'DIR_ONLY': False}},
        '8-11': {'/mnt/cache8': {'sip': '100', 'DIR_ONLY': False}, '/mnt/cache9': {'sip': '100', 'DIR_ONLY': False},
                 '/mnt/bfs': {'sip': '100', 'DIR_ONLY': False}, '/mnt/cache1': {'sip': '75', 'DIR_ONLY': False},
                 '/mnt/cache2': {'sip': '75', 'DIR_ONLY': False}, '/mnt/cache3': {'sip': '100', 'DIR_ONLY': False},
                 '/mnt/cache4': {'sip': '100', 'DIR_ONLY': False}, '/mnt/cache5': {'sip': '100', 'DIR_ONLY': False},
                 '/mnt/cache6': {'sip': '100', 'DIR_ONLY': False}, '/mnt/cache7': {'sip': '100', 'DIR_ONLY': False},
                 '/mnt/db': {'sip': '25', 'DIR_ONLY': False}, '/mnt/md': {'sip': '25', 'DIR_ONLY': False},
                 '/var/tmp': {'sip': '100', 'DIR_ONLY': False}, '/mnt/cache10': {'sip': '100', 'DIR_ONLY': False},
                 '/mnt/cache11': {'sip': '100', 'DIR_ONLY': False}},
        '5-5': {'/var/tmp': {'sip': '100', 'DIR_ONLY': False}, '/mnt/db': {'sip': '25', 'DIR_ONLY': False},
                '/mnt/md': {'sip': '25', 'DIR_ONLY': False}, '/mnt/bfs': {'sip': '100', 'DIR_ONLY': False},
                '/mnt/cache1': {'sip': '75', 'DIR_ONLY': False}, '/mnt/cache2': {'sip': '75', 'DIR_ONLY': False},
                '/mnt/cache3': {'sip': '100', 'DIR_ONLY': False}, '/mnt/cache4': {'sip': '100', 'DIR_ONLY': False},
                '/mnt/cache5': {'sip': '100', 'DIR_ONLY': False}},
        '5-4': {'/var/tmp': {'sip': '100', 'DIR_ONLY': False}, '/mnt/db': {'sip': '25', 'DIR_ONLY': False},
                '/mnt/md': {'sip': '25', 'DIR_ONLY': False}, '/mnt/bfs': {'sip': '100', 'DIR_ONLY': False},
                '/mnt/cache1': {'sip': '75', 'DIR_ONLY': False}, '/mnt/cache2': {'sip': '75', 'DIR_ONLY': False},
                '/mnt/cache3': {'sip': '100', 'DIR_ONLY': False}, '/mnt/cache4': {'sip': '100', 'DIR_ONLY': False}},
        '5-7': {'/mnt/bfs': {'sip': '100', 'DIR_ONLY': False}, '/mnt/cache1': {'sip': '75', 'DIR_ONLY': False},
                '/mnt/cache2': {'sip': '75', 'DIR_ONLY': False}, '/mnt/cache3': {'sip': '100', 'DIR_ONLY': False},
                '/mnt/cache4': {'sip': '100', 'DIR_ONLY': False}, '/mnt/cache5': {'sip': '100', 'DIR_ONLY': False},
                '/mnt/cache6': {'sip': '100', 'DIR_ONLY': False}, '/mnt/cache7': {'sip': '100', 'DIR_ONLY': False},
                '/mnt/db': {'sip': '25', 'DIR_ONLY': False}, '/mnt/md': {'sip': '25', 'DIR_ONLY': False},
                '/var/tmp': {'sip': '100', 'DIR_ONLY': False}},
        '5-6': {'/var/tmp': {'sip': '100', 'DIR_ONLY': False}, '/mnt/db': {'sip': '25', 'DIR_ONLY': False},
                '/mnt/md': {'sip': '25', 'DIR_ONLY': False}, '/mnt/bfs': {'sip': '100', 'DIR_ONLY': False},
                '/mnt/cache1': {'sip': '75', 'DIR_ONLY': False}, '/mnt/cache2': {'sip': '75', 'DIR_ONLY': False},
                '/mnt/cache3': {'sip': '100', 'DIR_ONLY': False}, '/mnt/cache4': {'sip': '100', 'DIR_ONLY': False},
                '/mnt/cache5': {'sip': '100', 'DIR_ONLY': False}, '/mnt/cache6': {'sip': '100', 'DIR_ONLY': False}},
        '5-1': {'/mnt/bfs': {'sip': '100', 'DIR_ONLY': False}, '/mnt/cache1': {'sip': '50', 'DIR_ONLY': False},
                '/var/tmp': {'sip': '100', 'DIR_ONLY': False}, '/mnt/db': {'sip': '25', 'DIR_ONLY': False},
                '/mnt/md': {'sip': '25', 'DIR_ONLY': False}},
        '5-0': {'/mnt/bfs': {'sip': '100', 'DIR_ONLY': False}, '/mnt/cache1': {'sip': 'NA', 'DIR_ONLY': True},
                '/var/tmp': {'sip': '100', 'DIR_ONLY': False}, '/mnt/db': {'sip': 'NA', 'DIR_ONLY': True},
                '/mnt/md': {'sip': 'NA', 'DIR_ONLY': True}},
        '3-9': {'/mnt/cache8': {'sip': '100', 'DIR_ONLY': False}, '/mnt/cache9': {'sip': '100', 'DIR_ONLY': False},
                '/mnt/bfs': {'sip': '100', 'DIR_ONLY': False}, '/mnt/cache1': {'sip': '75', 'DIR_ONLY': False},
                '/mnt/cache2': {'sip': '75', 'DIR_ONLY': False}, '/mnt/cache3': {'sip': '100', 'DIR_ONLY': False},
                '/mnt/cache4': {'sip': '100', 'DIR_ONLY': False}, '/mnt/cache5': {'sip': '100', 'DIR_ONLY': False},
                '/mnt/cache6': {'sip': '100', 'DIR_ONLY': False}, '/mnt/cache7': {'sip': '100', 'DIR_ONLY': False},
                '/mnt/db': {'sip': '25', 'DIR_ONLY': False}, '/mnt/md': {'sip': '25', 'DIR_ONLY': False},
                '/var/tmp': {'sip': '100', 'DIR_ONLY': False}},
        '3-8': {'/mnt/cache8': {'sip': '100', 'DIR_ONLY': False}, '/mnt/bfs': {'sip': '100', 'DIR_ONLY': False},
                '/mnt/cache1': {'sip': '75', 'DIR_ONLY': False}, '/mnt/cache2': {'sip': '75', 'DIR_ONLY': False},
                '/mnt/cache3': {'sip': '100', 'DIR_ONLY': False}, '/mnt/cache4': {'sip': '100', 'DIR_ONLY': False},
                '/mnt/cache5': {'sip': '100', 'DIR_ONLY': False}, '/mnt/cache6': {'sip': '100', 'DIR_ONLY': False},
                '/mnt/cache7': {'sip': '100', 'DIR_ONLY': False}, '/mnt/db': {'sip': '25', 'DIR_ONLY': False},
                '/mnt/md': {'sip': '25', 'DIR_ONLY': False}, '/var/tmp': {'sip': '100', 'DIR_ONLY': False}},
        '3-7': {'/mnt/bfs': {'sip': '100', 'DIR_ONLY': False}, '/mnt/cache1': {'sip': '75', 'DIR_ONLY': False},
                '/mnt/cache2': {'sip': '75', 'DIR_ONLY': False}, '/mnt/cache3': {'sip': '100', 'DIR_ONLY': False},
                '/mnt/cache4': {'sip': '100', 'DIR_ONLY': False}, '/mnt/cache5': {'sip': '100', 'DIR_ONLY': False},
                '/mnt/cache6': {'sip': '100', 'DIR_ONLY': False}, '/mnt/cache7': {'sip': '100', 'DIR_ONLY': False},
                '/mnt/db': {'sip': '25', 'DIR_ONLY': False}, '/mnt/md': {'sip': '25', 'DIR_ONLY': False},
                '/var/tmp': {'sip': '100', 'DIR_ONLY': False}},
        '3-6': {'/var/tmp': {'sip': '100', 'DIR_ONLY': False}, '/mnt/db': {'sip': '25', 'DIR_ONLY': False},
                '/mnt/md': {'sip': '25', 'DIR_ONLY': False}, '/mnt/bfs': {'sip': '100', 'DIR_ONLY': False},
                '/mnt/cache1': {'sip': '75', 'DIR_ONLY': False}, '/mnt/cache2': {'sip': '75', 'DIR_ONLY': False},
                '/mnt/cache3': {'sip': '100', 'DIR_ONLY': False}, '/mnt/cache4': {'sip': '100', 'DIR_ONLY': False},
                '/mnt/cache5': {'sip': '100', 'DIR_ONLY': False}, '/mnt/cache6': {'sip': '100', 'DIR_ONLY': False}},
        '3-5': {'/var/tmp': {'sip': '100', 'DIR_ONLY': False}, '/mnt/db': {'sip': '25', 'DIR_ONLY': False},
                '/mnt/md': {'sip': '25', 'DIR_ONLY': False}, '/mnt/bfs': {'sip': '100', 'DIR_ONLY': False},
                '/mnt/cache1': {'sip': '75', 'DIR_ONLY': False}, '/mnt/cache2': {'sip': '75', 'DIR_ONLY': False},
                '/mnt/cache3': {'sip': '100', 'DIR_ONLY': False}, '/mnt/cache4': {'sip': '100', 'DIR_ONLY': False},
                '/mnt/cache5': {'sip': '100', 'DIR_ONLY': False}},
        '3-4': {'/var/tmp': {'sip': '100', 'DIR_ONLY': False}, '/mnt/db': {'sip': '25', 'DIR_ONLY': False},
                '/mnt/md': {'sip': '25', 'DIR_ONLY': False}, '/mnt/bfs': {'sip': '100', 'DIR_ONLY': False},
                '/mnt/cache1': {'sip': '75', 'DIR_ONLY': False}, '/mnt/cache2': {'sip': '75', 'DIR_ONLY': False},
                '/mnt/cache3': {'sip': '100', 'DIR_ONLY': False}, '/mnt/cache4': {'sip': '100', 'DIR_ONLY': False}},
        '3-3': {'/mnt/bfs': {'sip': '100', 'DIR_ONLY': False}, '/mnt/cache1': {'sip': '75', 'DIR_ONLY': False},
                '/mnt/cache2': {'sip': '75', 'DIR_ONLY': False}, '/mnt/cache3': {'sip': '100', 'DIR_ONLY': False},
                '/mnt/db': {'sip': '25', 'DIR_ONLY': False}, '/mnt/md': {'sip': '25', 'DIR_ONLY': False},
                '/var/tmp': {'sip': '100', 'DIR_ONLY': False}},
        '3-2': {'/mnt/bfs': {'sip': '100', 'DIR_ONLY': False}, '/mnt/cache1': {'sip': '75', 'DIR_ONLY': False},
                '/mnt/cache2': {'sip': '75', 'DIR_ONLY': False}, '/mnt/db': {'sip': '25', 'DIR_ONLY': False},
                '/mnt/md': {'sip': '25', 'DIR_ONLY': False}, '/var/tmp': {'sip': '100', 'DIR_ONLY': False}},
        '3-1': {'/mnt/bfs': {'sip': '100', 'DIR_ONLY': False}, '/mnt/cache1': {'sip': '50', 'DIR_ONLY': False},
                '/var/tmp': {'sip': '100', 'DIR_ONLY': False}, '/mnt/db': {'sip': '25', 'DIR_ONLY': False},
                '/mnt/md': {'sip': '25', 'DIR_ONLY': False}},
        '3-0': {'/mnt/bfs': {'sip': '100', 'DIR_ONLY': False}, '/mnt/cache1': {'sip': 'NA', 'DIR_ONLY': True},
                '/var/tmp': {'sip': '100', 'DIR_ONLY': False}, '/mnt/db': {'sip': 'NA', 'DIR_ONLY': True},
                '/mnt/md': {'sip': 'NA', 'DIR_ONLY': True}},
        '11-6': {'/var/tmp': {'sip': '100', 'DIR_ONLY': False}, '/mnt/db': {'sip': '25', 'DIR_ONLY': False},
                 '/mnt/md': {'sip': '25', 'DIR_ONLY': False}, '/mnt/bfs': {'sip': '100', 'DIR_ONLY': False},
                 '/mnt/cache1': {'sip': '75', 'DIR_ONLY': False}, '/mnt/cache2': {'sip': '75', 'DIR_ONLY': False},
                 '/mnt/cache3': {'sip': '100', 'DIR_ONLY': False}, '/mnt/cache4': {'sip': '100', 'DIR_ONLY': False},
                 '/mnt/cache5': {'sip': '100', 'DIR_ONLY': False}, '/mnt/cache6': {'sip': '100', 'DIR_ONLY': False}},
        '2-10': {'/mnt/cache8': {'sip': '100', 'DIR_ONLY': False}, '/mnt/cache9': {'sip': '100', 'DIR_ONLY': False},
                 '/mnt/bfs': {'sip': '100', 'DIR_ONLY': False}, '/mnt/cache1': {'sip': '75', 'DIR_ONLY': False},
                 '/mnt/cache2': {'sip': '75', 'DIR_ONLY': False}, '/mnt/cache3': {'sip': '100', 'DIR_ONLY': False},
                 '/mnt/cache4': {'sip': '100', 'DIR_ONLY': False}, '/mnt/cache5': {'sip': '100', 'DIR_ONLY': False},
                 '/mnt/cache6': {'sip': '100', 'DIR_ONLY': False}, '/mnt/cache7': {'sip': '100', 'DIR_ONLY': False},
                 '/mnt/db': {'sip': '25', 'DIR_ONLY': False}, '/mnt/md': {'sip': '25', 'DIR_ONLY': False},
                 '/var/tmp': {'sip': '100', 'DIR_ONLY': False}, '/mnt/cache10': {'sip': '100', 'DIR_ONLY': False}},
        '2-11': {'/mnt/cache8': {'sip': '100', 'DIR_ONLY': False}, '/mnt/cache9': {'sip': '100', 'DIR_ONLY': False},
                 '/mnt/bfs': {'sip': '100', 'DIR_ONLY': False}, '/mnt/cache1': {'sip': '75', 'DIR_ONLY': False},
                 '/mnt/cache2': {'sip': '75', 'DIR_ONLY': False}, '/mnt/cache3': {'sip': '100', 'DIR_ONLY': False},
                 '/mnt/cache4': {'sip': '100', 'DIR_ONLY': False}, '/mnt/cache5': {'sip': '100', 'DIR_ONLY': False},
                 '/mnt/cache6': {'sip': '100', 'DIR_ONLY': False}, '/mnt/cache7': {'sip': '100', 'DIR_ONLY': False},
                 '/mnt/db': {'sip': '25', 'DIR_ONLY': False}, '/mnt/md': {'sip': '25', 'DIR_ONLY': False},
                 '/var/tmp': {'sip': '100', 'DIR_ONLY': False}, '/mnt/cache10': {'sip': '100', 'DIR_ONLY': False},
                 '/mnt/cache11': {'sip': '100', 'DIR_ONLY': False}},
        '2-12': {'/mnt/cache8': {'sip': '100', 'DIR_ONLY': False}, '/mnt/cache9': {'sip': '100', 'DIR_ONLY': False},
                 '/mnt/bfs': {'sip': '100', 'DIR_ONLY': False}, '/mnt/cache1': {'sip': '75', 'DIR_ONLY': False},
                 '/mnt/cache2': {'sip': '75', 'DIR_ONLY': False}, '/mnt/cache3': {'sip': '100', 'DIR_ONLY': False},
                 '/mnt/cache4': {'sip': '100', 'DIR_ONLY': False}, '/mnt/cache5': {'sip': '100', 'DIR_ONLY': False},
                 '/mnt/cache6': {'sip': '100', 'DIR_ONLY': False}, '/mnt/cache7': {'sip': '100', 'DIR_ONLY': False},
                 '/var/tmp': {'sip': '100', 'DIR_ONLY': False}, '/mnt/db': {'sip': '25', 'DIR_ONLY': False},
                 '/mnt/md': {'sip': '25', 'DIR_ONLY': False}, '/mnt/cache12': {'sip': '100', 'DIR_ONLY': False},
                 '/mnt/cache10': {'sip': '100', 'DIR_ONLY': False}, '/mnt/cache11': {'sip': '100', 'DIR_ONLY': False}},
        '2-13': {'/mnt/cache8': {'sip': '100', 'DIR_ONLY': False}, '/mnt/cache9': {'sip': '100', 'DIR_ONLY': False},
                 '/mnt/bfs': {'sip': '100', 'DIR_ONLY': False}, '/mnt/cache1': {'sip': '75', 'DIR_ONLY': False},
                 '/mnt/cache2': {'sip': '75', 'DIR_ONLY': False}, '/mnt/cache3': {'sip': '100', 'DIR_ONLY': False},
                 '/mnt/cache4': {'sip': '100', 'DIR_ONLY': False}, '/mnt/cache5': {'sip': '100', 'DIR_ONLY': False},
                 '/mnt/cache6': {'sip': '100', 'DIR_ONLY': False}, '/mnt/cache7': {'sip': '100', 'DIR_ONLY': False},
                 '/var/tmp': {'sip': '100', 'DIR_ONLY': False}, '/mnt/db': {'sip': '25', 'DIR_ONLY': False},
                 '/mnt/md': {'sip': '25', 'DIR_ONLY': False}, '/mnt/cache12': {'sip': '100', 'DIR_ONLY': False},
                 '/mnt/cache13': {'sip': '100', 'DIR_ONLY': False}, '/mnt/cache10': {'sip': '100', 'DIR_ONLY': False},
                 '/mnt/cache11': {'sip': '100', 'DIR_ONLY': False}}}

    hdd_defaults = {'boot_device': False, 'model': 'Virtual_disk', 'size': 10000000000.0, 'software_raid': False,
                    'type': 'disk'}
    ssd_defaults = {'boot_device': False, 'model': 'Virtual_disk', 'size': 2000000000.0, 'software_raid': False,
                    'type': 'ssd'}

    sata_map = ['sda', 'sdb', 'sdc', 'sdd', 'sde', 'sdf', 'sdg', 'sdh', 'sdi', 'sdj', 'sdk', 'sdl', 'sdm']
    ssd_map = ['sdn', 'sdo', 'sdp', 'sdq', 'sdr', 'sds', 'sdt', 'sdu', 'sdv', 'sdw', 'sdx', 'sdy', 'sdz']

    DEBUG = False

    @classmethod
    def setUpClass(cls):
        """
        Sets up the unittest, mocking a certain set of 3rd party libraries and extensions.
        This makes sure the unittests can be executed without those libraries installed
        """
        global client
        global sc

        client = SSHClient.load('127.0.0.1', 'rooter')
        sc = SetupController()

    @classmethod
    def setUp(cls):
        """
        (Re)Sets the stores on every test
        """
        pass

    @classmethod
    def tearDownClass(cls):
        """
        Clean up the unittest
        """
        pass

    @staticmethod
    def get_disk_config(hdds, ssds):
        disk_config = dict()
        for hdd in xrange(0, hdds):
            disk_config[PartitionLayout.sata_map[hdd]] = PartitionLayout.hdd_defaults
        for ssd in xrange(0, ssds):
            disk_config[PartitionLayout.ssd_map[ssd]] = PartitionLayout.ssd_defaults
        return disk_config

    @staticmethod
    def show_layout(proposed, disks):
        device_size_map = dict()
        for key, values in disks.iteritems():
            device_size_map['/dev/' + key] = values['size']

        keys = proposed.keys()
        keys.sort()
        key_map = list()
        for mp in keys:
            sub_keys = proposed[mp].keys()
            sub_keys.sort()
            mp_values = ''
            if not proposed[mp]['device'] or proposed[mp]['device'] in ['DIR_ONLY']:
                mp_values = ' {0} : {1:20}'.format('device', 'DIR_ONLY')
                print "{0:20} : {1}".format(mp, mp_values)
                key_map.append(mp)
                continue

            for sub_key in sub_keys:
                value = str(proposed[mp][sub_key])
                if sub_key == 'device' and value and value != 'DIR_ONLY':
                    size = device_size_map[value]
                    size_in_gb = int(size / 1000.0 / 1000.0 / 1000.0)
                    value = value + ' ({0} GB)'.format(size_in_gb)
                if sub_key in ['device']:
                    mp_values = mp_values + ' {0} : {1:20}'.format(sub_key, value)
                elif sub_key in ['label']:
                    mp_values = mp_values + ' {0} : {1:10}'.format(sub_key, value)
                else:
                    mp_values = mp_values + ' {0} : {1:5}'.format(sub_key, value)

            print "{0:20} : {1}".format(mp, mp_values)
            key_map.append(mp)

    def validate(self, layout, nr_of_hdds, nr_of_ssds):
        # key = nr_of_hdds + '-' + nr_of_ssds
        # value = (True|False, percentage)
        key = str(nr_of_hdds) + '-' + str(nr_of_ssds)
        actual_layout = eval(layout).values()[0]
        expected_layout = self.full_map[key]

        if not assert_equal(actual_layout, expected_layout):
            return True
        else:
            if self.DEBUG:
                print 'Actual layout:   {0}'.format(actual_layout)
                print 'Expected layout: {0}'.format(expected_layout)
                print diff(actual_layout, expected_layout)
            return False

    def test_partition_layout_generation(self):
        full_matrix = dict()
        valid = True
        for nr_of_hdds in xrange(0, 3):
            for nr_of_ssds in xrange(0, 3):
                disk_config = self.get_disk_config(nr_of_hdds, nr_of_ssds)
                layout, skipped = sc._generate_default_partition_layout(disk_config)

                if self.DEBUG:
                    print "Disk config: {0} hdd(s) - {1} ssd(s)".format(nr_of_hdds, nr_of_ssds)
                    print "Proposed disk layout"
                    self.show_layout(layout, disk_config)
                    print layout
                matrix = '{' + "'{0}-{1}':".format(nr_of_hdds, nr_of_ssds) + '{'
                keys = layout.keys()
                keys.sort()
                print
                for key in keys:
                    matrix += "'{0}':".format(key) + '{'
                    dir_only = layout[key]['device'] == 'DIR_ONLY'
                    matrix += "'DIR_ONLY':{0},'sip':'{1}',".format(dir_only,
                                                                   layout[key]['percentage'] if not dir_only else 'NA')
                    matrix += "},"
                matrix += "},}"

                if self.DEBUG:
                    full_matrix.update(eval(matrix))
                if not self.validate(matrix, nr_of_hdds, nr_of_ssds):
                    valid = False

        if self.DEBUG:
            print full_matrix

        self.assertTrue(valid, 'At least one generated config failed!')


    def test_interactive_menu(self):
        # Use a known config - and process expected menu structure

        def get_formated_lines(dl):
            fl = {}
            for k, v in dl.items():
                fl[k] = r"{0}\s*:\s*device\s*:\s*{1}.*label\s*:\s*{2}\s*percentage\s*:\s*{3}".format(k, v['device'], v['label'], v['percentage'])
            return fl

        def check_partition_layout_table(formated_lines):
            idxs = []
            for _ in range(len(formated_lines)):
                idxs.append(child.expect(formated_lines))

            assert len(set(idxs)) == len(formated_lines), "Proposed partition layout did not contain all expected lines."

        def pick_option(child, opt_name, fail_if_not_found = True):
            opt = [l for l in child.buffer.splitlines() if opt_name in l]
            assert opt or not fail_if_not_found, "Option {0} not found\n{1}".format(opt_name, child.before)
            if opt:
                opt = opt[0].split(":")[0].strip()
                child.sendline(opt)
            return bool(opt)


        disk_layout = ({'/mnt/bfs': {'device': '/dev/sdd', 'label': 'backendfs', 'percentage': 80},
                        '/mnt/cache1': {'device': '/dev/sdb', 'label': 'cache1', 'percentage': 50},
                        '/mnt/db': {'device': '/dev/sdb', 'label': 'db', 'percentage': 25},
                        '/mnt/md': {'device': '/dev/sdb', 'label': 'mdpath', 'percentage': 25},
                        '/var/tmp': {'device': '/dev/sdd', 'label': 'tempfs', 'percentage': 21}},
                       set(['sda', 'sdc']))

        child = pexpect.spawn("ovs")
        child.timeout = 300
        child.logfile = sys.stdout

        child.expect(":")

        child.sendline("from ovs.lib.setup import SetupController")
        child.sendline("from ovs.extensions.generic.sshclient import SSHClient")
        child.expect(":")

        child.sendline("client = SSHClient.load('127.0.0.1', 'rooter')")
        child.expect(":")

        child.sendline("sc = SetupController()")
        child.expect(":")

        child.sendline("disk_layout = " + str(disk_layout))
        child.expect(":")

        child.sendline("sc.apply_flexible_disk_layout(client, False, disk_layout[0])")
        child.expect("Proposed partition layout:")

        formated_lines = get_formated_lines(disk_layout[0])
        check_partition_layout_table(formated_lines.values())

        child.expect("Enter number or name; return for next page")

        #0: Add
        child.sendline("0")
        new_mountpoint = {'/mnt/cache2':  {'device'     : '/dev/sdc',
                                           'label'      : 'cache2',
                                           'percentage' : '50'}}
        child.expect("Enter mountpoint to add")
        child.sendline(new_mountpoint.keys()[0])
        check_partition_layout_table(formated_lines.values() + [new_mountpoint.keys()[0] + r"\s*:\s*device\s*:\s*DIR_ONLY"])

        #2: Update
        child.expect("Enter number or name; return for next page")
        child.sendline("2")
        child.expect("Choose mountpoint to update:")
        pick_option(child, new_mountpoint.keys()[0])

        update_dict = new_mountpoint[new_mountpoint.keys()[0]].copy()
        update_dict.update({'mountpoint':'/mnt/cache3'})

        child.expect("Make a choice")
        for opt in ["device", "label", "percentage", "mountpoint"]:
            child.expect("Make a choice")
            pick_option(child, opt)
            child.sendline(update_dict[opt])

        pick_option(child, "finish")

        disk_layout[0][update_dict['mountpoint']] = new_mountpoint[new_mountpoint.keys()[0]].copy()
        formated_lines = get_formated_lines(disk_layout[0])
        check_partition_layout_table(formated_lines.values())

        #3 Print
        child.expect("Enter number or name; return for next page")
        child.sendline("3")
        check_partition_layout_table(formated_lines.values())

        #1 Remove
        child.expect("Enter number or name; return for next page")
        child.sendline("1")
        child.expect("Enter mountpoint to remove")
        child.sendline(update_dict['mountpoint'])
        del disk_layout[0][update_dict['mountpoint']]
        formated_lines = get_formated_lines(disk_layout[0])
        check_partition_layout_table(formated_lines.values())

        #5 Quit
        child.expect("Enter number or name; return for next page")
        child.sendline("5")
        child.expect(":")

        child.kill(9)

if __name__ == '__main__':
    suite = unittest.TestLoader().loadTestsFromTestCase(PartitionLayout)
    unittest.TextTestRunner().run(suite)
