# Copyright 2014 CloudFounders NV
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""
KVM XML watcher
"""

from ovs.extensions.generic.system import Ovs
from xml.etree import ElementTree
from ovs.log.logHandler import LogHandler

import glob
import pyinotify
import os
import re
import shutil

logger = LogHandler('ovs.extensions', name='xml processor')


class Kxp(pyinotify.ProcessEvent):

    def __init__(self):
        """
        dummy constructor
        """
        pass

    def _recurse(self, treeitem):
        result = {}
        for key, item in treeitem.items():
            result[key] = item
        for child in treeitem.getchildren():
            result[child.tag] = self._recurse(child)
            for key, item in child.items():
                result[child.tag][key] = item
        return result

    def is_valid_regular_file(self, pathname):
        return os.path.isfile(pathname) and \
            not os.path.islink(pathname) and \
            pathname.endswith('.xml')

    def is_valid_symlink(self, pathname):
        return os.path.isfile(pathname) and \
            os.path.islink(pathname) and \
            pathname.endswith('.xml')

    def get_vpool_for_vm(self, pathname):
        regex = '/mnt/([^/]+)/(.+$)'
        try:
            tree = ElementTree.parse(pathname)
        except ElementTree.ParseError:
            return ''
        disks = [self._recurse(item) for item in tree.findall("devices/disk")]
        for disk in disks:
            if disk['device'] == 'disk':
                match = re.search(regex, disk['source']['file'])
                if match:
                    return match.group(1)
        return ''

    def process_IN_CLOSE_WRITE(self, event):

        logger.debug('path: {0} - name: {1} - close after write'.format(event.path, event.name))

    def process_IN_CREATE(self, event):

        logger.debug('path: {0} - name: {1} - create'.format(event.path, event.name))

    def process_IN_DELETE(self, event):

        logger.debug('path: {0} - name: {1} - deleted'.format(event.path, event.name))
        file_matcher = '/mnt/*/{0}/{1}'.format(Ovs.get_my_machine_id(), event.name)
        for found_file in glob.glob(file_matcher):
            if os.path.exists(found_file) and os.path.isfile(found_file):
                os.remove(found_file)
                logger.info('File on vpool deleted: {0}'.format(found_file))

    def process_IN_MODIFY(self, event):

        logger.debug('path: {0} - name: {1} - modified'.format(event.path, event.name))

    def process_IN_MOVED_FROM(self, event):

        logger.debug('path: {0} - name: {1} - moved from'.format(event.path, event.name))

    def process_IN_MOVED_TO(self, event):
        """
        Trigger to move vm.xml to matching vpool
        """
        logger.debug('path: {0} - name: {1} - moved to'.format(event.path, event.name))
        vpool_path = '/mnt/' + self.get_vpool_for_vm(event.pathname)
        if vpool_path == '/mnt/':
            logger.warning('Vmachine not on vpool or invalid xml format for {0}'.format(event.pathname))

        if os.path.exists(vpool_path):
            machine_id = Ovs.get_my_machine_id()
            target_path = vpool_path + '/' + machine_id + '/'
            target_xml = target_path + event.name
            if not os.path.exists(target_path):
                os.mkdir(target_path)
            shutil.copy2(event.pathname, target_xml)

    def process_IN_UNMOUNT(self, event):
        """
        Trigger to cleanup vm on target
        """
        logger.debug('path: {0} - name: {1} - fs unmounted!'.format(event.path, event.name))

    def process_default(self, event):

        logger.debug('path: {0} - name: {1} - default'.format(event.path, event.name))
