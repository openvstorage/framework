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
KVM XML watcher
"""
import os
import re
import glob
import shutil
import pyinotify
from ovs.dal.lists.vmachinelist import VMachineList
from ovs.extensions.generic.system import System
from ovs.log.log_handler import LogHandler
from xml.etree import ElementTree


class Kxp(pyinotify.ProcessEvent):

    def __init__(self):
        """
        Constructor
        """
        self._logger = LogHandler.get('extensions', name='xml processor')

    def _recurse(self, treeitem):
        result = {}
        for key, item in treeitem.items():
            result[key] = item
        for child in treeitem.getchildren():
            result[child.tag] = self._recurse(child)
            for key, item in child.items():
                result[child.tag][key] = item
        return result

    def _is_etc_watcher(self, pathname):
        return '/etc/libvirt/qemu' in pathname

    def _is_run_watcher(self, pathname):
        return '/run/libvirt/qemu' in pathname

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
                if 'file' in disk['source']:
                    match = re.search(regex, disk['source']['file'])
                elif 'dev' in disk['source']:
                    match = re.search(regex, disk['source']['dev'])
                else:
                    match = None
                if match:
                    return match.group(1)
        return ''

    def invalidate_vmachine_status(self, name):
        if not name.endswith('.xml'):
            return
        devicename = '{0}/{1}'.format(System.get_my_machine_id(), name)
        vm = VMachineList().get_by_devicename_and_vpool(devicename, None)
        if vm:
            vm.invalidate_dynamics()
            self._logger.debug('Hypervisor status invalidated for: {0}'.format(name))

    def process_IN_CLOSE_WRITE(self, event):

        self._logger.debug('path: {0} - name: {1} - close after write'.format(event.path, event.name))

    def process_IN_CREATE(self, event):

        self._logger.debug('path: {0} - name: {1} - create'.format(event.path, event.name))

    def process_IN_DELETE(self, event):

        try:
            self._logger.debug('path: {0} - name: {1} - deleted'.format(event.path, event.name))
            if self._is_etc_watcher(event.path):
                file_matcher = '/mnt/*/{0}/{1}'.format(System.get_my_machine_id(), event.name)
                for found_file in glob.glob(file_matcher):
                    if os.path.exists(found_file) and os.path.isfile(found_file):
                        os.remove(found_file)
                        self._logger.info('File on vpool deleted: {0}'.format(found_file))

            if self._is_run_watcher(event.path):
                self.invalidate_vmachine_status(event.name)
        except Exception as exception:
            self._logger.error('Exception during process_IN_DELETE: {0}'.format(str(exception)), print_msg=True)

    def process_IN_MODIFY(self, event):

        self._logger.debug('path: {0} - name: {1} - modified'.format(event.path, event.name))

    def process_IN_MOVED_FROM(self, event):

        self._logger.debug('path: {0} - name: {1} - moved from'.format(event.path, event.name))

    def process_IN_MOVED_TO(self, event):

        try:
            self._logger.debug('path: {0} - name: {1} - moved to'.format(event.path, event.name))

            if self._is_run_watcher(event.path):
                self.invalidate_vmachine_status(event.name)
                return

            vpool_path = '/mnt/' + self.get_vpool_for_vm(event.pathname)
            if vpool_path == '/mnt/':
                self._logger.warning('Vmachine not on vpool or invalid xml format for {0}'.format(event.pathname))

            if os.path.exists(vpool_path):
                machine_id = System.get_my_machine_id()
                target_path = vpool_path + '/' + machine_id + '/'
                target_xml = target_path + event.name
                if not os.path.exists(target_path):
                    os.mkdir(target_path)
                shutil.copy2(event.pathname, target_xml)
        except Exception as exception:
            self._logger.error('Exception during process_IN_MOVED_TO: {0}'.format(str(exception)), print_msg=True)

    def process_IN_UNMOUNT(self, event):
        """
        Trigger to cleanup vm on target
        """
        self._logger.debug('path: {0} - name: {1} - fs unmounted!'.format(event.path, event.name))

    def process_default(self, event):

        self._logger.debug('path: {0} - name: {1} - default'.format(event.path, event.name))
