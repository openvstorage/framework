from ovs.extensions.generic.system import Ovs
from xml.etree import ElementTree

import glob
import pyinotify
import os
import re
import shutil


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

        print 'path: {0} - name: {1} - close after write'.format(event.path, event.name)

    def process_IN_CREATE(self, event):

        print 'path: {0} - name: {1} - create'.format(event.path, event.name)

    def process_IN_DELETE(self, event):

        print 'path: {0} - name: {1} - deleted'.format(event.path, event.name)
        file_matcher = '/mnt/*/{0}/{1}'.format(
            Ovs.get_my_machine_id(), event.name)
        for file in glob.glob(file_matcher):
            if os.path.exists(file) and os.path.isfile(file):
                os.remove(file)
                print 'File on vpool deleted: {0}'.format(file)

    def process_IN_MODIFY(self, event):

        print 'path: {0} - name: {1} - modified'.format(event.path, event.name)

    def process_IN_MOVED_FROM(self, event):

        print 'path: {0} - name: {1} - moved from'.format(event.path, event.name)

    def process_IN_MOVED_TO(self, event):
        """
        Trigger to move vm.xml to matching vpool
        """
        print 'path: {0} - name: {1} - moved to'.format(event.path, event.name)
        vpool_path = '/mnt/' + self.get_vpool_for_vm(event.pathname)
        if vpool_path == '/mnt/':
            print 'Vmachine not on vpool or invalid xml format for {0}'.format(event.pathname)

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
        print 'path: {0} - name: {1} - fs unmounted!'.format(event.path, event.name)

    def process_default(self, event):

        print 'path: {0} - name: {1} - default'.format(event.path, event.name)
