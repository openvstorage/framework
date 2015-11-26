# Copyright 2014 iNuron NV
#
# Licensed under the Open vStorage Modified Apache License (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.openvstorage.org/license
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""
OVS Cinder Plugin autotests configuration module
"""
from ovs.dal.lists.storagerouterlist import StorageRouterList

# This node
with open('/etc/hostname') as hostname_file:
    hostname = hostname_file.read().strip()
IP = [storagerouter for storagerouter in StorageRouterList.get_storagerouters() if storagerouter.name == hostname][0].ip

PROCESS = 'screen'  # on openstack it is service
SHELL_DEBUG = True  # enable shell client debug

# AUTH
CINDER_USER = 'admin'
CINDER_PASS = 'rooter'
TENANT_NAME = 'admin'
CINDER_CONTROLLER = IP  # if the controller is on another node
AUTH_URL = 'http://%s:35357/v2.0'.format(CINDER_CONTROLLER)

# VPOOL
VPOOL_CLEANUP = False  # should the vpool be removed during tearDownClass
VPOOL_NAME = 'local'  # string, lowercase no strange characters
VPOOL_MOUNTPOINT = '/mnt/{0}'.format(VPOOL_NAME)
VPOOL_ROOT_DIR = '/mnt'
VPOOL_PORT = 12326  # make sure it is available (!) Todo: get highest available

FILE_TYPE = 'raw'  # should not be changed unless volumedriver changes
VOLUME_TYPE = 'local'  # depends on volume-type created
VOLUME_NAME = 'ovs-volume-{0}'  # last part will be uuid
VOLUME_SIZE = 2
SNAP_NAME = 'ovs-volume-snapshot-{0}'  # last part will be uuid
CLONE_NAME = 'ovs-clone-{0}'  # last part will be uuid
MOUNT_LOCATION = '{0}/testmount'.format(VPOOL_ROOT_DIR)

# IMAGE TEST
IMAGE_NAME = 'Fedora-x86_64-20-20140618-sda'
UPLOAD_IMAGE_NAME = 'Upload-OVS-image-{0}'  # last part will be uuid
