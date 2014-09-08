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
OVS Cinder Plugin autotests configuration module
"""

#This node # to do get automatic / 127.0.0.1
IP = '10.130.10.201'

#fix issue with add_vpool becoming interactive
import fabric.auth
fabric.auth.set_password('root', IP, 22, 'rooter') #needs root password in plaintext

PROCESS='screen' #on openstack it is service

#AUTH
CINDER_USER = 'admin'
CINDER_PASS = 'rooter'
TENANT_NAME = 'admin'
AUTH_URL = 'http://%s:35357/v2.0' % IP

#VPOOL
VPOOL_CLEANUP = False #should the vpool be removed during tearDownClass
VPOOL_NAME = 'local' #string, lowercase no strange characters
VPOOL_MOUNTPOINT = '/mnt/%s' % VPOOL_NAME
VPOOL_ROOT_DIR = '/mnt'
VPOOL_BFS = '%s/bfs' % VPOOL_ROOT_DIR
VPOOL_TEMP = '%s/temp' % VPOOL_ROOT_DIR
VPOOL_MD = '%s/md' % VPOOL_ROOT_DIR
VPOOL_CACHE = '%s/cache' % VPOOL_ROOT_DIR
VPOOL_PORT = 12326 #make sure it is available (!) Todo: get highest available

FILE_TYPE = 'raw' #should not be changed unless volumedriver changes
VOLUME_TYPE = 'ovs'
VOLUME_NAME = 'ovs-volume-%s' # last part will be uuid
VOLUME_SIZE = 2
SNAP_NAME = 'ovs-volume-snapshot-%s' # last part will be uuid
CLONE_NAME = 'ovs-clone-%s' # last part will be uuid
MOUNT_LOCATION = '%s/testmount' % VPOOL_ROOT_DIR

#IMAGE TEST
IMAGE_NAME = 'Fedora-x86_64-20-20140618-sda'
UPLOAD_IMAGE_NAME = 'Upload-OVS-image-%s' # last part will be uuid

