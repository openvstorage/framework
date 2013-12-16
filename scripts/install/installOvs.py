#!/usr/bin/env python
import os
from optparse import OptionParser

parser = OptionParser(description='CloudFrames vRun Setup')
parser.add_option('--no-filesystems', dest='filesystems', action="store_false", default=True,
                  help="Don't create partitions and filesystems")
(options, args) = parser.parse_args()

if options.filesystems:
    # Create partitions on HDD
    os.system('parted /dev/sdb -s mklabel gpt')
    os.system('parted /dev/sdb -s mkpart backendfs 2MB 90%')
    os.system('parted /dev/sdb -s mkpart distribfs 90% 100%')
    os.system('mkfs.ext4 /dev/sdb1 -L backendfs')
    os.system('mkfs.ext4 /dev/sdb2 -L distribfs')
    
    #Create partitions on SSD
    os.system('parted /dev/sdc -s mklabel gpt')
    os.system('parted /dev/sdc -s mkpart cache 2MB 50%')
    os.system('parted /dev/sdc -s mkpart db 50% 75%')
    os.system('parted /dev/sdc -s mkpart mdpath 75% 100%')
    os.system('mkfs.ext4 /dev/sdc1 -L cache')
    os.system('mkfs.ext4 /dev/sdc2 -L db')
    os.system('mkfs.ext4 /dev/sdc3 -L mdpath')
    os.system('mkdir /mnt/db')
    os.system('mkdir /mnt/cache')
    os.system('mkdir /mnt/md')
    os.system('mkdir /mnt/bfs')
    os.system('mkdir /mnt/dfs')
    
    # Add content to fstab
    fstab_content = """
    LABEL=db        /mnt/db    ext4    defaults,nobootwait,noatime,discard    0    2
    LABEL=cache     /mnt/cache ext4    defaults,nobootwait,noatime,discard    0    2
    LABEL=mdpath    /mnt/md    ext4    defaults,nobootwait,noatime,discard    0    2
    LABEL=backendfs /mnt/bfs   ext4    defaults,nobootwait,noatime,discard    0    2
    LABEL=distribfs /mnt/dfs   ext4    defaults,nobootwait,noatime,discard    0    2
    """
    fstab = open('/etc/fstab', 'a')
    fstab.write(fstab_content)
    fstab.close()

# Mount all filesystems
os.system('mountall')

# Install all software components
os.system('apt-get install python-pip')
os.system('pip install https://bitbucket.org/jumpscale/jumpscale_core/get/default.zip')
os.system('jpackage_update')

blob_user = raw_input('OpenvStorage blob username: ')
blob_password = raw_input('OpenvStorage blob password: ')
jp_openvstorage_blobstor = """
[jp_openvstorage]
ftp = ftp://{}:{}@10.100.129.101
http = http://10.100.129.101/ovs-blobstore
namespace = jpackages
localpath =
type = httpftp
""".format(blob_user, blob_password)

jp_openvstorage_repo = """
[openvstorage]
metadatafromtgz = 0
qualitylevel = unstable
metadatadownload = 
metadataupload = 
bitbucketaccount = openvstorage
bitbucketreponame = jp_openvstorage
blobstorremote = jp_openvstorage
blobstorlocal = jpackages_local
"""

blobstor_config = open('/opt/jumpscale/cfg/jsconfig/blobstor.cfg', 'a')
blobstor_config.write(jp_openvstorage_blobstor)
blobstor_config.close()

jp_sources_config = open('/opt/jumpscale/cfg/jpackages/sources.cfg', 'a')
jp_sources_config.write(jp_openvstorage_repo)
jp_sources_config.close()

os.system('jpackage_update')
os.system('jpackage_install -n core')
os.system('jpackage_install -n openvstorage')

