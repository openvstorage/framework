#!/usr/bin/env python
import os
import sys
from subprocess import call
from optparse import OptionParser


def run_command(command, fail=True):
    rcode = call(command.split(' '))
    if rcode != 0 and fail:
        raise Exception('{0} failed with return value {1}'.format(command, rcode))


if os.getegid() != 0:
    print 'This script should be executed as a user in the root group.'
    sys.exit(1)

parser = OptionParser(description='Open vStorage Setup')
parser.add_option('-n', '--no-filesystems', dest='filesystems', action="store_false", default=True,
                  help="Don't create partitions and filesystems")
parser.add_option('-c', '--clean', dest='clean', action="store_true", default=False,
                  help="Try to clean environment before reinstalling")
(options, args) = parser.parse_args()

if options.clean:
    print 'Trying to clean previous install...'
    run_command('service nfs-kernel-server stop', fail=False)
    run_command('pkill arakoon', fail=False)
    run_command('rm -rf /usr/local/lib/python2.7/*-packages/JumpScale*', fail=False)
    run_command('rm -rf /usr/local/lib/python2.7/dist-packages/jumpscale.pth', fail=False)
    run_command('rm -rf /opt/jumpscale', fail=False)
    run_command('rm -rf /opt/OpenvStorage', fail=False)
    run_command('rm -rf /mnt/db/arakoon /mnt/db/tlogs /mnt/cache/foc /mnt/cache/sco /mnt/cache/read', fail=False)

if options.filesystems:
    print 'Creating filesystems...'
    # Create partitions on HDD
    print '  On HDD...'
    run_command('umount /dev/sdb1', fail=False)
    run_command('umount /dev/sdb2', fail=False)
    run_command('umount /dev/sdb3', fail=False)
    run_command('parted /dev/sdb -s mklabel gpt')
    run_command('parted /dev/sdb -s mkpart backendfs 2MB 80%')
    run_command('parted /dev/sdb -s mkpart distribfs 80% 90%')
    run_command('parted /dev/sdb -s mkpart tempfs 90% 100%')
    run_command('mkfs.ext4 -q /dev/sdb1 -L backendfs')
    run_command('mkfs.ext4 -q /dev/sdb2 -L distribfs')
    run_command('mkfs.ext4 -q /dev/sdb3 -L tempfs')
    run_command('mkdir -p /mnt/bfs')
    run_command('mkdir -p /mnt/dfs')
    run_command('mkdir -p /var/tmp')

    # Create partitions on SSD
    print '  On SSD...'
    run_command('umount /dev/sdc1', fail=False)
    run_command('umount /dev/sdc2', fail=False)
    run_command('umount /dev/sdc3', fail=False)
    run_command('parted /dev/sdc -s mklabel gpt')
    run_command('parted /dev/sdc -s mkpart cache 2MB 50%')
    run_command('parted /dev/sdc -s mkpart db 50% 75%')
    run_command('parted /dev/sdc -s mkpart mdpath 75% 100%')
    run_command('mkfs.ext4 -q /dev/sdc1 -L cache')
    run_command('mkfs.ext4 -q /dev/sdc2 -L db')
    run_command('mkfs.ext4 -q /dev/sdc3 -L mdpath')
    run_command('mkdir -p /mnt/db')
    run_command('mkdir -p /mnt/cache')
    run_command('mkdir -p /mnt/md')

    # Add content to fstab
    print '  Updating /etc/fstab...'
    fstab_content = """
# BEGIN Open vStorage
LABEL=db        /mnt/db    ext4    defaults,nobootwait,noatime,discard    0    2
LABEL=cache     /mnt/cache ext4    defaults,nobootwait,noatime,discard    0    2
LABEL=mdpath    /mnt/md    ext4    defaults,nobootwait,noatime,discard    0    2
LABEL=backendfs /mnt/bfs   ext4    defaults,nobootwait,noatime,discard    0    2
LABEL=distribfs /mnt/dfs   ext4    defaults,nobootwait,noatime,discard    0    2
LABEL=tempfs    /var/tmp   ext4    defaults,nobootwait,noatime,discard    0    2
# END Open vStorage
"""
    must_update = False
    with open('/etc/fstab', 'r') as fstab:
        contents = fstab.read()
        if not '# BEGIN Open vStorage' in contents:
            contents += '\n'
            contents += fstab_content
            must_update = True
    if must_update:
        with open('/etc/fstab', 'w') as fstab:
            fstab.write(contents)

# Mount all filesystems
print 'Mounting filesystem...'
run_command('swapoff --all')
run_command('mountall -q')

supported_quality_levels = ['unstable', 'test', 'stable']
quality_level = raw_input('Enter qualitylevel to install from {0}: '.format(supported_quality_levels))
if not quality_level in supported_quality_levels:
    raise ValueError('Please specify correct qualitylevel, one of {0}'.format(supported_quality_levels))

# Install all software components
print 'Updating software...'
run_command('apt-get -y -qq update')
run_command('apt-get -y -qq install python-pip')
run_command('pip -q install -I https://bitbucket.org/jumpscale/jumpscale_core/get/default.zip')

jp_jumpscale_blobstor = """
[jpackages_local]
ftp =
type = local
http =
localpath = /opt/jpackagesftp
namespace = jpackages

[jpackages_remote]
ftp = ftp://publicrepo.incubaid.com
type = httpftp
http = http://publicrepo.incubaid.com
localpath =
namespace = jpackages
"""

jp_openvstorage_blobstor = """
[jp_openvstorage]
ftp = ftp://packages.cloudfounders.com
http = http://packages.cloudfounders.com/ovs
namespace = jpackages
localpath =
type = httpftp
"""

jp_jumpscale_repo = """
[jumpscale]
metadatafromtgz = 0
qualitylevel = test
metadatadownload =
metadataupload =
bitbucketaccount = jumpscale
bitbucketreponame = jp_jumpscale
blobstorremote = jpackages_remote
blobstorlocal = jpackages_local
"""

jp_openvstorage_repo = """
[openvstorage]
metadatafromtgz = 0
qualitylevel = %(qualityLevel)s
metadatadownload = http://packages.cloudfounders.com/metadataTgz
metadataupload = file://opt/jumpscale/var/jpackages/metatars
bitbucketaccount = openvstorage
bitbucketreponame = jp_openvstorage
blobstorremote = jp_openvstorage
blobstorlocal = jpackages_local
""" % {'qualityLevel': quality_level}

print 'Creating JumpScale configuration files...'
os.makedirs('/opt/jumpscale/cfg/jsconfig')
if not os.path.exists('/opt/jumpscale/cfg/jsconfig/blobstor.cfg'):
    blobstor_config = open('/opt/jumpscale/cfg/jsconfig/blobstor.cfg', 'w')
else:
    blobstor_config = open('/opt/jumpscale/cfg/jsconfig/blobstor.cfg', 'a')
blobstor_config.write(jp_jumpscale_blobstor)
blobstor_config.write(jp_openvstorage_blobstor)
blobstor_config.close()

os.makedirs('/opt/jumpscale/cfg/jpackages')
if not os.path.exists('/opt/jumpscale/cfg/jpackages/sources.cfg'):
    jp_sources_config = open('/opt/jumpscale/cfg/jpackages/sources.cfg', 'w')
else:
    jp_sources_config = open('/opt/jumpscale/cfg/jpackages/sources.cfg', 'a')
jp_sources_config.write(jp_jumpscale_repo)
jp_sources_config.write(jp_openvstorage_repo)
jp_sources_config.close()

run_command('jpackage_update')
run_command('jpackage_install -n core')
print 'Starting Open vStorage installation...'
run_command('jpackage_install -n openvstorage')

print 'Installation complete.'
