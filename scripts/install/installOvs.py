#!/usr/bin/env python
import os
import sys
import urllib2
import base64
import getpass
import hashlib
from random import choice
from string import lowercase
from subprocess import call, check_output
from optparse import OptionParser


def run_command(command, fail=True):
    """
    Executed a command
    """
    rcode = call(command.split(' '))
    if rcode != 0 and fail:
        raise Exception('{0} failed with return value {1}'.format(command, rcode))


def boxed_message(lines, character='+', maxlength=60):
    """
    Embeds a set of lines into a box
    """
    character = str(character)  # This must be a string
    corrected_lines = []
    for line in lines:
        if len(line) > maxlength:
            linepart = ''
            for word in line.split(' '):
                if len(linepart + ' ' + word) <= maxlength:
                    linepart += word + ' '
                elif len(word) >= maxlength:
                    if len(linepart) > 0:
                        corrected_lines.append(linepart.strip())
                        linepart = ''
                    corrected_lines.append(word.strip())
                else:
                    if len(linepart) > 0:
                        corrected_lines.append(linepart.strip())
                    linepart = word + ' '
            if len(linepart) > 0:
                corrected_lines.append(linepart.strip())
        else:
            corrected_lines.append(line)
    maxlen = len(max(corrected_lines, key=len))
    newlines = [character * (maxlen + 10)]
    for line in corrected_lines:
        newlines.append('{0}  {1}{2}  {3}'.format(character * 3, line, ' ' * (maxlen - len(line)),
                                                  character * 3))
    newlines.append(character * (maxlen + 10))
    return '\n'.join(newlines)


def ask_yesno(message="", default_value=None):
    """
    Asks the user a yes/no question
    """
    if default_value is None:
        ynstring = " (y/n):"
        failuremsg = "Illegal value. Press 'y' or 'n'."
    elif default_value is True:
        ynstring = " ([y]/n)"
        failuremsg = "Illegal value. Press 'y' or 'n' (or nothing for default)."
    elif default_value is False:
        ynstring = " (y/[n])"
        failuremsg = "Illegal value. Press 'y' or 'n' (or nothing for default)."
    else:
        raise ValueError("Invalid default value {0}".format(default_value))
    while True:
        result = raw_input(str(message) + ynstring).rstrip(chr(13))
        if not result and default_value is not None:
            return default_value
        if result.lower() in ('y', 'yes'):
            return True
        if result.lower() in ('n', 'no'):
            return False
        print failuremsg


def ask_string(message='', default_value=None):
    """
    Asks the user a question
    """
    default_string = ': ' if default_value is None else ' [{0}]: '.format(default_value)
    result = raw_input(str(message) + default_string).rstrip(chr(13))
    if not result and default_value is not None:
        return default_value
    return result


def ask_integer(question, min_value, max_value, default_value=None, invalid_message=None):
    """
    Asks an integer to the user
    """
    if invalid_message is None:
        invalid_message = 'Invalid input please try again.'
    if default_value is not None:
        question = '{0} [{1}]: '.format(question, default_value)
    while True:
        i = raw_input(question).rstrip()
        if i == '' and default_value is not None:
            i = str(default_value)
        if not i.isdigit():
            print invalid_message
        else:
            i = int(i)
            if min_value <= i <= max_value:
                return i
            else:
                print invalid_message


def ask_choice(choice_options, question=None, default_value=None):
    """
    Lets the user chose one of a set of options
    """
    if not choice_options:
        return None
    if len(choice_options) == 1:
        print "Found exactly one choice: {0}".format(choice_options[0])
        return choice_options[0]
    choice_options.sort()
    print '{0}Make a selection please: '.format(
        '{0}. '.format(question) if question is not None else ''
    )
    nr = 0
    default_nr = None
    for section in choice_options:
        nr += 1
        print '   {0}: {1}'.format(nr, section)
        if section == default_value:
            default_nr = nr

    result = ask_integer(
        question='   Select Nr: ',
        min_value=1,
        max_value=len(choice_options),
        default_value=default_nr
    )
    return choice_options[result - 1]


def find_in_list(items, search_string):
    for item in items:
        if search_string in item:
            return item
    return None


if os.getegid() != 0:
    print 'This script should be executed as a user in the root group.'
    sys.exit(1)

parser = OptionParser(description='Open vStorage Setup')
parser.add_option('-n', '--no-filesystems', dest='filesystems', action="store_false", default=True,
                  help="Don't create partitions and filesystems")
parser.add_option('-c', '--clean', dest='clean', action="store_true", default=False,
                  help="Try to clean environment before reinstalling")
(options, args) = parser.parse_args()

# Warning
print boxed_message(['WARNING. Use with caution.',
                     'This script assumes it is executed on a virtual machine',
                     'dedicated to Open vStorage. It will repartition the',
                     'disks without further interaction, destroying all data',
                     'present. If you want to install Open vStorage on an existing',
                     'machine, please refer to the Open vStorage documentation on',
                     'how to do so.'])
proceed = ask_yesno('Continue with the install?', True)
if not proceed:
    sys.exit(1)

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
    mounted = check_output("mount | cut -d ' ' -f 1", shell=True).strip().split('\n')
    # Create partitions on HDD
    print '  On HDD...'
    if '/dev/sdb1' in mounted:
        run_command('umount /dev/sdb1', fail=False)
    if '/dev/sdb2' in mounted:
        run_command('umount /dev/sdb2', fail=False)
    if '/dev/sdb3' in mounted:
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
    if '/dev/sdc1' in mounted:
        run_command('umount /dev/sdc1', fail=False)
    if '/dev/sdc2' in mounted:
        run_command('umount /dev/sdc2', fail=False)
    if '/dev/sdc3' in mounted:
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
run_command('mountall -q &> /dev/null')

supported_quality_levels = ['unstable', 'test']
quality_level = ask_choice(supported_quality_levels, question='Select qualitylevel', default_value='unstable')

# Requesting information
print 'Requesting information...'
configuration = {'openvstorage': {}}
configuration['openvstorage']['ovs.host.hypervisor'] = 'VMWARE'
configuration['openvstorage']['ovs.host.name'] = ask_string('Enter hypervisor hostname', default_value='esxi')
ip, username, password = None, 'root', None
while True:
    ip = ask_string('Enter hypervisor ip address', default_value=ip)
    username = ask_string('Enter hypervisor username', default_value=username)
    password = getpass.getpass()
    try:
        request = urllib2.Request('https://{0}/mob'.format(ip))
        auth = base64.encodestring('{0}:{1}'.format(username, password)).replace('\n', '')
        request.add_header("Authorization", "Basic %s" % auth)
        urllib2.urlopen(request).read()
        break
    except Exception as ex:
        print 'Could not connect to {0}: {1}'.format(ip, ex)
configuration['openvstorage']['ovs.host.ip'] = ip
configuration['openvstorage']['ovs.host.login'] = username
configuration['openvstorage']['ovs.host.password'] = password

configuration['openvstorage-core'] = {}
ipaddresses = check_output("ip a | grep 'inet ' | sed 's/\s\s*/ /g' | cut -d ' ' -f 3 | cut -d '/' -f 1", shell=True).strip().split('\n')
ipaddresses = [ip for ip in ipaddresses if ip != '127.0.0.1']
configuration['openvstorage-core']['ovs.grid.ip'] = ask_choice(ipaddresses, question='Choose public ip address')
mountpoints = [p.split(' ')[2] for p in check_output('mount -v'.split(' ')).strip().split('\n') if len(p.split(' ')) > 2 and ('/mnt/' in p.split(' ')[2] or '/var' in p.split(' ')[2])]
mountpoint = ask_choice(mountpoints, question='Select temporary FS mountpoint', default_value=find_in_list(mountpoints, 'tmp'))
mountpoints.remove(mountpoint)
configuration['openvstorage-core']['ovs.core.tempfs.mountpoint'] = mountpoint
unique_id = sorted(check_output("ip a | grep link/ether | sed 's/\s\s*/ /g' | cut -d ' ' -f 3 | sed 's/://g'", shell=True).strip().split('\n'))[0]
configuration['openvstorage-core']['ovs.core.memcache.localnode.name'] = unique_id
mountpoint = ask_choice(mountpoints, question='Select arakoon database mountpoint', default_value=find_in_list(mountpoints, 'db'))
mountpoints.remove(mountpoint)
configuration['openvstorage-core']['ovs.core.db.mountpoint'] = mountpoint
configuration['openvstorage-core']['ovs.core.db.arakoon.node.name'] = unique_id
mountpoint = ask_choice(mountpoints, question='Select distributed FS mountpoint', default_value=find_in_list(mountpoints, 'dfs'))
mountpoints.remove(mountpoint)
configuration['openvstorage-core']['volumedriver.filesystem.distributed'] = mountpoint
mountpoint = ask_choice(mountpoints, question='Select metadata mountpoint', default_value=find_in_list(mountpoints, 'md'))
mountpoints.remove(mountpoint)
configuration['openvstorage-core']['volumedriver.metadata'] = mountpoint
configuration['openvstorage-core']['volumedriver.arakoon.node.name'] = unique_id
configuration['openvstorage-core']['ovs.core.rabbitmq.localnode.name'] = unique_id

configuration['openvstorage-webapps'] = {}
configuration['openvstorage-webapps']['ovs.webapps.certificate.period'] = ask_integer('GUI certificate lifetime', min_value=1, max_value=365 * 10, default_value=365)

configuration['elasticsearch'] = {}
configuration['elasticsearch']['elasticsearch.cluster.name'] = ask_string('Enter elastic search cluster name', default_value='ovses')

configuration['grid'] = {}
configuration['grid']['grid.id'] = ask_integer('Enter grid ID (needs to be unique): ', min_value=1, max_value=32767)
configuration['grid']['grid.node.roles'] = 'node'

configuration['grid_master'] = {}
configuration['grid_master']['gridmaster.grid.id'] = configuration['grid']['grid.id']
configuration['grid_master']['gridmaster.useavahi'] = 1
configuration['grid_master']['gridmaster.superadminpasswd'] = hashlib.sha256(''.join(choice(lowercase) for i in range(25))).hexdigest()

configuration['osis'] = {}
configuration['osis']['osis.key'] = ''.join(choice(lowercase) for i in range(25))

if not os.path.exists('/opt/jumpscale/cfg/hrd'):
    os.makedirs('/opt/jumpscale/cfg/hrd')
for filename in configuration:
    with open('/opt/jumpscale/cfg/hrd/{0}.hrd'.format(filename), 'w') as hrd:
        hrd.write('\n'.join(['%s=%s' % i for i in configuration[filename].iteritems()]))

bitbucket_username = ask_string('Provide your bitbucket username')
bitbucket_password = getpass.getpass()
if not os.path.exists('/opt/jumpscale/cfg/jsconfig'):
    os.makedirs('/opt/jumpscale/cfg/jsconfig')
if not os.path.exists('/opt/jumpscale/cfg/jsconfig/bitbucket.cfg'):
    with open('/opt/jumpscale/cfg/jsconfig/bitbucket.cfg', 'w') as bitbucket:
        bitbucket.write(
            '[jumpscale]\nlogin = {0}\npasswd = {1}\n\n[openvstorage]\nlogin = {0}\npasswd = {1}\n'.format(
                bitbucket_username, bitbucket_password
            ))

# Branch mapping: key = our qualitylevel, value = jumpscale branch
branch_mapping = {'unstable': 'default',
                  'test': 'default'}
# Quality level mapping: key = our qualitylevel, value = jumpscale quality level
quality_mapping = {'unstable': 'test',
                   'test': 'test'}

# Install all software components
print 'Updating software...'
run_command('apt-get -y -qq update')
run_command('apt-get -y -qq install python-dev')
run_command('apt-get -y -qq install python-pip')
run_command('pip -q install -I https://bitbucket.org/jumpscale/jumpscale_core/get/{0}.zip'.format(branch_mapping[quality_level]))

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
qualitylevel = %(qualityLevel)s
metadatadownload =
metadataupload =
bitbucketaccount = jumpscale
bitbucketreponame = jp_jumpscale
blobstorremote = jpackages_remote
blobstorlocal = jpackages_local
""" % {'qualityLevel': quality_mapping[quality_level]}

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
if not os.path.exists('/opt/jumpscale/cfg/jsconfig'):
    os.makedirs('/opt/jumpscale/cfg/jsconfig')
if not os.path.exists('/opt/jumpscale/cfg/jsconfig/blobstor.cfg'):
    blobstor_config = open('/opt/jumpscale/cfg/jsconfig/blobstor.cfg', 'w')
else:
    blobstor_config = open('/opt/jumpscale/cfg/jsconfig/blobstor.cfg', 'a')
blobstor_config.write(jp_jumpscale_blobstor)
blobstor_config.write(jp_openvstorage_blobstor)
blobstor_config.close()

if not os.path.exists('/opt/jumpscale/cfg/jpackages'):
    os.makedirs('/opt/jumpscale/cfg/jpackages')
if not os.path.exists('/opt/jumpscale/cfg/jpackages/sources.cfg'):
    jp_sources_config = open('/opt/jumpscale/cfg/jpackages/sources.cfg', 'w')
else:
    jp_sources_config = open('/opt/jumpscale/cfg/jpackages/sources.cfg', 'a')
jp_sources_config.write(jp_jumpscale_repo)
jp_sources_config.write(jp_openvstorage_repo)
jp_sources_config.close()

# Starting installation
print 'Installing prerequisites...'
run_command('jpackage_update')
run_command('jpackage_install -n core')
print 'Starting Open vStorage installation...'
run_command('jpackage_install -n openvstorage')

print 'Installation complete.'
