#!/usr/bin/env python

"""
Based on
* [http://ceph.com/docs/master/rados/deployment/]
* [http://ceph.com/docs/master/install/install-ceph-gateway/]
* [Guide to add more nodes|http://switzernet.com/3/public/130925-ceph-cluster/index.htm]
"""

# @todo verify if any disks specified is not a member of an active mdadm array - if so -> raise
# @todo verify if an /etc/ceph/ceph.conf file already exists, this means a reentrant run

import subprocess
import socket
import os
from optparse import OptionParser

def run_command(command, fail=True):
    """
    Executed a command
    """
    rcode = subprocess.call(command.split(' '))
    if rcode != 0 and fail:
        raise Exception('{0} failed with return value {1}'.format(command, rcode))

parser = OptionParser(description='Open vStorage Ceph Deployment script')
parser.add_option('-d', '--disks', dest='disks',
                  help="Disks to use for Ceph OSD's")
parser.add_option('-a', '--host', dest='additional_hostnames',
                  help="Additional hosts to install")
(options, args) = parser.parse_args()

def disks_are_member_of_raid(disks):
    disk_in_raid = False
    if os.path.exists('/sbin/mdadm'):
        # check if disks list has active mdadm members
        output = subprocess.check_output(['cat', '/proc/mdstat'])
        for line in str(output).splitlines():
            if 'md' in line:
                for disk in disks:
                    disk_in_raid = disk in line
                    if disk_in_raid: break
            if disk_in_raid: break
    return disk_in_raid

if not options.additional_hostnames:
    hostname = socket.gethostname()
    disks = options.disks.split(',')
    ceph_disks = ''
    for disk in disks:
        ceph_disks += ' {}:{}'.format(hostname, disk)
    single_node = True;
else:
    single_node = False;
    hostname = socket.gethostname()
    disks = options.disks.split(',')
    ceph_disks = ''
    for disk in disks:
        ceph_disks += ' {}:{}'.format(hostname, disk)
    hosts = options.additional_hostnames.split(',')
    for host in hosts:
        hostname = hostname + ' ' + host
        for disk in disks:
            ceph_disks += ' {}:{}'.format(host, disk)

if disks and disks_are_member_of_raid(disks):
    raise RuntimeError("At least one disk out of {} is member of a raid configuration, cleanup raid config or select other disks".format(disks))

if os.path.exists('/etc/ceph/ceph.conf'):
    raise RuntimeError('Please cleanup first as a ceph.conf file already exists, script has been executed before!')

ceph_disks = ceph_disks[1:]  # Stripping first space
os.chdir('/root')
run_command('apt-get -y update')
run_command('apt-get -y install ceph-deploy')
run_command('ceph-deploy install {}'.format(hostname))
run_command('ceph-deploy new {}'.format(hostname))
run_command('ceph-deploy mon create {}'.format(hostname))
run_command('ceph-deploy gatherkeys {}'.format(socket.gethostname()), fail=False)
run_command('ceph-deploy gatherkeys {}'.format(socket.gethostname()), fail=False)
from time import sleep
sleep(5)
run_command('ceph-deploy gatherkeys {}'.format(socket.gethostname()))


ceph_disks = ceph_disks.split(' ')
for disk in ceph_disks:
    run_command('ceph-deploy disk zap {}'.format(disk))
    run_command('ceph-deploy osd create {}'.format(disk))
run_command('ceph-deploy mds create {}'.format(hostname))


run_command('service ceph start')
run_command('ceph health')

if not options.additional_hostnames:
    if len(disks) > 1:
        run_command('ceph osd getcrushmap -o /tmp/current_crushmap')
        run_command('crushtool -d /tmp/current_crushmap -o /tmp/current_crushmap_editable')
        fh = open('/tmp/current_crushmap_editable', 'r')
        fc = fh.readlines()
        fh.close()
        nfc = list()
        for line in fc:
            if line.find('step chooseleaf') > -1:
                nfc.append(line.replace('host', 'osd'))
            else:
                nfc.append(line)
        fh = open('/tmp/current_crushmap_editable', 'w')
        fh.write(''.join(nfc))
        fh.close()
        run_command('crushtool -c /tmp/current_crushmap_editable -o /tmp/new_crushmap')
        run_command('ceph osd setcrushmap -i /tmp/new_crushmap')

run_command('apt-get -y install apache2 libapache2-mod-fastcgi')
fh = open('/etc/apache2/apache2.conf', 'a')
fh.write('ServerName {}'.format(socket.gethostname()))
fh.close()
run_command('a2enmod rewrite')
run_command('a2enmod ssl')
run_command('mkdir /etc/apache2/ssl')
run_command('openssl req -x509 -nodes -days 365 -newkey rsa:2048 -keyout /etc/apache2/ssl/apache.key -out /etc/apache2/ssl/apache.crt')
run_command('service apache2 restart')

radosgw = """
FastCgiExternalServer /var/www/s3gw.fcgi -socket /tmp/radosgw.sock

<VirtualHost *:80>
        ServerName rados
        ServerAdmin engineering@cloudfounders.com
        DocumentRoot /var/www
        RewriteEngine On
        RewriteRule ^/([a-zA-Z0-9-_.]*)([/]?.*) /s3gw.fcgi?page=$1&params=$2&%{QUERY_STRING} [E=HTTP_AUTHORIZATION:%{HTTP:Authorization},L]

<IfModule mod_fastcgi.c>
                <Directory /var/www>
                        Options +ExecCGI
                        AllowOverride All
                        SetHandler fastcgi-script
                        Order allow,deny
                        Allow from all
                        AuthBasicAuthoritative Off
                </Directory>
        </IfModule>


        AllowEncodedSlashes On
        ErrorLog /var/log/apache2/error.log
        CustomLog /var/log/apache2/access.log combined
        ServerSignature Off
</VirtualHost>
"""
fh = open('/etc/apache2/sites-available/rgw.conf', 'w')
fh.write(radosgw)
fh.close

run_command('a2ensite rgw.conf')
run_command('a2dissite 000-default.conf')
run_command('service apache2 reload')

run_command('apt-get -y install radosgw')

fh = open('/etc/ceph/ceph.conf', 'a')
radosgw_ceph_config = """
[client.radosgw.gateway]
host = {}
keyring = /etc/ceph/keyring.radosgw.gateway
rgw socket path = /tmp/radosgw.sock
log file = /var/log/ceph/radosgw.log
""".format(socket.gethostname())
fh.write(radosgw_ceph_config)
fh.close()

os.makedirs('/var/lib/ceph/radosgw/ceph-radosgw.gateway')

fh = open('/var/www/s3gw.fcgi', 'w')
fcgi_content = """
#!/bin/sh
exec /usr/bin/radosgw -c /etc/ceph/ceph.conf -n client.radosgw.gateway
"""
fh.write(fcgi_content)
fh.close()


run_command('ceph-authtool --create-keyring /etc/ceph/keyring.radosgw.gateway')
run_command('chmod +r /etc/ceph/keyring.radosgw.gateway')

run_command('ceph-authtool /etc/ceph/keyring.radosgw.gateway -n client.radosgw.gateway --gen-key')
os.system("ceph-authtool -n client.radosgw.gateway --cap osd 'allow rwx' --cap mon 'allow rw' /etc/ceph/keyring.radosgw.gateway")

run_command('ceph -k /etc/ceph/ceph.client.admin.keyring auth add client.radosgw.gateway -i /etc/ceph/keyring.radosgw.gateway')

run_command('ceph osd pool create .rgw 100 100')
run_command('ceph osd pool create .rgw.control 100 100')
run_command('ceph osd pool create .rgw.gc 100 100')
run_command('ceph osd pool create .rgw.root 100 100')
run_command('ceph osd pool create .log 100 100')
run_command('ceph osd pool create .intent-log 100 100')
run_command('ceph osd pool create .usage 100 100')
run_command('ceph osd pool create .users 100 100')
run_command('ceph osd pool create .users.email 100 100')
run_command('ceph osd pool create .users.swift 100 100')
run_command('ceph osd pool create .users.uid 100 100')

if single_node & (len(disks) == 1):
    print "Single node, single disk"
    run_command('ceph osd pool set data size 1')
    run_command('ceph osd pool set metadata size 1')
    run_command('ceph osd pool set rbd size 1')
    run_command('ceph osd pool set .rgw size 1')
    run_command('ceph osd pool set .rgw.control size 1')
    run_command('ceph osd pool set .rgw.gc size 1')
    run_command('ceph osd pool set .rgw.root size 1')
    run_command('ceph osd pool set .log size 1')
    run_command('ceph osd pool set .intent-log size 1')
    run_command('ceph osd pool set .usage size 1')
    run_command('ceph osd pool set .users size 1')
    run_command('ceph osd pool set .users.email size 1')
    run_command('ceph osd pool set .users.swift size 1')
    run_command('ceph osd pool set .users.uid size 1')

run_command('service ceph restart')
run_command('service apache2 restart')
run_command('/etc/init.d/radosgw start')

run_command('radosgw-admin user create --uid=johndoe --display-name="John Doe" --email=john@example.com')

run_command('ceph status')
