#!/usr/bin/env bash
# Copyright (C) 2018 iNuron NV
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

set -e

repo=$1
repo_url=$2

# Update Ubuntu Software repository
echo "Installing packages from ${repo}"
echo "deb ${repo_url} ${repo} main" > /etc/apt/sources.list.d/ovsaptrepo.list
printf 'Package: *\nPin: origin apt-ee.openvstorage.com\nPin-Priority: 1000\n\nPackage: *\nPin: origin apt.openvstorage.com\nPin-Priority: 1000\n' > /etc/apt/preferences
apt-key adv --keyserver keyserver.ubuntu.com --recv-keys 4EFFB1E7
rm -rf /var/lib/apt/lists/*
apt-get update
apt-get install -y --force-yes rsyslog sudo openssl
apt-get install -y --force-yes openvstorage openvstorage-extensions

# Prepare to accept new code
echo "Preparing to accept new code"
rm -rf /opt/OpenvStorage
chmod 777 /opt
chmod -R 777 /var/log/ovs
chmod 777 /run

# Move over the Travis cloned code base. The repository code was mapped under /root/repo-code (see install_docker.sh)
echo "Copying the mapped code"
cp -R /root/repo-code/. /opt/OpenvStorage

# Further tweaks to run our tests
echo "Further tweaking the OVS install"
cp /opt/OpenvStorage/scripts/system/ovs /usr/bin/
chmod 777 /usr/bin/ovs
cd /opt/OpenvStorage/webapps/api; export PYTHONPATH=/opt/OpenvStorage:/opt/OpenvStorage/webapps:$PYTHONPATH; python manage.py syncdb --noinput
echo '{"configuration_store":"arakoon"}' > /opt/OpenvStorage/config/framework.json

# Run tests
echo "Running unittests"
export PYTHONPATH=${PYTHONPATH}:/opt/OpenvStorage:/opt/OpenvStorage/webapps; ovs unittest