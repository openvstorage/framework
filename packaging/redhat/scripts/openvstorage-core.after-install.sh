#!/usr/bin/env bash
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

pip install --upgrade pip
pip install rpyc
pip install pika
pip install datadiff
pip install celery
pip install librabbitmq>=1.5.2


python /opt/OpenvStorage/scripts/install/openvstorage-core.postinst.py "$Version" "$@"
chmod a+x /usr/bin/ovs
cp -r /usr/lib/python2.7/dist-packages/volumedriver/ /usr/lib/python2.7/site-packages/
chmod 777 /var/lock

echo -e '#  crontab entries for the openvstorage-core package
* *   * * * root /usr/bin/ovs monitor heartbeat
59 23 * * * root /opt/OpenvStorage/scripts/system/rotate-storagedriver-logs.sh
' >> /etc/crontab
service crond restart

service libvirtd start