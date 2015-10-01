#!/usr/bin/env bash
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