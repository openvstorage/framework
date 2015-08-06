# /etc/cron.d/openvstorage-core: crontab entries for the openvstorage-core package

* *   * * *  root  /usr/bin/ovs monitor heartbeat
59 23 * * *  root  /opt/OpenvStorage/scripts/system/rotate-storagedriver-logs.sh
