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

set -x

if [[ ! -z "$2" && ! -f /etc/ready_for_upgrade ]]
then
    echo -e '\n\nPlease start update through GUI because all nodes in the cluster need to be updated simultaneously!!!!!\n\n'
    exit 1
else
    if [ ! -f /etc/openvstorage_id ]
    then
        echo `openssl rand -base64 64 | tr -dc A-Z-a-z-0-9 | head -c 16` > /etc/openvstorage_id
    fi

    user_exists=$(id -a ovs > /dev/null 2>&1; echo $?)
    if [[ $user_exists -eq 1 ]]
    then
        echo 'Creating OVS user'
        useradd ovs -d /opt/OpenvStorage
        [ -f /etc/sudoers.d/ovs ] || echo '%ovs ALL=NOPASSWD: ALL' >> /etc/sudoers.d/ovs
    else
        echo 'OVS user already exists'
    fi

    # logging
    mkdir -p /var/log/ovs/volumedriver
    chown -R ovs:ovs /var/log/ovs
    chmod 750 /var/log/ovs
fi
