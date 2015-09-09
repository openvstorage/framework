#!/usr/bin/env bash
set -e

if [[ ! -z "$2" && ! -f /etc/ready_for_upgrade ]]
then
    echo -e '\n\nPlease start upgrade through GUI because all nodes in the cluster need to be upgraded simultaneously!!!!!\n\n'
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
