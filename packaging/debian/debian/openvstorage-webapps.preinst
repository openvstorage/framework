#!/usr/bin/env bash

if [[ ! -z "$2" && ! -f /etc/ready_for_upgrade ]]
then
    echo -e '\n\nPlease start upgrade through GUI because all nodes in the cluster need to be upgraded simultaneously!!!!!\n\n'
    exit 1
fi
