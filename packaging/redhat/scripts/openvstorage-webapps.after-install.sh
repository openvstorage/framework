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

pip install djangorestframework==2.3.12

mkdir -p /etc/nginx/sites-enabled

python /opt/OpenvStorage/scripts/install/openvstorage-webapps.postinst.py "$Version" "$@"

cp /etc/nginx/sites-enabled/* /etc/nginx/conf.d/
chmod 777 /etc/nginx/conf.d/*
service nginx restart
