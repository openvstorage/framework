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

DIR=${1%/}
if [ "$(ls -A $DIR/)" ]; then
     echo "Directory $DIR/ must be empty"
     exit 1
fi
export PYTHONPATH=$PYTHONPATH:/opt/OpenvStorage/lib/python2.7/site-packages/
mkdir $DIR/lib
epydoc --graph umlclasstree ovs.lib -o $DIR/lib -v --parse-only > /dev/null
mkdir $DIR/dal
epydoc --graph umlclasstree ovs.dal -o $DIR/dal -v > /dev/null
mkdir $DIR/extensions
epydoc --graph umlclasstree ovs.extensions -o $DIR/extensions -v > /dev/null
