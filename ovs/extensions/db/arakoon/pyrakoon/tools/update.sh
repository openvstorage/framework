#!/bin/sh
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

if [ "$#" -ne 1 ] ; then
  echo "Updates client from the Pyrakoon repo to the Open vStorage repo"
  echo "Usage: ./update.sh <openvstorage_dir>"
  echo "  openvstorage_dir: The root of a working directory or the Open vStorage repo"
  exit 1
else
  dir=`mktemp -d`
  cd $dir
  git clone https://github.com/openvstorage/pyrakoon.git
  cd pyrakoon
  git log -1 --format="%H" > $1/ovs/extensions/db/arakoon/pyrakoon/pyrakoon.version
  cd ..
  rm -rf $1/ovs/extensions/db/arakoon/pyrakoon/pyrakoon
  mv pyrakoon/pyrakoon $1/ovs/extensions/db/arakoon/pyrakoon/
  cd $1
  patch -p0 < ovs/extensions/db/arakoon/pyrakoon/tools/patches.diff
  cd $dir
  cd ..
  rm -rf $dir
fi
