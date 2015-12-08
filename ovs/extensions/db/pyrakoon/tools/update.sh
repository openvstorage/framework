#!/bin/sh
if [ "$#" -ne 1 ] ; then
  echo "Updates client from the Pyrakoon repo to the Open vStorage repo"
  echo "Usage: ./update.sh <openvstorage_dir>"
  echo "  openvstorage_dir: The root of a working directory or the Open vStorage repo"
  exit 1
else
  dir=`mktemp -d`
  cd $dir
  git clone https://github.com/Incubaid/pyrakoon.git
  rm -rf $1/ovs/extensions/db/pyrakoon/pyrakoon
  mv pyrakoon/pyrakoon $1/ovs/extensions/db/pyrakoon/
  find $1/ovs/extensions/db/pyrakoon/pyrakoon -type f -print0 | xargs -0 sed -i 's/from pyrakoon /from ovs.extensions.db.pyrakoon.pyrakoon /g'
  find $1/ovs/extensions/db/pyrakoon/pyrakoon -type f -print0 | xargs -0 sed -i 's/from pyrakoon./from ovs.extensions.db.pyrakoon.pyrakoon./g'
  find $1/ovs/extensions/db/pyrakoon/pyrakoon -type f -print0 | xargs -0 sed -i 's/import pyrakoon/import ovs.extensions.db.pyrakoon.pyrakoon/g'
  cd ..
  rm -rf $dir
fi
