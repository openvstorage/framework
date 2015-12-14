#!/bin/sh
if [ "$#" -ne 1 ] ; then
  echo "Updates client from the Pyrakoon repo to the Open vStorage repo"
  echo "Usage: ./update.sh <openvstorage_dir>"
  echo "  openvstorage_dir: The root of a working directory or the Open vStorage repo"
  exit 1
else
  dir=`mktemp -d`
  cd $dir
  git clone https://github.com/openvstorage/pyrakoon.git
  rm -rf $1/ovs/extensions/db/arakoon/pyrakoon/pyrakoon
  mv pyrakoon/pyrakoon $1/ovs/extensions/db/arakoon/pyrakoon/
  cd $1
  patch -p0 < ovs/extensions/db/arakoon/pyrakoon/tools/patches.diff
  cd $dir
  cd ..
  rm -rf $dir
fi
