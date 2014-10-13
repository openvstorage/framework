#!/bin/sh

if [ "$#" -ne 2 ] ; then
  echo "Copies Arakoon client from the Arakoon repo to the Open vStorage repo"
  echo "Usage: ./update.sh <arakoon_dir> <openvstorage_dir>"
  echo "  arakoon_dir: The root of a working directory of the Arakoon repo"
  echo "  openvstorage_dir: The root of a working directory or the Open vStorage repo"
  exit 1
else
  echo "Copying..."
  rm -rf $2/ovs/extensions/db/arakoon/arakoon/*
  cp -f $1/src/client/python/* $2/ovs/extensions/db/arakoon/arakoon/
  cp -f $1/pylabs/Compat.py $2/ovs/extensions/db/arakoon/arakoon/
  cp -f $1/pylabs/extensions/arakoon_ext/client/ArakoonClient.py $2/ovs/extensions/db/arakoon/arakoon/
  cp -f $1/pylabs/extensions/arakoon_ext/server/Arakoon*.py $2/ovs/extensions/db/arakoon/arakoon/
  cp -f $1/pylabs/extensions/arakoon_ext/server/RemoteControlProtocol.py $2/ovs/extensions/db/arakoon/arakoon/
  echo "Patching..."
  cd $2
  patch -p0 < ovs/extensions/db/arakoon/tools/correct_imports.diff
  echo "Done"
  exit 0
fi
