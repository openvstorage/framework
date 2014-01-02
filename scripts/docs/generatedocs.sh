#!/bin/bash
DIR=${1%/}
if [ "$(ls -A $DIR/)" ]; then
     echo "Directory $DIR/ must be empty"
     exit 1
fi
mkdir $DIR/lib
epydoc --graph umlclasstree ovs.lib -o $DIR/lib -v --no-sourcecode --no-private --parse-only > /dev/null
mkdir $DIR/dal
epydoc --graph umlclasstree ovs.dal -o $DIR/dal -v --no-sourcecode --no-private > /dev/null
mkdir $DIR/extensions
epydoc --graph umlclasstree ovs.extensions -o $DIR/extensions -v --no-sourcecode --no-private > /dev/null
mkdir $DIR/hypervisor
epydoc --graph umlclasstree ovs.hypervisor -o $DIR/hypervisor -v --no-sourcecode --no-private --parse-only > /dev/null
