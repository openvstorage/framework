#!/bin/bash
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
