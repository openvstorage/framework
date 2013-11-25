#!/bin/bash
rm -rf $1/*
mkdir $1/lib
epydoc --graph umlclasstree ovs.lib -o $1/lib -v --no-sourcecode --no-private --parse-only > /dev/null
mkdir $1/dal
epydoc --graph umlclasstree ovs.dal -o $1/dal -v --no-sourcecode --no-private > /dev/null
mkdir $1/extensions
epydoc --graph umlclasstree ovs.extensions -o $1/extensions -v --no-sourcecode --no-private > /dev/null
mkdir $1/hypervisor
epydoc --graph umlclasstree ovs.hypervisor -o $1/hypervisor -v --no-sourcecode --no-private --parse-only > /dev/null
