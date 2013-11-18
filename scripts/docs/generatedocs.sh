#!/bin/bash
epydoc --graph umlclasstree ovs.dal -o $1 -v --exclude=test --no-sourcecode --no-private
