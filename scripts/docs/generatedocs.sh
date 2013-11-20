#!/bin/bash
epydoc --graph umlclasstree ovs -o $1 -v --exclude=test --no-sourcecode --no-private
