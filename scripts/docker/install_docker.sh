#!/usr/bin/env bash
# Copyright (C) 2018 iNuron NV
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

# The current working directory is the root of the repository

set -e

distribution=${1:-'ubuntu:16.04'}
repo_url=${2:-'http://apt.openvstorage.com'}
# Determine the Ubuntu repository based on the Git branch
repo=`python scripts/docker/repository_resolver.py`
# Build the Docker image, pass CWD to the build
docker build --rm=true \
             --tag fwk \
             --file scripts/docker/${distribution}/Dockerfile \
             .
# Install OpenvStorage Framework, run it with stdin open and TTY to keep it running
docker run -it \
           -v $PWD:/root/repo-code/ \
           fwk \
           bash -c "bash /root/repo-code/scripts/docker/prepare_img.sh ${repo} ${repo_url}"
