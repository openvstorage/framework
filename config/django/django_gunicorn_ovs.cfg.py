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

import multiprocessing
import os

bind = '127.0.0.1:8002'  # avoid conflict with devstack heat api
workers = multiprocessing.cpu_count() + 1
backlog = 2048
worker_class = 'gevent'
worker_connections = 1000
timeout = 600
limit_request_line = 4096
limit_request_fields = 100
limit_request_field_size = 8190
debug = False
pidfile = '/var/run/ovs_api.pid'
daemon = False
user = 0
group = 0
loglevel = 'info'
proc_name = 'ovs_api'
profiling_prefix = '.profile.'
enable_profiling = False

if enable_profiling:
    import cProfile
    def post_fork(server, worker):
        orig_init_process_ = worker.init_process
        def profiling_init_process(self):
            orig_init_process = orig_init_process_
            ofile = '%s%s' % (profiling_prefix, os.getpid())
            print 'Profiling worker %s, output file: %s' % (worker, ofile)
            cProfile.runctx('orig_init_process()', globals(), locals(), ofile)
        worker.init_process = profiling_init_process.__get__(worker)
