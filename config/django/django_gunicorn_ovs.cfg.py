# Copyright 2014 CloudFounders NV
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

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
loglevel = 'debug'
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
