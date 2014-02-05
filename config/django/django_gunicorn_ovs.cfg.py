import multiprocessing
import os

bind = '127.0.0.1:8000'
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
