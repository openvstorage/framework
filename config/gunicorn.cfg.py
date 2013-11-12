import multiprocessing

bind = '127.0.0.1:8000'
workers = multiprocessing.cpu_count() * 2 + 1
backlog = 2048
worker_class = 'gevent'
worker_connections = 1000
timeout = 600
limit_request_line = 4096
limit_request_fields = 100
limit_request_field_size = 8190
debug = True
pidfile = '/var/run/ovs_api.pid'
daemon = False
user = 0
group = 0
loglevel = 'debug'
proc_name = 'ovs_api'