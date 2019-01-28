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

import os
import multiprocessing

"""
Server socket

  bind - The socket to bind.

      A string of the form: 'HOST', 'HOST:PORT', 'unix:PATH'.
      An IP is a valid HOST.

  backlog - The number of pending connections. This refers
      to the number of clients that can be waiting to be
      served. Exceeding this number results in the client
      getting an error when attempting to connect. It should
      only affect servers under significant load.

      Must be a positive integer. Generally set in the 64-2048
      range.
"""

bind = '127.0.0.1:8002'  # avoid conflict with devstack heat api
backlog = 2048

"""
Worker processes

  workers - The number of worker processes that this server
      should keep alive for handling requests.

      A positive integer generally in the 2-4 x $(NUM_CORES)
      range. You'll want to vary this a bit to find the best
      for your particular application's work load.

  worker_class - The type of workers to use. The default
      sync class should handle most 'normal' types of work
      loads. You'll want to read
      http://docs.gunicorn.org/en/latest/design.html#choosing-a-worker-type
      for information on when you might want to choose one
      of the other worker classes.

      A string referring to a Python path to a subclass of
      gunicorn.workers.base.Worker. The default provided values
      can be seen at
      http://docs.gunicorn.org/en/latest/settings.html#worker-class

  worker_connections - For the eventlet and gevent worker classes
      this limits the maximum number of simultaneous clients that
      a single process can handle.

      A positive integer generally set to around 1000.

  timeout - If a worker does not notify the master process in this
      number of seconds it is killed and a new worker is spawned
      to replace it.

      Generally set to thirty seconds. Only set this noticeably
      higher if you're sure of the repercussions for sync workers.
      For the non sync workers it just means that the worker
      process is still communicating and is not tied to the length
      of time required to handle a single request.

  keepalive - The number of seconds to wait for the next request
      on a Keep-Alive HTTP connection.

      A positive integer. Generally set in the 1-5 seconds range.
"""

workers = multiprocessing.cpu_count() + 1
worker_class = 'gevent'
worker_connections = 1000
timeout = 600

"""
Server mechanics

  daemon - Detach the main Gunicorn process from the controlling
      terminal with a standard fork/fork sequence.

      True or False

  raw_env - Pass environment variables to the execution environment.

  pidfile - The path to a pid file to write

      A path string or None to not write a pid file.

  user - Switch worker processes to run as this user.

      A valid user id (as an integer) or the name of a user that
      can be retrieved with a call to pwd.getpwnam(value) or None
      to not change the worker process user.

  group - Switch worker process to run as this group.

      A valid group id (as an integer) or the name of a user that
      can be retrieved with a call to pwd.getgrnam(value) or None
      to change the worker processes group.

  umask - A mask for file permissions written by Gunicorn. Note that
      this affects unix socket permissions.

      A valid value for the os.umask(mode) call or a string
      compatible with int(value, 0) (0 means Python guesses
      the base, so values like "0", "0xFF", "0022" are valid
      for decimal, hex, and octal representations)

  tmp_upload_dir - A directory to store temporary request data when
      requests are read. This will most likely be disappearing soon.

      A path to a directory where the process owner can write. Or
      None to signal that Python should choose one on its own.
"""

pidfile = '/var/run/ovs_api.pid'
daemon = False
user = 0
group = 0

"""
  Logging

  logfile - The path to a log file to write to.

      A path string. "-" means log to stdout.

  loglevel - The granularity of log output

      A string of "debug", "info", "warning", "error", "critical"
"""

loglevel = 'info'

"""
Process naming

  proc_name - A base to use with setproctitle to change the way
      that Gunicorn processes are reported in the system process
      table. This affects things like 'ps' and 'top'. If you're
      going to be running more than one instance of Gunicorn you'll
      probably want to set a name to tell them apart. This requires
      that you install the setproctitle module.

      A string or None to choose a default of something like 'gunicorn'.
"""

proc_name = 'ovs_api'

"""
Other options
"""

limit_request_line = 4096
limit_request_fields = 100
limit_request_field_size = 8190
debug = False

profiling_prefix = '.profile.'
enable_profiling = False

if enable_profiling:
    import cProfile

    def post_fork(server, worker):
        _ = server

        orig_init_process_ = worker.init_process

        def profiling_init_process(self):
            orig_init_process = orig_init_process_
            ofile = '%s%s' % (profiling_prefix, os.getpid())
            print 'Profiling worker %s, output file: %s' % (worker, ofile)
            cProfile.runctx('orig_init_process()', globals(), locals(), ofile)
        worker.init_process = profiling_init_process.__get__(worker)
