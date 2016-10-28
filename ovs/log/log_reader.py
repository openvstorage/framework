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
import re
import time
import subprocess

from stat import *
from subprocess import check_output
from datetime import date, datetime, timedelta
from ovs.extensions.generic.remote import remote
from ovs.dal.lists.storagerouterlist import StorageRouterList


# @todo support 16.04 logfiles
class LogFileTimeParser(object):
    """
    # @TODO Support for rotated log files - currently using the current year for 'Jan 01' dates.
    Extracts parts of a log file based on a start and end date
    Uses binary search logic to speed up searching
    Can filter out errors based on string patterns

    Hardly uses any memory - cpu intensive though
    Writes results to a file and returns the contents of this file to save memory

    Common usage: validate log files during testing
    """
    version = "0.02"

    # Set some initial values
    BUF_SIZE = 4096  # handle long lines, but put a limit to them
    REWIND = 100  # arbitrary, the optimal value is highly dependent on the structure of the file
    LIMIT = 75  # arbitrary, allow for a VERY large file, but stop it if it runs away
    PATHS_TO_FILES = ['/var/log/upstart/ovs-workers.log', '/var/log/upstart/ovs-webapp-api.log', '/var/log/ovs/lib.log',
                      '/var/log/ovs/extensions.log', 'ovs-arakoon-voldrv.log']

    FILE_PATH = '/tmp/file_reader_cache'
    FILE_PATH_REMOTE = '/tmp/file_reader_cache_all'
    TEMP_FILE_PATH = '/tmp/file_reader.py'
    POSSIBLE_MODES = ['search', 'error-search']

    # Timeout for ssh: 0.0 means no timeout
    TIME_OUT = 0.0
    # For benchmarking purposes
    start_time = time.time()

    line_date = ''
    line = None
    opened_file = None

    @staticmethod
    def _check_if_local(ip):
        """
        Lent from ci.tests.general_network to make it standalone
        :param ip:
        :return:
        """
        # Check for a local ip - the framework SSH client will not return paramiko if it is
        local_ips = [lip.strip() for lip in
                     check_output("ip a | grep 'inet ' | sed 's/\s\s*/ /g' | cut -d ' ' -f 3 | cut -d '/' -f 1",
                                  shell=True).strip().splitlines()]
        is_local = ip in local_ips
        return is_local

    @staticmethod
    def _execute_command(command, wait=True, shell=True):
        """
        Lent from ci.tests.general to make it standalone
        Execute a command on local node
        :param command: Command to execute
        :param wait: Wait for command to finish
        :param shell: Use shell
        :return: Output, error code
        """
        child_process = subprocess.Popen(command, shell=shell, stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE)

        if not wait:
            return child_process.pid
        out, error = child_process.communicate()
        return out, error, child_process.returncode

    @staticmethod
    def _check_available_space(hosts, username, password):
        required_space = 0
        output_dir = "/".join(LogFileTimeParser.FILE_PATH_REMOTE.strip("/").split('/')[:1])
        get_space_command = "df /tmp | cut -d ' ' -f 10 | grep -Eo [0-9]*"
        available_space = LogFileTimeParser._execute_command(get_space_command)
        from ovs.extensions.generic.sshclient import SSHClient
        for host in hosts:
            ssh_client = SSHClient(host, username=username, password=password)
            for f in LogFileTimeParser.PATHS_TO_FILES:
                command = "ls -al {0} | cut -d ' ' -f 5".format(f)
                try:
                    required_space += int(ssh_client.run(command))
                except ValueError:
                    pass
        if required_space > available_space:
            raise OverflowError('Would not be able to allocate {0} in {1}'.format(required_space, output_dir))

    @staticmethod
    def execute_search_on_remote(start=None, end=None, paths_to_file=PATHS_TO_FILES, hosts=None, debug=False, python_error=False,
                                 mode='search', username='root', password=None, suppress_return=False, search_patterns=None):
        """
        :param start: Starting date
        :type start: str / Datetime
        :param end: End date
        :type end: str / Datetime
        :param paths_to_file: List of paths of files that will be searched on all nodes
        :type paths_to_file: List of str
        :param debug: Boolean to debug or not
        :type debug: Boolean
        :param hosts: Ip of the nodes
        :type hosts: List of str
        :param mode: Search mode
        :type mode: str
        :param python_error: Whether only python errors should be checked
        :type python_error: Boolean
        :param username: Username of the user to login
        :type username: str
        :param password: Password of the user to login
        :type password: str
        :param suppress_return: only write to file and not return contents
        :type suppress_return: Boolean
        :return: Output of a file as string
        """

        # Validate parameter
        if mode not in LogFileTimeParser.POSSIBLE_MODES:
            raise ValueError("Mode '{0}' is not supported. Possible modes are {1}".format(mode, ', '.join(LogFileTimeParser.POSSIBLE_MODES)))

        # Setup default times
        if not start:
            start = (datetime.today() - timedelta(hours=1)).strftime("%Y-%m-%d %H:%M:%S")
        if not end:
            end = datetime.today().strftime("%Y-%m-%d %H:%M:%S")

        # Clear cache
        open(LogFileTimeParser.FILE_PATH_REMOTE, 'w').close()
        if not hosts:
            hosts = [LogFileTimeParser._clean_text(sr.ip) for sr in StorageRouterList.get_storagerouters()]
        LogFileTimeParser._check_available_space(hosts, username, password)

        # Setup remote instances
        with remote(hosts, [LogFileTimeParser], username=username, password=password) as remotes:
            for host in hosts:
                results = ''
                if mode == 'search':
                    # Execute search
                     results = remotes[host].LogFileTimeParser().get_lines_between_timestamps(start=start, end=end,
                                                                                            paths_to_file=paths_to_file,
                                                                                            search_patterns=search_patterns,
                                                                                            debug=debug, host=host)
                elif mode == 'error-search':
                    # Execute search
                    results = remotes[host].LogFileTimeParser().search_for_errors(start=start, end=end,
                                                                                paths_to_file=paths_to_file,
                                                                                debug=debug, host=host,
                                                                                python_error=python_error)
                # Append output to cache
                with open(LogFileTimeParser.FILE_PATH_REMOTE, 'a') as f2:
                    f2.write(str(results))
        if not suppress_return:
            with open(LogFileTimeParser.FILE_PATH_REMOTE, 'r') as f:
                return f.read()

    @staticmethod
    def _parse_date(text, validate=True):
        """

        :param text: A date
        :type text: str / Datetime
        :param validate: Check to throw errors on malformatted input or not
        :type validate: Boolean
        :return:
        """
        # Supports Aug 16 14:59:01 , 2016-08-16 09:23:09 Jun 1 2005  1:33:06PM (with or without seconds, microseconds)
        # Supports datetime objects aswell, no need to convert others to strings
        if isinstance(text, datetime):
            return text
        for fmt in ('%Y-%m-%d %H:%M:%S %f', '%Y-%m-%d %H:%M:%S', '%Y-%m-%d %H:%M',
                    '%b %d %H:%M:%S %f', '%b %d %H:%M', '%b %d %H:%M:%S',
                    '%b %d %Y %H:%M:%S %f', '%b %d %Y %H:%M', '%b %d %Y %H:%M:%S',
                    '%b %d %Y %I:%M:%S%p', '%b %d %Y %I:%M%p', '%b %d %Y %I:%M:%S%p %f'):
            try:
                # Add a year stamp to Jan 01 formats -- else we get default 1900
                if fmt in ['%b %d %H:%M:%S %f', '%b %d %H:%M', '%b %d %H:%M:%S']:
                    return datetime.strptime(text, fmt).replace(datetime.now().year)
                return datetime.strptime(text, fmt)
            except ValueError:
                pass
            except TypeError:
                return datetime.min
                # Happens for weirdly formatted strings without escaped chars
        if validate:
            raise ValueError("No valid date format found for '{0}'".format(text))
        else:
            # Cannot use NoneType to compare date times. Using minimum instead
            return datetime.min

    # Function to read lines from file and extract the date and time
    def _read_line(self):
        """
        Read a line from a file
        Return a tuple containing:
            the date/time in a format supported in parse_date om the line itself
        """
        try:
            # readline() reads a single line at the time
            self.line = self.opened_file.readline(self.BUF_SIZE)
        except:
            raise IOError("File I/O Error")
        if self.line == '':
            raise EOFError("EOF reached")
        # Remove \n from read lines.
        self.line = self.line.rstrip('\n')
        # Disabled these checks due to the presence of unserialized data in certain files
        # else:
        #     if len(self.line) >= self.BUF_SIZE:
        #         raise ValueError("Line length exceeds buffer size for line '{0}' in file '{1}'".format(
        #             self.line, self.opened_file.name))
        #     else:
        #         raise ValueError("Missing newline")
        words = self.line.split(' ')
        # This results into Jan 1 01:01:01 000000 or 1970-01-01 01:01:01 000000 or just plain text sentences
        if len(words) >= 3:
            self.line_date = self._parse_date(words[0] + " " + words[1] + " " + words[2], False)
        else:
            self.line_date = self._parse_date('', False)
        return self.line_date, self.line

    @staticmethod
    def _clean_text(text):
        if type(text) is list:
            text = '\n'.join(line.rstrip() for line in text)
        # This strip is absolutely necessary. Without it, channel.communicate() is never executed (odd but true)
        try:
            cleaned = text.strip().decode('utf-8', 'replace')
            for old, new in {u'\u2018': "'",
                             u'\u201a': "'",
                             u'\u201e': '"',
                             u'\u201c': '"'}.iteritems():
                cleaned = cleaned.replace(old, new)
            return cleaned
        except UnicodeDecodeError:
            print 'UnicodeDecodeError with output: {0}'.format(text)
        raise

    def get_lines_between_timestamps(self, start, end, paths_to_file=PATHS_TO_FILES, search_patterns=None,
                                     debug=False, host=None, suppress_return=False):
        """
        :param start: Starting date
        :type start: str / Datetime
        :param end: End date
        :type end: str / Datetime
        :param search_patterns: What error patterns should be recognized
        :type search_patterns: List of str
        :param paths_to_file: List of paths of files that will be searched on all nodes
        :type paths_to_file: List of str
        :param debug: Boolean to debug or not
        :type debug: Boolean
        :param host: ip of the node
        :type host: str
        :param suppress_return: only write to file and not return contents
        :type suppress_return: Boolean
        :return: Output of a file as string
        """
        if not start:
            start = (datetime.today() - timedelta(hours=1)).strftime("%Y-%m-%d %H:%M:%S")
        if not end:
            end = datetime.today().strftime("%Y-%m-%d %H:%M:%S")
        # For benchmarking purposes
        start_time = time.time()
        # Clear the file cache
        open(self.FILE_PATH, 'w').close()

        for path_to_file in paths_to_file:
            # Set some initial values
            count = 0
            try:
                size = os.stat(path_to_file)[ST_SIZE]
            except OSError:
                # Prevent not found errors from stopping the logger
                continue
            begin_range = 0
            mid_range = size / 2
            old_mid_range = mid_range
            end_range = size
            pos1 = pos2 = 0
            # Set the default writing prefix
            if host:
                write_prefix = "{0} - {1}: ".format(host, path_to_file)
            else:
                write_prefix = "{0}: ".format(path_to_file)

            # Test for times to be properly formatted, allow hh:mm or hh:mm:ss
            pattern = re.compile(r'(^[2][0-3]|[0-1][0-9]):[0-5][0-9](:[0-5][0-9])?$')

            # If only hours are supplied, match them to a day
            if pattern.match(start) or pattern.match(end):
                # Determine Time Range
                yesterday = date.fromordinal(date.today().toordinal() - 1).strftime("%Y-%m-%d")
                today = datetime.now().strftime("%Y-%m-%d")
                now = datetime.now().strftime("%R")
                if start > now or start > end:
                    search_start = yesterday
                else:
                    search_start = today
                if end > start > now:
                    search_end = yesterday
                else:
                    search_end = today
                search_start = self._parse_date(search_start + " " + start)
                search_end = self._parse_date(search_end + " " + end)
            else:
                # Set dates
                search_start = self._parse_date(start)
                search_end = self._parse_date(end)

            # Start with reading - open file
            try:
                self.opened_file = open(path_to_file, 'r')
            except:
                raise IOError("File Open Error")
            if debug:
                print("File: '{0}' Size: {1} Start: '{2}' End: '{3}'".format(path_to_file, size, search_start, search_end))

            # Seek using binary search -- ONLY WORKS ON FILES WHO ARE SORTED BY DATES (should be true for log files)
            try:
                while pos1 != end_range and old_mid_range != 0 and self.line_date != search_start:
                    self.opened_file.seek(mid_range)
                    # sync to self.line ending
                    self.line_date, self.line = self._read_line()
                    pos1 = self.opened_file.tell()
                    # if not beginning of file, discard first read
                    if mid_range > 0:
                        if debug:
                            print("...partial: (len: {0}) '{1}'".format((len(self.line)), self.line))
                        self.line_date, self.line = self._read_line()
                    pos2 = self.opened_file.tell()
                    count += 1
                    if debug:
                        print("#{0} Beginning: {1} Mid: {2} End: {3} P1: {4} P2: {5} Timestamp: '{6}'".
                              format(count, begin_range, mid_range, end_range, pos1, pos2, self.line_date))
                    if search_start > self.line_date:
                        begin_range = mid_range
                    else:
                        end_range = mid_range
                    old_mid_range = mid_range
                    mid_range = (begin_range + end_range) / 2
                    if count > self.LIMIT:
                        raise IndexError("ERROR: ITERATION LIMIT EXCEEDED")
                if debug:
                    print("...stopping: '{0}'".format(self.line))
                # Rewind a bit to make sure we didn't miss any
                seek = old_mid_range
                while self.line_date >= search_start and seek > 0:
                    if seek < self.REWIND:
                        seek = 0
                    else:
                        seek -= self.REWIND
                    if debug:
                        print("...rewinding")
                    self.opened_file.seek(seek)
                    # sync to self.line ending
                    self.line_date, self.line = self._read_line()
                    if debug:
                        print("...junk: '{0}'".format(self.line))
                    self.line_date, self.line = self._read_line()
                    if debug:
                        print("...comparing: '{0}'".format(self.line_date))

                # Scan forward
                while self.line_date < search_start:
                    if debug:
                        print("...skipping: '{0}'".format(self.line_date))
                    self.line_date, self.line = self._read_line()
                if debug:
                    print("...found: '{0}'".format(self.line))
                if debug:
                    print("Beginning: {0} Mid: {1} End: {2} P1: {3} P2: {4} Timestamp: '{5}'".
                          format(begin_range, mid_range, end_range, pos1, pos2, self.line_date))

                # Now that the preliminaries are out of the way, we just loop,
                # Reading lines and printing them until they are beyond the end of the range we want
                while self.line_date <= search_end:
                    # Search for the search patterns before printing
                    if search_patterns:
                        if any(pattern.lower() in self.line.lower() for pattern in search_patterns):
                            # Write to file to save memory and finally return its contents
                            with open(self.FILE_PATH, 'a') as f:
                                f.write(str(write_prefix + LogFileTimeParser._clean_text(self.line) + '\n'))
                    else:
                        # Write to file to save memory and finally return its contents
                        with open(self.FILE_PATH, 'a') as f:
                            f.write(str(write_prefix + LogFileTimeParser._clean_text(self.line) + '\n'))
                    self.line_date, self.line = self._read_line()
                if debug:
                    print("Start: '{0}' End: '{1}'".format(search_start, search_end))
            # Do not display EOFErrors:
            except EOFError:
                pass
            finally:
                # Try to close the file
                if self.opened_file is not None:
                    try:
                        self.opened_file.close()
                    except:
                        pass

        with open(self.FILE_PATH + '_debug', 'w') as debug_file:
            debug_file.write(str('Will read output'))
            debug_file.write(str("--- Searching took {0} seconds ---".format(time.time() - start_time)))
        # Return the contents of the file
        with open(self.FILE_PATH, 'r') as f:
            # Print execution time:
            if debug:
                print("--- Searching took {0} seconds ---".format(time.time() - start_time))
            if not suppress_return:
                # Check if first line isn't empty
                f.seek(0)
                if f.readline() == '':
                    return None
                return f.read()

    def search_for_errors(self, start, end, python_error=False, paths_to_file=PATHS_TO_FILES, debug=False, host=None,
                          suppress_return=False):
        """
        :param start: Starting date
        :type start: str / Datetime
        :param end: End date
        :type end: str / Datetime
        :param python_error: Whether only python errors should be checked
        :type python_error: Boolean
        :param paths_to_file: List of paths of files that will be searched on all nodes
        :type paths_to_file: List of str
        :param debug: Boolean to debug or not
        :type debug: Boolean
        :param host: ip of the node
        :type host: str
        :param suppress_return: only write to file and not return contents
        :type suppress_return: Boolean
        :return: Output of a file as string
        """
        if not start:
            start = (datetime.today() - timedelta(hours=1)).strftime("%Y-%m-%d %H:%M:%S")
        if not end:
            end = datetime.today().strftime("%Y-%m-%d %H:%M:%S")
        python_errors = ['Exception', 'StopIteration', 'StandardError', 'BufferError', 'ArithmeticError',
                         'FloatingPointError', 'OverflowError', 'ZeroDivisionError', 'AssertionError', 'AttributeError',
                         'EnvironmentError', 'IOError', 'OSError', 'WindowsError', 'VMSError', 'EOFError',
                         'ImportError', 'LookupError', 'IndexError', 'KeyError', 'MemoryError', 'NameError',
                         'UnboundLocalError', 'ReferenceError', 'RuntimeError', 'NotImplementedError', 'SyntaxError',
                         'IndentationError', 'TabError', 'SystemError', 'TypeError', 'ValueError', 'UnicodeError',
                         'UnicodeDecodeError', 'UnicodeEncodeError', 'UnicodeTranslateError']

        errors_patterns = ['not found', 'error', 'something went wrong', ' has no attribute',
                           'referenced before assignment', 'timeout', 'raised exception']
        if python_error:
            return self.get_lines_between_timestamps(start, end, paths_to_file, python_errors, debug, host,
                                                     suppress_return)
        else:
            return self.get_lines_between_timestamps(start, end, paths_to_file, errors_patterns, debug, host,
                                                     suppress_return)


