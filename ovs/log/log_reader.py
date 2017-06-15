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
import stat
import platform
import subprocess
import unicodedata
from datetime import date, datetime, timedelta
from ovs_extensions.generic.remote import remote
from ovs.extensions.generic.sshclient import SSHClient
from ovs.extensions.generic.system import System
from ovs.dal.lists.storagerouterlist import StorageRouterList
from ovs.log.log_handler import LogHandler


class LogFileTimeParser(object):
    """
    # @TODO Support for rotated log files

    Extracts parts of a log file based on a start and end date
    Uses binary search logic to speed up searching
    Can filter out errors based on string patterns

    Hardly uses any memory - cpu intensive though
    Writes results to a file and returns the contents of this file to save memory

    Common usage: validate log files during testing
    """
    # Set some initial values
    BUF_SIZE = 4096  # handle long lines, but put a limit to them
    REWIND = 100  # arbitrary, the optimal value is highly dependent on the structure of the file
    LIMIT = 75  # arbitrary, allow for a VERY large file, but stop it if it runs away
    STANDARD_SEARCH_LOCATIONS = ['/var/log/ovs/lib.log', '/var/log/ovs/extensions.log']

    INTERNAL_MAPPING = {'ovs-workers': {'file': '/var/log/upstart/ovs-workers.log',
                                        'journal': 'ovs-workers.service'},
                        'ovs-webapp-api': {'file': '/var/log/upstart/ovs-webapp-api.log',
                                           'journal': 'ovs-webapp-api.service'}}
    FILE_PATH = '/tmp/file_reader_cache'
    FILE_PATH_REMOTE = '/tmp/file_reader_cache_all'
    TEMP_FILE_PATH = '/tmp/file_reader.py'
    POSSIBLE_MODES = ['search', 'error-search']

    SUPPORTED_VERSIONS = {'Ubuntu': {'14.04': 'file',
                                     '16.04': 'journal'}}

    # Timeout for ssh: 0.0 means no timeout
    TIMEOUT = 0.0

    logger = LogHandler.get('log', name='log_reader')

    @staticmethod
    def _get_execution_mode():
        """
        Determines the execution mode: 14.04 -> log files 16.04 -> journal
        :return: execution mode
        """
        os_info = platform.linux_distribution()
        if os_info[0] in LogFileTimeParser.SUPPORTED_VERSIONS:
            if os_info[1] in LogFileTimeParser.SUPPORTED_VERSIONS[os_info[0]]:
                execution_mode = LogFileTimeParser.SUPPORTED_VERSIONS[os_info[0]][os_info[1]]
            else:
                raise RuntimeError('Logreading is not supported for {0} version {1}'.format(os_info[0], os_info[1]))
        else:
            raise RuntimeError('Logreading is not supported for {0}'.format(os_info[0]))
        return execution_mode

    @staticmethod
    def _check_available_space(hosts, username, password):
        required_space = 0
        output_dir = '/'.join(LogFileTimeParser.FILE_PATH_REMOTE.strip('/').rsplit('/', 1)[0])
        get_space_command = 'df /tmp | cut -d " " -f 10 | grep -Eo [0-9]*'
        local_client = SSHClient(System.get_my_storagerouter().ip, username=username, password=password)
        available_space = local_client.run(get_space_command, allow_insecure=True)

        for host in hosts:
            ssh_client = SSHClient(host, username=username, password=password)
            for file_path in LogFileTimeParser.STANDARD_SEARCH_LOCATIONS:
                command = 'ls -al {0} | cut -d " " -f 5'.format(file_path)
                try:
                    required_space += int(ssh_client.run(command, allow_insecure=True))
                except ValueError:
                    pass
        if required_space > available_space:
            raise OverflowError('Would not be able to allocate {0} in {1}'.format(required_space, output_dir))

    @staticmethod
    def execute_search_on_remote(since=None, until=None, search_locations=None, hosts=None, python_error=False,
                                 mode='search', username='root', password=None, suppress_return=False, search_patterns=None):
        """
        Searches all hosts for entries between given dates.
        Can be used standalone on the execution machine
        :param since: Starting date
        :type since: str / Datetime
        :param until: End date
        :type until: str / Datetime
        :param search_locations: list of paths of files / servicenames that will be searched on all nodes
        :type search_locations: list of str
        :param hosts: Ip of the nodes
        :type hosts: list of str
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
        :param search_patterns: What error patterns should be recognized
        :type search_patterns: list of str
        :return: Output of a file as string
        """
        # Validate parameter
        if mode not in LogFileTimeParser.POSSIBLE_MODES:
            raise ValueError('Mode "{0}" is not supported. Possible modes are {1}'.format(mode, ', '.join(LogFileTimeParser.POSSIBLE_MODES)))

        # Clear cache
        open(LogFileTimeParser.FILE_PATH_REMOTE, 'w').close()

        since, until, search_locations, hosts = LogFileTimeParser._default_starting_values(since, until, search_locations, hosts)

        # Setup remote instances
        with remote(hosts, [LogFileTimeParser], username=username, password=password) as remotes:
            for host in hosts:
                results = ''
                if mode == 'search':
                    # Execute search
                    results = remotes[host].LogFileTimeParser.get_lines_between_timestamps(since=since, until=until,
                                                                                           search_locations=search_locations,
                                                                                           search_patterns=search_patterns,
                                                                                           host=host)
                elif mode == 'error-search':
                    # Execute search
                    results = remotes[host].LogFileTimeParser.search_for_errors(since=since, until=until,
                                                                                search_locations=search_locations,
                                                                                host=host,
                                                                                python_error=python_error)
                # Append output to cache
                with open(LogFileTimeParser.FILE_PATH_REMOTE, 'a') as output_file:
                    output_file.write(str(results))
        if not suppress_return:
            with open(LogFileTimeParser.FILE_PATH_REMOTE, 'r') as output_file:
                return output_file.read()

    @staticmethod
    def _parse_date(text, validate=True, to_string=False):
        """
        Parses strings to datetime objects
        :param text: A date
        :type text: str / Datetime
        :param validate: Check to throw errors on malformatted input or not
        :type validate: bool
        :return:
        """
        def _get_datetime(_text, _validate):
            # Supports Aug 16 14:59:01 , 2016-08-16 09:23:09 Jun 1 2005  1:33:06PM (with or without seconds, microseconds)
            # Supports datetime objects as well, no need to convert others to strings
            if isinstance(_text, datetime):
                return _text
            for fmt in ('%Y-%m-%d %H:%M:%S %f', '%Y-%m-%d %H:%M:%S', '%Y-%m-%d %H:%M',
                        '%b %d %H:%M:%S %f', '%b %d %H:%M', '%b %d %H:%M:%S',
                        '%b %d %Y %H:%M:%S %f', '%b %d %Y %H:%M', '%b %d %Y %H:%M:%S',
                        '%b %d %Y %I:%M:%S%p', '%b %d %Y %I:%M%p', '%b %d %Y %I:%M:%S%p %f'):
                try:
                    # Add a year stamp to Jan 01 formats -- else we get default 1900
                    if fmt in ['%b %d %H:%M:%S %f', '%b %d %H:%M', '%b %d %H:%M:%S']:
                        return datetime.strptime(_text, fmt).replace(datetime.now().year)
                    return datetime.strptime(_text, fmt)
                except ValueError:
                    pass
                except TypeError:
                    return datetime.min
                    # Happens for weirdly formatted strings without escaped chars
            if _validate:
                raise ValueError('No valid date format found for "{0}"'.format(_text))
            else:
                # Cannot use NoneType to compare date times. Using minimum instead
                return datetime.min
        if to_string is True:
            return _get_datetime(text, validate).strftime('%Y-%m-%d %H:%M:%S')
        else:
            return _get_datetime(text, validate)

    # Function to read lines from file and extract the date and time
    @staticmethod
    def _read_line(opened_file, buf_size=BUF_SIZE, tail_amount=0):
        """
        Read a line from a file
        Return a tuple containing:
            the date/time in a format supported in parse_date om the line itself
        :param tail_amount: amount of lines to read from the end
        :type tail_amount: int
        """
        if tail_amount == 0:
            try:
                # readline() reads a single line at the time
                line = opened_file.readline(buf_size)
            except:
                raise IOError('File I/O Error')
            if line == "":
                # End of file reached
                raise EOFError("End of file.")
            # Remove \n from read lines.
            line = line.rstrip('\n')
            words = line.split(' ')
            # This results into Jan 1 01:01:01 000000 or 1970-01-01 01:01:01 000000 or just plain text sentences
            if len(words) >= 3:
                line_date = LogFileTimeParser._parse_date(words[0] + ' ' + words[1] + ' ' + words[2], False)
            else:
                line_date = LogFileTimeParser._parse_date('', False)
            return line_date, line
        else:
            opened_file.seek(0, 2)
            data_bytes = opened_file.tell()
            size = tail_amount + 1
            block = -1
            data = []
            while size > 0 and data_bytes > 0:
                if data_bytes - buf_size > 0:
                    # Seek back one whole BUFSIZ
                    opened_file.seek(block * buf_size, 2)
                    # read BUFFER
                    data.insert(0, opened_file.read(buf_size))
                else:
                    # file too small, start from begining
                    opened_file.seek(0, 0)
                    # only read what was not read
                    data.insert(0, opened_file.read(data_bytes))
                lines_found = data[0].count('\n')
                size -= lines_found
                data_bytes -= buf_size
                block -= 1
            # Extract line dates
            output = []
            lines = ''.join(data).splitlines()[-tail_amount:]
            for line in lines:
                words = line.split(' ')
                # This results into Jan 1 01:01:01 000000 or 1970-01-01 01:01:01 000000 or just plain text sentences
                if len(words) >= 3:
                    line_date = LogFileTimeParser._parse_date(words[0] + ' ' + words[1] + ' ' + words[2], False)
                else:
                    line_date = LogFileTimeParser._parse_date('', False)
                output.append((line_date, line))
            return output

    @staticmethod
    def _clean_text(text):
        if type(text) is list:
            text = '\n'.join(line.rstrip() for line in text)
        try:
            # This strip is absolutely necessary. Without it, channel.communicate() is never executed (odd but true)
            cleaned = text.strip()
            # I ? unicode
            if not isinstance(text, unicode):
                cleaned = unicode(cleaned.decode('utf-8', 'replace'))
            for old, new in {u'\u2018': "'",
                             u'\u2019': "'",
                             u'\u201a': "'",
                             u'\u201e': '"',
                             u'\u201c': '"',
                             u'\u25cf': '*'}.iteritems():
                cleaned = cleaned.replace(old, new)
            cleaned = unicodedata.normalize('NFKD', cleaned)
            cleaned = cleaned.encode('ascii', 'ignore')
            return cleaned
        except UnicodeDecodeError:
            LogFileTimeParser.logger.error('UnicodeDecodeError with output: {0}'.format(text))
            raise

    @staticmethod
    def get_lines_between_timestamps(since=None, until=None, search_locations=None, search_patterns=None, host=None, suppress_return=False):
        """
        Searches the given searchlocations for entires.
        Can be used standalone on the execution machine
        :param since: Starting date
        :type since: str / Datetime
        :param until: End date
        :type until: str / Datetime
        :param search_patterns: What error patterns should be recognized
        :type search_patterns: list of str
        :param search_locations: list of paths of files that will be searched on all nodes
        :type search_locations: list of str
        :param host: ip of the node
        :type host: str
        :param suppress_return: only write to file and not return contents
        :type suppress_return: bool
        :return: Output of a file as string
        """
        # Clear the file cache
        open(LogFileTimeParser.FILE_PATH, 'w').close()

        since, until, search_locations, _ = LogFileTimeParser._default_starting_values(since, until, search_locations)

        for search_location in search_locations:
            if os.path.isfile(search_location):
                LogFileTimeParser._search_file(since, until, search_location, search_patterns, host)
            else:
                try:
                    LogFileTimeParser._search_journal(since, until, search_location, search_patterns, host)
                except RuntimeError as ex:
                    if 'Failed to add filter for units: No data available'.lower() not in str(ex).lower():  # Means no unit file was found, ignore
                        raise
        # Return the contents of the file
        if not suppress_return and host is not None:
            with open(LogFileTimeParser.FILE_PATH, 'r') as f:
                # Check if first line isn't empty
                f.seek(0)
                if f.readline() == '':
                    return None
                return f.read()

    @staticmethod
    def _search_journal(since, until, search_location, search_patterns, host):
        """
        Searches journal for entries between specified dates
        :param since: Starting date
        :type since: str / Datetime
        :param until: End date
        :type until: str / Datetime
        :param search_patterns: What error patterns should be recognized
        :type search_patterns: list of str
        :param search_location: path of file that will be searched on all nodes
        :type search_location: str
        :param host: ip of the node
        :type host: str
        :return: Output of a file as string
        """
        # Set the default writing prefix
        if host:
            write_prefix = "{0} - {1}: ".format(host, search_location)
        else:
            write_prefix = "{0}: ".format(search_location)

        since = LogFileTimeParser._parse_date(since, to_string=True)
        until = LogFileTimeParser._parse_date(until, to_string=True)
        cmd = [
            "journalctl",
            "--unit",
            search_location,
            "--since",
            since,
            "--until",
            until,
            "--output",
            "cat",
            "--no-pager"
        ]
        p = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        stdout = p.stdout.read()
        stderr = p.stderr.read()
        if stderr != "":
            raise RuntimeError("Error occurred. Got {0}.".format(stderr))
        for line in stdout.splitlines():
            if search_patterns:
                if any(pattern.lower() in line.lower() for pattern in search_patterns):
                    # Write to file to save memory and finally return its contents
                    with open(LogFileTimeParser.FILE_PATH, 'a') as f:
                        f.write(str(write_prefix + LogFileTimeParser._clean_text(line) + '\n'))
            else:
                # Write to file to save memory and finally return its contents
                with open(LogFileTimeParser.FILE_PATH, 'a') as f:
                    f.write(str(write_prefix + LogFileTimeParser._clean_text(line) + '\n'))

    @staticmethod
    def _search_file(since, until, search_location, search_patterns, host, limit=LIMIT, rewind=REWIND):
        """
        Seaches a file for entries between the given dates
        :param since: Starting date
        :type since: str / Datetime
        :param until: End date
        :type until: str / Datetime
        :param search_patterns: What error patterns should be recognized
        :type search_patterns: list of str
        :param search_location: path of file that will be searched on all nodes
        :type search_location: str
        :param host: ip of the node
        :type host: str
        :return: Output of a file as string
        """
        # validate if file is present
        if not os.path.isfile(search_location):
            return
        # Set some initial values
        count = 0
        line_date = None

        size = os.stat(search_location)[stat.ST_SIZE]

        begin_range = 0
        mid_range = size / 2
        old_mid_range = mid_range
        end_range = size
        pos = 0
        # Set the default writing prefix
        if host:
            write_prefix = '{0} - {1}: '.format(host, search_location)
        else:
            write_prefix = '{0}: '.format(search_location)

        # Start with reading - open file
        with open(search_location, 'r') as opened_file:
            # Seek using binary search -- ONLY WORKS ON FILES WHO ARE SORTED BY DATES (should be true for log files)
            try:
                last_date, line = LogFileTimeParser._read_line(opened_file, tail_amount=1)[0]
                if last_date < since:
                    return
                while pos != end_range and old_mid_range != 0 and line_date != since:
                    # Check whether the since date in even in the file.
                    opened_file.seek(0, 2)
                    opened_file.seek(mid_range)
                    # sync to line ending
                    line_date, line = LogFileTimeParser._read_line(opened_file)
                    pos = opened_file.tell()
                    # if not beginning of file, discard first read due to potential wrong dates
                    if mid_range > 0:
                        line_date, line = LogFileTimeParser._read_line(opened_file)
                    count += 1
                    if since > line_date:
                        begin_range = mid_range
                    else:
                        end_range = mid_range
                    old_mid_range = mid_range
                    mid_range = (begin_range + end_range) / 2
                    if count > limit:
                        raise IndexError('ERROR: ITERATION LIMIT EXCEEDED')
                # Go back some steps and check if we did not miss any lines
                seek = old_mid_range
                while line_date >= since and seek > 0:
                    "Scanning backwards"
                    if seek < rewind:
                        seek = 0
                    else:
                        seek -= rewind
                    opened_file.seek(seek)
                    # sync to line ending
                    line_date, line = LogFileTimeParser._read_line(opened_file)

                # Scan forward in the current section
                while line_date < since:
                    line_date, line = LogFileTimeParser._read_line(opened_file)

                # Now that the preliminaries are out of the way, we just loop,
                # Reading lines and printing them until they are beyond the until of the range we want
                while line_date <= until:
                    # Search for the search patterns before printing
                    if search_patterns:
                        if any(pattern.lower() in line.lower() for pattern in search_patterns):
                            # Write to file to save memory and finally return its contents
                            with open(LogFileTimeParser.FILE_PATH, 'a') as f:
                                f.write(str(write_prefix + LogFileTimeParser._clean_text(line) + '\n'))
                    else:
                        # Write to file to save memory and finally return its contents
                        with open(LogFileTimeParser.FILE_PATH, 'a') as f:
                            f.write(str(write_prefix + LogFileTimeParser._clean_text(line) + '\n'))
                    line_date, line = LogFileTimeParser._read_line(opened_file)
            # Do not display EOFErrors:
            except EOFError:
                pass

    @staticmethod
    def search_for_errors(since=None, until=None, python_error=False, search_locations=None, host=None, suppress_return=False):
        """
        Searches for errors in the supplied files.
        Can be used standalone on the execution machine
        :param since: Starting date
        :type since: str / Datetime
        :param until: End date
        :type until: str / Datetime
        :param python_error: Whether only python errors should be checked
        :type python_error: bool
        :param search_locations: list of paths of files that will be searched on all nodes
        :type search_locations: list of str
        :param host: ip of the node
        :type host: str
        :param suppress_return: only write to file and not return contents
        :type suppress_return: bool
        :return: Output of a file as string
        """
        since, until, search_locations, _ = LogFileTimeParser._default_starting_values(since, until, search_locations)
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
            return LogFileTimeParser.get_lines_between_timestamps(since, until, search_locations, python_errors, host, suppress_return)
        else:
            return LogFileTimeParser.get_lines_between_timestamps(since, until, search_locations, errors_patterns, host, suppress_return)

    @staticmethod
    def _default_starting_values(since=None, until=None, search_locations=None, hosts=None):

        if until and since is not None and isinstance(since, str) and isinstance(until, str):
            # Test for times to be properly formatted, allow hh:mm or hh:mm:ss
            pattern = re.compile(r'(^[2][0-3]|[0-1][0-9]):[0-5][0-9](:[0-5][0-9])?$')
            # If only hours are supplied, match them to a day
            if pattern.match(since) or pattern.match(until):
                # Determine Time Range
                yesterday = date.fromordinal(date.today().toordinal() - 1).strftime('%Y-%m-%d')
                today = datetime.now().strftime('%Y-%m-%d')
                now = datetime.now().strftime('%R')
                if since > now or since > until:
                    search_start = yesterday
                else:
                    search_start = today
                if until > since > now:
                    search_end = yesterday
                else:
                    search_end = today
                since = LogFileTimeParser._parse_date(search_start + ' ' + since)
                until = LogFileTimeParser._parse_date(search_end + ' ' + until)
            else:
                # Set dates
                since = LogFileTimeParser._parse_date(since)
                until = LogFileTimeParser._parse_date(until)
        # Setup default times
        if since is None:
            since = (datetime.today() - timedelta(hours=1))
        if until is None:
            until = datetime.today()

        if search_locations is None:
            execution_mode = LogFileTimeParser._get_execution_mode()
            search_locations = LogFileTimeParser.STANDARD_SEARCH_LOCATIONS
            for service, info in LogFileTimeParser.INTERNAL_MAPPING.iteritems():
                search_locations.append(info.get(execution_mode))

        if not hosts:
            hosts = [sr.ip for sr in StorageRouterList.get_storagerouters()]

        return since, until, search_locations, hosts
