#!/usr/bin/env python
# Copyright (C) 2017 iNuron NV
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

"""
class RepoMapper
"""

import os
import re
from subprocess import Popen, PIPE


class RepoMapper(object):
    """
    Class responsible for fetching the repository based on the branch that Travis is building
    """
    MAPPING = {'master': 'unstable',
               'develop': 'fwk-develop'}

    @classmethod
    def get_repository(cls):
        """
        Gets the repository associated with the branch travis is working on
        :return: Name of the repository
        :rtype: str
        """
        branch = os.environ.get('TRAVIS_BRANCH')
        if branch not in cls.MAPPING:
            # If the branch is not in the mapping by default, figure out it's parent
            branch = cls.determine_parent()  # Let it throw an error when it could not determine the parent
            if branch not in cls.MAPPING:
                raise RuntimeError('Unable to fetch the right release for branch {0}'.format(branch))
        return cls.MAPPING[branch]

    @classmethod
    def determine_parent(cls):
        """
        When working with a branch which branched of from either master or develop
        knowing which branch it was is necessary to install the correct packaged
        :raises RunTimeError: If the branch to build is being used to go into a different branch than develop/master
        :raises RunTimeError: If the current HEAD cannot be determined
        :raises RunTimeError: If the parent branch name could not be found
        :return: Parent branch
        :rtype: str
        """
        # Validation
        if os.environ.get('TRAVIS_PULL_REQUEST') is False:
            # Travis will be performing a merge and run the tests on the result of the merge when it's a PR
            # To currently check which apt-repo to use, we need to restore the HEAD reference by checking out the branch we would test on
            # However this would mean we'd lose the merge and thus would be testing the wrong things
            raise RuntimeError('Travis is currently not supporting pull requests on branches other than master and develop')
        # Restore HEAD
        cls.restore_git_head()
        # Git show branch will output something like: (https://wincent.com/wiki/Understanding_the_output_of_%22git_show-branch%22)
        # ! [Documentation-typo] Correct typo     -> non head branch (indicated by the !) + latest commit
        #  ! [active_drive] Review comments       -> Currently checked out head (indicated by the *) + latest commit
        #   * [develop] Show osd status on detail page
        #    ! [master] Merge pull request #433 from openvstorage/develop
        # ----                                     -> Actual commit history (above are table headers of a sort)
        # +    [Documentation-typo] Correct typo  -> all the lines labelled with a * are commits on the HEAD branch (note how the + or * or - line up with the columns on the top)
        #  +   [active_drive] Review comments     -> all of the lines labelled with + are commits on another branch
        #  +   [active_drive^] Alba json cleanup  -> Any lines labelled with - (none in the example diagram) are merge commits
        current_branch_name = Popen(['git', 'rev-parse', '--abbrev-ref', 'HEAD'], stdout=PIPE, stderr=PIPE).communicate()[0].replace('\n', '')  # Alternative to git branch | grep \* | cut -d ' ' -f2
        # Check on which column the current branch lives
        show_branch_output = Popen(['git', 'show-branch', '--all'], stdout=PIPE, stderr=PIPE).communicate()[0]
        table_separator = re.compile("^[\-]*$")  # Table is separated from its header with only ---
        branch_filter = re.compile("^[^\[]*\[{0}".format(current_branch_name))  # Filter out results from own branch
        matching_commit_regex = None
        # Figure out on what column the current head resides
        in_table_header = True
        current_branch_column = None
        parent_branch_name = None
        for line in show_branch_output.splitlines():
            if in_table_header is True:
                if line.strip().startswith('*'):
                    # Found the current head line, determine the column
                    current_branch_column = line.index('*')
                    # Avoid fetching the commits with '*   ' as the columns should be completely filled
                    matching_commit_regex = re.compile("^.{%s}[^ ].*\[(origin/)?([\w-]*).*\].*" % current_branch_column)
                    continue
                if re.match(table_separator, line.strip()):
                    in_table_header = False
            else:
                if current_branch_column is None:
                    raise RuntimeError('Could not check the current head')
                if not re.match(branch_filter, line):
                    match = re.match(matching_commit_regex, line)
                    if match:
                        # Match found, name is within the second group
                        parent_branch_name = match.groups()[1]
                        # Clean it up
                        break
        if parent_branch_name is None:
            raise RuntimeError('Unable to fetch the parent branch name')
        return parent_branch_name

    @staticmethod
    def restore_git_head():
        """
        Attempts to restore a detached HEAD back to its HEAD
        Reconfigure the git config to fetch all remote metadata
        :return: None
        :rtype: NoneType
        """
        branch = os.environ.get('TRAVIS_BRANCH')
        travis_commit = os.environ.get('TRAVIS_COMMIT')
        # Restore HEAD reference by checking it out and discarding other commits up to the commit the build was triggered

        Popen(['git', 'checkout', str(branch)], stdout=PIPE, stderr=PIPE).communicate()
        Popen(['git', 'reset', '--hard', str(travis_commit)], stdout=PIPE, stderr=PIPE).communicate()
        # Fetch references to other branches to make sure we can detect of which branch we currently branch off
        Popen(['git', 'config', '--replace-all', 'remote.origin.fetch', '+refs/heads/*:refs/remotes/origin/*'], stdout=PIPE, stderr=PIPE).communicate()
        Popen(['git', 'fetch'], stdout=PIPE, stderr=PIPE).communicate()

    @staticmethod
    def remove_prefix(text, prefix):
        """
        Removes a given prefix from a string
        :param text: String to remove prefix from
        :param prefix: Prefix to remove
        :return: Passed in text with or without the prefix
        :rtype: str
        """
        if text.startswith(prefix):
            return text[len(prefix):]
        return text  # or whatever


if __name__ == '__main__':
    # Make sure it gets outputted to stdout for the Travis build to capture
    print RepoMapper.get_repository()
