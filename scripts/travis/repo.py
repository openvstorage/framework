#!/usr/bin/env python
import re
import os
from subprocess import check_output


class RepoMapper(object):

    mapping = {'master': 'unstable',
               'develop': 'fwk-develop'}

    @classmethod
    def get_repository(cls):
        """
        Gets the repository associated with the branch travis is working on
        :return:
        """
        branch = os.environ.get('TRAVIS_BRANCH')
        mapping = {'master': 'unstable',
                   'develop': 'fwk-develop'}
        if branch not in mapping:
            # If the branch is not in the mapping by default, figure out it's parent
            branch = cls.determine_parent()  # Let it throw an error when it could not determine the parent
            if branch not in mapping:
                raise RuntimeError('Unable to fetch the right repository for branch {0}'.format(branch))
        return mapping[branch]

    @classmethod
    def determine_parent(cls):
        """
        When working with a branch which branched of from either master or develop
        knowing which branch it was is necessary to install the correct packaged
        :return: parent branch
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
        current_branch_name = check_output('git rev-parse --abbrev-ref HEAD', shell=True).replace('\n', '')  # Alternative to git branch | grep \* | cut -d ' ' -f2
        # Check on which column the current branch lives
        show_branch_output = check_output('git show-branch --all', shell=True)
        table_separator = re.compile("^[\-]*$")  # Table is separated from its header with only ---
        branch_filter = re.compile("^[^\[]*\[{0}".format(current_branch_name))  # Filter out results from own branch
        matching_commit_regex = None
        # Figure out on what column the current head resides
        in_table_header = True
        commit_message_start = None
        parent_branch_name = None
        for line in show_branch_output.splitlines():
            if line.strip().startswith('*'):
                # Found the current head line, determine the column
                commit_message_start = line.index('*')
                # Avoid fetching the commits with '*   ' as the columns should be completely filled
                matching_commit_regex = re.compile("^.{%s}[^ ]" % commit_message_start)
                continue
            if re.match(table_separator, line.strip()):
                in_table_header = False
                continue
            if in_table_header is False:
                if commit_message_start is None:
                    raise RuntimeError('Could not check the current head')
                if not re.match(branch_filter, line):
                    if re.match(matching_commit_regex, line):
                        # Match found, extract the name from in between []
                        parent_branch_name = line.split('[', 1)[1].split(']', 1)[0]
                        # Clean it up
                        pattern = re.compile('[\^~].*')
                        parent_branch_name = re.sub(pattern, '', parent_branch_name)
                        parent_branch_name = cls.remove_prefix(parent_branch_name, 'origin/')
                        break
        if parent_branch_name is None:
            raise RuntimeError('Unable to fetch the parent branch name')
        return parent_branch_name

    @staticmethod
    def restore_git_head():
        branch = os.environ.get('TRAVIS_BRANCH')
        travis_commit = os.environ.get('TRAVIS_COMMIT')
        print "Resetting branch {0} to {1}".format(branch, travis_commit)
        # Restore HEAD reference by checking it out and discarding other commits up to the commit the build was triggered
        check_output('git checkout {0}'.format(branch), shell=True)
        check_output('git reset --hard {0}'.format(travis_commit))
        # Fetch references to other branches to make sure we can detect of which branch we currently branch off
        check_output('git config --replace-all remote.origin.fetch +refs/heads/*:refs/remotes/origin/*')
        check_output('git fetch')

    @staticmethod
    def remove_prefix(text, prefix):
        if text.startswith(prefix):
            return text[len(prefix):]
        return text  # or whatever

if __name__ == '__main__':
    print RepoMapper.get_repository()
