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

"""
SourceCollector module
"""

import os
import time
import re
from ConfigParser import RawConfigParser
from datetime import datetime
from subprocess import check_output


class SourceCollector(object):
    """
    SourceCollector class

    Responsible for creating a source archive which will contain:
    * All sources for that version
    * Versioning metadata
    * Full changelog
    It will also update the repo with all required versioning tags, if appropriate
    """

    repo = 'openvstorage/openvstorage'
    repo_path_metadata = '/tmp/repo_openvstorage_metadata'
    repo_path_code = '/tmp/repo_openvstorage_code'
    package_path = '/tmp/packages/openvstorage'

    def __init__(self):
        """
        Dummy init method, SourceCollector is static
        """
        raise NotImplementedError('SourceCollector is a static class')

    @staticmethod
    def collect(target, revision=None, suffix=None):
        """
        Executes the source collecting logic

        General steps:
        1. Figure out correct code revision, update code repo to that revision
        2. Tag that revision, if required
        3. Generate changelog, if required
        4. Generate version schema
        5. Build 'upstream source package'
        6. Use this 'upstream source package' for building distribution specific packages

        @param target: Specifies the pacakging target. Can be:
        * 'experimental' to package against a checked out repo/code
        * 'unstable', 'alpha', 'beta' for automatic packaging for a certain branch
        * ('release', '<release branch>') for autoamtic packaging for a release branch
        @param revision: Specifies an exact target revision:
        * Any existing tag for repackging 'alpha', 'beta' or 'release' packages
        @param suffix: A suffix for release packages (such as 'alpha', 'beta', 'rc1', 'rtm', ...)
        """

        print 'Collecting sources'

        if not os.path.exists(SourceCollector.repo_path_code):
            os.makedirs(SourceCollector.repo_path_code)
        if not os.path.exists(SourceCollector.repo_path_metadata):
            os.makedirs(SourceCollector.repo_path_metadata)
        if not os.path.exists(SourceCollector.package_path):
            os.makedirs(SourceCollector.package_path)

        # Update the metadata repo
        print '  Updating metadata'
        SourceCollector._hg_update_to(SourceCollector.repo_path_metadata, 'default')

        print '  Updating code'
        distribution = target  # experimental, unstable, alpha, beta, release
        if target == 'experimental':
            suffix = 'exp'
        elif isinstance(target, tuple) and target[0] == 'experimental':
            print '    Using feature branch {0}'.format(target[1])
            suffix = 'exp'
            distribution = target[0]
            SourceCollector._hg_update_to(SourceCollector.repo_path_code, target[1])
        elif target == 'unstable':
            suffix = 'rev'
            if revision is None:
                SourceCollector._hg_update_to(SourceCollector.repo_path_code, 'default')
            else:
                SourceCollector._hg_update_to(SourceCollector.repo_path_code, revision)
        elif target in ['alpha', 'beta']:
            branch = 'test' if target == 'alpha' else 'stable'
            suffix = target
            if revision is None:
                SourceCollector._hg_update_to(SourceCollector.repo_path_code, branch)
            else:
                SourceCollector._hg_update_to(SourceCollector.repo_path_code, revision)
        elif isinstance(target, tuple) and target[0] == 'release':
            if len(target) != 2 and revision is None:
                raise ValueError('In case a release build is requested, the exact release branch should be passed.')
            distribution = target[0]
            SourceCollector._hg_update_to(SourceCollector.repo_path_code, target[1] if revision is None else revision)
        else:
            raise ValueError('Invalid target specified')

        if distribution == 'release':
            raise NotImplementedError('Release packaging is not yet fully tested. Please fork the repo and test first')

        # Get parent brances
        branches = ['default']
        if distribution == 'alpha':
            branches.append('test')
        elif distribution == 'beta':
            branches += ['test', 'stable']
        elif distribution == 'release':
            branches += ['test', 'stable', target[1] if revision is None else revision]

        # Get current revision
        print '  Fetch current revision'
        current_revision = int(SourceCollector._run(
            'hg summary', SourceCollector.repo_path_code
        ).split('\n')[0].split(':')[1].strip())
        print '    Revision: {0}'.format(current_revision)

        # Get revision timestamp
        timestamp = eval(SourceCollector._run("hg log -r {0} --template '{{date}}'".format(current_revision),
                                              SourceCollector.repo_path_code))
        revision_date = datetime.fromtimestamp(timestamp)

        # Build version
        filename = '{0}/packaging/version.cfg'.format(SourceCollector.repo_path_code)
        parser = RawConfigParser()
        parser.read(filename)
        version = '{0}.{1}.{2}'.format(parser.get('main', 'major'),
                                       parser.get('main', 'minor'),
                                       parser.get('main', 'patch'))
        print '  Version: {0}'.format(version)

        # Load tag information
        tag_data = []
        print '  Loading tags'
        for raw_tag in SourceCollector._run('hg tags', SourceCollector.repo_path_metadata).split('\n'):
            parts = raw_tag.split(' ')
            tag = parts[0]
            match = re.search('^(?P<version>[0-9]+?\.[0-9]+?\.[0-9]+?)(-(?P<suffix>.+)\.(?P<build>[0-9]+))?$', tag)
            if match:
                match_dict = match.groupdict()
                tag_version = match_dict['version']
                tag_build = match_dict['build']
                tag_suffix = match_dict['suffix']
                rev_number, rev_hash = parts[-1].split(':')
                tag_data.append({'version': tag_version,
                                 'build': tag_build,
                                 'suffix': tag_suffix,
                                 'rev_number': rev_number,
                                 'rev_hash': rev_hash})

        # Build changelog
        increment_build = True
        changes_found = False
        other_changes = False
        changelog = []
        if target in ['test', 'stable', 'release']:
            print '  Generating changelog'
            changelog.append('Open vStorage')
            changelog.append('=============')
            changelog.append('')
            changelog.append('This changelog is generated based on DVCS. Due to the nature of DVCS the')
            changelog.append('order of changes in this document can be slightly different from reality.')
            log = SourceCollector._run(
                "hg log -f -b {0} --template '{{date|shortdate}} {{rev}} {{desc|firstline}}\n'".format(
                    ' -b '.join(branches)
                ), SourceCollector.repo_path_code
            )
            for log_line in log.strip().split('\n'):
                if SourceCollector._ignore_log(log_line):
                    continue
                date, log_revision, description = log_line.split(' ', 2)
                active_tag = None
                for tag in tag_data:
                    if tag['rev_number'] == log_revision and tag['suffix'] >= suffix:
                        active_tag = tag
                if active_tag is not None:
                    if changes_found is False:
                        increment_build = False
                    if other_changes is True:
                        changelog.append('* Internal updates')
                    changelog.append('\n{0}{1}\n'.format(
                        active_tag['version'],
                        '-{0}.{1}'.format(
                            active_tag['suffix'], active_tag['build']
                        ) if active_tag['suffix'] is not None else ''
                    ))
                    other_changes = False
                if re.match('^OVS\-[0-9]{1,5}', description):
                    changelog.append('* {0} - {1}'.format(date, description))
                else:
                    other_changes = True
                changes_found = True
            if other_changes is True:
                changelog.append('* Other internal updates')

        # Build buildnumber
        print '  Generating build'
        if distribution == 'experimental':
            build = int(time.time())
        elif distribution == 'unstable':
            build = current_revision
        else:
            builds = sorted(tag['build'] for tag in tag_data if tag['version'] == version and tag['suffix'] == suffix)
            if len(builds) > 0:
                build = int(builds[-1])
                if revision is None and increment_build is True:
                    build += 1
                else:
                    print '    No need to increment build'
            else:
                build = 1
        print '    Build: {0}'.format(build)

        # Save changelog
        if len(changelog) > 0:
            if increment_build is True:
                changelog.insert(5, '\n{0}{1}\n'.format(
                    version,
                    '-{0}.{1}'.format(suffix, build) if suffix is not None else ''
                ))
        with open('{0}/CHANGELOG.txt'.format(SourceCollector.repo_path_code), 'w') as changelog_file:
            changelog_file.write('\n'.join(changelog))

        # Version string. Examples:
        # * Build from local working directory
        #     1.2.0-exp.<timestamp>
        # * Unstable branch
        #     1.2.0-rev.<revision>
        # * Test branch
        #     1.2.0-alpha.<build>
        # * Stable branch
        #     1.2.0-beta.<build>
        # * Release branches (e.g. release_1_2)
        #     1.2.0-rc1.<build>  - release candidate 1
        #     1.2.0-rc2.<build>  - release candidate 2
        #     1.2.0              - final released version
        #     1.2.1              - hotfix for 1.2.0
        #     1.2.2              - hotfix for 1.2.1

        version_string = '{0}{1}'.format(
            version,
            '-{0}.{1}'.format(suffix, build) if suffix is not None else ''
        )
        print '  Full version: {0}'.format(version_string)

        # Tag revision
        if distribution in ['alpha', 'beta', 'release'] and revision is None and increment_build is True:
            print '  Tagging revision'
            SourceCollector._run(
                'hg tag -r {0} {1}'.format(current_revision, version_string),
                SourceCollector.repo_path_metadata
            )
            SourceCollector._run('hg push', SourceCollector.repo_path_metadata)

        # Building archive
        print '  Building archive'
        SourceCollector._run(
            "tar -czf {0}/openvstorage_{1}.tar.gz --transform 's,^,openvstorage-{1}/,' scripts/install scripts/system config ovs webapps *.txt".format(
                SourceCollector.package_path, version_string
            ), SourceCollector.repo_path_code
        )
        SourceCollector._run('rm -f CHANGELOG.txt', SourceCollector.repo_path_code)
        print '    Archive: {0}/openvstorage_{1}.tar.gz'.format(SourceCollector.package_path, version_string)

        print 'Done'

        return distribution, version, suffix, build, version_string, revision_date

    @staticmethod
    def _ignore_log(log_line):
        """
        Returns whether a mercurial log line should be ignored
        """
        if 'Added tag ' in log_line and ' for changeset ' in log_line:
            return True
        return False

    @staticmethod
    def _hg_update_to(path, revision):
        """
        Updates a given repo to a certain revision, cloning if it does not exist yet
        """
        if not os.path.exists('{0}/.hg'.format(path)):
            SourceCollector._run('hg clone https://bitbucket.org/{0} {1}'.format(SourceCollector.repo, path), path)
        SourceCollector._run('hg pull -u', path)
        SourceCollector._run('hg update -r {0}'.format(revision), path)

    @staticmethod
    def _run(command, working_directory):
        """
        Runs a comment, returning the output
        """
        os.chdir(working_directory)
        return check_output(command, shell=True)
