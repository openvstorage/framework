# Copyright 2014 Open vStorage NV
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
import re
import time
from datetime import datetime
from subprocess import check_output
from ConfigParser import RawConfigParser


class SourceCollector(object):
    """
    SourceCollector class

    Responsible for creating a source archive which will contain:
    * All sources for that version
    * Versioning metadata
    * Full changelog
    It will also update the repo with all required versioning tags, if appropriate
    """

    package_path = '{0}/packages/{1}'
    repo_path_code = '{0}/repo_{1}_code'
    repo_path_metadata = '{0}/repo_{1}_metadata'

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
        suffix_map = {'beta': 'beta',
                      'alpha': 'alpha',
                      'release': suffix,
                      'unstable': 'rev',
                      'experimental': 'exp'}
        branch_map = {'beta': 'stable',
                      'alpha': 'test',
                      'unstable': 'master'}

        filename = '{0}/../settings.cfg'.format(os.path.dirname(os.path.abspath(__file__)))
        settings = RawConfigParser()
        settings.read(filename)

        repo_path_code = SourceCollector.repo_path_code.format(settings.get('packaging', 'working_dir'), settings.get('packaging', 'package_name'))
        repo_path_metadata = SourceCollector.repo_path_metadata.format(settings.get('packaging', 'working_dir'), settings.get('packaging', 'package_name'))
        package_path = SourceCollector.package_path.format(settings.get('packaging', 'working_dir'), settings.get('packaging', 'package_name'))

        print 'Validating input parameters'

        distribution = target  # experimental, unstable, alpha, beta, release
        if isinstance(target, basestring):
            if target not in ['experimental', 'unstable', 'alpha', 'beta']:
                raise ValueError('Invalid target specified')
            suffix = suffix_map[target]

        elif isinstance(target, tuple):
            if len(target) != 2 or target[0] not in ['experimental', 'release']:
                raise ValueError('Invalid target specified')
            if target[0] == 'release' and revision is None:
                raise ValueError('In case a release build is requested, the exact release branch should be passed.')
            if target[0] == 'release':
                raise NotImplementedError('Release packaging is not yet fully tested. Please fork the repo and test first')
            suffix = suffix_map[target[0]]
            distribution = target[0]
            branch_map[distribution] = target[1]

        else:
            raise ValueError('Invalid target specified')

        print 'Collecting sources'
        for directory in [repo_path_code, repo_path_metadata, package_path]:
            if not os.path.exists(directory):
                os.makedirs(directory)

        # Update the metadata repo
        print '  Updating metadata'
        SourceCollector._git_checkout_to(path=repo_path_metadata,
                                         revision='master',
                                         settings=settings)
        known_branches = []
        for branch in SourceCollector.run(command='git branch -r',
                                          working_directory=repo_path_metadata).splitlines():
            if 'origin/HEAD' in branch:
                continue
            known_branches.append(branch.strip().replace('origin/', ''))

        if target != 'experimental':
            print '  Updating code'
            if branch_map[distribution] not in known_branches:
                raise ValueError('Unknown branch "{0}" specified'.format(branch_map[distribution]))
            SourceCollector._git_checkout_to(path=repo_path_code,
                                             revision=branch_map[distribution] if revision is None else revision,
                                             settings=settings)

        # Get parent branches
        branches = ['master']
        if distribution == 'alpha':
            branches.append('test')
        elif distribution == 'beta':
            branches.extend(['test', 'stable'])
        elif distribution == 'release':
            branches.extend(['test', 'stable', target[1] if revision is None else revision])

        # Get current revision and date
        print '  Fetch current revision'
        revision_number = SourceCollector.run(command='git rev-list HEAD | wc -l',
                                              working_directory=repo_path_code).strip()
        revision_hash, revision_date = SourceCollector.run(command='git show HEAD --pretty --format="%h|%at" -s',
                                                           working_directory=repo_path_code).strip().split('|')
        revision_date = datetime.fromtimestamp(float(revision_date))
        current_revision = '{0}.{1}'.format(revision_number, revision_hash)
        print '    Revision: {0}'.format(current_revision)

        # Build version
        version = '{0}.{1}.{2}'.format(settings.get('version', 'major'),
                                       settings.get('version', 'minor'),
                                       settings.get('version', 'patch'))
        print '  Version: {0}'.format(version)

        # Load tag information
        tag_data = []
        print '  Loading tags'
        for raw_tag in SourceCollector.run(command='git show-ref --tags',
                                           working_directory=repo_path_metadata).splitlines():
            parts = raw_tag.strip().split(' ')
            rev_hash = parts[0]
            tag = parts[1].replace('refs/tags/', '')
            match = re.search('^(?P<version>[0-9]+?\.[0-9]+?\.[0-9]+?)(-(?P<suffix>.+)\.(?P<build>[0-9]+))?$', tag)
            if match:
                match_dict = match.groupdict()
                tag_version = match_dict['version']
                tag_build = match_dict['build']
                tag_suffix = match_dict['suffix']
                tag_data.append({'version': tag_version,
                                 'build': int(tag_build),
                                 'suffix': tag_suffix,
                                 'rev_hash': rev_hash})

        # Build changelog
        increment_build = True
        changes_found = False
        other_changes = False
        changelog = []
        if target in ['alpha', 'beta', 'release']:
            print '  Generating changelog'
            changelog.append(settings.get('packaging', 'product_name'))
            changelog.append('=============')
            changelog.append('')
            changelog.append('This changelog is generated based on DVCS. Due to the nature of DVCS the')
            changelog.append('order of changes in this document can be slightly different from reality.')
            changelog.append('')
            log = SourceCollector.run(command='git --no-pager log origin/{0} --date-order --pretty --format="%at|%H|%s"'.format(branch_map[target]),
                                      working_directory=repo_path_code)
            for log_line in log.strip().splitlines():
                if 'Added tag ' in log_line and ' for changeset ' in log_line:
                    continue

                timestamp, log_hash, description = log_line.split('|')
                log_date = datetime.fromtimestamp(float(timestamp))
                active_tag = None
                for tag in tag_data:
                    if tag['rev_hash'] == log_hash and tag['suffix'] >= suffix:
                        active_tag = tag
                if active_tag is not None:
                    if changes_found is False:
                        increment_build = False
                    if other_changes is True:
                        changelog.append('* Internal updates')
                    changelog.append('\n{0}{1}\n'.format(active_tag['version'],
                                                         '-{0}.{1}'.format(active_tag['suffix'], active_tag['build']) if active_tag['suffix'] is not None else ''))
                    other_changes = False
                if re.match('^OVS\-[0-9]{1,5}', description):
                    changelog.append('* {0} - {1}'.format(log_date.strftime('%Y-%m-%d'), description))
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
                build = builds[-1]
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
                changelog.insert(5, '\n{0}{1}\n'.format(version,
                                                        '-{0}.{1}'.format(suffix, build) if suffix is not None else ''))
        with open('{0}/CHANGELOG.txt'.format(repo_path_code), 'w') as changelog_file:
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

        version_string = '{0}{1}'.format(version, '-{0}.{1}'.format(suffix, build) if suffix is not None else '')
        print '  Full version: {0}'.format(version_string)

        # Tag revision
        if distribution in ['alpha', 'beta', 'release'] and revision is None and increment_build is True:
            print '  Tagging revision'
            SourceCollector.run(command='git tag -a {0} {1} -m "Added tag {0} for changeset {1}"'.format(version_string, revision_hash),
                                working_directory=repo_path_metadata)
            SourceCollector.run(command='git push origin --tags',
                                working_directory=repo_path_metadata)

        # Building archive
        print '  Building archive'
        SourceCollector.run(command="tar -czf {0}/{1}_{2}.tar.gz {3}".format(package_path,
                                                                             settings.get('packaging', 'package_name'),
                                                                             version_string,
                                                                             settings.get('packaging', 'source_contents').format(
                                                                                 settings.get('packaging', 'package_name'),
                                                                                 version_string)
                                                                             ),
                            working_directory=repo_path_code)
        SourceCollector.run(command='rm -f CHANGELOG.txt',
                            working_directory=repo_path_code)
        print '    Archive: {0}/{1}_{2}.tar.gz'.format(package_path, settings.get('packaging', 'package_name'), version_string)

        print 'Done'

        return distribution, version_string, revision_date

    @staticmethod
    def _git_checkout_to(path, revision, settings):
        """
        Updates a given repo to a certain revision, cloning if it does not exist yet
        """
        if not os.path.exists('{0}/.git'.format(path)):
            SourceCollector.run('git clone {0} {1}'.format(settings.get('packaging', 'repo'), path), path)
        SourceCollector.run('git pull', path)
        SourceCollector.run('git fetch -p', path)
        SourceCollector.run('git checkout {0}'.format(revision), path)

    @staticmethod
    def run(command, working_directory):
        """
        Runs a comment, returning the output
        """
        os.chdir(working_directory)
        return check_output(command, shell=True)
