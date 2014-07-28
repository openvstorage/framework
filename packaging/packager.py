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
Packager module

There are 5 distributions:
1. experimental: Used for developers to package agains local checked out repo.
   e.g.: openvstorage-core_1.1.0.1397221667_1_all.deb
2. unstable: General packages against unstable branch. No changelog, revision-based buildnumbers
   e.g.: openvstorage-core_1.1.0.345_1_all.deb
3. test: Packages against test branch. Tag-based build numbers, changelog generated from commit messages
   e.g.: openvstorage-core_1.1.0.3_1_all_.deb
4. stable: Packages against stable branch. Tag-based build numbers, changelog generated from commit messages
   e.g.: openvstorage-core_1.1.0.4_1_all.deb
5. release: Packages against specified release branches. Tag-based build numbers, changelog generated from commit message
   e.g.: openvstorage-core_1.1.0.26-rc1_1_all.deb
"""

from optparse import OptionParser
from sourcecollector import SourceCollector
from debian import DebianPackager

if __name__ == '__main__':
    parser = OptionParser(description='Open vStorage packager')
    parser.add_option('-d', '--target', dest='target', default='unstable')
    parser.add_option('-r', '--revision', dest='revision', default=None)
    parser.add_option('-s', '--suffix', dest='suffix', default=None)
    options, args = parser.parse_args()

    target = options.target
    if target.startswith('release'):
        if not target.startswith('release,'):
            raise RuntimeError("In case a release target is specified, it should be of the format: 'release,<release branch>'")
        else:
            target = tuple(target.split(','))
    if target.startswith('experimental,'):
        target = tuple(target.split(','))

    # 1. Collect sources
    source_metadata = SourceCollector.collect(target=target, revision=options.revision, suffix=options.suffix)

    if source_metadata is not None:
        # 2. Build & Upload packages
        #    - Debian
        DebianPackager.package(source_metadata)
        DebianPackager.upload(source_metadata)
