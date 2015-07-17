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
Debian packager module
"""
import os
import shutil
from sourcecollector import SourceCollector


class DebianPackager(object):
    """
    DebianPackager class

    Responsible for creating debian packages from the source archive
    """

    def __init__(self):
        """
        Dummy init method, DebianPackager is static
        """
        raise NotImplementedError('DebianPackager is a static class')

    @staticmethod
    def package(source_metadata):
        """
        Packages a given package.
        """
        distribution, version_string, revision_date = source_metadata

        # Prepare
        # /<pp>/debian
        debian_folder = '{0}/debian'.format(SourceCollector.package_path)
        if os.path.exists(debian_folder):
            shutil.rmtree(debian_folder)
        # /<rp>/packaging/debian -> /<pp>/debian
        shutil.copytree('/tmp/repo_openvstorage_code/packaging/debian', debian_folder)

        # Rename tgz
        # /<pp>/openvstorage_1.2.3.tar.gz -> /<pp>/debian/openvstorage_1.2.3.orig.tar.gz
        shutil.copyfile('{0}/openvstorage_{1}.tar.gz'.format(SourceCollector.package_path, version_string),
                        '{0}/openvstorage_{1}.orig.tar.gz'.format(debian_folder, version_string))
        # /<pp>/debian/openvstorage-1.2.3/...
        SourceCollector.run(command='tar -xzf openvstorage_{0}.orig.tar.gz'.format(version_string),
                            working_directory=debian_folder)

        # Move the debian package metadata into the extracted source
        # /<pp>/debian/debian -> /<pp>/debian/openvstorage-1.2.3/
        SourceCollector.run(command='mv {0}/debian {0}/openvstorage-{1}/'.format(debian_folder, version_string),
                            working_directory=SourceCollector.package_path)

        # Build changelog entry
        with open('{0}/openvstorage-{1}/debian/changelog'.format(debian_folder, version_string), 'w') as changelog_file:
            changelog_file.write("""openvstorage ({0}-1) {1}; urgency=low

  * For changes, see individual changelogs

 -- Packaging System <engineering@openvstorage.com>  {2}
""".format(version_string, distribution, revision_date.strftime('%a, %d %b %Y %H:%M:%S +0000')))

        # Some more tweaks
        SourceCollector.run(command='chmod 770 {0}/openvstorage-{1}/debian/rules'.format(debian_folder, version_string),
                            working_directory=SourceCollector.package_path)
        SourceCollector.run(command="sed -i -e 's/__NEW_VERSION__/{0}/' *.*".format(version_string),
                            working_directory='{0}/openvstorage-{1}/debian'.format(debian_folder, version_string))

        # Build the package
        SourceCollector.run(command='dpkg-buildpackage',
                            working_directory='{0}/openvstorage-{1}'.format(debian_folder, version_string))

    @staticmethod
    def upload(source_metadata):
        """
        Uploads a given set of packages
        """
        version_string = source_metadata[1]
        new_package = version_string not in SourceCollector.run(command='ssh ovs-apt@packages.cloudfounders.com "grep \'openvstorage_{0}-1_amd64\' /data/www/apt/*/Packages" || true'.format(version_string),
                                                                working_directory=SourceCollector.package_path)
        print 'Uploading {0} package: {1}'.format('new' if new_package else 'existing', 'openvstorage_{0}-1_amd64'.format(version_string))
        SourceCollector.run(command='dput -c {0}/debian/dput.cfg ovs-apt {0}/debian/openvstorage_{1}-1_amd64.changes'.format(SourceCollector.package_path, version_string),
                            working_directory=SourceCollector.package_path)
        SourceCollector.run(command='ssh ovs-apt@packages.cloudfounders.com "mini-dinstall -b{0}"'.format('' if new_package else ' --no-db'),
                            working_directory=SourceCollector.package_path)
