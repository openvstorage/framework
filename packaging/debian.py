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
Debian packager module
"""
import shutil
import datetime
import os
from subprocess import check_output


class DebianPackager(object):
    """
    DebianPackager class

    Responsible for creating debian packages from the source archive
    """

    repo_path_code = '/tmp/repo_openvstorage_code'
    package_path = '/tmp/packages'

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
        distribution, version, suffix, build, version_string, revision_date = source_metadata

        # Prepare
        # /<pp>/debian
        folder = '{0}/{1}'.format(DebianPackager.package_path, 'debian')
        if os.path.exists(folder):
            shutil.rmtree(folder)
        # /<rp>/packaging/debian -> /<pp>/debian
        shutil.copytree('{0}/packaging/{1}'.format(DebianPackager.repo_path_code, 'debian'), folder)

        # Rename tgz
        # /<pp>/openvstorage_1.2.3.tar.gz -> /<pp>/debian/openvstorage_1.2.3.orig.tar.gz
        shutil.copyfile('{0}/openvstorage_{1}.tar.gz'.format(DebianPackager.package_path, version_string),
                        '{0}/debian/openvstorage_{1}.orig.tar.gz'.format(DebianPackager.package_path, version_string))
        # /<pp>/debian/openvstorage-1.2.3/...
        DebianPackager._run('tar -xzf openvstorage_{0}.orig.tar.gz'.format(version_string),
                            '{0}/debian/'.format(DebianPackager.package_path))

        # Move the debian package metadata into the extracted source
        # /<pp>/debian/debian -> /<pp>/debian/openvstorage-1.2.3/
        DebianPackager._run('mv {0}/debian/debian {0}/debian/openvstorage-{1}/'.format(DebianPackager.package_path, version_string),
                            DebianPackager.package_path)

        # Build changelog entry
        with open('{0}/debian/openvstorage-{1}/debian/changelog'.format(DebianPackager.package_path, version_string), 'w') as changelog_file:
            changelog_file.write('' +
"""openvstorage ({0}-1) {1}; urgency=low

  * For changes, see individual changelogs

 -- Packaging System <info@cloudfounders.com>  {2}
""".format(version_string, distribution, revision_date.strftime('%a, %d %b %Y %H:%M:%S +0000')))

        # Some more tweaks
        DebianPackager._run('chmod 770 {0}/debian/openvstorage-{1}/debian/rules'.format(DebianPackager.package_path, version_string),
                            DebianPackager.package_path)
        DebianPackager._run("sed -i -e 's/__NEW_VERSION__/{0}/' *.*".format(version_string),
                            '{0}/debian/openvstorage-{1}/debian'.format(DebianPackager.package_path, version_string))

        # Build the package
        DebianPackager._run('dpkg-buildpackage', '{0}/debian/openvstorage-{1}'.format(DebianPackager.package_path, version_string))

    @staticmethod
    def upload(source_metadata):
        """
        Uploads a given set of packages
        """
        distribution, version, suffix, build, version_string, revision_date = source_metadata
        DebianPackager._run('dput -c {0}/debian/dput.cfg ovs-apt {0}/debian/openvstorage_{1}-1_amd64.changes'.format(DebianPackager.package_path, version_string),
                            DebianPackager.package_path)

    @staticmethod
    def _run(command, working_directory):
        """
        Runs a comment, returning the output
        """
        os.chdir(working_directory)
        return check_output(command, shell=True)
