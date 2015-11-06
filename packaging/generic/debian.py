# Copyright 2014 iNuron NV
#
# Licensed under the Open vStorage Non-Commercial License, Version 1.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.openvstorage.org/OVS_NON_COMMERCIAL
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
from ConfigParser import RawConfigParser
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

        filename = '{0}/../settings.cfg'.format(os.path.dirname(os.path.abspath(__file__)))
        settings = RawConfigParser()
        settings.read(filename)

        package_name = settings.get('packaging', 'package_name')
        repo_path_code = SourceCollector.repo_path_code.format(settings.get('packaging', 'working_dir'), package_name)
        package_path = SourceCollector.package_path.format(settings.get('packaging', 'working_dir'), package_name)

        # Prepare
        # /<pp>/debian
        debian_folder = '{0}/debian'.format(package_path)
        if os.path.exists(debian_folder):
            shutil.rmtree(debian_folder)
        # /<rp>/packaging/debian -> /<pp>/debian
        shutil.copytree('{0}/packaging/debian'.format(repo_path_code), debian_folder)

        # Rename tgz
        # /<pp>/<packagename>_1.2.3.tar.gz -> /<pp>/debian/<packagename>_1.2.3.orig.tar.gz
        shutil.copyfile('{0}/{1}_{2}.tar.gz'.format(package_path, package_name, version_string),
                        '{0}/{1}_{2}.orig.tar.gz'.format(debian_folder, package_name, version_string))
        # /<pp>/debian/<packagename>-1.2.3/...
        SourceCollector.run(command='tar -xzf {0}_{1}.orig.tar.gz'.format(package_name, version_string),
                            working_directory=debian_folder)

        # Move the debian package metadata into the extracted source
        # /<pp>/debian/debian -> /<pp>/debian/<packagename>-1.2.3/
        SourceCollector.run(command='mv {0}/debian {0}/{1}-{2}/'.format(debian_folder, package_name, version_string),
                            working_directory=package_path)

        # Build changelog entry
        with open('{0}/{1}-{2}/debian/changelog'.format(debian_folder, package_name, version_string), 'w') as changelog_file:
            changelog_file.write("""{0} ({1}-1) {2}; urgency=low

  * For changes, see individual changelogs

 -- Packaging System <engineering@openvstorage.com>  {3}
""".format(package_name, version_string, distribution, revision_date.strftime('%a, %d %b %Y %H:%M:%S +0000')))

        # Some more tweaks
        SourceCollector.run(command='chmod 770 {0}/{1}-{2}/debian/rules'.format(debian_folder, package_name, version_string),
                            working_directory=package_path)
        SourceCollector.run(command="sed -i -e 's/__NEW_VERSION__/{0}/' *.*".format(version_string),
                            working_directory='{0}/{1}-{2}/debian'.format(debian_folder, package_name, version_string))

        # Build the package
        SourceCollector.run(command='dpkg-buildpackage',
                            working_directory='{0}/{1}-{2}'.format(debian_folder, package_name, version_string))

    @staticmethod
    def upload(source_metadata):
        """
        Uploads a given set of packages
        """

        filename = '{0}/../settings.cfg'.format(os.path.dirname(os.path.abspath(__file__)))
        settings = RawConfigParser()
        settings.read(filename)

        package_name = settings.get('packaging', 'package_name')
        package_path = SourceCollector.package_path.format(settings.get('packaging', 'working_dir'), package_name)

        # Get release name from the repo code settings.cfg not master settings.cfg
        repo_path_code = SourceCollector.repo_path_code.format(settings.get('packaging', 'working_dir'), settings.get('packaging', 'package_name'))
        filename = '{0}/packaging/settings.cfg'.format(repo_path_code)
        code_settings = RawConfigParser()
        code_settings.read(filename)
        releasename = code_settings.get('version', 'releasename').lower()
        target, version_string, _ = source_metadata

        user = "upload"
        server = "172.20.3.16"
        repo_root_path = "/usr/share/repo"
        upload_path = os.path.join(repo_root_path, target, releasename)

        print("Uploading {0} {1}".format(package_name, version_string))
        debs_path = os.path.join(package_path, 'debian')
        deb_packages = [filename for filename in os.listdir(debs_path) if filename.endswith('.deb')]

        create_releasename_command = "ssh {0}@{1} mkdir -p {2}".format(user, server, upload_path)
        SourceCollector.run(command=create_releasename_command,
                            working_directory=debs_path)

        for deb_package in deb_packages:
            source_path = os.path.join(debs_path, deb_package)
            destination_path = os.path.join(upload_path, deb_package)

            check_package_command = "ssh {0}@{1} ls {2}".format(user, server, upload_path)
            existing_packages = SourceCollector.run(command=check_package_command,
                                                    working_directory=debs_path).split()
            upload = True
            for package in existing_packages:
                if package == deb_package:
                    print("Package already uploaded, done...")
                    upload = False
                    continue
            if upload:
                scp_command = "scp {0} {1}@{2}:{3}".format(source_path, user, server, destination_path)
                SourceCollector.run(command=scp_command,
                                    working_directory=debs_path)
                remote_command = "ssh {0}@{1} reprepro -Vb {2}/debian includedeb {3}-{4} {5}".format(user, server, repo_root_path, releasename, target, destination_path)
                SourceCollector.run(command=remote_command,
                                    working_directory=debs_path)
