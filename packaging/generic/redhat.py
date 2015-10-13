# Copyright 2015 Open vStorage NV
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
RPM packager module
"""
import os
import shutil
from ConfigParser import RawConfigParser
from sourcecollector import SourceCollector
ROOT_PATH = os.path.dirname(os.path.abspath(__file__))


class RPMPackager(object):
    """
    RPMPackager class

    Responsible for creating rpm packages from the source archive
    """

    def __init__(self):
        """
        Dummy init method, RPMPackager is static
        """
        raise NotImplementedError('RPMPackager is a static class')

    @staticmethod
    def package(source_metadata):
        """
        Packages a given package.
        """
        distribution, version_string, revision_date = source_metadata

        root_path = ROOT_PATH
        filename = '{0}/../settings.cfg'.format(root_path)
        settings = RawConfigParser()
        settings.read(filename)

        package_name = settings.get('packaging', 'package_name')
        repo_path_code = SourceCollector.repo_path_code.format(settings.get('packaging', 'working_dir'), package_name)
        package_path = SourceCollector.package_path.format(settings.get('packaging', 'working_dir'), package_name)

        # Prepare
        redhat_folder = '{0}/redhat'.format(package_path)
        if os.path.exists(redhat_folder):
            shutil.rmtree(redhat_folder)
        os.mkdir(redhat_folder)

        # load config
        config_dir = '{0}/../redhat/cfgs'.format(root_path)
        packages = os.listdir(config_dir)
        for package in packages:
            package_filename = '{0}/{1}'.format(config_dir, package)
            package_cfg = RawConfigParser()
            package_cfg.read(package_filename)

            package_name = package_cfg.get('main', 'name')
            dirs = package_cfg.get('main', 'dirs')
            files = package_cfg.get('main', 'files')
            depends_packages = package_cfg.get('main', 'depends').replace('$Version', version_string.replace('-', '_'))

            depends = ""
            if depends_packages != '':
                depends = []
                for depends_package in depends_packages.split(','):
                    depends.append('-d "{}"'.format(depends_package.strip()))
                depends = ' '.join(depends)

            package_root_path = os.path.join(package_path, package_name)
            if os.path.exists(package_root_path):
                shutil.rmtree(package_root_path)
            os.mkdir(package_root_path)

            for dir_ in dirs.split(','):
                dir_ = dir_.strip()
                if dir_ != "''":
                    source_dir, dest_location = dir_.split('=')
                    # source_dir = dir to copy - from repo root
                    # dest_location = dir under which to copy the source_dir
                    source_full_path = os.path.join(repo_path_code, source_dir.strip())
                    dest_full_path = os.path.join(package_root_path, dest_location.strip())
                    shutil.copytree(source_full_path, dest_full_path)
            for file_ in files.split(','):
                file_ = file_.strip()
                if file_ != "''" and file_ != '':
                    source_file, dest_location = file_.split('=')
                    source_full_path = os.path.join(repo_path_code, source_file.strip())
                    dest_full_path = os.path.join(package_root_path, dest_location.strip())

                    if not os.path.exists(dest_full_path):
                        os.makedirs(dest_full_path)
                    shutil.copy(source_full_path, dest_full_path)
            before_install, after_install = ' ', ' '
            script_root = '{0}/../redhat/scripts'.format(root_path)
            before_install_script = '{0}.before-install.sh'.format(package_name)
            before_install_script_path = os.path.join(script_root, before_install_script)
            if os.path.exists(before_install_script_path):
                before_install = ' --before-install {0} '.format(before_install_script_path)
            after_install_script = '{0}.after-install.sh'.format(package_name)
            after_install_script_path = os.path.join(script_root, after_install_script)
            if os.path.exists(after_install_script_path):
                after_install = ' --after-install {0} '.format(after_install_script_path)
                SourceCollector.run(command="sed -i -e 's/$Version/{0}/g' {1}".format(version_string,
                                                                                      after_install_script_path),
                                    working_directory='{0}'.format(script_root))

            params = {'version': version_string,
                      'package_name': package_cfg.get('main', 'name'),
                      'summary': package_cfg.get('main', 'summary'),
                      'license': package_cfg.get('main', 'license'),
                      'URL': package_cfg.get('main', 'URL'),
                      'source': package_cfg.get('main', 'source'),
                      'arch': package_cfg.get('main', 'arch'),
                      'description': package_cfg.get('main', 'description'),
                      'maintainer': package_cfg.get('main', 'maintainer'),
                      'depends': depends,
                      'package_root': package_root_path,
                      'before_install': before_install,
                      'after_install': after_install,
            }

            command = """fpm -s dir -t rpm -n {package_name} -v {version} --description "{description}" --maintainer "{maintainer}" --license "{license}" --url {URL} -a {arch} --vendor "Open vStorage" {depends}{before_install}{after_install} --prefix=/ -C {package_root}""".format(**params)

            SourceCollector.run(command,
                                working_directory=redhat_folder)
            print(os.listdir(redhat_folder))

    @staticmethod
    def upload(source_metadata):
        """
        Uploads a given set of packages
        """
        _ = source_metadata
        root_path = ROOT_PATH
        filename = '{0}/../settings.cfg'.format(root_path)
        settings = RawConfigParser()
        settings.read(filename)

        package_name = settings.get('packaging', 'package_name')
        package_path = SourceCollector.package_path.format(settings.get('packaging', 'working_dir'), package_name)

        redhat_folder = '{0}/redhat'.format(package_path)
        destination_folder = '/usr/share/repo/CentOS/7/x86_64/'
        destination_server = '172.20.3.17'
        user = 'upload'

        packages = os.listdir(redhat_folder)
        for package in packages:
            package_source_path = os.path.join(redhat_folder, package)

            command = 'scp {0} {1}@{2}:{3}'.format(package_source_path, user, destination_server, destination_folder)
            print('Uploading package {0}'.format(package))
            SourceCollector.run(command,
                                working_directory=redhat_folder)
        if len(packages) > 0:
            command = 'ssh {0}@{1} createrepo --update {2}'.format(user, destination_server, destination_folder)
            SourceCollector.run(command,
                                working_directory=redhat_folder)
