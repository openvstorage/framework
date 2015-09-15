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

        filename = '{0}/../settings.cfg'.format(os.path.dirname(os.path.abspath(__file__)))
        settings = RawConfigParser()
        settings.read(filename)

        package_name = settings.get('packaging', 'package_name')
        repo_path_code = SourceCollector.repo_path_code.format(settings.get('packaging', 'working_dir'), package_name)
        package_path = SourceCollector.package_path.format(settings.get('packaging', 'working_dir'), package_name)

        # Prepare
        #@TODO


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

        version_string = source_metadata[1]

        #@TODO