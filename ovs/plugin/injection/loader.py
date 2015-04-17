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
Framework loader module
"""


class Loader(object):
    """
    Loader class
    """

    def __init__(self):
        """
        Empty constructor
        """
        pass

    @staticmethod
    def load(module):
        from ConfigParser import RawConfigParser
        config = RawConfigParser()
        config.read('/opt/OpenvStorage/ovs/plugin/injection/settings.cfg')
        if config.has_option('main', 'framework_{0}'.format(module.__name__.lower())):
            framework = config.get('main', 'framework_{0}'.format(module.__name__.lower()))
        else:
            framework = config.get('main', 'framework')
        return framework
