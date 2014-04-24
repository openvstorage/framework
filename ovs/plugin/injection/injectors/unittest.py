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
Unittest Injector module
"""


class Injector(object):
    """
    Injector class, provides all logic to inject. However, the unittest injector
    only provides functionality required in the unittests
    """

    def __init__(self):
        """
        This class should be fully static
        """
        raise RuntimeError('This class should not be instantiated.')

    @staticmethod
    def inject_configuration(provider):
        """ Injects the Config module """
        def get(key):
            return key
        provider.get = staticmethod(get)
        return provider

    @staticmethod
    def inject_tools(provider):
        """ Injects the Tools module """
        return provider

    @staticmethod
    def inject_package(provider):
        """ Injects the Package module """
        return provider

    @staticmethod
    def inject_service(provider):
        """ Injects the Service module """
        return provider

    @staticmethod
    def inject_console(provider):
        """ Injects the Console module """
        _ = provider
        return provider

    @staticmethod
    def inject_logger(provider):
        """ Injects the Logger module """
        return provider

    @staticmethod
    def inject_process(provider):
        """ Injects the Process module """
        return provider
