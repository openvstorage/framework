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
Injector module
"""
from ovs.plugin.injection.loader import Loader


class Injector(object):
    """
    Injector class, provides all logic to inject
    """

    def __init__(self):
        """
        This class should be fully static
        """
        raise RuntimeError('This class should not be instantiated.')

    @staticmethod
    def inject(module):
        """ Inject module logic and return updated module """
        framework = Loader.load(module)
        injector_module = __import__(name='ovs.plugin.injection.injectors.{0}'.format(framework),
                                     globals=globals(),
                                     locals=locals(),
                                     fromlist=['Injector'],
                                     level=0)
        injector = getattr(injector_module, 'Injector')
        inject = getattr(injector, 'inject_{0}'.format(module.__name__.lower()))
        return inject(module)
