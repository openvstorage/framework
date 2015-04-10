# Copyright 2015 CloudFounders NV
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
Module containing certain helper classes providing various logic
"""
import os
import imp
import random
import string
import inspect


class Toolbox(object):
    """
    Generic class for various methods
    """

    @staticmethod
    def fetch_hooks(hook_type, hook):
        """
        Load hooks
        """
        functions = []
        path = '{0}/../'.format(os.path.dirname(__file__))
        for filename in os.listdir(path):
            if os.path.isfile(os.path.join(path, filename)) and filename.endswith('.py') and filename != '__init__.py':
                name = filename.replace('.py', '')
                module = imp.load_source(name, os.path.join(path, filename))
                for member in inspect.getmembers(module):
                    if inspect.isclass(member[1]) \
                            and member[1].__module__ == name \
                            and 'object' in [base.__name__ for base in member[1].__bases__]:
                        for submember in inspect.getmembers(member[1]):
                            if hasattr(submember[1], 'hooks') \
                                    and isinstance(submember[1].hooks, dict) \
                                    and hook_type in submember[1].hooks \
                                    and isinstance(submember[1].hooks[hook_type], list) \
                                    and hook in submember[1].hooks[hook_type]:
                                functions.append(submember[1])
        return functions

    @staticmethod
    def get_hash(length=16):
        """
        Generates a random hash
        """
        return ''.join(random.choice(string.ascii_letters + string.digits) for _ in range(length))
