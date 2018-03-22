# Copyright (C) 2017 iNuron NV
#
# This file is part of Open vStorage Open Source Edition (OSE),
# as available from
#
#      http://www.openvstorage.org and
#      http://www.openvstorage.com.
#
# This file is free software; you can redistribute it and/or modify it
# under the terms of the GNU Affero General Public License v3 (GNU AGPLv3)
# as published by the Free Software Foundation, in version 3 as it comes
# in the LICENSE.txt file of the Open vStorage OSE distribution.
#
# Open vStorage is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY of any kind.

"""
Module which contains an extended YAML encoder
"""

import yaml
from collections import OrderedDict


class YamlExtender(object):
    """
    Extends YAML functionality, providing a way to encode and decode an OrderedDict
    This class is to be registered in the kombu registry and set as serializer within celery (see celery_run)
    Kombu registry:
    from kombu.serialization import register
    register('ovsyaml', YamlExtender.ordered_dump, YamlExtender.ordered_load, content_type='application/x-yaml', content_encoding='utf-8')
    The order does not matter for celery results: use the safe_load instead of the ordered_load to keep normal dict representations
    register('ovsyaml', YamlExtender.ordered_dump, yaml.safe_load, content_type='application/x-yaml', content_encoding='utf-8')
    """

    @staticmethod
    def ordered_load(stream, loader=yaml.SafeLoader, object_pairs_hook=OrderedDict):
        """
        Loads a stream of data as an OrderedDict
        Usage: ordered_load(stream, yaml.SafeLoader)
        :param stream: Stream to load
        :param loader: Loader class to use (yaml.Loader, yaml.SafeLoader, yaml.BaseLoader, ...)
        :param object_pairs_hook: Object to cast the deserialized data as
        :return: The instance of the object_pairs_hook
        """
        class OrderedLoader(loader):  # Create a class which inherits the given Loader so we don't overrule the default loading behaviour
            pass

        def construct_mapping(loader_implementation, node):
            loader_implementation.flatten_mapping(node)
            return object_pairs_hook(loader_implementation.construct_pairs(node))

        OrderedLoader.add_constructor(yaml.resolver.BaseResolver.DEFAULT_MAPPING_TAG, construct_mapping)
        return yaml.load(stream, OrderedLoader)

    @staticmethod
    def ordered_dump(data, stream=None, dumper=yaml.SafeDumper, **kwargs):
        """
        YAML dumper which can dump OrderedDicts
        Usage: ordered_dump(data, Dumper=yaml.SafeDumper)
        :param data: Data to dump
        :param stream: Stream to use for dumping
        :param dumper: Dumper class to use (eg. yaml.Dumper, yaml.SafeDumper, yaml.BaseDumper, ...)
        :return:
        """
        class OrderedDumper(dumper):  # Create a class which inherits the given Dumper so we don't overrule the default dumping behaviour
            pass

        def _dict_representer(dumper_implementation, data_to_dump):
            return dumper_implementation.represent_mapping(yaml.resolver.BaseResolver.DEFAULT_MAPPING_TAG, data_to_dump.items())

        OrderedDumper.add_representer(OrderedDict, _dict_representer)
        return yaml.dump(data, stream, OrderedDumper, **kwargs)
