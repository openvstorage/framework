# Copyright (C) 2016 iNuron NV
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
Base storagedriver config
Exposes methods for all sections of a complete storagedriver config
"""
from subprocess import check_output


class VolumeDriverConfigOption(object):
    """
    Config option read out by 'volumedriver_fs', '--config-help-markdown'
    """
    _options_by_component_and_key = {}

    def __init__(self, component, key, default_value, dynamically_reconfigurable, remarks):
        #  type: (str, str, any, bool, str) -> None
        """
        Initialize a new option
        Format of the command is '| component | key | default value | dynamically reconfigurable | remarks |'
        :param component: Volumedriver component
        :type component: str
        :param key: Option key
        :type key: str
        :param default_value: Default value
        :param dynamically_reconfigurable:
        :param remarks:
        """
        self.component = component
        self.key = key
        self.default_value = default_value
        self.dynamically_reconfigurable = dynamically_reconfigurable
        self.remarks = remarks

    @classmethod
    def get_all_options(cls):
        # type: () -> List[VolumeDriverConfigOption]
        """
        Get all possible options
        :return: A list with all options
        :rtype: List[VolumeDriverConfigOption]
        """
        if not cls._options_by_component_and_key:
            cls._cache_options()
        options = []
        for component, component_keys_options in cls._options_by_component_and_key.iteritems():
            for component_key, option in component_keys_options.iteritems():
                options.append(option)
        return options

    @classmethod
    def get_option_by_component_and_key(cls, component, key):
        # type: (str, str) -> VolumeDriverConfigOption
        """
        Retrieve an option by component and key
        Returns None when no option was found
        :param component: Component to retrieve
        :type component: str
        :param key: Key to retrieve from component
        :type key: str
        :return: Option if found else none
        :rtype: VolumeDriverConfigOption
        """
        if not cls._options_by_component_and_key:
            cls._cache_options()
        return cls._options_by_component_and_key.get(component, {}).get(key)

    @classmethod
    def get_options_by_component(cls, component):
        # type: (str) -> List[VolumeDriverConfigOption]
        """
        Retrieve all options for a component
        :param component: Component to retrieve options for
        :type component: str
        :return: All options associated with the component
        :rtype: List[VolumeDriverConfigOption]
        """
        if not cls._options_by_component_and_key:
            cls._cache_options()
        options = []
        component_keys_options = cls._options_by_component_and_key.get(component)
        for component_key, option in component_keys_options.iteritems():
            options.append(option)
        return options

    @classmethod
    def _cache_options(cls):
        """
        Cache all possible options
        :return: None
        :rtype: NoneType
        """
        options = []
        markdown = check_output(['volumedriver_fs', '--config-help-markdown'])
        for line in markdown.split('\n|')[2:]:
            options.append(cls.from_option_line('|' + line))
        options_by_component_and_key = {}
        for option in options:
            if option.component not in options_by_component_and_key:
                options_by_component_and_key[option.component] = {}
            if option.key not in options_by_component_and_key:
                options_by_component_and_key[option.component][option.key] = option
        cls._options_by_component_and_key = options_by_component_and_key

    @classmethod
    def from_option_line(cls, line):
        # type: (str) -> VolumeDriverConfigOption
        """
        Convert an option line string to an VolumeDriverConfigOption instance
        :param line: Line string to parse
        :type line: str
        :return: The VolumeDriverConfigOption instnace
        :rtype: VolumeDriverConfigOption
        """
        stripped = [s.strip() for s in line.split('|')[1:-1]]
        print line
        if len(stripped) > 5:
            # A remark with a '|' in it
            remark_pieces = stripped[4:]
            stripped[4] = '|'.join(remark_pieces)
            print 1, stripped
            stripped = stripped[:5]
            print 2, stripped
        component, key, default_value, dynamically_reconfigurable_string, remarks = stripped
        return cls(component, key, default_value, dynamically_reconfigurable_string == 'yes', remarks)


class BaseStorageDriverConfig(object):

    def to_dict(self):
        # type: () -> Dict[str, any]
        """
        Convert the current config set to a dictionary
        :return: Dict
        :rtype: Dict[str, any]
        """
        return vars(self)

    def __eq__(self, other):
        # type: (BaseStorageDriverConfig) -> bool
        """
        Check if the other value is equal to this instance
        :param other: Other value to compare to
        :type other: BaseStorageDriverConfig
        :return: True if equal else false
        :rtype: bool
        """
        return isinstance(other, type(self)) and self.to_dict() == other.to_dict()

    def __ne__(self, other):
        # type: (BaseStorageDriverConfig) -> bool
        """
        Check if the other value is not equal to this instance
        :param other: Other value to compare to
        :type other: BaseStorageDriverConfig
        :return: True if not equal else false
        :rtype: bool
        """
        return not other == self

    def get_difference(self, other):
        # type: (BaseStorageDriverConfig) -> Dict[str, any]
        """
        Get all keys that differ from the other value
        :param other: Other instance
        :type other: BaseStorageDriverConfig
        :return:
        """
        diff_keys = []
        if not isinstance(other, type(self)):
            raise ValueError('Other is not of type {0}'.format(BaseStorageDriverConfig))
        for key, value in other.to_dict().iteritems():
            if value != getattr(self, key):
                diff_keys.append(key)
        return diff_keys

    @staticmethod
    def _is_dynamically_reloadable(component, key):
        # type: (str, str) -> bool
        """
        Determine if an option within a section is dynamically reloadable
        :param component: Config section look into
        :type component: str
        :param key: Config key to look for
        :type key: str
        :return: True if the option is dynamically reloadable
        :rtype: bool
        """
        option = VolumeDriverConfigOption.get_option_by_component_and_key(component, key)
        return option and option.dynamically_reconfigurable
