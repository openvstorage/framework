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
from functools import wraps
from subprocess import check_output


def ensure_options(f):
    """
    Ensure that config has its possible options cached
    """
    @wraps(f)
    def wrap(*args, **kwargs):
        if not VolumeDriverConfigOption._options:
            VolumeDriverConfigOption._read_cache_volumedriver_options()
        return f(*args, **kwargs)
    return wrap


class VolumeDriverConfigOption(object):
    """
    Config option read out by 'volumedriver_fs', '--config-help-markdown'
    """
    _options = []

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
    @ensure_options
    def get_all_options(cls):
        # type: () -> List[VolumeDriverConfigOption]
        """
        Get all possible options
        :return: A list with all options
        :rtype: List[VolumeDriverConfigOption]
        """
        return cls._options

    @classmethod
    @ensure_options
    def get_option_by_component_and_key(cls, component, key):
        # type: (str, str) -> Union[VolumeDriverConfigOption, None]
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
        return next((option for option in cls._options if option.key == key and option.component == component), None)

    @classmethod
    @ensure_options
    def get_options_by_component(cls, component):
        # type: (str) -> List[VolumeDriverConfigOption]
        """
        Retrieve all options for a component
        :param component: Component to retrieve options for
        :type component: str
        :return: All options associated with the component
        :rtype: List[VolumeDriverConfigOption]
        """
        return [option for option in cls._options if option.component == component]

    @classmethod
    def _read_cache_volumedriver_options(cls):
        """
        Cache all possible options
        :return: None
        :rtype: NoneType
        """
        options = []
        markdown = check_output(['volumedriver_fs', '--config-help-markdown'])
        for line in markdown.split('\n|')[2:]:
            options.append(cls.parse_markdown_line('|' + line))
        cls._options = options

    @classmethod
    def parse_markdown_line(cls, line):
        # type: (str) -> VolumeDriverConfigOption
        """
        Convert an option line string to an VolumeDriverConfigOption instance
        :param line: Line string to parse
        :type line: str
        :return: The VolumeDriverConfigOption instnace
        :rtype: VolumeDriverConfigOption
        """
        stripped = [s.strip() for s in line.split('|')[1:-1]]
        if len(stripped) > 5:
            # A remark with a '|' in it
            remark_pieces = stripped[4:]
            stripped[4] = '|'.join(remark_pieces)
            stripped = stripped[:5]
        component, key, default_value, dynamically_reconfigurable_string, remarks = stripped
        return cls(component, key, default_value, dynamically_reconfigurable_string == 'yes', remarks)


class BaseStorageDriverConfig(object):

    component_identifier = 'base'

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
        # type: (BaseStorageDriverConfig) -> Dict[str, Tuple[any, any]]
        """
        Get all keys that differ from the other value
        :param other: Other instance
        :type other: BaseStorageDriverConfig
        :return: A Dict with all keys that different mapped with the value of the current item and the compared item
        :rtype: Dict[str, Tuple[any, any]]
        """
        diff = {}
        if not isinstance(other, type(self)):
            raise ValueError('Other is not of type {0}'.format(BaseStorageDriverConfig))
        for key, value in other.to_dict().iteritems():
            self_value = getattr(self, key)
            if value != self_value:
                diff[key] = (value, self_value)
        return diff

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
        return bool(option) and option.dynamically_reconfigurable

    @classmethod
    def is_dynamically_reloadable(cls, key):
        """
        :param key: Config key to look for
        :type key: str
        :return: True if the option is dynamically reloadable
        :rtype: bool
        """
        if cls.component_identifier == BaseStorageDriverConfig.component_identifier:
            raise NotImplementedError('Component identifier has not been implemented by the current class')
        return cls._is_dynamically_reloadable(cls.component_identifier, key)
