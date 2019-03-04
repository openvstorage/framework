# Copyright (C) 2019 iNuron NV
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

from ...constants.dal import DATAOBJECT_NAME


class MetaClass(type):
    """
    This metaclass provides dynamic __doc__ generation feeding doc generators
    """

    def __new__(mcs, name, bases, dct):
        """
        Overrides instance creation of all DataObject instances
        """
        if name != DATAOBJECT_NAME:
            # Property instantiation
            for internal in ['_properties', '_relations', '_dynamics']:
                data = set()
                for base in bases:  # Extend properties for deeper inheritance
                    if hasattr(base, internal):  # if the base already ran the metaclass: append to current class
                        data.update(getattr(base, internal))
                if '_{0}_{1}'.format(name, internal) in dct:  # instance._Testobject__properties. __properties cannot get overruled by inheritance
                    data.update(dct.pop('_{0}_{1}'.format(name, internal)))
                dct[internal] = list(data)
            # Doc generation - properties
            for prop in dct['_properties']:
                docstring = prop.docstring
                if isinstance(prop.property_type, type):
                    itemtype = prop.property_type.__name__
                    extra_info = ''
                else:
                    itemtype = 'Enum({0})'.format(prop.property_type[0].__class__.__name__)
                    extra_info = '(enum values: {0})'.format(', '.join(prop.property_type))
                dct[prop.name] = property(
                    doc='[persistent] {0} {1}\n@type: {2}'.format(docstring, extra_info, itemtype)
                )
            # Doc generation - relations
            for relation in dct['_relations']:
                itemtype = relation.foreign_type.__name__ if relation.foreign_type is not None else name
                dct[relation.name] = property(
                    doc='[relation] one-to-{0} relation with {1}.{2}\n@type: {3}'.format(
                        'one' if relation.onetoone else 'many',
                        itemtype,
                        relation.foreign_key,
                        itemtype
                    )
                )
            # Doc generation - dynamics
            for dynamic in dct['_dynamics']:
                if bases[0].__name__ == 'DataObject':
                    if '_{0}'.format(dynamic.name) not in dct:
                        raise LookupError('Dynamic property {0} in {1} could not be resolved'.format(dynamic.name, name))
                    method = dct['_{0}'.format(dynamic.name)]
                else:
                    methods = [getattr(base, '_{0}'.format(dynamic.name)) for base in bases if hasattr(base, '_{0}'.format(dynamic.name))]
                    if len(methods) == 0:
                        raise LookupError('Dynamic property {0} in {1} could not be resolved'.format(dynamic.name, name))
                    method = methods[0]
                docstring = method.__doc__.strip()
                if isinstance(dynamic.return_type, type):
                    itemtype = dynamic.return_type.__name__
                    extra_info = ''
                else:
                    itemtype = 'Enum({0})'.format(dynamic.return_type[0].__class__.__name__)
                    extra_info = '(enum values: {0})'.format(', '.join(dynamic.return_type))
                dct[dynamic.name] = property(
                    fget=method,
                    doc='[dynamic] ({0}s) {1} {2}\n@rtype: {3}'.format(dynamic.timeout, docstring, extra_info, itemtype)
                )

        return super(MetaClass, mcs).__new__(mcs, name, bases, dct)
