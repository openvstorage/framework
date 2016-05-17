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
The Open vStorage Data Access Layer contains hybrid objects, providing a transparent access method
for data that consists of persistent model objects extended with extra reality properties covered by
a caching layer.

Most Open vStorage objects contain certain data that is used in the Open vStorage engine. However,
since various objects have properties that are managed by third party components, the architecture
is designed in such way that those properties are stored/managed by the components themselves. A
great example here is "disk", which contains metadata like 'name' or 'description' that are owned
by the Open vStorage engine. However, it also contains properties like 'used size' which is managed
by the volume driver.

To provide a single point of access to this data, hybrid objects are provided. In the above
scenario, a hybrid "disk" object has properties 'name', 'description' and 'used_size'. Where the
first two are backed by a persistent storage layer, and the last one by the volumedriver.

To provide fast access without too much overhead, an additional caching layer is added. When an
object is requested, it will be retrieved from cache first, and if not available, retrieved from
the persistent storage. The reality properties have individual caching settings which allows every
property cache timeout to be configured individually.
"""
