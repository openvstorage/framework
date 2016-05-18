// Copyright (C) 2016 iNuron NV
//
// This file is part of Open vStorage Open Source Edition (OSE),
// as available from
//
//      http://www.openvstorage.org and
//      http://www.openvstorage.com.
//
// This file is free software; you can redistribute it and/or modify it
// under the terms of the GNU Affero General Public License v3 (GNU AGPLv3)
// as published by the Free Software Foundation, in version 3 as it comes
// in the LICENSE.txt file of the Open vStorage OSE distribution.
//
// Open vStorage is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY of any kind.
/*global define */
define(['jquery', 'ovs/generic'], function($, generic) {
    "use strict";
    function handleEvent(data) {
        if (data.type === 'vmachine_deleted' ||
            data.type === 'vmachine_created' ||
            data.type === 'vmachine_renamed' ||
            data.type === 'vdisk_attached' ||
            data.type === 'vdisk_detached') {
            generic.alertInfo(
                $.t('ovs:events.' + data.type),
                $.t('ovs:events.' + data.type + '_content', data.metadata)
            );
        }
    }

    return {
        handleEvent: handleEvent
    };
});
