// Copyright 2014 CloudFounders NV
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
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
