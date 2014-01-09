// license see http://www.openvstorage.com/licenses/opensource/
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
