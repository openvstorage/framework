// Copyright (C) 2017 iNuron NV
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
/**
 * Service to help with vdisk related tasks
 */
define([
    'jquery', 'knockout',
    'ovs/api', 'ovs/shared'
], function ($, ko,
             api, shared) {
    function VDiskService() {
        var self = this;

        /**
         * Loads the vdisk
         * @param guid: guid of the vdisk
         * @returns {Promise<T>}
         */
        self.loadVDisk = function (guid) {
            return api.get('vdisks/' + guid)
        };


        /**
         * Get the vdisk handle
         * @param options: options of the vdisk
         * @returns {Promise<T>}
         */
        self.loadVDisksHandle = function (options) {
            return api.get('vdisks', { queryparams: options })
                .then(shared.tasks.wait)
        };


        /**
         * Loads the config params of the vdisk
         * @param guid: Additional query params.
         * @returns {Promise<T>}
         */
        self.loadConfig = function (guid) {
            return api.get('vdisks/' + guid + '/get_config_params')
                .then(shared.tasks.wait)
        };
        /**
         * Update the configuration parameters of the VDisk
         * @param guid: Guid of the VDisk
         * @param new_config_params: New config params
         * @returns {Promise<T>}
         */
        self.setConfig = function (guid, new_config_params) {
                return api.post('vdisks/' + guid + '/set_config_params', {
                    data: { new_config_params: new_config_params}
                })
                .then(shared.tasks.wait)
        };
        /**
         * Set the VDisk as template
         * @param guid: Guid of the VDisk
         * @returns {Promise<T>}
         */
        self.setAsTemplate = function (guid){
            return api.post('vdisks/' + guid + '/set_as_template')
                .then(shared.tasks.wait)
        };
        /**
         * Scrub the VDisk
         * @param guid: Guid of the VDisk
         * @returns {Promise<T>}
         */
        self.scrub = function (guid){
            return api.post('vdisks/' + guid + '/scrub')
                .then(shared.tasks.wait)
        };
        /**
         * Remove a snapshot from the VDisk
         * @param guid: Guid of the VDisk
         * @param snapshotid: ID of the snapshot
         * @returns {Promise<T>}
         */
        self.removeSnapshot = function (guid, snapshotid){
            return api.post('vdisks/' + guid + '/remove_snapshot', {
                    data: { snapshot_id: snapshotid }
                }).then(shared.tasks.wait)
        };
        /**
         * Remove a VDisk
         * @param guid: Guid of the VDisk
         * @returns {Promise<T>}
         */
        self.removeVDisk = function (guid){
            return api.del('vdisks/' + guid)
                .then(shared.tasks.wait)
        };
        /**
         * Restart a VDisk
         * @param guid: Guid of the VDisk
         * @returns {Promise<T>}
         */
        self.restart = function (guid){
            return api.post('vdisks/' + guid + '/restart')
                .then(shared.tasks.wait)
        };
        /**
         * Cleans the device name of a VDisk
         * @param name: Name of the VDisk
         * @returns {string}
         */
        self.cleanDeviceName = function(name) {
            var cleaned = name.replace(/^(\/)+|(\/)+$/g, '').replace(/ /g,"_").replace(/[^a-zA-Z0-9-_\.\/]+/g, "");
            while (cleaned.indexOf('//') > -1) {
                cleaned = cleaned.replace(/\/\//g, '/');
            }
            if (cleaned.length > 4 && cleaned.slice(-4) === '.raw') {
                return cleaned;
            }
            return cleaned + '.raw';
        }
    }
    return new VDiskService();

});
