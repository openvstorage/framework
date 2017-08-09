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
define([
    'jquery', 'knockout', 'ovs/generic'
], function($, ko, generic){
    "use strict";
    // Variables
    var listPrefix = 'list_of_';
    var typeMapping = {  // Types for formBuilding
        'ip': {
            'extender': {regex: generic.ipRegex},
            'inputType': 'text',  // default if missing
            'inputItems': null  // default if missing
        },
        'port': {
            'extender': {numeric: {min: 1, max: 65536}}
        },
        'integer': {
            'inputType': 'text',
            'extender': {numeric: {}}
        }
    };

    var formQuestions = ko.observableArray([]); // Keep a local value for caching
    var formFieldMapping = ko.observable({}); // Keep a local value for caching

    function generateFormData(metadata, actionMapping) {
        /**
         * Generate formData that can be used to generate a form
         * @param metadata: metadata to base form generation on. This is an object with possible actions and metadata about that action
         * If a particular action can be done (value = true) and the metadata surrounding the actions is also present: this form builder will
         * return an array with questions to be asked and validation based on the types provided via the metadata
         * The metadata aspect is an object filled with the name of param the api expects and the type
         * Current supported types: ip, port, integer and their list_of_ variants
         * Example: {clear: true, fill: true, fill_add: false, fill_metadata: {count: "integer"}}
         * @param actionMapping: Additional mutations to the form data
         * Consists of an object with keys that are identical to the param the api expects and value
         * Text types:
         * {
            'extender': {regex: generic.ipRegex}, // Custom extender to use
            'inputType': 'text',  // default if missing  // Custom input type
            'inputItems': null,  // default if missing  // Custom inputItems - used for dropdown
            'group': 0,  // Sorting purposes
            'displayOn': ['gather']  // Display on a particular step
           }
         * Dropdown
         * 'osd_type': {
            'fieldMap': 'type',  // Translate osd_type to type so in the form it will be self.data.formdata().type
            'inputType': 'dropdown',  // Generate dropdown, needs items
            'inputItems': ko.observableArray(['ASD', 'AD']),
            'group': 1,
            'displayOn': ['gather']
           }
         * @type {Array}
         */
        // Reset cache
        formQuestions([]);
        formFieldMapping({});

        var formData = {'questions': formQuestions, 'fieldMapping': formFieldMapping};
        for (var actionKey in metadata) {
            if (!metadata.hasOwnProperty(actionKey)) {
                continue;
            }
            if (metadata[actionKey] !== true) {
                continue
            }
            // Question data is stored within the key_metadata part
            var metadataKey = actionKey + '_metadata';
            for (var field in metadata[metadataKey]) {
                if (!metadata[metadataKey].hasOwnProperty(field)) {
                    continue;
                }
                // Defaults
                var type = metadata[metadataKey][field];
                var gatheredData = gatherItemData(field, type, actionMapping, formFieldMapping);
                var target = gatheredData.target;
                var inputItems = gatheredData.inputItems;
                var inputType = gatheredData.inputType;
                var group = gatheredData.group;
                var display = gatheredData.display;
                var observable = gatheredData.observable;
                var linkedObservable = gatheredData.linkedObservable;
                var formItem = generateFormItem(observable, field, group, display, target, inputType, inputItems, linkedObservable);
                formQuestions.push(formItem);
            }
        }
        formQuestions.sort(function (a, b) {
            if (a().group() === b().group()) {
                return a().id().localeCompare(b().id());
            }
            return a().group() < b().group() ? -1 : 1;
        });
        return formData
    }

    function gatherItemData(field, type, actionMapping, fieldMapping) {
        /**
         * Gather all relevant data to generate a form item
         * @param fieldMapping: optional param, used by special formBuilders
         */
        // Defaults
        actionMapping = (typeof actionMapping !== 'undefined') ? actionMapping : {};
        fieldMapping = (typeof fieldMapping !== 'undefined') ? fieldMapping : formFieldMapping;
        var group = 0;
        var target = field;
        var arrayType = false;
        var inputType = 'text';
        var display = undefined;
        var extender = undefined;
        var inputItems = undefined;
        // Check if list of known types
        if (type !== undefined && type.startsWith(listPrefix)) {
            arrayType = true;
            type = type.slice(listPrefix.length)  // Slice of the prefix (slice to start at the beginning)
        }
        // Check based on known types
        if (type in typeMapping) {
            target = typeMapping[type].fieldMap || target;
            inputItems = typeMapping[type].inputItems || inputItems;
            inputType = typeMapping[type].inputType || inputType;
            group = typeMapping[type].group || group;
            display = typeMapping[type].displayOn || display;
            extender = typeMapping[type].extender || extender;
        }
        // Optionally overrule with provided mapping
        if (field in actionMapping) {
            // Possibly determine target, extenders and inputType/items
            target = actionMapping[field].fieldMap || target;
            inputItems = actionMapping[field].inputItems || inputItems;
            inputType = actionMapping[field].inputType || inputType;
            group = actionMapping[field].group || group;
            display = actionMapping[field].displayOn || display;
            extender = actionMapping[field].extender || extender;
        }
        // Add data-binding
        var observable = ko.observable();
        var linkedObservable = undefined;
        if (arrayType === true) {
            // In case of an array, the consumer of the form items should be checking on the values of the array
            // Check if it has already been cached
            if (field in fieldMapping()) {
                linkedObservable = fieldMapping()[field]['observable']
            }
            else {
                linkedObservable = ko.observableArray([]);
            }
            if (extender !== undefined) {
                observable = observable.extend(extender)
            }
        }
        else {
            if (extender !== undefined) {
                observable = observable.extend(extender)
            }
        }
        return {
            'target': target,
            'arrayType': arrayType,
            'inputItems': inputItems,
            'inputType': inputType,
            'group': group,
            'display': display,
            'extender': extender,
            'observable': observable,
            'linkedObservable': linkedObservable
        }
    }

    function generateFormItem(observable, field, group, display, target, inputType, inputItems, linkedObservable, fieldMapping) {
        /**
         * Generate a form item
         * @param fieldMapping: optional param. This fieldmapping should be filled in
         */
        // Defaults
        group = (typeof group !== 'undefined') ? group : undefined;
        display = (typeof display !== 'undefined') ? display : undefined;
        inputType = (typeof inputType !== 'undefined') ? inputType : undefined;
        inputItems = (typeof inputItems !== 'undefined') ? inputItems : undefined;
        linkedObservable = (typeof linkedObservable !== 'undefined') ? linkedObservable : undefined;  // Used with arrays.
        fieldMapping = (typeof fieldMapping !== 'undefined') ? fieldMapping : formFieldMapping;
        var id = field;
        var extendable = false;
        // The linkedObservable can be the container for multiple formItems data
        if (linkedObservable !== undefined && linkedObservable.isObservableArray) {
            extendable = true;
            if (!linkedObservable().contains(observable)) {
                id += linkedObservable.push(observable) -1;
            }
        }
        var formItem = ko.observable({
            'id': ko.observable(id),
            'data': observable,  // Item corresponding to this input
            'field': ko.observable(field),
            'group': ko.observable(group),
            'display': ko.observable(display),
            'mappedField': ko.observable(target),
            'input': ko.observable({
                'type': inputType,  // If type = dropdown, will be populated with items
                'items': inputItems
            }),
            'extendable': ko.observable(extendable)
        });
        if (linkedObservable !== undefined && linkedObservable.isObservableArray) {
            var ids = [];
            if (field in fieldMapping()){
                ids = fieldMapping()[field].ids
            }
            ids.push(id);
            fieldMapping()[field] = {'observable': linkedObservable, 'ids': ids};
        }
        else {
            fieldMapping()[field] = {'observable': observable, 'ids': [id]};
        }
        return formItem
    }

    function getInsertIndex(formItem, questions) {
        /**
         * Returns the index where the item should be inserted.
         * Keeps sorting in mind
         */
        questions = (typeof questions !== 'undefined') ? questions : formQuestions;
        var insertIndex = -1;
        if (questions().length === 0) {
            return 0
        }
        if (formItem().group() < questions()[0]().group()) {
            return 0
        }
        if (formItem().group() > questions()[questions().length - 1]().group()) {
            return questions().length - 1
        }
        // Get a quick search of to see if the group value is actually in
        var firstIndex = questions().brSearchFirst(formItem().group(), 'group');
        // If it has been found, this will result in faster lookup speed
        if (firstIndex !== -1) {
            var lastIndex = questions().brSearchLast(formItem().group(), 'group');
            insertIndex = lastIndex + 1;
        }
        else {
            // No grouped items found for this group. Just insert a new group
            $.each(questions(), function(index, _formItem){
                if (_formItem().group() > formItem().group()) {
                    insertIndex = index + 1;
                    return false // Break
                }

            });
        }
        return insertIndex
    }

    function insertGeneratedFormItem(field, metadata, actionMapping, questions, fieldMapping){
        /**
         * Used to insert a new question dynamically into the form
         * Think of a PLUS icon to add new entries to a list
         * These items should always be grouped with a separate group
         * @param field: field to insert a form item for
         * @param metadata: metadata to use
         * @param questions: observable array, defaults to the cached questions
         */
        questions = (typeof questions !== 'undefined') ? questions : formQuestions;
        fieldMapping = (typeof fieldMapping !== 'undefined') ? fieldMapping : formFieldMapping;
        // Search the current
        for (var actionKey in metadata) {
            if (!metadata.hasOwnProperty(actionKey)) {
                continue;
            }
            if (metadata[actionKey] !== true) {
                continue
            }
            // Question data is stored within the key_metadata part
            var metadataKey = actionKey + '_metadata';
            if (!(metadataKey in metadata)) {
                continue
            }
            if (field in metadata[metadataKey]) {
                // Defaults
                var type = metadata[metadataKey][field];
                var gatheredData = gatherItemData(field, type, actionMapping, fieldMapping);
                var target = gatheredData.target;
                var inputItems = gatheredData.inputItems;
                var inputType = gatheredData.inputType;
                var group = gatheredData.group;
                var display = gatheredData.display;
                var observable = gatheredData.observable;
                var linkedObservable = gatheredData.linkedObservable;
                var arrayType = gatheredData.arrayType;
                var formItem = generateFormItem(observable, field, group, display, target, inputType, inputItems, linkedObservable, fieldMapping);
                var insertIndex = getInsertIndex(formItem, questions);
                if (arrayType === true) {
                    formItem().extendable(true);
                    if (questions().length !== 0 && insertIndex !== 0) {
                        var previousItem = questions()[insertIndex -1];
                        // The id consists of the field and an index.
                        var regex = new RegExp('^' + formItem().field());
                        var previousId = previousItem().id().replace(regex, '');
                        if ($.isNumeric(previousId)) {
                            previousItem().extendable(false);
                        }
                    }
                }
                questions.splice(insertIndex, 0, formItem);  // Insert the item where it belongs
            }
        }
    }

    function removeFormItem(index, questions, fieldMapping) {
        /**
         * Remove an item from the form
         * @index: required param. Index of the item to remove
         * @param questions: optional param: questions to use, required if fieldMapping is provided
         * @param fieldMapping: optional param, fieldMapping to use ,required if questions is provided
         */
        questions = (typeof questions !== 'undefined') ? questions : formQuestions;
        fieldMapping = (typeof fieldMapping !== 'undefined') ? fieldMapping : formFieldMapping;
        var formItem =questions.splice(index, 1)[0];
        // Remove left over data entries
        fieldMapping()[formItem().field()].observable.remove(formItem().data);
        var ids = fieldMapping()[formItem().field()].ids;
        var spliceIndex = ids.indexOf(formItem().id());
        ids.splice(spliceIndex, 1);
    }

    function validateForm(translatePrefix, questions) {
        /**
         * Validates a form based on questions this formbuilder could make.
         * @param questions: array of observables
         */
        questions = (typeof questions !== 'undefined') ? questions : formQuestions;
        var reasons = [], fields = [];
        if (ko.isObservable(questions) && questions.isObservableArray) {
            questions = questions()
        }
        $.each(questions, function(index, formItem){
            var observable = formItem().data;
            if (observable() === undefined || (typeof observable.valid === 'function' && !observable.valid())){
                fields.push(formItem().id());
                reasons.push($.t(translatePrefix + formItem().field()))
            }
        });
        return {value: reasons.length === 0, reasons: reasons, fields: fields};
    }

    function gatherData(fieldMapping) {
        /**
         * Returns a JS object with all data mapped to the requested fields
         */
        fieldMapping = (typeof fieldMapping !== 'undefined') ? fieldMapping : formFieldMapping;
        var data = ko.toJS(fieldMapping);  // Get value from every observable within this map
        for (var field in data) {
            if (!data.hasOwnProperty(field)) {
                continue;
            }
            data[field] = data[field].observable  // Just get the observable value
        }
        return data
    }

    return {
        'typeMapping': typeMapping,
        'generateFormData': generateFormData,
        'gatherItemData': gatherItemData,
        'generateFormItem': generateFormItem,
        'insertGeneratedFormItem': insertGeneratedFormItem,
        'removeFormItem': removeFormItem,
        'validateForm': validateForm,
        'gatherData': gatherData
    };
});