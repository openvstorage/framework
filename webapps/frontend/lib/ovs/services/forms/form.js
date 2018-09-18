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
            'extender': {numeric: {min: 1, max: 65535}}
        },
        'integer': {
            'inputType': 'widget',
            'widgetName': 'numberinput',
            'extender': {numeric: {}}
        }
    };


    /**
     *  Form class which holds data about the current form
     * @param metadata: Metadata to base form generation on. This is an object with possible actions and metadata about that action
     * If a particular action can be done (value = true) and the metadata surrounding the actions is also present: this form builder will
     * return an array with questions to be asked and validation based on the types provided via the metadata.
     * The metadata aspect is an object filled with the name of the param expected by the API and the type.
     * Current supported types: ip, port, integer and their list_of_ variants
     * Example: {clear: true, fill: true, fill_add: false, fill_metadata: {count: "integer"}}
     * @param formMapping: Additional mutations to the form data
         * Consists of an object with keys that are identical to the params and value the API expects
         * Text types:
         * {
            'extender': {regex: generic.ipRegex}, // Custom extender to use
            'inputType': 'text',  // default if missing  // Custom input type
            'inputItems': null,  // default if missing  // Custom inputItems - used for dropdown
            'group': 0,  // Sorting purposes
            'displayOn': ['gather']  // Display on a particular step
            'value: 10  // Starting value for the item, defaults to undefined
           }
         * Dropdown:
         * 'osd_type': {
            'fieldMap': 'type',  // Translate osd_type to type so in the form it will be self.data.formdata().type
            'inputType': 'dropdown',  // Generate dropdown, needs items
            'inputItems': ko.observableArray(['ASD', 'AD']),
            'group': 1,
            'displayOn': ['gather']
           }
     * @constructor
     */
    function Form(metadata, formMapping) {
        this.metadata = metadata;
        this.formMapping = formMapping;
        this.fieldMapping = {};
        this.questions = ko.observableArray([]);
        // Get questions and type mapping
        this.generateQuestions();

    }
    Form.prototype = {
        /**
         *
         * @param metadata: Metadata to base form generation on. This is an object with possible actions and metadata about that action
         * If a particular action can be done (value = true) and the metadata surrounding the actions is also present: this form builder will
         * return an array with questions to be asked and validation based on the types provided via the metadata.
         * The metadata aspect is an object filled with the name of the param expected by the API and the type.
         * Current supported types: ip, port, integer and their list_of_ variants
         * Example: {clear: true, fill: true, fill_add: false, fill_metadata: {count: "integer"}}
         * @returns {Array}
         */
        getFieldTypesFromMetadata: function(metadata) {
            var fieldTypes = [];
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
                    fieldTypes.push({field: field, type: metadata[metadataKey][field]});
                }
            }
            return fieldTypes
        },
        /**
         * Generate the formData
         * @returns {{questions: *, fieldMapping: *}}
         */
        generateQuestions: function() {
            var self = this;
            $.each(Form.prototype.getFieldTypesFromMetadata(this.metadata), function(index, fieldTypeMap) {
                var formItemData = Form.prototype.gatherItemData.call(self, fieldTypeMap.field, fieldTypeMap.type);
                self.questions.push(formItemData.toFormItem(self.fieldMapping));
            });
            this.questions.sort(function (a, b) {
                if (a.group() === b.group()) {
                    return a.id().localeCompare(b.id());
                }
                return a.group() < b.group() ? -1 : 1;
            });
        },

        /**
         * Gather all formData into a JS object
         * @returns {*}
         */
        gatherData: function() {
            /**
             * Returns a JS object with all data mapped to the requested fields
             */
            var data = ko.toJS(this.fieldMapping);  // Get value from every observable within this map
            for (var field in data) {
                if (!data.hasOwnProperty(field)) {
                    continue;
                }
                data[field] = data[field].observable;  // Just get the observable value
            }
            return data
        },
        /**
         * Validates a form based on questions this formbuilder could make.
         * @param translatePrefix: Translation prefix to use. Appends the field of the form to the prefix
         * and tries to fetch the translation
         */
        validateForm: function(translatePrefix) {
            var reasons = [], fields = [];
            $.each(ko.utils.unwrapObservable(this.questions), function(index, formItem){
                var observable = formItem.data;
                if (observable() === undefined || (typeof observable.valid === 'function' && !observable.valid())){
                    fields.push(formItem.id());
                    reasons.push($.t(translatePrefix + formItem.field()))
                }
            });
            return {value: reasons.length === 0, reasons: reasons, fields: fields};
        },
        /**
         * Returns the index where the item should be inserted.
         * Keeps sorting in mind
         */
        getInsertIndex: function(formItem) {
            var questions = ko.utils.unwrapObservable(this.questions);
            var formItemGroup = formItem.group();
            if (questions.length === 0 || formItemGroup < questions[0]().group()) {
                return 0
            }
            if (formItemGroup > questions[questions().length - 1]().group()) {
                return questions().length - 1
            }
            // Get a quick search of to see if the group value is actually in
            var insertIndex = -1;
            var firstIndex = questions.brSearchFirst(formItemGroup, 'group');
            // If it has been found, this will result in faster lookup speed
            if (firstIndex !== -1) {
                var lastIndex = questions.brSearchLast(formItemGroup, 'group');
                insertIndex = lastIndex + 1;
            }
            else {
                // No grouped items found for this group. Just insert a new group
                $.each(questions, function(index, _formItem){
                    if (_formItem().group() > formItemGroup) {
                        insertIndex = index + 1;
                        return false; // Break
                    }
                });
            }
            return insertIndex
        },
        /**
         * Used to insert a new question dynamically into the form
         * Think of a PLUS icon to add new entries to a list
         * These items should always be grouped with a separate group
         * @param field: Field to insert a form item for
         */
        insertGeneratedFormItem: function(field){
            for (var fieldTypeMap in Form.prototype.getFieldTypesFromMetadata(this.metadata)) {
                if (field === fieldTypeMap.field) {
                    var formItemData = Form.prototype.gatherItemData(fieldTypeMap.field, fieldTypeMap.type);
                    var formItem = formItemData.toFormItem(this.fieldMapping);
                    var insertIndex = Form.prototype.getInsertIndex(formItem);
                    if (formItemData.arrayType) {
                        // Other inputs for this field may no longer get extended
                        $.each(this.questions(), function(index, question) {
                            question = question();
                            if (question.field() === formItem().field()) {
                                question.extendable(false)
                            }
                        });
                        // Current item is extendable though
                        formItem().extendable(true);
                    }
                    this.questions.splice(insertIndex, 0, formItem);  // Insert the item where it belongs
                }

            }
        },
        /**
         * Remove an item from the form
         * @param index: Required param. Index of the item to remove
         */
        removeFormItem: function(index) {
            var formItem =this.questions.splice(ko.utils.unwrapObservable(index), 1)[0];
            // Remove left over data entries
            this.fieldMapping[formItem().field()].observable.remove(formItem().data);
            var ids = this.fieldMapping[formItem().field()].ids;
            var spliceIndex = ids.indexOf(formItem().id());
            ids.splice(spliceIndex, 1);
        },
        /**
         * Gather all relevant data to generate a form item
         * @param field: Field to generate the form item from
         * @param type: Type to generate the form item from
         * @returns {FormItemData}
         */
        gatherItemData: function(field, type) {
            // Defaults
            var group = 0;
            var target = field;
            var arrayType = false;
            var inputType = 'text';
            var display = [];
            var extender = undefined;
            var inputItems = undefined;
            var widgetName = undefined;
            var value = undefined;
            // Check if list of known types
            if (type !== undefined && type.startsWith(listPrefix)) {
                arrayType = true;
                type = type.slice(listPrefix.length);  // Slice of the prefix (slice to start at the beginning)
            }
            // Check based on known types and the provided mapping
            $.each([{key: type, mapping: typeMapping}, {key: field, mapping: this.formMapping}], function(index, keyMap) {
                if (keyMap.key in keyMap.mapping){
                    target = keyMap.mapping[keyMap.key].fieldMap || target;
                    inputItems = keyMap.mapping[keyMap.key].inputItems || inputItems;
                    inputType = keyMap.mapping[keyMap.key].inputType || inputType;
                    group = keyMap.mapping[keyMap.key].group || group;
                    display = keyMap.mapping[keyMap.key].displayOn || display;
                    extender = keyMap.mapping[keyMap.key].extender || extender;
                    widgetName = keyMap.mapping[keyMap.key].widgetName || widgetName;
                    value = keyMap.mapping[keyMap.key].value || value;
                }
            });
            // Add data-binding
            var observable = ko.observable(value);
            var linkedObservable = undefined;
            if (arrayType === true) {
                // In case of an array, the consumer of the form items should be checking on the values of the array
                // Check if it has already been cached
                if (field in this.fieldMapping) {
                    linkedObservable = this.fieldMapping[field]['observable']
                }
                else {
                    linkedObservable = ko.observableArray([]);
                }
                if (extender) {
                    observable = observable.extend(extender)
                }
            }
            else {
                if (extender) {
                    observable = observable.extend(extender)
                }
            }
            return new FormItemData(field, observable, target, arrayType, inputItems, inputType, group, display, extender, linkedObservable, widgetName)
        }
    };

    /**
     * Creates a new FormItemData. These are objects that hold all metadata about an input item
     * @param field: Field to represent
     * @param observable: Observable which holds the data about the object
     * @param target: Name of the property within the formData (Optional)
     * @param arrayType: Holds an array as data (Optional)
     * @param inputItems: List of items that can be chosen from (like in dropdowns) (Optional)
     * @param inputType: Name of the input type (Optional)
     * @param group: Group to display the item in (Optional)
     * @param display: Page to display this item on (Optional)
     * @param extender: Possible extender to extend the data observable with (Optional)
     * @param linkedObservable: Used with array types. Contains all observables to track (Optional)
     * @param widgetName: Name of the widget to use (if any) (Optional)
     * @constructor
     */
    function FormItemData(field, observable, target, arrayType, inputItems, inputType, group, display, extender, linkedObservable, widgetName) {
        this.field = field;
        this.target = target;
        this.arrayType = arrayType;
        this.inputItems = inputItems;
        this.inputType = inputType;
        this.group = group;
        this.display = display;
        this.extender = extender;
        this.observable = observable;
        this.linkedObservable = linkedObservable;
        this.wdigetName = widgetName;
    }
    FormItemData.prototype = {
        toFormItem: function(fieldMapping) {
            var id = this.field;
            var extendable = false;
            // The linkedObservable can be the container for multiple formItems data
            if (this.linkedObservable && this.linkedObservable.isObservableArray) {
                extendable = true;
                if (!this.linkedObservable().contains(this.observable)) {
                    id += this.linkedObservable.push(this.observable) - 1;  // Push returns the amount of elements in the observableArray
                }
            }
            // Link item to the form
            if (this.linkedObservable && this.linkedObservable.isObservableArray) {
                var ids = [];
                if (this.field in fieldMapping){
                    ids = fieldMapping[this.field].ids
                }
                ids.push(id);
                fieldMapping[this.field] = {'observable': this.linkedObservable, 'ids': ids};
            }
            else {
                fieldMapping[this.field] = {'observable': this.observable, 'ids': [id]};
            }
            return {
                'id': ko.observable(id),
                'data': this.observable,  // Item corresponding to this input
                'field': ko.observable(this.field),
                'group': ko.observable(this.group),
                'display': ko.observable(this.display),
                'mappedField': ko.observable(this.target),
                'input': ko.observable({
                    'type': this.inputType,  // If type = dropdown, will be populated with items
                    'items': this.inputItems,
                    'widgetName': this.widgetName  // if type = widget, a name will be filled in here
                }),
                'extendable': ko.observable(extendable)
            }
        }
    };
    return Form;

});
