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
     * @param translationPrefix: Translation prefix to use in the form.
     * It will use the prefix + field to make translations
     * If there is a translation of prefix + field + _help, it will display the help text too
     * Can be changed using setTranslationPrefix
     * @param displayPage: Page to display the form on. Used to control which questions to display in case of multipaged forms
     * Can be changed using setDisplayPage
     * @constructor
     */
    function Form(metadata, formMapping, translationPrefix, displayPage) {
        var self = this;

        this.metadata = metadata;
        this.formMapping = formMapping;
        this.fieldMapping = {};
        this.questions = ko.observableArray([]);
        // FormItems use this prefix for their translation and help text computed
        this.translationPrefix = ko.observable(translationPrefix);
        this.displayPage = ko.observable(displayPage);
        // Get questions and type mapping
        this.generateQuestions();

        // Used to cache form validation
        this.validation = ko.pureComputed(function(){
            return self.validateForm()
        })

    }
    Form.prototype = {
        /**
         * Set the translation prefix of the form. Used when switching from pages for different translations
         * @param translationPrefix
         * @returns {*}
         */
        setTranslationPrefix: function(translationPrefix) {
            return this.translationPrefix(translationPrefix)
        },
        setDisplayPage: function(displayPage) {
            return this.displayPage(displayPage)
        },
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
                var formItem = Form.prototype.generateFormItem.call(self, fieldTypeMap.field, fieldTypeMap.type);
                self.questions.push(formItem);
            });
            this.questions.sort(function (a, b) {
                if (a.group === b.group) {
                    return a.id.localeCompare(b.id);
                }
                return a.group < b.group ? -1 : 1;
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
         * and tries to fetch the translation. Defaults to translationPrefix of the form + 'invalid_FIELD'
         */
        validateForm: function(translatePrefix) {
            translatePrefix = translatePrefix || this.translationPrefix() + 'invalid_';
            var reasons = [], fields = [];
            $.each(ko.utils.unwrapObservable(this.questions), function(index, formItem){
                var observable = formItem.data;
                if (observable() === undefined || (generic.isFunction(observable.valid) && !observable.valid())){
                    fields.push(formItem.id);
                    reasons.push($.t(translatePrefix + formItem.field))
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
            var formItemGroup = formItem.group;
            if (questions.length === 0 || formItemGroup < questions[0].group) {
                return 0
            }
            if (formItemGroup > questions[questions.length - 1].group) {
                return questions.length - 1
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
                    if (_formItem().group > formItemGroup) {
                        insertIndex = index + 1;
                        return false; // Break
                    }
                });
            }
            return insertIndex
        },
        /**
         * Generate a form item for a given field and type
         * @param field: Field to generate item for
         * @param type: Type to use for generation
         */
        generateFormItem: function(field, type) {
            // Gather all data from the form and link up the observable
            var group = 0;
            var target = field;
            var arrayType = false;
            var inputType = 'text';
            var display = [];
            var inputTextFormatFunc = function(item) { return item; };
            var extender, inputItems, widgetName, value;
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
                    inputTextFormatFunc = keyMap.mapping[keyMap.key].inputTextFormatFunc || inputTextFormatFunc;
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
            // Cast all data into a usable object
            var id = field;
            var extendable = false;
            // The linkedObservable can be the container for multiple formItems data
            if (linkedObservable && linkedObservable.isObservableArray) {
                extendable = true;
                if (!linkedObservable().contains(observable)) {
                    id += linkedObservable.push(observable) - 1;  // Push returns the amount of elements in the observableArray
                }
            }
            // Link item to the form
            if (linkedObservable && linkedObservable.isObservableArray) {
                var ids = [];
                if (field in this.fieldMapping){
                    ids = this.fieldMapping[field].ids
                }
                ids.push(id);
                this.fieldMapping[field] = {'observable': linkedObservable, 'ids': ids};
            }
            else {
                this.fieldMapping[field] = {'observable': observable, 'ids': [id]};
            }
            var formInput = new FormInput(inputType, inputItems, widgetName, inputTextFormatFunc);
            return new FormItem(this, id, observable, field, group, display, target, formInput, extendable)
        },
        /**
         * Used to insert a new question dynamically into the form
         * Think of a PLUS icon to add new entries to a list
         * These items should always be grouped with a separate group
         * @param field: Field to insert a form item for
         */
        insertGenerateFormItem: function(field){
            var self = this;
            $.each(Form.prototype.getFieldTypesFromMetadata(self.metadata), function(index, fieldTypeMap){
                if (field === fieldTypeMap.field) {
                    var formItem = Form.prototype.generateFormItem.call(self, fieldTypeMap.field, fieldTypeMap.type);
                    var insertIndex = Form.prototype.getInsertIndex.call(self, formItem);
                    if (formItem.extendable()) {
                        // Other inputs for this field may no longer get extended
                        $.each(self.questions(), function(index, question) {
                            if (question.field === formItem.field) {
                                question.extendable(false)
                            }
                        });
                    }
                    self.questions.splice(insertIndex, 0, formItem);  // Insert the item where it belongs
                }

            })
        },
        /**
         * Remove an item from the form
         * @param index: Required param. Index of the item to remove
         */
        removeFormItem: function(index) {
            var formItem =this.questions.splice(ko.utils.unwrapObservable(index), 1)[0];
            // Remove left over data entries
            this.fieldMapping[formItem.field].observable.remove(formItem.data);
            var ids = this.fieldMapping[formItem.field].ids;
            var spliceIndex = ids.indexOf(formItem.id);
            ids.splice(spliceIndex, 1);
        }
    };

    /**
     * Represents a form input type
     * @param type: Type of the input
     * @param items: Items for the input
     * @param widgetName: Name of the widget to use. Required when type == widget
     * @param textFormatFunc: Text format function. Used to format dropdown texts
     * @constructor
     */
    function FormInput(type, items, widgetName, textFormatFunc) {
        this.type = type;
        this.items = items;
        this.widgetName = widgetName;
        this.textFormatFunc = textFormatFunc;
    }

    /**
     * Represent a form item
     * @param form: Form that the item is a part of
     * @param id: ID of the item. Used for labeling
     * @param data: Data observable which will holds the data entered
     * @param field: Field that the item represent
     * @param group: Display group
     * @param display: Pages to display the item on
     * @param mappedField: Name of the property within the formData
     * @param input: Input type to use
     * @param extendable: Checks if the item is extendable
     * @constructor
     */
    function FormItem(form, id, data, field, group, display, mappedField, input, extendable) {
        var self = this;

        this.form = form;
        this.id = id;
        this.field = field;
        this.group = group;
        this.display = display;
        this.mappedField = mappedField;
        this.input = input;
        // Observables
        this.extendable = ko.observable(extendable);  // Adjusted when inserting a new item
        this.data = data; // Observable

        this.hasHelpText = ko.pureComputed(function() {
            var key = self.helpTextTranslation();
            return key !== $.t(key);
        });
        this.translation = ko.pureComputed(function() {
            return self.form.translationPrefix() + self.field;
        });
        this.extendTranslation = ko.pureComputed(function() {
            return self.form.translationPrefix() + 'add_' + self.field;
        });
        this.helpTextTranslation = ko.pureComputed(function() {
            return self.form.translationPrefix() + self.field + '_help';
        });
        this.canDisplay = ko.pureComputed(function() {
            return self.display.length === 0 || self.display.contains(self.form.displayPage());
        });

    }
    return Form;

});
