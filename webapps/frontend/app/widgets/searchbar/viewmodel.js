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
define([
    'durandal/app', 'jquery', 'knockout', 'ovs/generic'
], function(app, $, ko, generic) {
    "use strict";

    /**
     *  Constructor for a search bar object
     *  Please note that using this search bar requires some change to the loading functions passed to the pager
     *  The loading functions need to return an ajax call so aborting can occur
     *  This search bar will emit an event when the query is updated (event: query:update) and the pager will listen on these events
     *  See vdisks for an example
     */
    return function() {
        var self = this;

        // Variables
        var operatorMap = {
            '=': 'EQUALS',
            '!=': 'NOT_EQUALS',
            'IN': 'IN',
            '~': 'CONTAINS',
            '>=': ['GT', 'EQUALS'],  // Order matters as the loop will iterate these options first
            '<=': ['LT', 'EQUALS'],
            '>': 'GT',
            '<': 'LT'
        };

        self.fieldMap = {'sr_guid': 'storagerouter_guid'};
        self.defaultField = 'guid';
        self.defaultQuery = undefined;

        // Observables
        self.search =       ko.observable("");
        self._query =       ko.observable();
        self.placeholder =  ko.observable("");
        self.width =        ko.observable("20em");

        // Computed
        self.query = ko.computed({
            // Computed for easier subscription management
            read: function() {
                return self._query()
            },
            write: function(query) {
                self._query(query);
                app.trigger('query:update', query)  // Notify subscribers
            }
        });

        // Functions
        /**
         * Computes the data to return (which is dumped inside the 'return' observable
         * Cases to handle:
         * a_value a_field:a_value a_field:operator+value a_field:operator+"value"
         * Current behaviour will chain all filters as 'AND'
         */
        self.getData = function() {
            var query = buildQuery(self.search(), self.defaultField, self.fieldMap, self.defaultQuery);
            self.query(query);
            return query
        };

        /**
         * Build an items entry for a given search entry
         */
        var buildQuery = function(searchString, defaultField, fieldMap, defaultQuery) {
            var field, operator, value, query, queryItems;
            if (defaultQuery === undefined) {
                queryItems = [];
            } else {
                queryItems = defaultQuery.items.slice()
            }
            query = {
                'type': 'AND',
                'items': queryItems
            };
            if (searchString === '') {
                return query
            }
            $.each(searchString.split(' '), function(index, entry) {
                var split_entry = entry.split(':');
                if (split_entry.length === 1) {
                    // Supply default field
                    field = defaultField;
                    value = split_entry[0];
                    if (isNaN(value)) {
                        operator = operatorMap['~']
                    } else {
                        operator = operatorMap['='];
                        value = parseFloat(value)
                    }
                }
                else {
                    // Potentially has quotes in it
                    field = extractField(cleanQuotes(split_entry[0]), fieldMap);
                    // Determine operator
                    $.each(operatorMap, function(opCode, opValue) {
                        if (split_entry[1].startsWith(opCode) === true) {
                            operator = opValue;
                            // Slice of the operator from the value
                            value = cleanQuotes(split_entry[1].slice( split_entry[1].indexOf(opCode) + opCode.length ));
                            return false  // Break
                        }
                    });
                    if (operator === undefined) {
                       value = cleanQuotes(split_entry[1]);
                       if (isNaN(value)) { operator = operatorMap['~'] }
                        else { operator = operatorMap['='] }
                    }
                    if (!isNaN(value)) {
                        value = parseFloat(value)
                    }
                }
                if (operator instanceof Array) {
                    // Chain these with OR
                    var orItems = [];
                    var orQuery = {
                        'type': 'OR',
                        'items': orItems
                    };
                    $.each(operator, function(opIndex, op){
                        orItems.push([field, op, value])
                    });
                    queryItems.push(orQuery)
                } else {
                    queryItems.push([field, operator, value])
                }
            });
            return query
        };

        var extractField = function(field, fieldMap) {
            if (fieldMap === undefined) { fieldMap = self.fieldMap}
            if (field in fieldMap) {return fieldMap[field]}
            return field
        };

        var testQueryBuilder = function() {
            var defaultField = 'guid';
            var fieldMap = {};
            var tests = [['', []],
                ['test', [['guid', 'EQUALS', 'test']]],
                ['test:IN"test"', [['test', 'IN', 'test']]],
                ['"test":!=test', [['test', 'NOT_EQUALS', 'test']]],
                ['test', [['test', 'EQUALS', 'test'], ['guid', 'EQUALS', 'test']], {items: [['test', 'EQUALS', 'test']]}]
            ];
            $.each(tests, function(index, testCase){
                var defaultQuery;
                if (testCase.length === 3) {
                    defaultQuery = testCase[2]
                }
                var query = buildQuery(testCase[0], defaultField, fieldMap, defaultQuery);
                if (generic.objectEquals(query.items, testCase[1]) === false) {
                    throw new Error(query.items.toString() + testCase[1].toString())
                }
            })
        };

        var cleanQuotes = function(text) {
            // Slicing is faster than regex :)
            if (text.startsWith("'") || text.startsWith('"')) {
                text = text.slice(1)
            }
            if (text.endsWith("'") || text.endsWith('"')) {
                text = text.slice(0, -1)
            }
            return text
        };

        // Durandal
        /**
         * Activator for the widget
         * @param settings: settings to supply
         * Settings is an object which should have:
         * - return: the observable to use to return the searching api data
         * And optionally has:
         * - placeholder: placeholder to show on the input field
         * - fieldMap: object which has the options as keys and the field as value (eg. {sr_guid: storagerouter_guid}
         * - default field: default field to map to when no fields are specified. eg( my_disk sr_guid:a_guid) with default field = name would result into: {name: my_disk sr_guid:a_guid}
         * @type settings: object
         */
        self.activate = function(settings) {
            if (!settings.hasOwnProperty('query')) {
                throw 'Query should be specified'
            }
            self.defaultQuery = settings['defaultQuery'];
            self._query = settings['query'];
            self.defaultField = generic.tryGet(settings, 'defaultField', null);
            self.placeholder(generic.tryGet(settings, 'placeholder', ''));
            self.width(generic.tryGet(settings, 'width', '20em'));
            $.extend(self.fieldMap, generic.tryGet(settings, 'fieldMap', {}));
        };

    };
});
