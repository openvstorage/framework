// Copyright (C) 2018 iNuron NV
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
define(['jquery', 'i18next'],
    function($, i18n) {
    "use strict";

    var defaultLanguage = 'en-US';
    /**
     * Controls translations within the GUI
     * @constructor
     */
    function TranslationService(){
        this.defaultLanguage = defaultLanguage;
        this.language = defaultLanguage;
    }
    TranslationService.prototype = {
        /**
         * Set the language of the front-end
         * Forces re-translation of the UI
         * @param language: Language to set
         * @return {Promise<T>}
         */
        setLanguage: function (language) {
            this.language = language;
            return $.when().then(function () {
                i18n.setLng(language, function () {
                    $('html').i18n(); // Force retranslation of complete UI
                });
            })
        },
        /**
         * Reset the language to the default one and re-translation
         * @return {Promise<T>}
         */
        resetLanguage: function() {
            return this.setLanguage(defaultLanguage)
        }
    };
    return new TranslationService()
});
