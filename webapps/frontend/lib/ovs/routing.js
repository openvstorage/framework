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
define(['jquery'], function($) {
    "use strict";
    var mainRoutes, siteRoutes, buildSiteRoutes, loadHash;

    mainRoutes = [
       { route: '',              moduleId: 'viewmodels/redirect', nav: false },
       { route: ':mode*details', moduleId: 'viewmodels/index',    nav: false }
    ];
    siteRoutes = [
        { route: '',                     moduleId: 'dashboard',            title: $.t('ovs:dashboard.title'),             titlecode: 'ovs:dashboard.title',                nav: false, main: false },
        { route: 'storagerouters',       moduleId: 'storagerouters',       title: $.t('ovs:storagerouters.title'),        titlecode: 'ovs:storagerouters.title',        nav: true,  main: true  },
        { route: 'storagerouters/:guid', moduleId: 'storagerouter-detail', title: $.t('ovs:storagerouters.detail.title'), titlecode: 'ovs:storagerouters.detail.title', nav: false, main: false },
        { route: 'vpools',               moduleId: 'vpools',               title: $.t('ovs:vpools.title'),                titlecode: 'ovs:vpools.title',                   nav: true,  main: true  },
        { route: 'vpool/:guid',          moduleId: 'vpool-detail',         title: $.t('ovs:vpools.detail.title'),         titlecode: 'ovs:vpools.detail.title',            nav: false, main: false },
        { route: 'vmachines',            moduleId: 'vmachines',            title: $.t('ovs:vmachines.title'),             titlecode: 'ovs:vmachines.title',                nav: true,  main: true  },
        { route: 'vmachine/:guid',       moduleId: 'vmachine-detail',      title: $.t('ovs:vmachines.detail.title'),      titlecode: 'ovs:vmachines.detail.title',         nav: false, main: false },
        { route: 'vdisks',               moduleId: 'vdisks',               title: $.t('ovs:vdisks.title'),                titlecode: 'ovs:vdisks.title',                   nav: true,  main: true  },
        { route: 'vdisk/:guid',          moduleId: 'vdisk-detail',         title: $.t('ovs:vdisks.detail.title'),         titlecode: 'ovs:vdisks.detail.title',            nav: false, main: false },
        { route: 'vtemplates',           moduleId: 'vtemplates',           title: $.t('ovs:vtemplates.title'),            titlecode: 'ovs:vtemplates.title',               nav: true,  main: true  },
        { route: 'statistics',           moduleId: 'statistics',           title: $.t('ovs:statistics.title'),            titlecode: 'ovs:statistics.title',               nav: false, main: false },
        { route: 'login',                moduleId: 'login',                title: $.t('ovs:login.title'),                 titlecode: 'ovs:login.title',                    nav: false, main: false },
        { route: 'about',                moduleId: 'about',                title: $.t('ovs:about.title'),                 titlecode: 'ovs:about.title',                    nav: true,  main: false },
        { route: 'hmc',                  moduleId: 'pmachines',            title: $.t('ovs:pmachines.title'),             titlecode: 'ovs:pmachines.title',                nav: true,  main: false }
    ];

    buildSiteRoutes = function(mode) {
        var i;
        for (i = 0; i < siteRoutes.length; i += 1) {
            siteRoutes[i].hash = '#' + mode + '/' + siteRoutes[i].route;
        }
    };
    loadHash = function(module, params) {
        var i, item, hash;
        params = params || {};
        module = '/' + module;
        for (i = 0; i < siteRoutes.length; i += 1) {
            if (siteRoutes[i].moduleId.indexOf(module, siteRoutes[i].moduleId.length - module.length) !== -1) {
                hash = siteRoutes[i].hash;
                for (item in params) {
                    if (params.hasOwnProperty(item)) {
                        hash = hash.replace(':' + item, params[item].call ? params[item]() : params[item]);
                    }
                }
                return hash;
            }
        }
        return '/';
    };

    return {
        mainRoutes     : mainRoutes,
        siteRoutes     : siteRoutes,
        buildSiteRoutes: buildSiteRoutes,
        loadHash       : loadHash
    };
});
