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
/*global define, describe, spyOn, it, expect, jasmine */
define(['ovs/refresher'], function(Refresher) {
    'use strict';
    describe('Refresher', function() {
        it('refresher behaves correctly', function() {
            jasmine.Clock.useMock();
            var refresher = new Refresher(),
                value = 0,
                dummy = {
                    load: function() {
                        value += 1;
                    }
                };
            spyOn(dummy, 'load').andCallThrough();

            refresher.init(dummy.load, 100);
            expect(dummy.load).not.toHaveBeenCalled();
            expect(refresher.stop).not.toThrow();
            refresher.start();
            refresher.run();
            expect(dummy.load.callCount).toEqual(1);
            jasmine.Clock.tick(90);
            expect(dummy.load.callCount).toEqual(1);
            jasmine.Clock.tick(20);
            expect(dummy.load.callCount).toEqual(2);
            jasmine.Clock.tick(200);
            expect(dummy.load.callCount).toEqual(4);
            refresher.stop();
            jasmine.Clock.tick(200);
            expect(dummy.load.callCount).toEqual(4);
            expect(value).toBe(4);
        });
    });
});
