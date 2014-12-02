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
