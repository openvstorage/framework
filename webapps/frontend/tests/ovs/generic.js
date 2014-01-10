// license see http://www.openvstorage.com/licenses/opensource/
/*global define, describe, spyOn, it, expect, jasmine */
define([
    'ovs/generic', 'knockout', 'jquery',
    'ovs/extensions/knockout-helpers'
], function(generic, ko, $) {
    'use strict';
    describe('Generic', function() {
        beforeEach(function() {
            $.t = function(code) {
                return code;
            };
        });

        it('getTimestamp should generate timestamp', function() {
            expect(generic.getTimestamp()).toBeCloseTo((new Date()).getTime(), -1);
        });

        it('getBytesByHuman should format correctly', function() {
            var namespace = 'ovs:generic.';
            expect(generic.formatBytes(1)).toBe('1.00 ' + namespace + 'b');
            expect(generic.formatBytes(1000)).toBe('0.98 ' + namespace + 'kib');
            expect(generic.formatBytes(2 * 1000)).toBe('1.95 ' + namespace + 'kib');
            expect(generic.formatBytes(3 * 1000)).toBe('2.93 ' + namespace + 'kib');
            expect(generic.formatBytes(3 * 1024 * 1000)).toBe('2.93 ' + namespace + 'mib');
            expect(generic.formatBytes(3 * 1024 * 1024 * 1000)).toBe('2.93 ' + namespace + 'gib');
            expect(generic.formatBytes(3 * 1024 * 1024 * 1024 * 1000)).toBe('2.93 ' + namespace + 'tib');
            expect(generic.formatBytes(3 * 1024 * 1024 * 1024 * 1024)).toBe('3.00 ' + namespace + 'tib');
        });

        it('padRight to pad correctly', function() {
            expect(generic.padRight('test', ' ', 2)).toBe('test');
            expect(generic.padRight('test', ' ', 10)).toBe('test      ');
            expect(generic.padRight('test', '+', 10)).toBe('test++++++');
        });

        it('tryGet should behave correctly', function() {
            expect(generic.tryGet({}, 'abc', 1234)).toBe(1234);
            expect(generic.tryGet({ abc: 1234 }, 'abc')).toBe(1234);
            expect(generic.tryGet({ abc: 1234 }, 'abc', 5678)).toBe(1234);
            expect(generic.tryGet({ abc: 1234 }, 'xyz', 5678)).toBe(5678);
        });

        it('setting and getting cookies', function() {
            var value = generic.getTimestamp().toString();
            generic.setCookie('abc', value);
            generic.setCookie('generic_unittest', value, { seconds: 10 });
            generic.setCookie('def', value, { seconds: 10 });
            expect(generic.getCookie('generic_unittest')).toBe(value);
            expect(generic.getCookie('generic_unittest_x')).toBe(undefined);
        });

        it('keys should list all object keys', function() {
            Object.prototype.invalidValue = 0;
            expect(generic.keys({ abc: 1, def: 2, xyz: 3 })).toEqual(['abc', 'def', 'xyz']);
        });

        it('removeElement should remove the correct item', function() {
            var array = [123, 456, 789];
            generic.removeElement(array, 456);
            expect(array).toEqual([123, 789]);
            generic.removeElement(array, 0);
        });

        it('smooth should smooth a transition', function() {
            // Steps at: 75, 150, 225, 300
            jasmine.Clock.useMock();
            var testModel = {
                    value: ko.observable(undefined)
                },
                smoother = function(value) { return value; };
            // Smooth undefined > 100
            generic.smooth(testModel.value, undefined, 100, 1, smoother);
            jasmine.Clock.tick(80);  // 80
            expect(testModel.value()).toBe(100);
            // Smooth 100 > 160
            generic.smooth(testModel.value, testModel.value(), 160, 3, smoother);
            jasmine.Clock.tick(50);  // 50
            expect(testModel.value()).toBe(100);
            jasmine.Clock.tick(50);  // 100
            expect(testModel.value()).toBe(120);
            jasmine.Clock.tick(40);  // 140
            expect(testModel.value()).toBe(120);
            jasmine.Clock.tick(15);  // 155
            expect(testModel.value()).toBe(140);
            jasmine.Clock.tick(100);  // 255
            expect(testModel.value()).toBe(160);
            jasmine.Clock.tick(100);  // 355
            expect(testModel.value()).toBe(160);
            testModel.value(100);
            // Smooth 100 > 100
            generic.smooth(testModel.value, testModel.value(), 100, 2, smoother);
            expect(testModel.value()).toBe(100);
            // Smooth 100 > 103
            generic.smooth(testModel.value, testModel.value(), 103, 2, smoother);
            jasmine.Clock.tick(80);  // 80
            expect(testModel.value()).toBe(102);
            jasmine.Clock.tick(80);  // 160
            expect(testModel.value()).toBe(103);
            testModel.value(100.5);
            // Smooth 100.5 > 103.5
            generic.smooth(testModel.value, testModel.value(), 103.5, 2, smoother);
            jasmine.Clock.tick(80);  // 80
            expect(testModel.value()).toBe(102);
            jasmine.Clock.tick(80);  // 160
            expect(testModel.value()).toBe(103.5);
        });

        it('delta observables behave correctly', function() {
            var time = 0, formatter = function(value) { return value; },
                testModel = {
                    value: ko.deltaObservable(formatter)
                };
            spyOn(Date.prototype, 'getTime').andCallFake(function() { return time; });
            expect(testModel.value.initialized()).toBe(false);
            testModel.value(0);
            expect(testModel.value.initialized()).toBe(false);
            time += 1000;
            testModel.value(0);
            expect(testModel.value.initialized()).toBe(true);
            time += 2000;
            testModel.value(10);
            expect(testModel.value()).toBe(5);
            time += 1000;
            testModel.value(10);
            expect(testModel.value()).toBe(0);
            testModel.value({ value: 10, timestamp: 5000 });
            testModel.value({ value: 20, timestamp: 7000 });
            expect(testModel.value()).toBe(5);
            expect(time).toBe(4000);
        });

        it('alerting should work correctly', function() {
            spyOn($, 'pnotify').andCallFake(function(data) {
                return data;
            });
            expect(generic.alert('abc', 'def')).toEqual({
                title: 'abc',
                text: 'def',
                nonblock: true,
                delay: 3000
            });
            expect(generic.alertInfo('abc', 'def')).toEqual({
                title: 'abc',
                text: 'def',
                nonblock: true,
                delay: 3000,
                type: 'info'
            });
            expect(generic.alertSuccess('abc', 'def')).toEqual({
                title: 'abc',
                text: 'def',
                nonblock: true,
                delay: 3000,
                type: 'success'
            });
            expect(generic.alertError('abc', 'def')).toEqual({
                title: 'abc',
                text: 'def',
                nonblock: true,
                delay: 3000,
                type: 'error'
            });
        });

        it('xhrAbort abort if its a correct token with the correct state', function() {
            var aborts = 0,
                token = {
                    abort: function() {
                        aborts += 1;
                    },
                    state: function() {
                        return undefined;
                    }
                };
            spyOn(token, 'abort').andCallThrough();
            generic.xhrAbort(undefined);
            expect(token.abort).not.toHaveBeenCalled();
            generic.xhrAbort(token);
            expect(token.abort).not.toHaveBeenCalled();
            token.state = function() { return 'pending'; };
            generic.xhrAbort(token);
            expect(token.abort).toHaveBeenCalled();
            expect(aborts).toBe(1);
            var token2 = {
                abort: function() {
                    throw 'error';
                },
                state: function() {
                    return 'pending';
                }
            };
            expect(function() { generic.xhrAbort(token2); }).not.toThrow();
        });
    });
});
