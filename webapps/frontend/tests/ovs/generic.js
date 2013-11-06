/*global define, describe, spyOn, it, expect, jasmine */
define(['ovs/generic', 'knockout', 'jquery'], function(generic, ko, $) {
    'use strict';
    describe('Generic', function() {
        it('getTimestamp should generate timestamp', function() {
            expect(generic.getTimestamp()).toBeCloseTo((new Date()).getTime(), -1);
        });

        it('getBytesByHuman should format correctly', function() {
            expect(generic.getBytesHuman(1)).toBe('1 B');
            expect(generic.getBytesHuman(1000)).toBe('1000 B');
            expect(generic.getBytesHuman(2 * 1000)).toBe('2000 B');
            expect(generic.getBytesHuman(3 * 1000)).toBe('2.93 KiB');
            expect(generic.getBytesHuman(3 * 1024 * 1000)).toBe('2.93 MiB');
            expect(generic.getBytesHuman(3 * 1024 * 1024 * 1000)).toBe('2.93 GiB');
            expect(generic.getBytesHuman(3 * 1024 * 1024 * 1024 * 1000)).toBe('2.93 TiB');
            expect(generic.getBytesHuman(3 * 1024 * 1024 * 1024 * 1024)).toBe('3 TiB');
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
            // Default (at time of writing test) 3 steps
            jasmine.Clock.useMock();
            var testModel = {
                value: ko.observable(undefined)
            };
            // Smooth undefined > 100
            generic.smooth(testModel.value, 100, 1);
            jasmine.Clock.tick(80);  // 80
            expect(testModel.value()).toBe(100);
            // Smooth 100 > 160
            generic.smooth(testModel.value, 160);
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
            generic.smooth(testModel.value, 100, 2);
            expect(testModel.value()).toBe(100);
            // Smooth 100 > 103
            generic.smooth(testModel.value, 103, 2);
            jasmine.Clock.tick(80);  // 80
            expect(testModel.value()).toBe(102);
            jasmine.Clock.tick(80);  // 160
            expect(testModel.value()).toBe(103);
            testModel.value(100.5);
            // Smooth 100.5 > 103.5
            generic.smooth(testModel.value, 103.5, 2);
            jasmine.Clock.tick(80);  // 80
            expect(testModel.value()).toBe(102);
            jasmine.Clock.tick(80);  // 160
            expect(testModel.value()).toBe(103.5);
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