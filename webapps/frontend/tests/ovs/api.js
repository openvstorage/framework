define(['ovs/api', 'ovs/shared', 'ovs/generic', 'jquery'], function(api, shared, generic, $) {
    'use strict';
    describe('API', function() {
        beforeEach(function() {
            spyOn($, 'ajax').andCallFake(function(url, data) {
                return $.Deferred(function(deferred) {
                    deferred.resolve({
                        url : url,
                        data: data
                    });
                }).promise();
            });
        });
        shared.authentication = { header: function() { return 'abc'; } };

        it('every call should have a timestamp attached', function() {
            var returnValue, finished = false;

            runs(function() {
                api.get('api/dummy', {}, {})
                    .done(function(value) {
                        returnValue = value;
                        finished = true;
                    });
            });
            waitsFor(function() { return finished; }, 'The call should complete', 250);
            runs(function() {
                expect(returnValue.url).toContain('timestamp=' + generic.getTimestamp().toString().substr(0, 10));
            });
        });

        it('get should use http GET', function() {
            var returnValue, finished = false;

            runs(function() {
                api.get('api/dummy', {}, {})
                    .done(function(value) {
                        returnValue = value;
                        finished = true;
                    });
            });
            waitsFor(function() { return finished; }, 'The call should complete', 250);
            runs(function() {
                expect(returnValue.data.type).toBe('GET');
            });
        });

        it('post should use http POST', function() {
            var returnValue, finished = false;

            runs(function() {
                api.post('api/dummy', {}, {})
                    .done(function(value) {
                        returnValue = value;
                        finished = true;
                    });
            });
            waitsFor(function() { return finished; }, 'The call should complete', 250);
            runs(function() {
                expect(returnValue.data.type).toBe('POST');
            });
        });

        it('del should use http DELETE', function() {
            var returnValue, finished = false;

            runs(function() {
                api.del('api/dummy', {}, {})
                    .done(function(value) {
                        returnValue = value;
                        finished = true;
                    });
            });
            waitsFor(function() { return finished; }, 'The call should complete', 250);
            runs(function() {
                expect(returnValue.data.type).toBe('DELETE');
            });
        });

        it('calldata should be correct', function() {
            var expectedData = {
                    type: 'GET',
                    timeout: 1000 * 60 * 60,
                    contentType: 'application/json',
                    data: JSON.stringify({ abc: 123, def: 456 }),
                    headers: {
                        'Authorization': 'abc',
                        'X-CSRFToken'  : 'def'
                    }
                },
                returnValue, finished = false;

            runs(function() {
                generic.setCookie('csrftoken', 'def', { seconds: 10 });
                api.get('api/dummy', { abc: 123, def: 456 }, {})
                    .done(function(value) {
                        returnValue = value;
                        finished = true;
                    });
            });
            waitsFor(function() { return finished; }, 'The call should complete', 250);
            runs(function() {
                expect(returnValue.data).toEqual(expectedData);
            });
        });

        it('filter values should be in the querystring', function() {
            var returnValue, finished = false;

            runs(function() {
                api.get('api/dummy', {}, { abc: 123, def: 456 })
                    .done(function(value) {
                        returnValue = value;
                        finished = true;
                    });
            });
            waitsFor(function() { return finished; }, 'The call should complete', 250);
            runs(function() {
                expect(returnValue.url).toContain('abc=123&def=456');
            });
        });
    });

    describe('API2', function() {
        beforeEach(function() {
            spyOn($, 'ajax').andCallFake(function(url, data) {
                return $.Deferred(function(deferred) {
                    deferred.reject({ readyState: 1, status: 1 }, 'textStatus', 'errorThrown');
                }).promise();
            });
        });
        shared.authentication = { header: function() { return 'abc'; } };

        it('a failed call should reject the promise', function() {
            var returnValue, finished = false;

            runs(function() {
                api.get('api/dummy', {}, {})
                    .fail(function(xmlHttpRequest, textStatus, errorThrown) {
                        returnValue = {
                            xmlHttpRequest: xmlHttpRequest,
                            textStatus    : textStatus,
                            errorThrown   : errorThrown
                        };
                        finished = true;
                    });
            });
            waitsFor(function() { return finished; }, 'The call should complete', 250);
            runs(function() {
                expect(returnValue.xmlHttpRequest).toEqual({ readyState: 1, status: 1 });
                expect(returnValue.textStatus).toBe('textStatus');
                expect(returnValue.errorThrown).toBe('errorThrown');
            });
        })
    });
});