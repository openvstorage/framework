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