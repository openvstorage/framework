// Copyright 2015 CloudFounders NV
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
/*global define, window */
define([
    'knockout', 'jquery', 'd3',
    'ovs/generic'
], function(ko, $, d3, generic) {
    "use strict";
    return function () {
        var self = this;

        // Variables
        self.text          = undefined;
        self.unique        = 'e_' + generic.getTimestamp() + '_' + Math.random().toString().substr(2, 10);
        self.subscriber    = undefined;
        self.width         = 0;
        self.d3d           = {};
        self.setupFinished = false;

        // Computed
        self.trigger = ko.computed(function() {
            var data = [];
            if (self.disk === undefined) {
                return data;
            }
            data.push(self.disk.trigger());
            $.each(self.disk.partitions(), function(index, partition) {
                data.push(partition.trigger());
            });
            return data;
        });

        // Functions
        self.draw = function() {
            var partition, disk
            if (self.disk === undefined || self.disk.loaded() === false) {
                return;
            }
            if (self.setupFinished === false) {
                self.d3d.x = d3.time.scale()
                    .domain([0, self.disk.size()])
                    .range([0, self.width]);
                self.d3d.svg = d3.select('#' + self.unique).append('svg')
                    .attr('width', self.width)
                    .attr('height', self.height)
                    .style('font-size', 10);
                self.d3d.background = self.d3d.svg.append('g')
                    .attr('transform', 'translate(0,16)');
                self.d3d.foreground = self.d3d.svg.append('g')
                    .attr('transform', 'translate(0,0)');
                disk = self.d3d.background.append('g')
                    .attr('class', 'disk');
                disk.append('rect')
                    .attr('class', 'background')
                    .style('fill', '#eeeeee')
                    .attr('width', self.width)
                    .attr('height', self.height - 30);
                disk.append('circle')
                    .attr('class', 'state')
                    .attr({ cy: (self.height - 29) / 2 })
                    .attr('r', 7);
                disk.append('text')
                    .attr('class', 'info-first')
                    .attr({ y: 13 })
                    .text(function () {
                        return $.t('ovs:generic.rawdisk');
                    });
                disk.append('text')
                    .attr('class', 'info-second')
                    .attr({ y: 25 });
                self.d3d.foreground.append('line')
                    .attr('stroke', '#666666')
                    .attr('stroke-width', 2)
                    .attr({ x1: 1, y1: 13 })
                    .attr({ x2: 1, y2: self.height - 14 });
                self.d3d.foreground.append('text')
                    .attr({ x: 1, y: 10 })
                    .attr('text-anchor', 'left')
                    .text(generic.formatBytes(0));
                self.d3d.foreground.append('line')
                    .attr('stroke', '#666666')
                    .attr('stroke-width', 2)
                    .attr({ x1: self.width - 1, y1: 13 })
                    .attr({ x2: self.width - 1, y2: self.height - 14 });
                self.d3d.foreground.append('text')
                    .attr('class', 'disk-end-text')
                    .attr({ x: self.width - 1, y: 10 })
                    .attr('text-anchor', 'end');
                self.setupFinished = true;
            }
            self.d3d.x.domain([0, self.disk.size()]);
            self.d3d.partitions = self.d3d.background
                .selectAll('.partition')
                .data(self.disk.partitions(), function(entry) {
                    return entry.guid();
                });
            self.d3d.foreground.select('.disk-end-text')
                .text(generic.formatBytes(self.disk.size()));
            self.d3d.background.select('.disk .state')
                .style('fill', function() {
                    if (['ERROR', 'MISSING'].contains(self.disk.state())) {
                        return 'red';
                    }
                    return 'green';
                })
                .attr({ cx: function() {
                    var partitions = self.disk.partitions(), lastPartition, start;
                    if (partitions.length === 0) {
                        return 15;
                    }
                    lastPartition = partitions[partitions.length - 1];
                    start = Math.round(self.d3d.x(lastPartition.offset() + lastPartition.size()));
                    return start + 15;
                }})
                .style('display', function() {
                    var partitions = self.disk.partitions(), lastPartition, start;
                    if (partitions.length === 0) {
                        return null;
                    }
                    lastPartition = partitions[partitions.length - 1];
                    start = Math.round(self.d3d.x(lastPartition.offset() + lastPartition.size()));
                    return start < self.width ? null : 'none';
                });
            self.d3d.background.select('.disk .info-first')
                .attr({ x: function() {
                    var partitions = self.disk.partitions(), lastPartition, start;
                    if (partitions.length === 0) {
                        return 28;
                    }
                    lastPartition = partitions[partitions.length - 1];
                    start = Math.round(self.d3d.x(lastPartition.offset() + lastPartition.size()));
                    return start + 28;
                }});
            self.d3d.background.select('.disk .info-second')
                .attr({ x: function() {
                    var partitions = self.disk.partitions(), lastPartition, start;
                    if (partitions.length === 0) {
                        return 28;
                    }
                    lastPartition = partitions[partitions.length - 1];
                    start = Math.round(self.d3d.x(lastPartition.offset() + lastPartition.size()));
                    return start + 28;
                }})
                .text(function () {
                    var size = self.disk.size();
                    $.each(self.disk.partitions(), function(index, entry) {
                        size -= entry.size();
                    });
                    return $.t('ovs:generic.spaceleft', { amount: generic.formatBytes(size) });
                });
            partition = self.d3d.partitions.enter()
                .append('g')
                .attr('class', 'partition');
            partition.append('rect')
                .attr('class', 'background')
                .style('fill', '#cccccc')
                .attr('height', self.height - 30);
            partition.append('line')
                .attr('class', 'start-line')
                .attr('stroke', '#666666')
                .attr('stroke-width', 2)
                .attr({ x1: 1, y1: 0 })
                .attr({ x1: 1, y1: self.height - 26 });
            partition.append('text')
                .attr('class', 'start-text')
                .attr({ x: 1, y: self.height - 16 });
            partition.append('line')
                .attr('class', 'end-line')
                .attr('stroke', '#666666')
                .attr('stroke-width', 2);
            partition.append('text')
                .attr('class', 'end-text');
            partition.append('circle')
                .attr('class', 'state')
                .attr({ cx: 15, cy: (self.height - 29) / 2 })
                .attr('r', 7);
            partition.append('text')
                .attr('class', 'info-first')
                .attr({ x: 28, y: 13 });
            partition.append('text')
                .attr('class', 'info-second')
                .attr({ x: 28, y: 25 });
            self.d3d.partitions
                .attr('transform', function(entry) {
                    return 'translate(' + Math.round(self.d3d.x(entry.offset())) + ',0)';
                });
            self.d3d.partitions.select('.background')
                .attr('width', function(entry) {
                    return Math.round(self.d3d.x(entry.size()));
                });
            self.d3d.partitions.select('.start-line')
                .style('display', function(entry, index) {
                    if (index === 0) { return null; }
                    var previousEntry = self.d3d.partitions.data()[index - 1];
                    return Math.round(self.d3d.x(entry.offset())) === Math.round(self.d3d.x(previousEntry.offset() + previousEntry.size())) ? 'none' : null;
                });
            self.d3d.partitions.select('.start-text')
                .attr('text-anchor', function(entry, index) {
                    if (index === 0) { return 'left'; }
                    return 'middle';
                })
                .style('display', function(entry, index) {
                    if (index === 0) { return null; }
                    var previousEntry = self.d3d.partitions.data()[index - 1];
                    return Math.round(self.d3d.x(entry.offset())) === Math.round(self.d3d.x(previousEntry.offset() + previousEntry.size())) ? 'none' : null;
                })
                .text(function(entry) {
                    return generic.formatBytes(entry.offset());
                });
            self.d3d.partitions.select('.end-line')
                .attr({ x1: function(entry) { return Math.round(self.d3d.x(entry.size())) - 1; }, y1: 0 })
                .attr({ x2: function (entry) { return Math.round(self.d3d.x(entry.size())) - 1; }, y2: self.height - 26 });
            self.d3d.partitions.select('.end-text')
                .attr({ x: function(entry) { return Math.round(self.d3d.x(entry.size())) - 1; }, y: self.height - 16 })
                .attr('text-anchor', function(entry, index) {
                    if (index === self.d3d.partitions.data().length - 1) { return 'end'; }
                    return 'middle';
                })
                .text(function(entry) {
                    return generic.formatBytes(entry.offset() + entry.size());
                });
            self.d3d.partitions.select('.state')
                .style('fill', function(entry) {
                    if (['ERROR', 'MISSING'].contains(entry.state())) {
                        return 'red';
                    }
                    return 'green';
                });
            self.d3d.partitions.select('.info-first')
                .text(function (entry) {
                    var entries = [];
                    if (entry.mountpoint() !== null) {
                        entries.push($.t('ovs:generic.mountpoint') + ': ' + entry.mountpoint());
                    }
                    if (entry.filesystem() !== null) {
                        entries.push($.t('ovs:generic.filesystem') + ': ' + entry.filesystem());
                    }
                    if (entry.filesystem() === null && entry.mountpoint() === null) {
                        entries.push($.t('ovs:generic.rawpartition'));
                    }
                    return entries.join(' - ');
                });
            self.d3d.partitions.select('.info-second')
                .text(function (entry) {
                    var entries = [];
                    if (entry.size() !== null) {
                        entries.push($.t('ovs:generic.size') + ': ' + generic.formatBytes(entry.size()));
                    }
                    return entries.join(' - ');
                })
                .style('display', function(entry) {
                    return entry.size() === null ? 'none' : null;
                });
            self.d3d.partitions.exit()
                .remove();
        };

        // Durandal
        self.activate = function(settings) {
            if (!settings.hasOwnProperty('disk')) {
                throw 'Disk should be specified';
            }
            self.disk = settings.disk;
            self.width = generic.tryGet(settings, 'width', 500);
            self.height = 60;
            self.subscriber = self.disk.trigger.subscribe(function() {
                self.draw();
            });
        };
        self.deactivate = function() {
            if (self.subscriber !== undefined) {
                self.subscriber.dispose();
            }
        };
        self.compositionComplete = function() {
            self.draw();
        };
    };
});
