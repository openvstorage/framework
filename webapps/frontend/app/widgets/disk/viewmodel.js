// Copyright 2015 Open vStorage NV
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
        self.subscribers   = [];
        self.width         = 0;
        self.d3d           = {};
        self.setupFinished = false;

        // Observables
        self.disk           = ko.observable();
        self.height         = ko.observable(60);
        self._collapsed     = ko.observable(true);

        // Computed
        self.extendedHeight = ko.computed(function() {
            if (self.disk() === undefined) {
                return self.height();
            }
            var maxUsage = 0;
            $.each(self.disk().enhancedPartitions(), function(index, partition) {
                if (partition.mountpoint() !== null && partition.usage().length > 0) {
                    maxUsage = Math.max(maxUsage, partition.usage().length);
                }
            });
            if (maxUsage === 0) {
                return self.height();
            }
            return self.height() + 18 + 16 + maxUsage * 18 - 2;
        });
        self.collapsed = ko.computed({
            write: function(value) {
                self._collapsed(value);
            },
            read: function() {
                if (self.disk() === undefined) {
                    return self._collapsed();
                }
                var hasMountpoint = false;
                $.each(self.disk().enhancedPartitions(), function(index, partition) {
                    if (partition.mountpoint() !== null && partition.usage().length > 0) {
                        hasMountpoint = true;
                    }
                });
                if (!hasMountpoint) {
                    return true;
                }
                return self._collapsed();
            }
        });
        self.trigger = ko.computed(function() {
            var data = [];
            if (self.disk() === undefined) {
                return data;
            }
            data.push(self.disk().trigger());
            $.each(self.disk().enhancedPartitions(), function(index, partition) {
                data.push(partition.trigger());
            });
            $.each(self.disk().emptySpaces(), function(index, space) {
                data.push(space.offset);
            });
            return data;
        });

        // Functions
        self.draw = function() {
            var partition, space, line, usage;
            if (self.disk() === undefined || self.disk().loaded() === false) {
                return;
            }

            line = d3.svg.line()
                .x(function(d) { return d.x; })
                .y(function(d) { return d.y; })
                .interpolate('basis');

            if (self.setupFinished === false) {
                // Prepare scale
                self.d3d.x = d3.time.scale()
                    .domain([0, self.disk().size()])
                    .range([0, self.width]);

                // Prepare svg
                self.d3d.svg = d3.select('#' + self.unique).append('svg')
                    .attr('width', self.width)
                    .style('font-size', 10);
                self.d3d.background = self.d3d.svg
                    .append('g')
                    .attr('class', 'background');
                self.d3d.content = self.d3d.svg
                    .append('g')
                    .attr('class', 'content');
                self.d3d.foreground = self.d3d.svg
                    .append('g')
                    .attr('class', 'foreground');

                // Add disk
                self.d3d.disk = self.d3d.background
                    .append('g')
                    .attr('class', 'disk');
                self.d3d.disk.append('rect')
                    .attr('class', 'background')
                    .attr('transform', 'translate(0,16)')
                    .attr('width', self.width)
                    .style('fill', '#eeeeee');
                self.d3d.disk.append('line')
                    .attr('class', 'start-line')
                    .attr('stroke', '#666666')
                    .attr('stroke-width', 2)
                    .attr({ x1: 1, y1: 13 });
                self.d3d.disk.append('text')
                    .attr('class', 'start-text')
                    .attr({ x: 1, y: 10 })
                    .attr('text-anchor', 'left')
                    .text(generic.formatBytes(0));
                self.d3d.disk.append('line')
                    .attr('class', 'end-line')
                    .attr('stroke', '#666666')
                    .attr('stroke-width', 2)
                    .attr({ x1: self.width - 1, y1: 13 });
                self.d3d.disk.append('text')
                    .attr('class', 'end-text')
                    .attr({ x: self.width - 1, y: 10 })
                    .attr('text-anchor', 'end');
                self.d3d.disk.append('text')
                    .attr('class', 'model')
                    .attr({ x: self.width / 2, y: 10 })
                    .style('text-anchor', 'middle')
                    .text(self.disk().diskModel().replace('_', ' '));
                self.setupFinished = true;
            }
            // Update the scale domain
            self.d3d.x.domain([0, self.disk().size()]);

            // Do some precalculations
            $.each(self.disk().enhancedPartitions(), function(index, partition) {
                partition.left = Math.round(self.d3d.x(partition.offset()));
                partition.width = Math.round(self.d3d.x(partition.size()));
                partition.right = partition.left + partition.width;
                partition.filledLeft = Math.round(self.d3d.x(partition.filledOffset));
                partition.filledWidth = Math.round(self.d3d.x(partition.filledSize));
                partition.relativeLeft = partition.filledLeft - partition.left;
                var previousLeft = 0;
                $.each(partition.usage(), function(jndex, usage) {
                    usage.filledLeft = previousLeft;
                    usage.filledWidth = usage.size !== null ? Math.round(self.d3d.x(usage.size)) : partition.filledWidth;
                    previousLeft += usage.filledWidth;
                });
            });
            $.each(self.disk().emptySpaces(), function(index, space) {
                space.left = Math.round(self.d3d.x(space.offset));
                space.width = Math.round(self.d3d.x(space.size));
                space.right = space.left + space.width;
            });

            // Updating general height
            self.d3d.svg
                .transition()
                .attr('height', self.collapsed() ? self.height() : self.extendedHeight());

            // Update disk information
            self.d3d.disk.select('.background')
                .attr('height', self.height() - 30);
            self.d3d.disk.select('.end-text')
                .text(generic.formatBytes(self.disk().size()));
            self.d3d.disk.select('.start-line')
                .attr({ x2: 1, y2: self.height() - 14 });
            self.d3d.disk.select('.end-line')
                .attr({ x2: self.width - 1, y2: self.height() - 14 });

            // Refresh data for empty spaces
            self.d3d.emptySpaces = self.d3d.content
                .selectAll('.empty-space')
                .data(self.disk().emptySpaces(), function(entry) {
                    return entry.offset;
                });
            // Enter empty spaces
            space = self.d3d.emptySpaces.enter()
                .append('g')
                .attr('class', 'empty-space');
            space.append('text')
                .attr('class', 'info-first')
                .text(function () { return $.t('ovs:generic.rawdisk'); })
                .attr({ x: 7, y: 13 });
            space.append('text')
                .attr('class', 'info-second')
                .attr({ x: 7, y: 25 });
            // Update empty spaces
            self.d3d.emptySpaces
                //.transition()
                .attr('transform', function(entry) { return 'translate(' + entry.left + ',16)'; });
            self.d3d.emptySpaces.select('.info-first')
                .style('display', function(entry) {
                    return entry.width > this.getComputedTextLength() + 30 ? null : 'none';
                });
            self.d3d.emptySpaces.select('.info-second')
                .text(function (entry) {
                    return $.t('ovs:generic.sizeamount', { amount: generic.formatBytes(entry.size) });
                })
                .style('display', function(entry) {
                    return entry.width > this.getComputedTextLength() + 30 ? null : 'none';
                });
            // Remove obsolete empty spaces
            self.d3d.emptySpaces.exit()
                .remove();

            // Refresh data for partitions
            self.d3d.partitions = self.d3d.content
                .selectAll('.partition')
                .data(self.disk().enhancedPartitions(), function(entry) {
                    return entry.guid();
                });
            // Enter partitions
            partition = self.d3d.partitions.enter().append('g')
                .attr('class', 'partition');
            partition.append('rect')
                .attr('class', 'background')
                .style('fill', '#dddddd');
            partition.append('line')
                .attr('class', 'start-line')
                .attr('stroke', '#666666')
                .attr('stroke-width', 2)
                .attr({ x1: 1, y1: 0 });
            partition.append('path')
                .attr('class', 'start-line-extended')
                .attr('stroke', '#666666')
                .attr('stroke-width', 2)
                .attr('fill', 'none');
            partition.append('text')
                .attr('class', 'start-text')
                .attr('text-anchor', 'left');
            partition.append('line')
                .attr('class', 'end-line')
                .attr('stroke', '#666666')
                .attr('stroke-width', 2);
            partition.append('path')
                .attr('class', 'end-line-extended')
                .attr('stroke', '#666666')
                .attr('stroke-width', 2)
                .attr('fill', 'none');
            partition.append('text')
                .attr('class', 'end-text');
            partition.append('circle')
                .attr('class', 'state')
                .attr('id', function(entry) { return 'e_' + entry.guid(); })
                .attr({ cx: 15, cy: 15 })
                .attr('r', 7);
            partition.append('text')
                .attr('class', 'collapser fa')
                .style('font-size', 12)
                .attr('y', 20);
            partition.append('text')
                .attr('class', 'info-first')
                .attr({ x: 28, y: 13 });
            partition.append('text')
                .attr('class', 'info-second')
                .attr({ x: 28, y: 25 });
            partition.append('text')
                .attr('class', 'usage-text')
                .text($.t('ovs:generic.partitionusage'));
            partition.append('g')
                .attr('class', 'usage-container');
            partition.append('rect')
                .attr('class', 'mask')
                .style('fill-opacity', 0)
                .style('stroke-opacity', 0)
                .on('click', function(entry) {
                    if (entry.mountpoint() !== null && entry.usage().length > 0) {
                        self.collapsed(!self.collapsed());
                    }
                });
            // Update partitions
            self.d3d.partitions
                //.transition()
                .attr('transform', function(entry) {
                    return 'translate(' + entry.left + ',16)';
                });
            self.d3d.partitions.select('.background')
                .attr('width', function(entry) { return entry.width; })
                .attr('height', self.height() - 30);
            self.d3d.partitions.select('.start-line')
                .style('display', function(entry, index) {
                    if (index === 0) { return null; }
                    var previousEntry = self.d3d.partitions.data()[index - 1];
                    return entry.left === previousEntry.right ? 'none' : null;
                })
                .attr({ x2: 1, y2: self.height() - 26 });
            self.d3d.partitions.select('.start-line-extended')
                .attr('d', function(entry) {
                    var x1, y1, y2, x2, y3, y4, offset, points;
                    x1 = 1;
                    x2 = self.collapsed() || entry.mountpoint() === null ? 1 : entry.relativeLeft + 1;
                    y1 = self.height() - 26;
                    y2 = self.collapsed() || entry.mountpoint() === null ? y1 : self.height() - 13;
                    y3 = self.collapsed() || entry.mountpoint() === null ? y1 : y2 + 10;
                    y4 = self.collapsed() || entry.mountpoint() === null ? y1 : self.extendedHeight() - 13;
                    offset = (y3 - y2) / 3;
                    points = [
                        { x: x1, y: y1 },
                        { x: x1, y: y2 },
                        { x: x1, y: y2 + offset },
                        { x: x2, y: y3 - offset },
                        { x: x2, y: y3 },
                        { x: x2, y: y4 }
                    ];
                    return line(points);
                })
                .style('display', function(entry, index) {
                    if (index === 0) { return null; }
                    var previousEntry = self.d3d.partitions.data()[index - 1];
                    return entry.left === previousEntry.right ? 'none' : null;
                });
            self.d3d.partitions.select('.start-text')
                .transition()
                .text(function(entry) {
                    return generic.formatBytes(entry.offset());
                })
                .style('display', function(entry, index) {
                    if (index === 0) { return null; }
                    var previousEntry = self.d3d.partitions.data()[index - 1];
                    return entry.left === previousEntry.right ? 'none' : null;
                })
                .attr({ x: self.collapsed() ? 1 : 5, y: self.height() - 16 });
            self.d3d.partitions.select('.end-line')
                .attr({ x1: function(entry) { return entry.width - 1; }, y1: 0 })
                .attr({ x2: function (entry) { return entry.width - 1; }, y2: self.height() - 26 });
            self.d3d.partitions.select('.end-line-extended')
                .attr('d', function(entry, index) {
                    var x1, y1, y2, x2, y3, y4, offset, points,
                        nextEntry = self.d3d.partitions.data()[index + 1],
                        isLastEntry = index === self.d3d.partitions.data().length - 1,
                        isShared = !isLastEntry && entry.right === nextEntry.left;
                    x1 = entry.width - 1;
                    x2 = self.collapsed() || entry.mountpoint() === null ? x1 : entry.relativeLeft + entry.filledWidth - 1;
                    if (x1 !== x2 && x2 !== self.width - 1 && !isShared && !isLastEntry) {
                        x2 = x2 + 2;
                    }
                    y1 = self.height() - (isShared ? 13 : 27);
                    y2 = self.collapsed() || entry.mountpoint() === null || isShared ? y1 : y1 + 13;
                    y3 = self.collapsed() || entry.mountpoint() === null ? y1 : y2 + 10;
                    y4 = self.collapsed() || entry.mountpoint() === null ? y1 : self.extendedHeight() - 13;
                    offset = (y3 - y2) / 3;
                    points = [
                        { x: x1, y: y1 },
                        { x: x1, y: y2 },
                        { x: x1, y: y2 + offset },
                        { x: x2, y: y3 - offset },
                        { x: x2, y: y3 },
                        { x: x2, y: y4 }
                    ];
                    return line(points);
                });
            self.d3d.partitions.select('.end-text')
                .transition()
                .text(function(entry) {
                    return generic.formatBytes(entry.offset() + entry.size());
                })
                .attr({ x: function(entry, index) {
                    var nextEntry = self.d3d.partitions.data()[index + 1],
                        x = entry.width,
                        offset = self.collapsed() || entry.mountpoint() === null ? 1 : 5;
                    if (index === self.d3d.partitions.data().length - 1) { return x - offset; }
                    if (nextEntry.left !== entry.right) { return x - offset; }
                    return x;
                }, y: self.height() - 16 })
                .attr('text-anchor', function(entry, index) {
                    var nextEntry = self.d3d.partitions.data()[index + 1];
                    if (index === self.d3d.partitions.data().length - 1) { return 'end'; }
                    if (nextEntry.left !== entry.right) { return 'end'; }
                    return 'middle';
                });
            self.d3d.partitions.select('.state')
                .transition()
                .style('fill', function(entry) {
                    return ['ERROR', 'MISSING'].contains(entry.state()) ? 'red' : 'green';
                });
            self.d3d.partitions.select('.collapser')
                .text(function() {
                    var text = $.t('ovs:icons.' + (self.collapsed() ? 'expand' : 'collapse'));
                    return String.fromCharCode(parseInt(text.substring(3, 7), 16));
                })
                .style('display', function(entry) {
                    return entry.mountpoint() !== null && entry.usage().length > 0 ? null : 'none';
                })
                .attr('x', function(entry) {
                    return entry.width - 20;
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
                        entries.push($.t('ovs:generic.sizeamount', { amount: generic.formatBytes(entry.size()) }));
                    }
                    return entries.join(' - ');
                })
                .style('display', function(entry) {
                    return entry.size() === null ? 'none' : null;
                });
            self.d3d.partitions.select('.mask')
                .attr('class', function(entry) {
                    return entry.mountpoint() !== null && entry.usage().length > 0 ? 'mask hand' : 'mask';
                })
                .attr('width', function(entry) { return entry.width; })
                .attr('height', self.height() - 30);
            self.d3d.partitions.select('.usage-text')
                .attr({ x: function(entry, index) {
                    if (entry.mountpoint() === null) { return 0; }
                    var offset = 3, previousEntry;
                    if (index === 0) {
                        offset = 5;
                    } else {
                        previousEntry = self.d3d.partitions.data()[index - 1];
                        if (entry.left !== previousEntry.right) {
                            offset = 5;
                        }
                    }
                    return entry.relativeLeft + offset;
                }, y: self.height() + 12 })
                .style('display', function(entry) {
                    return entry.mountpoint() === null ? 'none' : null;
                });

            // Usages
            self.d3d.usages = self.d3d.partitions.select('.usage-container')
                .attr('transform', function(entry, index) {
                    if (entry.mountpoint() === null) { return 'translate(0,0)'; }
                    var offset = 0, previousEntry, x, y;
                    if (index === 0) {
                        offset = 2;
                    } else {
                        previousEntry = self.d3d.partitions.data()[index - 1];
                        if (entry.left !== previousEntry.right) {
                            offset = 2;
                        }
                    }
                    x = entry.relativeLeft + offset;
                    y = self.height() + 18;
                    return 'translate(' + x + ',' + y + ')';
                })
                .style('display', function(entry) {
                    return entry.mountpoint() === null ? 'none' : null;
                })
                .selectAll('.usage')
                .data(function(entry) { return entry.usage(); });
            usage = self.d3d.usages.enter().append('g')
                .attr('class', 'usage');
            usage.append('rect')
                .attr('class', 'background')
                .style('fill', '#dddddd')
                .attr('height', 16);
            usage.append('text')
                .attr('class', 'data');
            self.d3d.usages.select('.background')
                .attr({ x: 0, y: function(usage, index) {
                    return index * 18;
                }})
                .attr('width', function(usage) { return usage.filledWidth - 4; });
            self.d3d.usages.select('.data')
                .attr({ x: function(usage) {
                    return this.getComputedTextLength() + 3 < usage.filledWidth - 3 ? 3 : usage.filledWidth;
                }, y: function(usage, index) {
                    return index * 18 + 12;
                }})
                .text(function(usage, index, partitionIndex) {
                    var information = [], entry = self.d3d.partitions.data()[partitionIndex];
                    if (usage.type === 'cache') {
                        information.push($.t('ovs:generic.caches.' + usage.metadata.type));
                    } else if (usage.type === 'backend') {
                        information.push($.t('ovs:generic.backendtypes.' + usage.metadata.type));
                    } else if (usage.type === 'temp') {
                        information.push($.t('ovs:generic.tempspace'));
                    } else {
                        information.push(usage.type);
                    }
                    information.push($.t(usage.size !== null ? 'ovs:generic.sizeamount' : 'ovs:generic.sizeunspecified', {
                        amount: generic.formatBytes(usage.size !== null ? usage.size : entry.size())
                    }));
                    return information.join(' - ');
                });
            // Remove obsolete partitions
            self.d3d.partitions.exit()
                .remove();
        };

        // Durandal
        self.activate = function(settings) {
            if (!settings.hasOwnProperty('disk')) {
                throw 'Disk should be specified';
            }
            self.disk(settings.disk);
            self.width = generic.tryGet(settings, 'width', 500);
            self.subscribers = [
                self.trigger.subscribe(self.draw),
                self.collapsed.subscribe(self.draw),
                self.extendedHeight.subscribe(self.draw)
            ];
        };
        self.deactivate = function() {
            $.each(self.subscribers, function(index, subscriber) {
                subscriber.dispose();
            });
        };
        self.compositionComplete = function() {
            self.draw();
        };
    };
});
