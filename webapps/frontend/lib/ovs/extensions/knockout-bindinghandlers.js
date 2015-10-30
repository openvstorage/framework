// Copyright 2014 iNuron NV
//
// Licensed under the Open vStorage Non-Commercial License, Version 1.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.openvstorage.org/OVS_NON_COMMERCIAL
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
/*global define */
define([
    'knockout', 'jquery', 'd3', 'ovs/generic', 'd3p/slider'
], function(ko, $, d3, generic, Slider) {
    "use strict";
    ko.bindingHandlers.status = {
        init: function(element) {
            var id = 'id_' + generic.getTimestamp() + '_' + Math.random().toString().substr(2, 10), svg;
            $(element).html('<div></div>');
            $($(element).children()[0]).attr('id', id);
            svg = d3.select('#' + id).append('svg')
                .attr('class', 'svg')
                .attr('width', 14)
                .attr('height', 14);
            svg.append('circle')
                .attr('class', 'circle')
                .attr('cx', 7)
                .attr('cy', 7)
                .attr('r', 7)
                .style('fill', 'gray');
        },
        update: function(element, valueAccessor) {
            var value, colorOption, color, id;
            value = valueAccessor();
            for (colorOption in value.colors) {
                if (value.colors.hasOwnProperty(colorOption) && value.colors[colorOption]) {
                    color = colorOption;
                }
            }
            if (color === undefined) {
                color = value.defaultColor;
            }
            id = $($(element).children()[0]).attr('id');
            d3.select('#' + id).select('.circle')
                .style('fill', color);
        }
    };
    ko.bindingHandlers.gauge = {
        init: function(element) {
            var id = 'id_' + generic.getTimestamp() + '_' + Math.random().toString().substr(2, 10),
                svg, arc;
            $(element).html('<div></div>');
            $($(element).children()[0]).attr('id', id);
            svg = d3.select('#' + id).append('svg')
                .attr('class', 'svg')
                .style('display', 'block')
                .style('margin', 'auto')
                .attr('width', 300)
                .attr('height', 200);
            arc = d3.svg.arc()
                .innerRadius(80)
                .outerRadius(150)
                .startAngle(generic.deg2rad(-90))
                .endAngle(generic.deg2rad(90));
            svg.append('path')
                .attr('class', 'background-arc')
                .attr('d', arc)
                .style('fill', '#edebeb')
                .attr('transform', 'translate(150, 160)');
            svg.append('path')
                .attr('class', 'foreground-arc');
            svg.append('text')
                .text('0.00 ' + $.t('ovs:generic.iops'))
                .attr('class', 'secondary-text')
                .attr('text-anchor', 'middle')
                .style('font-weight', 'bold')
                .style('font-size', 25)
                .style('fill', 'grey')
                .attr('transform', 'translate(150, 195)');
            svg.append('text')
                .text('0.00 %')
                .attr('class', 'primary-text')
                .attr('text-anchor', 'middle')
                .style('font-weight', 'bold')
                .style('font-size', 25)
                .attr('transform', 'translate(150, 150)');
        },
        update: function(element, valueAccessor) {
            var value, id, arc, percentage, color;
            value = valueAccessor();
            if (!isNaN(value.primary.raw())) {
                percentage = value.primary.raw();
                id = $($(element).children()[0]).attr('id');
                color = d3.scale.linear().domain([0, 50, 100]).range(['red', 'orange', 'green']);
                arc = d3.svg.arc()
                    .innerRadius(80)
                    .outerRadius(150)
                    .startAngle(generic.deg2rad(-90))
                    .endAngle(generic.deg2rad((180 / 100 * percentage) - 90));
                d3.select('#' + id).select('.foreground-arc')
                    .attr('d', arc)
                    .style('fill', color(percentage))
                    .attr('transform', 'translate(150, 160)');
                d3.select('#' + id).select('.primary-text')
                    .text(value.primary());
            }
            if (!isNaN(value.secondary.raw())) {
                d3.select('#' + id).select('.secondary-text')
                    .text(value.secondary() + ' ' + $.t('ovs:generic.iops'));
            }
        }
    };
    ko.bindingHandlers.popover = {
        init: function(element, valueAccessor) {
            var value = valueAccessor();
            $(element).popover({
                html: true,
                placement: 'auto',
                trigger: 'focus',
                title: $.t(value.title),
                content: $.t(value.content)
            });
        },
        update: function(element, valueAccessor) {
            var value = valueAccessor();
            $(element).popover('destroy');
            $(element).popover({
                html: true,
                placement: 'auto',
                trigger: 'focus',
                title: $.t(value.title),
                content: $.t(value.content)
            });
        }
    };
    ko.bindingHandlers.tooltip = {
        init: function(element, valueAccessor, allBindings) {
            var value = valueAccessor(), title,
                placement = allBindings.get('placement');
            if (placement === undefined) {
                placement = 'auto top';
            }
            if (value !== undefined && value !== '') {
                title = $.t(value);
                $(element).tooltip({
                    html: true,
                    placement: placement,
                    title: title
                });
            }
        },
        update: function(element, valueAccessor, allBindings) {
            var value = valueAccessor(), title,
                placement = allBindings.get('placement');
            if (placement === undefined) {
                placement = 'auto top';
            }
            $(element).tooltip('destroy');
            if (value !== undefined && value !== '') {
                title = $.t(value);
                $(element).tooltip({
                    html: true,
                    placement: placement,
                    title: title
                });
            }
        }
    };
    ko.bindingHandlers.timeago = {
        init: function(element, valueAccessor) {
            var value = valueAccessor(), date;
            if (value !== undefined && value !== '') {
                date = new Date(value * 1000);
                $(element).attr('title', date.toISOString());
                $(element).timeago();
            }
        },
        update: function(element, valueAccessor) {
            var value = valueAccessor(), date;
            if (value !== undefined && value !== '') {
                date = new Date(value * 1000);
                if ($(element).attr('title') === undefined) {
                    $(element).attr('title', date.toISOString());
                    $(element).timeago();
                } else {
                    $(element).attr('title', date.toISOString());
                    $(element).timeago('updateFromDOM');
                }
            }
        }
    };
    ko.bindingHandlers.translate = {
        init: function(element, valueAccessor) {
            var value = valueAccessor();
            $(element).html($.t(value, { defaultValue: '' }));
        },
        update: function(element, valueAccessor) {
            var value = valueAccessor();
            $(element).html($.t(value, { defaultValue: '' }));
        }
    };
    ko.bindingHandlers.shortText = {
        init: function(element, valueAccessor, allBindings) {
            var shortValue, value = valueAccessor(),
                maxLength = allBindings.get('maxLength');
            if (maxLength !== undefined) {
                if (value.length > maxLength - 3) {
                    shortValue = value.substr(0, maxLength - 3) + '&hellip;';
                    $(element).html('<abbr title="' + value + '"><span>' + shortValue + '</span></abbr>');
                    return;
                }
            }
            $(element).html(value);
        },
        update: function(element, valueAccessor, allBindings) {
            var shortValue, value = valueAccessor(),
                maxLength = allBindings.get('maxLength');
            if (maxLength !== undefined) {
                if (value.length > maxLength - 3) {
                    shortValue = value.substr(0, maxLength - 3) + '&hellip;';
                    $(element).html('<abbr title="' + value + '"><span>' + shortValue + '</span></abbr>');
                    return;
                }
            }
            $(element).html(value);
        }
    };
    ko.bindingHandlers.slider = {
        init: function(element, valueAccessor) {
            var value = valueAccessor(),
                id = 'id_' + generic.getTimestamp() + '_' + Math.random().toString().substr(2, 10);
            $(element).attr('id', id);
            $(element).data('slider', new Slider());
            d3.select('#' + id).call(
                $(element).data('slider')
                    .value(value())
                    .axis(true)
                    .min(value.min)
                    .max(value.max)
                    .step(1)
                    .on('slide', function(event, newValue) {
                        value(newValue);
                    })
            );
        },
        update: function(element, valueAccessor) {
            var value = valueAccessor();
            $(element).data('slider').value(value());
        }
    };
    ko.bindingHandlers.let = {
        'init': function(element, valueAccessor, allBindingsAccessor, viewModel, bindingContext) {
            // Make a modified binding context, with extra properties, and apply it to descendant elements
            var innerContext = bindingContext.extend(valueAccessor());
            ko.applyBindingsToDescendants(innerContext, element);
            return { controlsDescendantBindings: true };
        }
    };
    ko.virtualElements.allowedBindings['let'] = true;
    ko.bindingHandlers.pie = {
        init: function(element) {
            var id = 'id_' + generic.getTimestamp() + '_' + Math.random().toString().substr(2, 10), size = 200, svg;
            $(element).html('<div></div>');
            $($(element).children()[0]).attr('id', id);
            svg = d3.select('#' + id).append('svg')
                .attr('class', 'svg')
                .attr('width', size * 2.5)
                .attr('height', size);
            svg.append('g')
                .attr('class', 'container')
                .attr('transform', 'translate(' + size / 2 + ',' + size / 2 + ')');
            svg.append('g')
                .attr('class', 'legendcontainer')
                .attr('transform', 'translate(' + (size + 20) + ',0)');
        },
        update: function(element, valueAccessor) {
            var id, g, pie, path, arc, legend, entry, size = 200, data = valueAccessor(),
                color = d3.scale.ordinal().range([
                    '#e6e6e6', '#b2b2b2', '#808080',
                    '#377eb8', '#4daf4a', '#984ea3', '#ff7f00', '#ffff33', '#a65628', '#f781bf', '#999999'
                ]);

            arc = d3.svg.arc()
                .outerRadius(size / 2)
                .innerRadius(0);
            pie = d3.layout.pie()
                .sort(null)
                .value(function(d) { return Math.round(d.percentage * 100) / 100; });

            id = $($(element).children()[0]).attr('id');

            g = d3.select('#' + id).select('.container');
            g.datum(data).selectAll('path')
                .data(pie, function(d) { return d.data.name; })
                .style('fill', function(d) { return color(d.data.name); })
                .transition()
                .duration(250)
                .attrTween('d', function(a) {
                    var i = d3.interpolate(this._current, a);
                    this._current = i(0);
                    return function(t) {
                        return arc(i(t));
                    };
                });
            g.datum(data).selectAll('path')
                .data(pie, function(d) { return d.data.name; })
                .enter().append('path')
                .style('fill', function(d) { return color(d.data.name); })
                .attr('d', arc)
                .each(function(d) { this._current = d; });
            g.datum(data).selectAll('path')
                .data(pie, function(d) { return d.data.name; })
                .exit().remove();

            legend = d3.select('#' + id).select('.legendcontainer');
            legend.selectAll('g')
                .data(data, function(d) { return d.name; })
                .select('text')
                .text(function(d) {
                    var text = d.name + ' (' + generic.formatBytes(d.value);
                    if (d.hasOwnProperty('percentage')) {
                        text += ' - ' + generic.formatPercentage(d.percentage);
                    }
                    text += ')';
                    return text;
                });
            entry = legend.selectAll('g')
                .data(data, function(d) { return d.name; })
                .enter()
                .append('g')
                .attr('class', 'legend')
                .attr('transform', function(d, i) {
                    var height = 25,
                        vert = i * height;
                    return 'translate(0,' + vert + ')';
                });
            entry.append('rect')
                .attr('width', 16)
                .attr('height', 16)
                .style('fill', function(d) { return color(d.name); })
            entry.append('text')
                .attr('x', 20)
                .attr('y', 14)
                .text(function(d) {
                    var text = d.name + ' (' + generic.formatBytes(d.value);
                    if (d.hasOwnProperty('percentage')) {
                        text += ' - ' + generic.formatPercentage(d.percentage);
                    }
                    text += ')';
                    return text;
                });
            legend.selectAll('g')
                .data(data, function(d) { return d.name; })
                .exit().remove();
        }
    };
});
