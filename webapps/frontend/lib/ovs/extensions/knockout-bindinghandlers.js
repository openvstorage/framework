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
define(['knockout', 'jquery', 'd3', 'ovs/generic'], function(ko, $, d3, generic) {
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
                trigger: 'click',
                title: $.t(value.title),
                content: $.t(value.content)
            });
        },
        update: function(element, valueAccessor) {
            var value = valueAccessor();
            $(element).tooltip('destroy');
            $(element).popover({
                html: true,
                placement: 'auto',
                trigger: 'click',
                title: $.t(value.title),
                content: $.t(value.content)
            });
        }
    };
    ko.bindingHandlers.tooltip = {
        init: function(element, valueAccessor) {
            var value = valueAccessor(), title;
            if (value !== undefined && value !== '') {
                title = $.t(value);
                $(element).tooltip({
                    html: true,
                    placement: 'auto top',
                    title: title
                });
            }
        },
        update: function(element, valueAccessor) {
            var value = valueAccessor(), title;
            $(element).tooltip('destroy');
            if (value !== undefined && value !== '') {
                title = $.t(value);
                $(element).tooltip({
                    html: true,
                    placement: 'auto top',
                    title: title
                });
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
            var id = 'id_' + generic.getTimestamp() + '_' + Math.random().toString().substr(2, 10), height = 200;
            $(element).html('<div></div>');
            $($(element).children()[0]).attr('id', id);
            d3.select('#' + id).append('svg')
                .attr('class', 'svg')
                .attr('width', height * 2.5)
                .attr('height', height)
                .append('g')
                .attr('class', 'container')
                .attr('transform', 'translate(' + height / 2 + ',' + height / 2 + ')');
        },
        update: function(element, valueAccessor) {
            var id, g, pie, arc, legend, size = 200, data = valueAccessor(),
                color = d3.scale.ordinal().range([
                    '#e6e6e6', '#b2b2b2', '#808080',
                    '#377eb8', '#4daf4a', '#984ea3', '#ff7f00', '#ffff33', '#a65628', '#f781bf', '#999999'
                ]);
            arc = d3.svg.arc()
                .outerRadius(size / 2)
                .innerRadius(0);
            pie = d3.layout.pie()
                .sort(null)
                .value(function(d) { return d.value; });
            id = $($(element).children()[0]).attr('id');
            g = d3.select('#' + id).select('.container')
                .selectAll('.arc')
                .data(pie(data))
                .enter()
                .append('g')
                .attr('class', 'arc');
            g.append('path')
                .attr('d', arc)
                .style('fill', function(d) {
                    var text = d.data.name + ' (' + generic.formatBytes(d.data.value);
                    if (d.data.hasOwnProperty('percentage')) {
                        text += ' - ' + generic.formatPercentage(d.data.percentage);
                    }
                    text += ')';
                    return color(text);
                });
            legend = d3.select('#' + id).select('.svg').selectAll('.legend')
                .data(color.domain())
                .enter()
                .append('g')
                .attr('class', 'legend')
                .attr('transform', function(d, i) {
                    var height = 25,
                        horz = 220,
                        vert = i * height;
                    return 'translate(' + horz + ',' + vert + ')';
                });
            legend.append('rect')
                .attr('width', 15)
                .attr('height', 15)
                .style('fill', color)
                .style('stroke', color);
            legend.append('text')
                .attr('x', 20)
                .attr('y', 14)
                .text(function(d) { return d; });
        }
    };
});
