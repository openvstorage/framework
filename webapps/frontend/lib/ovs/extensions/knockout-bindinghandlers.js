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
                .attr('height', 205);
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
                maxLength = allBindings.get('maxLength'),
                middle = allBindings.get('middle') === true;
            if (maxLength !== undefined) {
                if (value.length > maxLength - 3) {
                    if (middle === true) {
                        shortValue = value.substr(0, Math.floor((maxLength - 3) / 2)) + '&hellip;' + value.substr(value.length - Math.ceil((maxLength - 3) / 2));
                        $(element).html('<abbr title="' + value + '"><span>' + shortValue + '</span></abbr>');
                        return;
                    }
                    shortValue = value.substr(0, maxLength - 3) + '&hellip;';
                    $(element).html('<abbr title="' + value + '"><span>' + shortValue + '</span></abbr>');
                    return;
                }
            }
            $(element).html(value);
        },
        update: function(element, valueAccessor, allBindings) {
            var shortValue, value = valueAccessor(),
                maxLength = allBindings.get('maxLength'),
                middle = allBindings.get('middle') === true;
            if (maxLength !== undefined) {
                if (value.length > maxLength - 3) {
                    if (middle === true) {
                        shortValue = value.substr(0, Math.floor((maxLength - 3) / 2)) + '&hellip;' + value.substr(value.length - Math.ceil((maxLength - 3) / 2));
                        $(element).html('<abbr title="' + value + '"><span>' + shortValue + '</span></abbr>');
                        return;
                    }
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
        init: function(element, valueAccessor, allBindingsAccessor, viewModel, bindingContext) {
            // Make a modified binding context, with extra properties, and apply it to descendant elements
            var innerContext = bindingContext.extend(valueAccessor());
            ko.applyBindingsToDescendants(innerContext, element);
            return { controlsDescendantBindings: true };
        }
    };
    ko.virtualElements.allowedBindings['let'] = true;
    ko.bindingHandlers.pie = {
        init: function(element) {
            var id = 'id_' + generic.getTimestamp() + '_' + Math.random().toString().substr(2, 10);
            var size = 200;
            $(element).html('<div></div>');
            $($(element).children()[0]).attr('id', id);
            // Create new Scalable Vector Graphics (SVG) container and attach it to the bound element
            var svg = d3.select('#' + id)
                .append('svg')
                    .attr('class', 'svg')
                    .attr('width', size * 2.5)  // Set the width and height of our visualization (these will be attributes of the <svg> tag
                    .attr('height', size);
            svg.append('g')  // Make a group to hold the pie chart
                .attr('class', 'container')  //
                .attr('transform', 'translate(' + size / 2 + ',' + size / 2 + ')');  // Move the center of the pie chart from 0, 0 to radius, radius
            svg.append('g')  // Make a group to hold the legend
                .attr('class', 'legendcontainer')
                .attr('transform', 'translate(' + (size + 20) + ',0)'); // Move the center of the legend next to the pie chart
        },
        update: function(element, valueAccessor) {
            // Default color range
            var color = d3.scale.ordinal().range(['#e6e6e6', '#b2b2b2', '#808080','#377eb8', '#4daf4a', '#984ea3',
                '#ff7f00', '#ffff33', '#a65628', '#f781bf', '#999999']);
            var size=200;
            var data = valueAccessor();
            var id = $($(element).children()[0]).attr('id');
            var arc = d3.svg.arc()  // This will create <path> elements for us using arc data
                .outerRadius(size / 2)
                .innerRadius(0);
            var pie = d3.layout.pie()  // This will create arc data for us given a list of values
                .sort(null)
                .value(function(d) {  // We must tell it to access the 'percentage' of each element in our data array
                    return Math.round(d.percentage * 100) / 100;
                });

            // Build the pie chart
            var g = d3.select('#' + id).select('.container');  // The g element is the element with .container of our svg (which is identified by the id)
            // Register animation when updates occur
            g.datum(data)  // Set the bound data for the element
                .selectAll('path')  // This selects all <path> elements
                // Associate the generated pie data (an array of arcs, each having startAngle, endAngle and value properties)
                // The datum data is bound within the data key of this object ({data: DATUM_DATA_OBJECT, value: 1, startAngle: 0, endAngle: 6.283185307179586, padAngle: 0})
                .data(pie, function(d) { return d.data.name; })
                // Set the background color of all the selected elements
                .style('fill', function(d) {
                    var data = d.data;
                    if (data.hasOwnProperty('color')) { return data.color }
                    else { return color(data.name); }
                })
                .transition()  // Start a transition
                .duration(250)  // Set the transition duration (milliseconds)
                // Transition the value of the attribute with the specified name (here 'd') according to the specified function.
                // The starting and ending value of the transition are determined by tween; the tween function is invoked
                // when the transition starts on each element, being passed the current datum d, the current index i and
                // the current attribute value a (function tween(d, i, a)), with the this context as the current DOM element.
                .attrTween('d', function(d) {
                    // interpolate from _current (the _current holds the previous angles) to the new angles.
                    // During the transition, _current is updated in-place by d3.interpolate.
                    var i = d3.interpolate(this._current, d);
                    this._current = i(0);
                    return function(t) {
                        return arc(i(t));
                    };
                });
            // Bind the data into the pie chart
            g.datum(data)
                .selectAll('path')
                .data(pie, function(d) { return d.data.name; })
                .enter()  // This will create <g> elements for every "extra" data element that should be associated with a selection. The result is creating a <g> for every object in the data array
                    // The following functions apply to the newly created items only
                    .append('path')  // Create a <path> element for the slice
                        .style('fill', function(d) {
                            var data = d.data;
                            if (data.hasOwnProperty('color')) { return data.color }
                            else { return color(data.name); }
                        })
                        .attr('d', arc)  // This creates the actual SVG path using the associated data (pie) with the arc drawing function
                    .each(function(d) {  // Store the angles within the element
                        // Invokes the specified function for each element in the current selection,
                        // passing in the current datum d and index i, with the this context of the current DOM element (function(d, i))
                        this._current = d;
                    });
            // Remove obsolete arcs of the pie chart
            g.datum(data)
                .selectAll('path')
                .data(pie, function(d) { return d.data.name; })
                .exit()  // Returns the exit selection: existing DOM elements in the current selection for which no new data element was found.
                    .remove();  // Removes the elements in the current selection from the current document

            // Build the legend
            var legend = d3.select('#' + id).select('.legendcontainer');
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
            // Create containers for the legend
            var entries = legend.selectAll('g')
                .data(data, function(d) {
                    return d.name;
                })
                .enter()
                    .append('g')
                    .attr('class', 'legend')
                    .attr('transform', function(d, i) {
                        var height = 25;
                        var vert = i * height;
                        return 'translate(0,' + vert + ')';
                    });
            // Add rectangle with color
            entries.append('rect')
                .attr('width', 16)
                .attr('height', 16)
                .style('fill', function(d) {
                    if (d.hasOwnProperty('color')) { return d.color }
                    else { return color(d.name); }
                });
            // Add text
            entries.append('text')
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
            // Remove obsolete entries of the legend
            legend.selectAll('g')
                .data(data, function(d) {
                    return d.name;
                })
                .exit()
                    .remove();
        }
    };
});
