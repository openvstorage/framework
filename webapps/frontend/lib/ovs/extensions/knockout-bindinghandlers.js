// license see http://www.openvstorage.com/licenses/opensource/
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
                .attr('cx', 6)
                .attr('cy', 8)
                .attr('r', 6)
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
            if (value.primary.initialized()) {
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
                if (value.secondary.initialized()) {
                    d3.select('#' + id).select('.secondary-text')
                        .text(value.secondary() + ' ' + $.t('ovs:generic.iops'));
                }
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
        }
    };
});
