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
