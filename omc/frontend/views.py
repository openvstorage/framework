from django.shortcuts import render_to_response
from django.template import RequestContext

import logging
import re
import datetime
from omc import settings

logger = logging.getLogger(settings.BASE_NAME)


def dashboard(request, **arguments):
    return render_to_response("dashboard.html", RequestContext(request, {}))


def statistics(request, **arguments):
    import memcache

    # Get first memcached URI
    match = re.match("([.\w]+:\d+)", settings.CACHES['default']['LOCATION'])
    if not match:
        raise RuntimeError("Could not find memcache configuration")

    client = memcache.Client([match.group(1)])
    stats = client.get_stats()[0][1]
    for key in stats.keys():
        try:
            value = int(stats[key])
            if key == "uptime":
                value = datetime.timedelta(seconds=value)
            elif key == "time":
                value = datetime.datetime.fromtimestamp(value)
            stats[key] = value
        except:
            pass

    return render_to_response("statistics.html", RequestContext(request, {'stats': stats,
                                                                         'hit_rate': (100 * stats['get_hits'] / stats['cmd_get']) if stats['cmd_get'] else 0,
                                                                         'time': datetime.datetime.now()}))
