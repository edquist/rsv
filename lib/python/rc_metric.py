#!/usr/bin/env python

import re

import Table

def new_table(header, options):
    table_ = Table.Table((58, 20))
    if options.list_wide:
        table_.truncate = False
    else:
        table_.truncate_leftright = True
    table_.makeFormat()
    table_.makeHeader(header, 'Service')
    return table_


def list_metrics(rsv, options, pattern):
    """ List metrics to the screen """

    rsv.log("INFO", "Listing all metrics")
    retlines = []
    num_metrics_displayed = 0
    num_disabled_metrics = 0

    metrics = rsv.get_metric_info()

    tables = {} # to hold one table per host
    tables['DISABLED'] = new_table('DISABLED METRICS', options)

    for metric_name in metrics.keys():
        metric = metrics[metric_name]

        if pattern and not re.search(pattern, metric_name):
            continue

        type = metric.get_type()
        ret_list_uri = []
        ret_list_status = []
        for uri in probe.urilist:
            if not uri in tables:
                tables[uri] = new_table("Metrics running on host: " + uri, options)

            # If the user supplied --host, only show that host's metrics
            if options.uri and options.uri != uri:
                continue

            rets = probe.status(uri)
            rsv.log("DEBUG", "Metric %s (%s): %s on %s" % (metric, type, rets, uri))
            if rets == "ENABLED":
                ret_list_uri.append(uri)
            else:
                if not rets in ret_list_status:
                    ret_list_status.append(rets)

        if not ret_list_uri:
            # should I just add DISABLED?
            # if multiple status are appearing probably there is an error
            for i in ret_list_status:
                tables['DISABLED'].addToBuffer(metric, type)
                num_disabled_metrics += 1
            continue

        for i in ret_list_uri:                        
            tables[i].addToBuffer(metric, type)

        num_metrics_displayed += 1

    # After looping on all the probes, create the output
    for host in sorted(tables.keys()):
        if host != "DISABLED" and not tables[host].isBufferEmpty():
            retlines.append(tables[host].getHeader())
            retlines += tables[host].formatBuffer()
            retlines += "\n"

    if options.list_all:
        if num_disabled_metrics > 0:
            retlines.append("The following metrics are not enabled on any host:")
            retlines.append(tables["DISABLED"].getHeader())
            retlines += tables["DISABLED"].formatBuffer()
    elif num_disabled_metrics > 0:
        tmp = ""
        if pattern:
            tmp = " that match the supplied pattern"
        retlines.append("The are %i disabled metrics%s.  Use --all to display them." % \
                        (num_disabled_metrics, tmp))
            

    if not metrics:
        rsv.log("ERROR", "No installed metrics!")
    else:
        print '\n' + '\n'.join(retlines) + '\n'
        if num_metrics_displayed == 0:
            print "No metrics matched your query.\n"

    return True
