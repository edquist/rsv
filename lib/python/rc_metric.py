#!/usr/bin/env python

import re

import Host
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

    metrics = rsv.get_metric_info()
    hosts   = rsv.get_host_info()
    used_metrics = {}

    # Form a table for each host listing enabled metrics
    for hostname in hosts:
        host = hosts[hostname]
        table = new_table("Metrics running on host: %s" % hostname, options)

        enabled_metrics = host.get_enabled_metrics()

        if enabled_metrics:
            for metric in host.get_enabled_metrics():
                used_metrics[metric] = 1
                if pattern and not re.search(pattern, metric):
                    continue
                
                # todo - add metricType here
                metric_type = metrics[metric].get_type()
                table.addToBuffer(metric, metric_type)
                num_metrics_displayed += 1
        else:
            # todo - add message?
            pass

        # We don't skip this host earlier in the loop so that we can get
        # a correct number for the disabled hosts.
        if options.host and options.host != hostname:
            rsv.log("DEBUG", "Not displaying host '%s' because --host %s was supplied." %
                    (hostname, options.host))
            continue

        if not table.isBufferEmpty():
            retlines.append(table.getHeader())
            retlines += table.formatBuffer()
            retlines += "\n"
                                

    # Find the set of metrics not enabled on any host
    num_disabled_metrics = 0
    table = new_table('DISABLED METRICS', options)        
    for metric in metrics:
        if metric not in used_metrics:
            if pattern and not re.search(pattern, metric):
                continue
            num_disabled_metrics += 1
            metric_type = metrics[metric].get_type()
            table.addToBuffer(metric, metric_type)

    # Display disabled metrics
    if options.list_all:
        if num_disabled_metrics > 0:
            retlines.append("The following metrics are not enabled on any host:")
            retlines.append(table.getHeader())
            retlines += table.formatBuffer()
    elif num_disabled_metrics > 0:
        tmp = ""
        if pattern:
            tmp = " that match the supplied pattern"
        retlines.append("The are %i metrics not enabled on any host%s.  Use --all to display them." %
                        (num_disabled_metrics, tmp))
            

    # Display the result
    if not metrics:
        rsv.log("ERROR", "No installed metrics!")
    else:
        print '\n' + '\n'.join(retlines) + '\n'
        if num_metrics_displayed == 0:
            print "No metrics matched your query.\n"

    return True
