#!/usr/bin/env python

import re
import sys

import Host
import Table
import Condor
import Metric
import Consumer

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
        table = new_table("Metrics running against host: %s" % hostname, options)

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



def start(rsv, hostname=None, metrics=None):
    """ Start all metrics - or supplied metrics """

    condor = Condor.Condor(rsv)

    if not condor.is_condor_running():
        rsv.log("CRITICAL", "condor-cron is not running.  Cannot start RSV jobs")
        return False

    if not metrics:
        errors = 0
        for host in rsv.get_host_info().values():
            for metric_name in host.get_enabled_metrics():
                metric = Metric.Metric(metric_name, rsv, host.host)
                if not condor.start_metric(metric, host):
                    errors += 1

        for consumer_name in rsv.get_enabled_consumers():
            consumer = Consumer.Consumer(consumer_name, rsv)
            if not condor.start_consumer(consumer):
                errors += 1

        if errors > 0:
            return False

        return True
        
    else:
        if not host:
            rsv.log("ERROR", "When starting specific metrics you must also specify a host.")

        for metric in metrics:
            rsv.log("INFO", "Starting metric %s" % metric)
            # todo - start single metric!
            

def stop(rsv, host, metrics):
    """ Stop all metrics - or supplied metrics """

    condor = Condor.Condor(rsv)

    if not condor.is_condor_running():
        rsv.log("CRITICAL", "condor-cron is not running.")
        sys.exit(1)

    if len(metrics) == 0:
        rsv.echo("Stopping all metrics on all hosts")
        if not condor.stop_condor_jobs("OSGRSV==\"metrics\""):
            rsv.log("ERROR", "Problem stopping metrics.")
            return False
            
        rsv.echo("Stopping consumers")
        if not condor.stop_condor_jobs("OSGRSV==\"consumers\""):
            rsv.log("ERROR", "Problem stopping consumers.")
            return False

        return True
        
    else:
        if not host:
            rsv.log("ERROR", "When stopping specific metrics with --off you must also specify a host.")

        for metric in metrics:
            rsv.log("INFO", "Stopping metric %s" % metric)
            # todo - stop metric!
