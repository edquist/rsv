#!/usr/bin/env python

import re
import sys

import Host
import Table
import Condor
import Metric
import Consumer

import pdb

def new_table(header, options):
    """ Return a new table with default dimensions """
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
    for host in hosts:
        table = new_table("Metrics running against host: %s" % host.host, options)

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
        if options.host and options.host != host.host:
            rsv.log("DEBUG", "Not displaying host '%s' because --host %s was supplied." %
                    (host.host, options.host))
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


def job_list(rsv, hostname=None):
    """ Display jobs running similar to condor_cron_q but in a better format """
    condor = Condor.Condor(rsv)

    if condor.display_jobs(hostname):
        return True
    else:
        return False


def start(rsv, jobs=None, hostname=None):
    """ Start all metrics and consumers - or supplied set of them """

    condor = Condor.Condor(rsv)

    if not condor.is_condor_running():
        rsv.log("CRITICAL", "condor-cron is not running.  Cannot start RSV jobs")
        return False

    # 
    # If we are not passed specific jobs to start, start all metrics and consumers
    #
    if not jobs:
        num_errors = 0

        # Start all the metrics for each host
        for host in rsv.get_host_info():
            enabled_metrics = host.get_enabled_metrics()
            if len(enabled_metrics) > 0:
                rsv.echo("Starting %s metrics for host '%s'." % (len(enabled_metrics), host.host))
                for metric_name in enabled_metrics:
                    metric = Metric.Metric(metric_name, rsv, host.host)
                    if not condor.start_metric(metric, host):
                        num_errors += 1

        # Start the consumers
        rsv.echo("Starting consumers.")
        for consumer_name in rsv.get_enabled_consumers():
            consumer = Consumer.Consumer(consumer_name, rsv)
            if not condor.start_consumer(consumer):
                num_errors += 1

        if num_errors > 0:
            return False

        return True

    #
    # Start only the specified set of metrics / consumers
    #
    else:
        host = None
        if hostname:
            host = Host.Host(hostname, rsv)
            # TODO - catch host not having configuration and print better error?

        # Since a user can input either metric of consumer names we need to get a list
        # of the installed metrics and consumers and check which category a job is in.
        available_metrics   = rsv.get_installed_metrics()
        available_consumers = rsv.get_installed_consumers()

        num_errors = 0

        for job in jobs:
            if job in available_metrics and job in available_consumers:
                rsv.log("WARNING", "Both a metric and a consumer are installed with the name '%s'. " +
                        "Not starting either one" % job)
                num_errors += 1
            elif job in available_metrics:
                if not host:
                    rsv.log("ERROR", "When starting specific metrics you must also specify a host.")
                    num_errors += 1
                    continue

                rsv.echo("Starting metric '%s' against host '%s'" % (job, host.host))
                metric = Metric.Metric(job, rsv, hostname)
                if not condor.start_metric(metric, host):
                    num_errors += 1
            elif job in available_consumers:
                rsv.echo("Starting consumer %s" % job)
                consumer = Consumer.Consumer(job, rsv)
                if not condor.start_consumer(consumer):
                    num_errors += 1
            else:
                rsv.log("WARNING", "Supplied job '%s' is not an installed metric or consumer" % job)
                num_errors += 1

        if num_errors > 0:
            rsv.log("ERROR", "%s jobs could not be started" % num_errors)
            return False
        else:
            return True
            

def stop(rsv, jobs=None, hostname=None):
    """ Stop all metrics - or supplied metrics """

    condor = Condor.Condor(rsv)

    if not condor.is_condor_running():
        rsv.log("CRITICAL", "condor-cron is not running.")
        sys.exit(1)

    #
    # If no list of jobs is specified, stop all the metrics and consumers
    #
    if len(jobs) == 0:
        rsv.echo("Stopping all metrics on all hosts.")
        if not condor.stop_jobs("OSGRSV==\"metrics\""):
            rsv.log("ERROR", "Problem stopping metrics.")
            return False
            
        rsv.echo("Stopping consumers.")
        if not condor.stop_jobs("OSGRSV==\"consumers\""):
            rsv.log("ERROR", "Problem stopping consumers.")
            return False

        return True

    #
    # Stop only the specified metrics / consumers
    #
    else:
        host = None
        if hostname:
            host = Host.Host(hostname, rsv)
            # TODO - catch host not having configuration and print better error?

        # Since a user can input either metric of consumer names we need to get a list
        # of the installed metrics and consumers and check which category a job is in.
        available_metrics   = rsv.get_installed_metrics()
        available_consumers = rsv.get_installed_consumers()

        num_errors = 0

        for job in jobs:
            if job in available_metrics and job in available_consumers:
                rsv.log("WARNING", "Both a metric and a consumer are installed with the name '%s'. " +
                        "Not stopping either one" % job)
                num_errors += 1
            elif job in available_metrics:
                if not host:
                    rsv.log("ERROR", "When stopping specific metrics you must also specify a host.")
                    num_errors += 1
                    continue

                rsv.echo("Stopping metric '%s' for host '%s'" % (job, host.host))
                metric = Metric.Metric(job, rsv, hostname)
                if not condor.stop_jobs("OSGRSVUniqueName==\"%s\"" % metric.get_unique_name()):
                    num_errors += 1
            elif job in available_consumers:
                rsv.echo("Stopping consumer %s" % job)
                consumer = Consumer.Consumer(job, rsv)
                if not condor.stop_jobs("OSGRSVUniqueName==\"%s\"" % consumer.get_unique_name()):
                    num_errors += 1
            else:
                rsv.log("WARNING", "Supplied job '%s' is not an installed metric or consumer" % job)
                num_errors += 1

        if num_errors > 0:
            rsv.log("ERROR", "%s jobs could not be stopped" % num_errors)
            return False
        else:
            return True


def enable(rsv, jobs, hostname=None):
    """ Enable the specified metrics against the specified hosts.  This can also be used to enable
    consumers, in which case the host does not need to be passed. """

    host = None
    if hostname:
        host = Host.Host(hostname, rsv)

    # Since a user can input either metric of consumer names we need to get a list
    # of the installed metrics and consumers and check which category a job is in.
    available_metrics   = rsv.get_installed_metrics()
    available_consumers = rsv.get_installed_consumers()

    write_host_config = False

    num_errors = 0

    if not jobs:
        rsv.echo("ERROR: You must supply metrics/consumers to enable")
        return False

    for job in jobs:
        if job in available_metrics and job in available_consumers:
            rsv.log("WARNING", "Both a metric and a consumer are installed with the name '%s'. " +
                    "Not enabling either one." % job)
            num_errors += 1
        elif job in available_metrics:
            if not host:
                rsv.log("ERROR", "When enabling specific metrics you must also specify a host.")
                num_errors += 1
                continue

            rsv.echo("Enabling metric '%s' for host '%s'" % (job, host.host))

            if host.metric_enabled(job):
                rsv.echo("   Metric already enabled")
            else:
                host.set_config(job, 1)
                write_host_config = True

        elif job in available_consumers:
            rsv.echo("Enabling consumer %s" % job)
            consumer = Consumer.Consumer(job, rsv)
            # TODO - enable consumer
        else:
            rsv.log("WARNING", "Supplied job '%s' is not an installed metric or consumer" % job)
            num_errors += 1

    if write_host_config:
        host.write_config_file()

    
    if num_errors > 0:
        rsv.log("ERROR", "%s jobs could not be enabled." % num_errors)
        return False
    else:
        return True


def disable(rsv, jobs, hostname=None):
    """ Enable the specified metrics against the specified hosts.  This can also be used to enable
    consumers, in which case the host does not need to be passed. """

    host = None
    if hostname:
        host = Host.Host(hostname, rsv)

    # Since a user can input either metric of consumer names we need to get a list
    # of the installed metrics and consumers and check which category a job is in.
    available_metrics   = rsv.get_installed_metrics()
    available_consumers = rsv.get_installed_consumers()

    write_host_config = False

    num_errors = 0

    if not jobs:
        rsv.echo("ERROR: You must supply metrics/consumers to disable")
        return False

    for job in jobs:
        if job in available_metrics and job in available_consumers:
            rsv.log("WARNING", "Both a metric and a consumer are installed with the name '%s'. " +
                    "Not disabling either one." % job)
            num_errors += 1
        elif job in available_metrics:
            if not host:
                rsv.log("ERROR", "When disabling specific metrics you must also specify a host.")
                num_errors += 1
                continue

            rsv.echo("Disabling metric '%s' for host '%s'" % (job, host.host))

            if not host.metric_enabled(job):
                rsv.echo("   Metric already disabled")
            else:
                host.set_config(job, 0)
                write_host_config = True

        elif job in available_consumers:
            rsv.echo("Disabling consumer %s" % job)
            consumer = Consumer.Consumer(job, rsv)
            # TODO - disable consumer
        else:
            rsv.log("WARNING", "Supplied job '%s' is not an installed metric or consumer" % job)
            num_errors += 1

    if write_host_config:
        host.write_config_file()

    
    if num_errors > 0:
        rsv.log("ERROR", "%s jobs could not be disabled." % num_errors)
        return False
    else:
        return True
