#!/usr/bin/env python

# Standard libraries
import re
import os
import sys
import ConfigParser
from pwd import getpwnam
from optparse import OptionParser

# RSV libraries
import RSV
import Metric
import results
import sysutils

# todo - remove before releasing
import pdb


#
# Globals
#
OPENSSL_EXE = "/usr/bin/openssl"
VALID_OUTPUT_FORMATS = ["wlcg", "brief"]


def process_arguments():
    """Process the command line arguments and populate global variables"""

    #
    # Define the options to parse on the command line
    #
    usage = "usage: %prog -m <METRIC> -u <HOST> [more options]"
    parser = OptionParser(usage=usage)
    parser.add_option("-m", "--metric", dest="metric", help="Metric to run")
    parser.add_option("-u", "--host",   dest="uri",    help="Host to test")
    parser.add_option("-v", "--verbose", dest="verbose", default=1, type="int",
                      help="Verbosity level (0-3). [Default=%default]")
    parser.add_option("--vdt-location", dest="vdt_location",
                      help="Supersedes VDT_LOCATION environment variable")

    (options, args) = parser.parse_args()

    #
    # Do error checking on the options supplied
    #
    if options.vdt_location:
        log("Using VDT_LOCATION supplied on command line", 1)
    else:
        options.vdt_location = RSV.get_osg_location()

    if not options.vdt_location:
        parser.error("You must have VDT_LOCATION set in your environment.\n" +
                     "  Either source setup.sh or pass --vdt-location")

    if not options.metric:
        parser.error("You must provide a metric to run")


    # Validate the host, and if necessary, split off the port
    if not options.uri:
        parser.error("You must provide a URI to test against")

    if options.uri.find(":") == -1:
        options.host = options.uri
    else:
        (options.host, options.port) = re.split(":", options.uri, 1)


    return options



def validate_config(rsv, metric):
    """ Perform validation on config values """

    rsv.log("INFO", "Validating configuration:")

    #
    # make sure that the user is valid, and we are either that user or root
    #
    rsv.log("INFO", "Validating user:")
    try:
        user = rsv.config.get("rsv", "user")
    except ConfigParser.NoOptionError:
        rsv.log("ERROR", "'user' is missing in rsv.conf.  Set this value to your RSV user", 4)
        clean_up(1)

    try:
        (desired_uid, desired_gid) = getpwnam(user)[2:4]
    except KeyError:
        rsv.log("ERROR", "The '%s' user defined in rsv.conf does not exist" % user, 4)
        clean_up(1)

    # If appropriate, switch UID/GID
    sysutils.switch_user(rsv, user, desired_uid, desired_gid)

                
    #
    # "details_data_trim_length" must be an integer because we will use it later
    # in a splice
    #
    try:
        rsv.config.getint("rsv", "details_data_trim_length")
    except ConfigParser.NoOptionError:
        # We set a default for this, but just to be safe set it again here.
        rsv.config.set("rsv", "details_data_trim_length", "10000")
    except ValueError:
        rsv.log("ERROR: details_data_trim_length must be an integer.  It is set to '%s'"
                % rsv.config.get("rsv", "details_data_trim_length"))
        clean_up(1)


    #
    # job_timeout must be an integer because we will use it later in an alarm call
    #
    try:
        rsv.config.getint("rsv", "job-timeout")
    except ConfigParser.NoOptionError:
        # We set a default for this, but just to be safe...
        rsv.config.set("rsv", "job-timeout", "300")
    except ValueError:
        rsv.log("ERROR", "job-timeout must be an integer.  It is set to '%s'" %
                rsv.config.get("rsv", "job-timeout"))
        clean_up(1)


    #
    # warn if consumers are missing
    #
    try:
        consumers = rsv.config.get("rsv", "consumers")
        rsv.log("INFO", "Registered consumers: %s" % consumers, 0)
    except ConfigParser.NoOptionError:
        rsv.config.set("rsv", "consumers", "")
        rsv.log("WARNING", "no consumers are registered in rsv.conf.  This means that" +
                "records will not be sent to a central collector for availability" +
                "statistics.")


    #
    # check vital configuration for the job
    #
    if not metric.config_get("service-type") or not metric.config_get("execute"):
        rsv.log("ERROR", "metric configuration is missing 'service-type' or 'execute' " +
                "declaration.  This is likely caused by a missing or corrupt metric " +
                "configuration file")
        clean_up(1)


    # 
    # Check the desired output format
    #
    try:
        output_format = metric.config_get("output-format").lower()
        if output_format not in VALID_OUTPUT_FORMATS:
            valid_formats = " ".join(VALID_OUTPUT_FORMATS)
            rsv.log("ERROR", "output-format '%s' is not supported.  Valid formats: %s\n" %
                    (output_format, valid_formats))
            clean_up(1)
                    
    except ConfigParser.NoOptionError:
        rsv.log("ERROR", "desired output-format is missing.\n" +
                "This is likely caused by a missing or corrupt metric configuration file")
        clean_up(1)


    return



def check_proxy(rsv, metric):
    """ Determine if we're using a service cert or user proxy and
    validate appropriately """

    rsv.log("INFO", "Checking proxy:")

    if metric.config_val("need-proxy", "false"):
        rsv.log("INFO", "Skipping proxy check because need-proxy=false", 4)
        return

    # First look for the service certificate.  Since this is the preferred option,
    # it will override the proxy-file if both are set.
    try:
        service_cert  = rsv.config.get("rsv", "service-cert")
        service_key   = rsv.config.get("rsv", "service-key")
        service_proxy = rsv.config.get("rsv", "service-proxy")
        renew_service_certificate_proxy(rsv, service_cert, service_key, service_proxy)
        return
    except ConfigParser.NoOptionError:
        rsv.log("INFO", "Not using service certificate.  Checking for user proxy", 4)
        pass

    # If the service certificate is not available, look for a user proxy file
    try:
        proxy_file = rsv.config.get("rsv", "proxy-file")
        check_user_proxy(rsv, metric, proxy_file)
        return
    except ConfigParser.NoOptionError:
        pass

    # If we won't have a proxy, and need-proxy was not set above, we gotta bail
    results.no_proxy_found(rsv, metric)



def renew_service_certificate_proxy(rsv, cert, key, proxy):
    """ Check the service certificate.  If it is expiring soon, renew it. """

    rsv.log("INFO", "Checking service certificate proxy:", 4)

    hours_til_expiry = 4
    seconds_til_expiry = hours_til_expiry * 60 * 60
    (ret, out) = sysutils.system("%s x509 -in %s -noout -enddate -checkend %s" %
                                 (OPENSSL_EXE, proxy, seconds_til_expiry))
    
    if ret == 0:
        log("INFO", "Service certificate valid for at least %s hours." % hours_til_expiry, 4)
    else:
        log("INFO", "Service certificate proxy expiring within %s hours.  Renewing it." %
            hours_til_expiry, 4)

        grid_proxy_init_exe = os.path.join(rsv.vdt_location, "globus", "bin", "grid-proxy-init")
        (ret, out) = sysutils.system("%s -cert %s -key %s -valid 6:00 -debug -out %s" %
                                     (grid_proxy_init_exe, cert, key, proxy))

        if ret:
            results.service_proxy_renewal_failed(rsv, metric, cert, key, proxy, out)

    # Globus needs help finding the service proxy since it probably does not have the
    # default naming scheme of /tmp/x509_u<UID>
    os.environ["X509_USER_PROXY"] = proxy
    os.environ["X509_PROXY_FILE"] = proxy

    # todo - need to tell RSVv3 probes about this proxy

    return



def check_user_proxy(rsv, metric, proxy_file):
    """ Check that a proxy file is valid """

    rsv.log("INFO", "Checking user proxy", 4)
    
    # Check that the file exists on disk
    if not os.path.exists(proxy_file):
        results.missing_user_proxy(rsv, metric, proxy_file)

    # Check that the proxy is not expiring in the next 10 minutes.  globus-job-run
    # doesn't seem to like a proxy that has a lifetime of less than 3 hours anyways,
    # so this check might need to be adjusted if that behavior is more understood.
    minutes_til_expiration = 10
    seconds_til_expiration = minutes_til_expiration * 60
    (ret, out) = sysutils.system("%s x509 -in %s -noout -enddate -checkend %s" %
                                 (OPENSSL_EXE, proxy_file, seconds_til_expiration))
    if ret:
        results.expired_user_proxy(rsv, metric, proxy_file, out, minutes_til_expiration)

    # Just in case this isn't the default /tmp/x509_u<UID> we'll explicitly set it
    os.environ["X509_USER_PROXY"] = proxy_file
    os.environ["X509_PROXY_FILE"] = proxy_file

    return



def ping_test(rsv, metric, options):
    """ Ping the remote host to make sure it's alive before we attempt
    to run jobs """

    rsv.log("INFO", "Pinging host %s:" % options.uri)

    # Send a single ping, with a timeout.  We just want to know if we can reach
    # the remote host, we don't care about the latency unless it exceeds the timeout
    (ret, out) = sysutils.system("/bin/ping -W 3 -c 1 " + options.host)

    # If we can't ping the host, don't bother doing anything else
    if ret:
        results.ping_failure(rsv, metric, out)
        
    rsv.log("INFO", "Ping successful", 4)
    return



def parse_job_output(rsv, metric, output):
    """ Parse the job output from the worker script """

    if(metric.config_val("output-format", "wlcg")):
        parse_job_output_wlcg(rsv, metric, output)
    elif(metric.config_val("output-format", "brief")):
        parse_job_output_brief(rsv, metric, output)
    else:
        rsv.log("ERROR", "output format unknown")

        

def parse_job_output_wlcg(rsv, metric, output):
    results.wlcg_result(rsv, metric, output)



def parse_job_output_brief(rsv, metric, output):

    status = None
    details = None

    lines = output.split("\n")

    if lines[0] == "JOB RESULTS:":
        status = lines[1].strip()
        details = "\n".join(lines[2:])

    if status and details:
        results.brief_result(rsv, metric, status, details)
    else:
        rsv.log("ERROR", "invalid data returned from job.")

        # We want to display the trimmed output, unless we're in full verbose mode
        if not rsv.quiet and OPTIONS.verbose < 3:
            trim_length = rsv.config.get("rsv", "details-data-trim-length")
            rsv.log("Displaying first %s bytes of output (use -v3 for full output)" %
                    trim_length, 1)
            output = output[:trim_length]
        else:
            rsv.log("DEBUG", "Displaying full output received from command:")
            
        log(output, 1)
        sys.exit(1)


def execute_job(rsv, metric):
    """ Execute the job """

    jobmanager  = metric.config_get("jobmanager")
    job_timeout = rsv.config.get("rsv", "job-timeout")

    if not jobmanager or not job_timeout:
        rsv.log("CRITICAL", "ej1: jobmanager or job-timeout not defined in config")
        sys.exit(1)

    #
    # Build the custom parameters to the script
    #
    args = metric.get_args_string()

    #
    # Set the environment for the job
    #
    rsv.log("INFO", "Setting up job environment:")
    original_environment = os.environ.copy()

    env = metric.get_environment()
    for var in env.keys():
        (action, value) = env[var]
        action = action.upper()
        rsv.log("DEBUG", "Var: '%s' Action: '%s' Value: '%s'" % (var, action, value), 4)
        if action == "APPEND":
            if var in os.environ:
                os.environ[var] = os.environ[var] + ":" + value
            else:
                os.environ[var] = value
        elif action == "PREPEND":
            if var in os.environ:
                os.environ[var] = value + ":" + os.environ[var]
            else:
                os.environ[var] = value
        elif action == "SET":
            os.environ[var] = value
        elif action == "UNSET":
            if var in os.environ:
                del os.environ[var]



    #
    # Build the command line for the job
    #
    if metric.config_val("execute", "local"):
        job = "%s -m %s -u %s %s" % (metric.executable,
                                     metric.name,
                                     metric.host,
                                     args)

    elif metric.config_val("execute", "remote-globus"):
        globus_job_run_exe = os.path.join(rsv.vdt_location, "globus", "bin", "globus-job-run")
        job = "%s %s/jobmanager-%s -s %s -- -m %s -u %s %s" % (globus_job_run_exe,
                                                               metric.host,
                                                               jobmanager,
                                                               metric.executable,
                                                               metric.name,
                                                               metric.host,
                                                               args)


    rsv.log("INFO", "Running command '%s'" % job)

    (ret, out) = sysutils.system_with_timeout(job, job_timeout)


    #
    # Restore the environment
    # 
    os.environ = original_environment


    #
    # Handle the output
    #

    # todo - (None, None) will be returned on a timeout.  This could maybe be improved
    # by throwing an exception?  My knowledge of Python is weak here.
    if ret == None and out == None:
        results.job_timed_out(rsv, metric, job, job_timeout)
        
    if ret:
        if metric.config_val("execute", "local"):
            results.local_job_failed(rsv, metric, job, out)
        elif metric.config_val("execute", "remote-globus"):
            results.remote_globus_job_failed(rsv, metric, job, out)
        
    parse_job_output(rsv, metric, out)

    return


def rsv_defaults(rsv):
    """
    This is where to declare defaults for config knobs.
    Any defaults should have a comment explaining them.
    """

    defaults = {}

    def set_default_value(section, option, value):
        if section not in defaults:
            defaults[section] = {}
        defaults[section][option] = value

    # Just in case the details data returned is enormous, we'll set the default
    # to trim it down to in bytes.  A value of 0 means no trimming.
    set_default_value("rsv", "details-data-trim-length", 10000)

    # Set the job timeout default in seconds
    set_default_value("rsv", "job-timeout", 300)

    return defaults


def metric_defaults(rsv, metric_name):
    """
    This is where to declare defaults for metric config knobs.
    Any defaults should have a comment explaining them.
    """

    defaults = {}

    def set_default_value(section, option, value):
        if section not in defaults:
            defaults[section] = {}
        defaults[section][option] = value

    # We want most remote Globus jobs to execute on the CE headnode, so they
    # need to use the fork jobmanager (unless they declare something different)
    set_default_value(metric_name, "jobmanager", "fork")

    # The only metricType that any current metric has is "status".  So instead
    # of declaring it in every single <metric>.conf file, we'll set it here but
    # still make it possible to configure in case it is needed in the future.
    set_default_value(metric_name, "metric-type", "status")

    return defaults


def clean_up(exit=0):
    """ This will always be called before exiting.  Clean up any temporary
    files """

    sys.exit(exit)



def main_run_rsv_metric():
    """ Main subroutine: directs program flow """

    # Process the command line and initialize
    options = process_arguments()

    rsv = RSV.RSV(options.vdt_location, options.verbose)
    defaults = rsv_defaults(rsv)
    rsv.setup_config(defaults)

    # Load configuration files
    defaults = metric_defaults(rsv, options.metric)
    metric = Metric.Metric(options.metric, rsv, defaults, options.uri)
    validate_config(rsv, metric)

    # Check for some basic error conditions
    check_proxy(rsv, metric)
    ping_test(rsv, metric, options)

    # Run the job and parse the result
    execute_job(rsv, metric)

    return



if __name__ == "__main__":
    progname = os.path.basename(sys.argv[0])
    if progname == 'run-rsv-metric' or progname == 'run-rsv-metric.py':
        if not main_run_rsv_metric():
            sys.exit(1)
        else:
            sys.exit(0)
    else:
        print "Wrong invocation!"
        sys.exit(1)
