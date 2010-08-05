#!/usr/bin/env python

# Standard libraries
import os
import sys
from optparse import OptionParser
import ConfigParser

# RSV libraries
import conf
import utils
import results

# todo - remove before releasing
import pdb


#
# Declare some variables globally so that we don't have to pass them around
#
CONFIG  = ConfigParser.RawConfigParser()
OPTIONS = None
RSV_LOC = None

OPENSSL_EXE = "/usr/bin/openssl"

def main():
    """ Main subroutine: directs program flow """

    process_arguments()

    conf.load_config()

    check_proxy()

    pdb.set_trace()

    ping_test()
    
    execute_job()

    return



def clean_up():
    """ This will always be called before exiting.  Clean up any temporary
    files """

    pass



def log(message, level, indent=0):
    """ Print a message based on the verbosity level
    Current verbosity levels:
    0 - absolutely nothing
    1 - [DEFAULT] standard messages
    2 - additional debugging output
    3 - absolutely everything
    """

    if(OPTIONS.verbose >= level):
        # Only indent for debugging messages.
        if OPTIONS.verbose > 1 and indent != 0:
            message = " "*indent + message
        print message
    return



def process_arguments():
    """Process the command line arguments and populate global variables"""

    global OPTIONS

    #
    # Define the options to parse on the command line
    #
    usage = "usage: %prog -m <METRIC> -u <HOST> [more options]"
    parser = OptionParser(usage=usage)
    parser.add_option("-m", "--metric",  dest="metric", help="Metric to run")
    parser.add_option("-u", "--uri",     dest="uri",    help="URI to probe")
    parser.add_option("-v", "--verbose", dest="verbose", default=1, type="int",
                      help="Verbosity level (0-3). [Default=%default]")
    parser.add_option("--vdt-location", dest="vdt_location",
                      help="Supersedes VDT_LOCATION environment variable")

    (OPTIONS, args) = parser.parse_args()

    #
    # Do error checking on the options supplied
    #
    if OPTIONS.vdt_location:
        log("Using alternate VDT_LOCATION supplied on command line", 1)
    elif "VDT_LOCATION" in os.environ:
        OPTIONS.vdt_location = os.environ["VDT_LOCATION"]
    else:
        parser.error("You must have VDT_LOCATION set in your environment.\n" +\
                     "  Either source setup.sh or pass --vdt-location")

    # Set RSV_LOC as a shortcut for using in other code
    global RSV_LOC
    RSV_LOC = os.path.join(OPTIONS.vdt_location, "osg-rsv")

    if not OPTIONS.metric:
        parser.error("You must provide a metric to run")

    OPTIONS.executable = os.path.join(RSV_LOC, "bin", "metrics", OPTIONS.metric)
    if not os.path.exists(OPTIONS.executable):
        log("ERROR: Metric does not exist at %s" % OPTIONS.executable, 1)
        sys.exit(1)

    if not OPTIONS.uri:
        parser.error("You must provide a URI to test against")

    return




def ping_test():
    """ Ping the remote host to make sure it's alive before we attempt
    to run jobs """

    log("Pinging host %s:" % OPTIONS.uri, 2)

    # Send a single ping, with a timeout.  We just want to know if we can reach
    # the remote host, we don't care about the latency unless it exceeds the timeout
    (ret, out) = utils.system("/bin/ping -W 3 -c 1 " + OPTIONS.uri)

    # If we can't ping the host, don't bother doing anything else
    if ret:
        results.ping_failure(out)
        
    log("Ping successful", 2, 4)
    return




def check_proxy():
    """ Determine if we're using a service cert or user proxy and
    validate appropriately """

    if config_val(OPTIONS.metric, "need-proxy", "false"):
        log("Skipping proxy check because need-proxy=false", 2)
        return

    # First look for the service certificate.  Since this is the preferred option,
    # it will override the proxy-file if both are set.
    try:
        service_cert  = CONFIG.get("rsv", "service-cert")
        service_key   = CONFIG.get("rsv", "service-key")
        service_proxy = CONFIG.get("rsv", "service-proxy")
        renew_service_certificate_proxy(service_cert, service_key, service_proxy)
        return
    except ConfigParser.NoOptionError:
        pass

    # If the service certificate is not available, look for a user proxy file
    try:
        proxy_file = CONFIG.get("rsv", "proxy-file")
        check_user_proxy(proxy_file)
        return
    except ConfigParser.NoOptionError:
        pass

    # If we won't have a proxy, and need-proxy was not set above, we gotta bail
    results.no_proxy_found()



def renew_service_certificate_proxy(cert, key, proxy):
    """ Check the service certificate.  If it is expiring soon, renew it. """

    log("Checking service certificate proxy:", 2, 0)

    hours_til_expiry = 6
    seconds_til_expiry = hours_til_expiry * 60 * 60
    (ret, out) = utils.system("%s x509 -in %s -noout -enddate -checkend %s" %
                              (OPENSSL_EXE, proxy, seconds_til_expiry))
    
    if ret == 0:
        log("Service certificate valid for at least %s hours." % hours_til_expiry, 2, 4)
    else:
        log("Service certificate proxy expiring within %s hours.  Renewing it." %
            hours_til_expiry, 2, 4)

        grid_proxy_init_exe = os.path.join(OPTIONS.vdt_location, "globus", "bin", "grid-proxy-init")
        (ret, out) = utils.system("%s -cert %s -key %s -valid 12:00 -debug -out %s" %
                                  (grid_proxy_init_exe, cert, key, proxy))

        if ret:
            results.service_proxy_renewal_failed(cert, key, proxy, out)

    # Globus needs help finding the service proxy since it probably does not have the
    # default naming scheme of /tmp/x509_u<UID>
    os.environ["X509_USER_PROXY"] = proxy
    os.environ["X509_PROXY_FILE"] = proxy

    return



def check_user_proxy(proxy_file):
    """ Check that a proxy file is valid """

    log("Checking user proxy", 2, 0)
    
    # Check that the file exists on disk
    if not os.path.exists(proxy_file):
        results.missing_user_proxy(proxy_file)

    # Check that the proxy is not expiring in the next 10 minutes.  globus-job-run
    # doesn't seem to like a proxy that has a lifetime of less than 3 hours anyways,
    # so this check might need to be adjusted if that behavior is more understood.
    minutes_til_expiration = 10
    seconds_til_expiration = minutes_til_expiration * 60
    (ret, out) = utils.system("%s x509 -in %s -noout -enddate -checkend %s" %
                              (OPENSSL_EXE, proxy_file, seconds_til_expiration))
    if ret:
        results.expired_user_proxy(proxy_file, out, minutes_til_expiration)

    # Just in case this isn't the default /tmp/x509_u<UID> we'll explicitly set it
    os.environ["X509_USER_PROXY"] = proxy_file
    os.environ["X509_PROXY_FILE"] = proxy_file

    return



def parse_job_output(output):
    """ Parse the job output from the worker script """

    if(config_val(OPTIONS.metric, "output-format", "wlcg")):
        parse_job_output_wlcg(output)
    elif(config_val(OPTIONS.metric, "output-format", "brief")):
        parse_job_output_brief(output)
    else:
        log("ERROR: output format unknown", 1)
        

def parse_job_output_wlcg(output):
    results.print_wlcg_result(output)

def parse_job_output_brief(output):

    status = None
    details = None

    lines = output.split("\n")

    if lines[0] == "JOB RESULTS:":
        status = lines[1].strip()
        details = "\n".join(lines[2:])

    if status and details:
        results.print_result(status, details)
    else:
        log("ERROR: invalid data returned from job.", 1)

        # We want to display the trimmed output, unless we're in full verbose mode
        if OPTIONS.verbose != 0 and OPTIONS.verbose < 3:
            trim_length = CONFIG.get("rsv", "details-data-trim-length")
            log("Displaying first %s bytes of output (use -v3 for full output)" %
                trim_length, 1)
            output = output[:trim_length]
        else:
            log("Displaying full output received from command:", 3)
            
        log(output, 1)
        sys.exit(1)


def execute_job():
    """ Execute the job """

    try:
        jobmanager  = CONFIG.get(OPTIONS.metric, "jobmanager")
        job_timeout = CONFIG.get("rsv", "job_timeout")
    except ConfigParser.NoOptionError:
        fatal("ej1: jobmanager or job_timeout not defined in config")

    #
    # Build the custom parameters to the script
    #
    args_section = OPTIONS.metric + " args"
    args = ""
    try:
        for option in CONFIG.options(args_section):
            args += "--%s %s " % (option, CONFIG.get(args_section, option))
    except ConfigParser.NoSectionError:
        log("No '%s' section found" % args_section, 2, 0)
    

    #
    # Set the environment for the job
    #
    log("Setting up job environment:", 2, 0)
    original_environment = os.environ.copy()

    section = OPTIONS.metric + " env"
    for var in CONFIG.options(section):
        (action, value) = CONFIG.get(section, var)
        log("Var: '%s' Action: '%s' Value: '%s'" % (var, action, value), 3, 4)
        if action.upper() == "APPEND":
            if var in os.environ:
                os.environ[var] = os.environ[var] + ":" + value
            else:
                os.environ[var] = value
        elif action.upper() == "PREPEND":
            if var in os.environ:
                os.environ[var] = value + ":" + os.environ[var]
            else:
                os.environ[var] = value
        elif action.upper() == "SET":
            os.environ[var] = value
        elif action.upper() == "UNSET":
            if var in os.environ:
                del os.environ[var]



    #
    # Build the command line for the job
    #
    if config_val(OPTIONS.metric, "execute", "local"):
        job = "%s -m %s -u %s %s" % (OPTIONS.executable,
                                     OPTIONS.metric,
                                     OPTIONS.uri,
                                     args)

    elif config_val(OPTIONS.metric, "execute", "remote"):
        globus_job_run_exe = os.path.join(OPTIONS.vdt_location, "globus", "bin", "globus-job-run")
        job = "%s %s/jobmanager-%s -s %s -- -m %s -u %s %s" % (globus_job_run_exe,
                                                            OPTIONS.uri,
                                                            jobmanager,
                                                            OPTIONS.executable,
                                                            OPTIONS.metric,
                                                            OPTIONS.uri,
                                                            args)


    log("Running command '%s'" % job, 2)

    (ret, out) = utils.system_with_timeout(job, job_timeout)


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
        results.job_timed_out(job, job_timeout)
        
    if ret:
        if config_val(OPTIONS.metric, "execute", "remote"):
            results.remote_job_failed(job, out)
        results.local_job_failed(job, out)
        
    parse_job_output(out)

    return



def config_val(section, key, value, case_sensitive=0):
    """ Check if key is in config, and if it equals val. """

    try:
        if case_sensitive == 0:
             if CONFIG.get(section, key).lower() == str(value).lower():
                return True
        else:
            if CONFIG.get(section, key) == str(value):
                return True
    except ConfigParser.NoOptionError:
        return False
        
    return False


def fatal(msg=None):
    """ For critical errors that we don't know the cause of - such as a value not being
    in the configuration that we thought we had a default for. """

    output  = "ERROR: An unexpected internal error has occurred.  "
    output += "Please re-run this script with -v3 and send the output to the developer."
    if msg != None:
        output += "Error message: %s" % msg

    log(output, 1, 0)
    sys.exit(1)
