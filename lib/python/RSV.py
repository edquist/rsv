#!/usr/bin/env python

""" Shared code between rsv-control and run-rsv-metric """

# System libraries
import os
import re
import sys
import logging
import ConfigParser

# RSV libraries
import Host
import Metric
import Results
import Sysutils

OPENSSL_EXE = "/usr/bin/openssl"

class RSV:
    """ Class to load and store configuration information about this install
    of RSV.  This could be replaced with a singleton pattern to reduce the need
    to pass the instance around in functions. """
    
    vdt_location = None
    rsv_location = None
    sysutils = None
    results = None
    config = None
    logger = None
    proxy = None
    quiet = 0

    def __init__(self, vdt_location=None, verbosity=1):

        # Setup rsv_location
        if vdt_location:
            self.vdt_location = vdt_location
            self.rsv_location = os.path.join(vdt_location, "osg-rsv")
        else:
            self.vdt_location = get_osg_location()
            self.rsv_location = os.path.join(self.vdt_location, "osg-rsv")

        # For any messages that won't go through the logger
        if verbosity == 0:
            self.quiet = 1

        # Instantiate our helper objects
        self.sysutils = Sysutils.Sysutils(self)
        self.results  = Results.Results(self)

        # Setup the logger
        self.init_logging(verbosity)

        # Setup the initial configuration
        self.config = ConfigParser.RawConfigParser()
        self.config.optionxform = str
        self.setup_config()


    def setup_config(self):
        """ Load configuration """
        defaults = get_rsv_defaults()
        if defaults:
            for section in defaults.keys():
                if not self.config.has_section(section):
                    self.config.add_section(section)
                    
                for item in defaults[section].keys():
                    self.config.set(section, item, defaults[section][item])

        self.load_config_file(os.path.join(self.rsv_location, "etc", "rsv.conf"), 1)



    def load_config_file(self, config_file, required):
        """ Parse a configuration file in INI form. """
    
        self.log("INFO", "reading configuration file " + config_file, 4)

        if not os.path.exists(config_file):
            if required:
                self.log("ERROR", "missing required configuration file '%s'" % config_file)
                sys.exit(1)
            else:
                self.log("INFO", "configuration file does not exist '%s'" % config_file, 4)
                return

        try:
            self.config.read(config_file)
        except ConfigParser.ParsingError, err:
            self.log("CRITICAL", err)
            sys.exit(1)

        return


    
    def get_installed_metrics(self):
        """ Return a list of installed metrics """
        metrics_dir = os.path.join(self.rsv_location, "bin", "metrics")
        try:
            files = os.listdir(metrics_dir)
            files.sort()
            metrics = []
            for entry in files:
                # Each metric should be something like org.osg.
                # This pattern will specifically not match '.', '..', '.svn', etc
                if re.search("\w\.\w", entry):
                    metrics.append(entry)
            return metrics
        except OSError, err:
            self.log("ERROR", "The metrics directory (%s) could not be accessed.  Error msg: %s" %
                     (metrics_dir, err))
            return []


    def get_installed_consumers(self):
        """ Return a list of installed consumers """
        consumers_dir = os.path.join(self.rsv_location, "bin", "consumers")
        try:
            files = os.listdir(consumers_dir)
            files.sort()
            consumers = []
            for entry in files:
                if re.search("-consumer$", entry):
                    consumers.append(entry)
            return consumers
        except OSError:
            self.log("ERROR", "The consumers directory (%s) could not be accessed.  Error msg: %s" %
                     (consumers_dir, err))
            return []
        
        
    def get_metric_info(self):
        """ Return a dictionary with information about each installed metric """

        metrics = {}
        for metric in self.get_installed_metrics():
            metrics[metric] = Metric.Metric(metric, self)

        return metrics



    def get_hosts(self):
        """ Return a list of hosts that have configuration files """

        conf_dir = os.path.join(self.rsv_location, "etc")
        try:
            config_files = os.listdir(conf_dir)
            hosts = []
            for config_file in config_files:
                # Somewhat arbitrary pattern, but it won't match '.', '..', or '.svn'
                if re.search("\.conf$", config_file) and config_file != "rsv.conf":
                    host = re.sub("\.conf$", "", config_file)
                    hosts.append(host)
            return hosts
        except OSError:
            # todo - check for permission problem
            self.log("ERROR", "The conf directory does not exist (%s)" % conf_dir)



    def get_host_info(self):
        """ Return a list containing one Host instance for each configured host """

        hosts = []
        for host in self.get_hosts():
            hosts.append(Host.Host(host, self))

        return hosts



    def init_logging(self, verbosity):
        """ Initialize the logger """

        logger = logging.StreamHandler(sys.stdout)
        formatter = logging.Formatter("%(levelname)s: %(message)s")
        logger.setFormatter(formatter)
        if verbosity == 0:
            logger.setLevel(logging.CRITICAL)
        elif verbosity == 1:
            logger.setLevel(logging.WARNING)
        elif verbosity == 2:
            logger.setLevel(logging.INFO)
        elif verbosity == 3:
            logger.setLevel(logging.DEBUG)

        self.logger = logging.getLogger()
        self.logger.addHandler(logger)


    def log(self, level, message, indent=0):
        """ Interface to logger """
        level = level.lower()

        if indent > 0:
            message = " "*indent + message

        if level == "debug":
            self.logger.debug(message)
        elif level == "info":
            self.logger.info(message)
        elif level == "warning":
            self.logger.warning(message)
        elif level == "error":
            self.logger.error(message)
        elif level == "critical":
            self.logger.critical(message)
        else:
            self.logger.warning("Invalid level (%s) passed to RSV.log." % level)
            self.logger.warning(message)

    def echo(self, message, indent=0):
        """ Print a message unless verbosity level==0 (quiet) """
        
        if self.quiet:
            return
        else:
            if indent > 0:
                message = " "*indent + message

            print message
        


    def get_metric_log_dir(self):
        """ Return the directory to store condor log/out/err files for metrics """
        return os.path.join(self.rsv_location, "logs", "metrics")

    def get_consumer_log_dir(self):
        """ Return the directory to store condor log/out/err files for consumers """
        return os.path.join(self.rsv_location, "logs", "consumers")

    def get_user(self):
        """ Return the user defined in rsv.conf """
        try:
            return self.config.get("rsv", "user")
        except ConfigParser.NoOptionError:
            self.log("ERROR", "'user' not defined in rsv.conf")
            return ""


    def get_enabled_consumers(self):
        """ Return a list of all consumers enabled in rsv.conf """
        
        try:
            consumers = []
            for consumer in re.split("\s*,\s*", self.config.get("rsv", "consumers")):
                if not consumer.isspace():
                    consumers.append(consumer)
            return consumers
        except ConfigParser.NoOptionError:
            self.log("WARNING", "No consumers defined in rsv.conf")
            return []


    def get_wrapper(self):
        """ Return the wrapper script that will run the metrics """
        return os.path.join(self.rsv_location, "bin", "run-rsv-metric")


    def get_proxy(self):
        """ Return the path of the proxy file being used """
        return self.proxy


    def check_proxy(self, metric):
        """ Determine if we're using a service cert or user proxy and
        validate appropriately """

        self.log("INFO", "Checking proxy:")

        if metric.config_val("need-proxy", "false"):
            self.log("INFO", "Skipping proxy check because need-proxy=false", 4)
            return

        # First look for the service certificate.  Since this is the preferred option,
        # it will override the proxy-file if both are set.
        try:
            service_cert  = self.config.get("rsv", "service-cert")
            service_key   = self.config.get("rsv", "service-key")
            service_proxy = self.config.get("rsv", "service-proxy")
            self.renew_service_certificate_proxy(metric, service_cert, service_key, service_proxy)
            self.proxy = service_proxy
            return
        except ConfigParser.NoOptionError:
            self.log("INFO", "Not using service certificate.  Checking for user proxy", 4)
            pass

        # If the service certificate is not available, look for a user proxy file
        try:
            proxy_file = self.config.get("rsv", "proxy-file")
            check_user_proxy(metric, proxy_file)
            self.proxy = proxy_file
            return
        except ConfigParser.NoOptionError:
            pass

        # If we won't have a proxy, and need-proxy was not set above, we bail
        self.results.no_proxy_found(metric)



    def renew_service_certificate_proxy(self, metric, cert, key, proxy):
        """ Check the service certificate.  If it is expiring soon, renew it. """

        self.log("INFO", "Checking service certificate proxy:", 4)

        hours_til_expiry = 4
        seconds_til_expiry = hours_til_expiry * 60 * 60
        (ret, out) = self.run_command("%s x509 -in %s -noout -enddate -checkend %s" %
                                      (OPENSSL_EXE, proxy, seconds_til_expiry))

        if ret == 0:
            self.log("INFO", "Service certificate valid for at least %s hours." % hours_til_expiry, 4)
        else:
            self.log("INFO", "Service certificate proxy expiring within %s hours.  Renewing it." %
                    hours_til_expiry, 4)

            grid_proxy_init_exe = os.path.join(self.vdt_location, "globus", "bin", "grid-proxy-init")
            (ret, out) = self.run_command("%s -cert %s -key %s -valid 6:00 -debug -out %s" %
                                          (grid_proxy_init_exe, cert, key, proxy))

            if ret:
                self.results.service_proxy_renewal_failed(metric, cert, key, proxy, out)

        # Globus needs help finding the service proxy since it probably does not have the
        # default naming scheme of /tmp/x509_u<UID>
        os.environ["X509_USER_PROXY"] = proxy
        os.environ["X509_PROXY_FILE"] = proxy

        # todo - need to tell RSVv3 probes about this proxy

        return



    def check_user_proxy(self, metric, proxy_file):
        """ Check that a proxy file is valid """

        self.log("INFO", "Checking user proxy", 4)

        # Check that the file exists on disk
        if not os.path.exists(proxy_file):
            self.results.missing_user_proxy(metric, proxy_file)

        # Check that the proxy is not expiring in the next 10 minutes.  globus-job-run
        # doesn't seem to like a proxy that has a lifetime of less than 3 hours anyways,
        # so this check might need to be adjusted if that behavior is more understood.
        minutes_til_expiration = 10
        seconds_til_expiration = minutes_til_expiration * 60
        (ret, out) = self.run_command("%s x509 -in %s -noout -enddate -checkend %s" %
                                      (OPENSSL_EXE, proxy_file, seconds_til_expiration))
        if ret:
            self.results.expired_user_proxy(metric, proxy_file, out, minutes_til_expiration)

        # Just in case this isn't the default /tmp/x509_u<UID> we'll explicitly set it
        os.environ["X509_USER_PROXY"] = proxy_file
        os.environ["X509_PROXY_FILE"] = proxy_file

        return


    def run_command(self, command, timeout=None):
        """ Wrapper for Sysutils.system """
        if timeout:
            return self.sysutils.system(command, timeout)
        else:
            # Use the timeout declared in the config file
            timeout = self.config.getint("rsv", "job-timeout")
            return self.sysutils.system(command, timeout)

# End of RSV class


def get_osg_location():
    """ Find the path to OSG root directory """
    return os.environ.get("OSG_LOCATION", os.environ.get("VDT_LOCATION", ""))


def get_rsv_defaults():
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
