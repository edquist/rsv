#!/usr/bin/env python

""" Shared code between rsv-control and run-rsv-metric """

# System libraries
import os
import re
import sys
import logging
import ConfigParser

# RSV libraries
import Metric

import pdb


class RSV:
    vdt_location = None
    rsv_location = None
    config = None
    logger = None
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

        # Setup the logger
        self.init_logging(verbosity)

        # Setup the initial configuration
        self.config = ConfigParser.RawConfigParser()
        self.config.optionxform = str


    def setup_config(self, defaults=None):
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

        # todo - add some error catching here
        self.config.read(config_file)

        return


    
    def get_installed_metrics(self):
        """ Return a list of installed metrics """
        metrics_dir = os.path.join(self.rsv_location, "bin", "metrics")
        try:
            files = os.listdir(metrics_dir)
            files.sort()
            metrics = []
            for file in files:
                # Somewhat arbitrary pattern, but it won't match '.', '..', or '.svn'
                if re.search("\w\.\w", file):
                    metrics.append(file)
            return metrics
        except OSError:
            self.log("ERROR", "The metrics directory does not exist (%s)" % metrics_dir)
            pass


        
    def get_metric_info(self):
        """ Return a dictionary with information about each installed metric """

        dict = {}
        for metric in self.get_installed_metrics():
            dict[metric] = Metric.Metric(metric, self)

        return dict



    def init_logging(self, verbosity):

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
            selg.logger.warning(message)


# End of RSV class


def get_osg_location():
    """ Find the path to OSG root directory """
    return os.environ.get("OSG_LOCATION", os.environ.get("VDT_LOCATION", ""))
