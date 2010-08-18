#!/usr/bin/env python

import os
import sys
import ConfigParser

class Host:
    """ Instantiable class to read and store configuration about a single host """

    rsv = None
    host = None
    config = None
    conf_dir = None
    
    def __init__(self, host, rsv):
        self.host = host
        self.rsv  = rsv

        self.conf_dir = os.path.join(rsv.rsv_location, "etc")

        # Load configuration
        self.config = ConfigParser.RawConfigParser()
        self.config.optionxform = str  # Make keys case-sensitive
        self.load_config()



    def load_config(self):
        """ Load host specific configuration file """

        config_file = os.path.join(self.conf_dir, self.host + ".conf")
        if not os.path.exists(config_file):
            self.rsv.log("ERROR", "Host config file '%s' does not exist" % config_file)
            return
        else:
            try:
                self.config.read(config_file)
            except ConfigParser.ParsingError, err:
                self.rsv.log("CRITICAL", err)
                sys.exit(1)


    def metric_enabled(self, metric_name):
        """ Return true if the specified metric is enabled, false otherwise """

        try:
            value = self.config.get(self.host, metric_name)
            if not value or value == "off":
                return False
            return True
        
        except ConfigParser.NoOptionError:
            return False
        

    def get_enabled_metrics(self):
        """ Return a list of all metrics enabled to run against this host """
        
        enabled_metrics = []
        try:
            for metric in self.config.options(self.host):
                if self.metric_enabled(metric):
                    enabled_metrics.append(metric)
        except ConfigParser.NoSectionError:
            pass

        return enabled_metrics
