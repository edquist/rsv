#!/usr/bin/env python

import os
import ConfigParser

class Host:
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

        # Load the metric's general configuration file
        file = os.path.join(self.conf_dir, self.host + ".conf")
        if not os.path.exists(file):
            self.rsv.log("ERROR", "Host config file '%s' does not exist" % file)
            return
        else:
            try:
                self.config.read(file)
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

        enabled_metrics = []
        try:
            for metric in self.config.options(self.host):
                if self.metric_enabled(metric):
                    enabled_metrics.append(metric)
        except ConfigParser.NoSectionError:
            pass

        return enabled_metrics
