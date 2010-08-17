#!/usr/bin/env python

import os
import re
import sys
import ConfigParser

class Consumer:
    rsv = None
    name = None
    config = None
    conf_dir = None
    executable = None


    def __init__(self, consumer, rsv):
        # Initialize vars
        self.name = consumer
        self.rsv  = rsv
        self.conf_dir = os.path.join(rsv.rsv_location, "etc", "consumers")

        # Find executable
        self.executable = os.path.join(rsv.rsv_location, "bin", "consumers", consumer)
        if not os.path.exists(self.executable):
            rsv.log("ERROR", "Consumer does not exist at %s" % self.executable)
            sys.exit(1)

        # Load configuration
        defaults = get_consumer_defaults(consumer)
        self.config = ConfigParser.RawConfigParser()
        self.config.optionxform = str
        self.load_config(defaults)


    def load_config(self, defaults):

        if defaults:
            for section in defaults.keys():
                if not self.config.has_section(section):
                    self.config.add_section(section)
                    
                for item in defaults[section].keys():
                    self.config.set(section, item, defaults[section][item])

        # Load the metric's general configuration file
        file = os.path.join(self.conf_dir, self.name + ".conf")
        if not os.path.exists(file):
            self.rsv.log("INFO", "Consumer config file '%s' does not exist" % file)
            return
        else:
            try:
                self.config.read(file)
            except ConfigParser.ParsingError, err:
                self.rsv.log("CRITICAL", err)
                sys.exit(1)



    def config_get(self, key):
        try:
            return self.config.get(self.name, key)
        except ConfigParser.NoOptionError:
            self.rsv.log("DEBUG", "consumer.config_get - no key '%s'" % key)
            return None



    def config_val(self, key, value, case_sensitive=0):
        """ Check if key is in config, and if it equals val. """

        try:
            if case_sensitive == 0:
                if self.config.get(self.name, key).lower() == str(value).lower():
                    return True
            else:
                if self.config.get(self.name, key) == str(value):
                    return True
        except ConfigParser.NoOptionError:
            return False

        return False


    def get_unique_name(self):
        return self.name



def get_consumer_defaults(consumer_name):
    """ Load consumer default values """
    defaults = {}
    def set_default_value(section, option, value):
        if section not in defaults:
            defaults[section] = {}
        defaults[section][option] = value

    # There are currently no consumer defaults

    return defaults
