from functools import wraps
import sys
import os
from ConfigParser import SafeConfigParser, NoOptionError
from datetime import datetime
import logging

class TinyETL:
    def __init__(self, name, env):
        """
        name [str] -> Pass in a string identifier for this task.
        env [env object] -> Pass in the env object provided by Fabric.
        """
        # If there are no tasks to be run at invocation, 
        # don't bother with the rest of the object __init__
        if env.tasks == []:
            return

        self.name = name
        self.dry_run = self._this_is_a_dry_run(env)
        self.config = self._parse_config(env)
        
        if not self.dry_run:
            self.logdir = self.config.get('etlconfig', 'logdir')
            self.logname = "{}_{}".format(self.name, datetime.now().strftime('%Y-%m-%d_%H:%M:%S')) 
            self.logfile = os.path.join(self.logdir, self.logname + '.log')
            self.logger = self._create_logger()

    def _this_is_a_dry_run(self, env):
        """ Determines if this is a dry run. """
        try:
            dry_run = env.dry_run
        except AttributeError:
            raise SystemExit("Please provide either 'True' or 'False' to dry_run.")

        if dry_run not in ('True', 'False'):
            raise SystemExit("Please provide either 'True' or 'False' to dry_run.")
        else:
            # Convert the passed-in string val to a bool before returning
            return {'True': True, 'False': False}.get(dry_run)

    def _parse_config(self, env):
        """ Parses the config file passed in by `--set config=</path/to/some/file>` """
        try:
            config_file = env.config
        except AttributeError:
            raise SystemExit("Please provide a config file location to config.")

        # Make sure the file exists 
        if os.path.exists(config_file):
            return self._verify_config_file(config_file)
        else:
            raise SystemExit("Please ensure a config file exists at: ".format(config_file))

    def _verify_config_file(self, config_file):
        """ Ensure that all required sections are present in the config file. """
        parser = SafeConfigParser()
        parser.read(config_file)

        required_attributes = ['logdir',]
        err = ''
        for attr in required_attributes:
            try:
                parser.get('etlconfig', attr)
            except NoOptionError:
                err += "Please set the {} attribute in the config file.\n".format(attr)
   
        if not err == '':
            raise SystemExit(err)
        else:
            return parser

    def _timestamp(self):
        return datetime.now().strftime('%Y-%m-%d :: %H:%M:%S')

    def _create_logger(self):
        # See https://wingware.com/psupport/python-manual/2.3/lib/node304.html
        logger = logging.getLogger(self.name)
        hdlr = logging.FileHandler(self.logfile)
        formatter = logging.Formatter('%(asctime)s %(levelname)s %(message)s')
        hdlr.setFormatter(formatter)
        logger.addHandler(hdlr) 
        logger.setLevel(logging.DEBUG)
        return logger

    def log(self, f):
        @wraps(f)
        def logwrapper(*args, **kwargs):
            if self.dry_run:
                print('I would have run {} if this were the real deal!'.format(f.__name__))
            else:
                self.logger.info("Call {}".format(f.__name__))
                return f(*args, **kwargs)
        return logwrapper

