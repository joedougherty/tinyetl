from functools import wraps
import sys
import os
from datetime import datetime
import logging
import requests

class TinyETL:
    """Manages facts about an ETL Process.

    Provides a consistent interface for storing log location,
    temporary data locations, and a way to facilitate dry-run
    and logging on Fabric-based ETL scripts.

    USAGE:
    =====

    etl = TinyETL(
        'an_etl_job',
        env=env, # This `env` will be provided by Fabric. [from fabric.api import env]
        log_dir="/path/to/a/log/directory",
        tmpdata_dir="/path/to/tmpdata/directory"
    )

    Instantiating this object will alter the behavior of your fabfile.py.
    Specifically, fab will require you to set the `dry_run` parameter explicitly
    if you'll be invoking a task.

    `fab --list` will work as expected.
    `fab main_task` will complain that `dry_run` has not be explicitly set.

    INVOCATION:
    ==========

    `fab main_task --set dry_run=True`

    LOG DECORATOR:
    =============

    This also provides a decorator for any tasks you want to log. 
    Apply `@etl.log` as the innermost decorator to a task and it 
    will be logged.
    """

    def __init__(self, desc, env, log_dir=None, tmpdata_dir=None):
        """
        desc [str] -> Docstring description of this task.
        env [env object] -> The env object provided by Fabric.
        log_dir [str] (optional) -> Absolute path to the directory to store logs in.
        tmpdata_dir [str] (optional) ->  Absolute path to the directory to store temp data in.
        """
        # If there are no tasks to be run at invocation, 
        # don't bother with the rest of the object __init__
        if env.tasks == []:
            return

        self.desc = desc
        self.dry_run = self._this_is_a_dry_run(env)
        
        if not self.dry_run:
            if not os.path.exists(log_dir):
                raise SystemExit("{} does not exist. Please create the log directory.".format(log_dir))
            else:
                self.log_dir = log_dir

            if not os.path.exists(tmpdata_dir):
                raise SystemExit("{} does not exist. Please create the tmp data directory.".format(log_dir))
            else:
                self.tmpdata_dir = tmpdata_dir
            self.logname = "{}_{}".format(self.name, datetime.now().strftime('%Y-%m-%d_%H:%M:%S')) 
            self.logfile = os.path.join(self.log_dir, self.logname + '.log')
            self.logger = self._create_logger()
        else:
            print(self.desc)

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
                print('[DRY RUN] :: {}'.format(f.__name__))
            else:
                self.logger.info("Running {}".format(f.__name__))
                return f(*args, **kwargs)
        return logwrapper
    
    def timestamp(self):
        return datetime.now().strftime('%Y-%m-%d_%H:%M:%S')

    def download_file(self, endpoint, file_to_write_to):
        r = requests.get(endpoint)

        if r.status_code != 200:
            self.logger.error("Attempt to download {} failed with code {}.".format(endpoint, r.status_code))
        else:   
            with open(file_to_write_to, "wb") as f:
                f.write(r.content)
