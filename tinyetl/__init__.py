from functools import wraps
import sys
import os
from datetime import datetime
import logging
import requests
import traceback

class TinyETL:
    """Manages facts about an ETL Process.

    Provides a consistent interface for storing log location,
    temporary data locations, and a way to facilitate dry-run
    and logging on Fabric-based ETL scripts.

    USAGE:
    =====

    etl = TinyETL(
        'an_etl_job',
        long_desc,
        env=env, # This `env` will be provided by Fabric. [from fabric.api import env]
        log_dir="/path/to/a/log/directory",
        tmpdata_dir="/path/to/tmpdata/directory",
        # Optionally, Create additional runtime attributes here 
        another_relevant_dir="path/to/relevant/dir"
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

    def __init__(self, name, long_desc, env, log_dir, tmpdata_dir, **kwargs):
        """
        name [str] -> Short name to ETL task. Used in creating logfile names.
        long_desc [str] -> Docstring description of this task.
        env [env object] -> The env object provided by Fabric.
        log_dir [str] -> Absolute path to the directory to store logs in.
        tmpdata_dir [str] ->  Absolute path to the directory to store temp data in.
        """
        # If there are no tasks to be run at invocation, 
        # don't bother with the rest of the object __init__
        if env.tasks == []:
            return

        self.name = name
        self.long_desc = long_desc
        self.dry_run = self._this_is_a_dry_run(env)

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

        # This allows the user to store relevant data on the
        # object they've created, without needing to anticipate
        # every possible type of value a user may want to store.
        self.__dict__.update(kwargs)

    def usage(self):
        msg = "Please provide either 'True' or 'False' to dry_run.\n"
        msg += "Usage: fab <tasks> --set dry_run=[True|False]"
        raise SystemExit(msg)

    def _this_is_a_dry_run(self, env):
        """ Determines if this is a dry run. """
        try:
            dry_run = env.dry_run
        except AttributeError:
            self.usage()

        if dry_run not in ('True', 'False'):
            self.usage()
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
                print('[DRY RUN] :: {}()'.format(f.__name__))
            else:
                current_info = "Running {}".format(f.__name__)
                print(current_info)
                self.logger.info(current_info)

                try:
                    return f(*args, **kwargs)
                except Exception as e:
                    self.logger.exception(traceback.format_exc())
                    raise Exception(e)

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

    def __str__(self):
        info = """
Standard Attributes:
===================

ETL Name: {}
Long Description: {}

Log location: {}
Temp data location: {}
        """.format(self.name, self.long_desc, self.log_dir, self.tmpdata_dir)

        standard = ('name', 'long_desc', 'log_dir', 'tmpdata_dir', 'logger', 'dry_run')

        user_defined_attrs = ""
        for k, v in self.__dict__.iteritems():
            if k not in standard:
                user_defined_attrs += "{}: {}\n".format(k.title(), v)

        if user_defined_attrs == "":
            return info 
        else:
            user_defined_attrs = "\nUser-defined Attributes:\n" + "=======================\n\n" + user_defined_attrs
            return info + user_defined_attrs

