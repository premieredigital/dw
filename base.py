import os
import sys
import logging
import time
import pytz
import datetime
import glob
from pathlib import Path
import subprocess

logger = logging.getLogger(__name__) # placeholder

BQ_PROJECT_ID = 'first-outlet-750'
BQ_DATASET_ID = 'data_warehouse'

# This requires $ bq command line tool, to download that, do:
# $ sudo snap install google-cloud-sdk
# And the first time you use it, you will need to authenticate:
# $ gcloud auth login
# And finally, to set the correct project, run
# $ gcloud set project first-outlet-750


class Warehouse:
    """
    See example.py for an example usage. The main method is called `run()`.

    This requires $ bq command line tool, to download that, do:
       
       $ sudo snap install google-cloud-sdk
       
    And the first time you use it, you will need to authenticate:

        $ gcloud auth login

    And finally, to set the correct project, run
        
        $ gcloud set project first-outlet-750

    """
    def __init__(self):

        # for internal tracking -- start time and PST start date
        self._t0 = time.time()
        self._pst_start = datetime.datetime.now(pytz.timezone('America/Los_Angeles'))
        self._base_folder = os.path.dirname(__file__)
        self._lockfile = None

        # The script name will also be the database table name
        # By default it will be the part of the filename after 'script_'.
        # For example, a file called "script_charting.py' will create a table name of `charting`.
        self.script_name = None

        # The integrity field is used to see if the script has already
        # Run for that time interval. For example, if we want the script
        # To run hourly, we can use the integrity_field: {script_name}+{date}+{hour}
        self.integrity_field = None

        # Database connection initiated in connect() method
        self.cursor = None
        self.connection = None

        # For better performance, the data should be partitioned on a
        # DATE, DATETIME, or TIMESTAMP field
        self.partition_on_field = None

        # When doing an incremental update, a field can be provided
        # And the script will grab the MAX value of that field and update
        # To only ingest rows with a field value greater than that max.
        # Common fields could be an auto-incrementing "ID" or timestamp.
        self.incremental_field = None

        # This is the SQL that will be run by the application.
        # Note that the incremental_field is added on, that will be
        # Added to the query by the script (it does not need to be included
        # in the SQL query). Additionally, if a `partition_on_field` is supplied
        # that field name must be in the results.
        # The field name will be the column name of the table, so alias any
        # fields that are not easily readable.
        self.sql = None
        self.sql_args = None
        self.schema = None
        self.sql_formatted = None # for the application, after all formatting has been done



    def error_handler(self, *args, **kwargs):
        """Placeholder error handler to be overriden as needed.
        Can raise a custom class Exception or anything else."""
        self.teardown(from_error=True)
        logger.debug('*** SCRIPT EXITED AFTER %.4fs ***' % (time.time() - self._t0))
        sys.exit(1)



    def run(self, script_name=None, integrity_field=None, sql=None, sql_args=None, partition_on_field=None, incremental_field=None):
        """This is the wrapper that calls all the methods.
        Overwrite any of these methods in the individual scripts that call this."""

        # Script setup
        self.set_script_name(script_name)
        self.set_integrity_field(integrity_field)
        self.set_logging()
        self.connect()
        if not self.check_if_should_run():
            logger.info('***EXITING SCRIPT***')
            self.error_handler()

        # Actual Script code
        logger.debug('*** STARTING SCRIPT %s AT %s PST ***' % (self.integrity_field, self._pst_start))
        self.setup(sql=sql, sql_args=sql_args, partition_on_field=partition_on_field)
        self.create_table(sql=self.sql_formatted, partition_on_field=partition_on_field)
        self.run_query(sql=self.sql_formatted, incremental_field=incremental_field)
        self.save_to_bq(partition_on_field)
        self.teardown()
        logger.debug('*** FINISHED SCRIPT -- Took %.4fs to complete ***' % (time.time() - self._t0))



    def set_script_name(self, name=None):
        """This will set both the script name and table name. By default, filename of script."""
        if name: self.script_name = name; return
        script_name = [arg for arg in sys.argv if arg.endswith('.py')]
        if not script_name: raise RuntimeError('Set a custom `script_name` method or run with a file arg')
        self.script_name = script_name[0].split('_')[1].replace('.py','')



    def set_integrity_field(self, integrity_field=None):
        """Set a custom IntegrityField, if not set, will default to {script_name}_{date}."""
        if integrity_field: self.integrity_field = integrity_field; return
        script_name = self.script_name
        start_date = self._pst_start.date()
        self.integrity_field = '%s_%s' % (script_name, start_date)



    def set_logging(self, *args, **kwargs):
        """Set up how the code should be logged. By default will log/print to stdout
        and to a log file in var/$script_name.log"""
        global logger;
        logger = logging.getLogger(self.script_name)
        logger.setLevel(logging.DEBUG)

        # Stream handler -- act like print function
        stream_handler = logging.StreamHandler()
        stream_handler.formatter = logging.Formatter(
            f'[{self.integrity_field}:pid:%(process)d' + ' %(asctime)s' + 
            '] - %(levelname)s %(name)s:%(funcName)s:L%(lineno)s: %(message)s'
        )
        logger.addHandler(stream_handler)

        # File handler
        file_handler = logging.FileHandler(os.path.join(self._base_folder, 'var', '%s.log' % self.script_name))
        file_handler.formatter = stream_handler.formatter
        logger.addHandler(file_handler)
        logger.debug('hello')



    def connect(self, *args, **kwargs):
        import pymysql
        conn = pymysql.connect(user=os.environ['DB_USER'], password=os.environ['DB_PASS'], database=os.environ['DB_NAME'], host=os.environ['DB_HOST'], charset='utf8')
        self.conn = conn
        self.cursor = conn.cursor()



    def check_if_should_run(self, force_run=False):
        """By default, we will run if the $integrity_id isn't in the `proc` folder.
        If 'force' is in sys.argv or force_run=True or force_run=True"""
        force_run = (force_run is True) or ('force' in sys.argv)
        fp_in_proc = [path for path in glob.glob(os.path.join(self._base_folder, 'proc/*/*'))
                        if self.integrity_field in path]
        # if not found, run
        if (not fp_in_proc):
            logger.info('No previous process found, running new script.')
            return True
        else:
            logger.info('Previous %s process found...' % ('running' if 'running' in fp_in_proc else 'completed'))
            if (not force_run):
                logger.info('Exiting script as `force` was not applied.')
                return False
            else:
                logger.info('`Force` applied, so running script anyways')
                # if `force` is applied and we have a previously-running process, let's kill it
                fp_in_proc = fp_in_proc[0]
                previous_pid = int(os.path.basename(fp_in_proc).split('_')[0]) if 'running' in fp_in_proc else None
                if previous_pid:
                    try:
                        os.kill(previous_pid, 9)
                    except ProcessLookupError:
                        pass
                    os.remove(fp_in_proc)
                return True



    def setup(self, sql, sql_args=None, partition_on_field=None):
        """Any code that needs to run before the actual script starts, for example
        if we want to save an entry to our DB saying the script it running.
        By default we will just save a `running` pid file.
        """
        # Create a lock-file: could also use https://linux.die.net/man/1/lockfile, but this is easy enough
        self._lockfile = '%s/proc/running/%s_%s.lock' % (self._base_folder, os.getpid(), self.integrity_field)
        logger.debug('Creating empty process file %s' % self._lockfile)
        Path(self._lockfile).touch()
        
        # Pre-format the SQL if there are any args:
        if sql and (not sql_args):
            self.formatted_sql = sql
        else:
            # https://pymysql.readthedocs.io/en/latest/modules/cursors.html#pymysql.cursors.Cursor.mogrify
            logger.debug('Formatting SQL with args')
            self.formatted_sql = self.cursor.mogrify(sql, sql_args)

        # Build the schema
        # These are the current Type mappings to the BQ type.
        # The BQ types are listed here: https://cloud.google.com/bigquery/docs/schemas#cli
        # The MySQL types are listed here: https://github.com/mysql/mysql-server/blob/4869291f7ee258e136ef03f5a50135fe7329ffb9/router/src/mock_server/src/mysql_protocol_common.h
        # Please add in additional types, as needed
        TYPE_CODE_TO_BQ_TYPE = {
            253: 'STRING',
            252: 'STRING',
            2:   'INT64',
            3:   'INT64',
            1:   'INT64',
            246: 'NUMERIC',
            4:   'NUMERIC',
            10:  'DATE',
            12:  'DATETIME',
            7:   'DATETIME',
            11:  'TIME',
        }
        logger.debug('Building SQL schema')
        self.cursor.execute('SELECT * FROM (%s) _tbl LIMIT 0' % (sql,))
        SCHEMA = ','.join(['%s:%s' % (item[0], TYPE_CODE_TO_BQ_TYPE[item[1]]) for item in self.cursor.description])
        self.schema = SCHEMA



    def create_table(self, sql, partition_on_field=None):
        """If the BQ table doesn't already exist, create it."""
        existing_datasets = subprocess.check_output('bq ls %s' % BQ_DATASET_ID, shell=True) # in bytes
        TABLE_NAME = self.script_name
        if TABLE_NAME.encode() in (existing_datasets.split()):
            logger.debug('BQ table %s already exists' % TABLE_NAME)
        else:
            logger.info('BQ table %s does not exist, creating new table.' % TABLE_NAME)
            partition_arg = '' if not partition_on_field else '--time_partitioning_field=%s' % partition_on_field
            cmd = subprocess.run(
                    f'bq mk --table {partition_arg} {BQ_DATASET_ID}.{TABLE_NAME} {self.schema}', 
                    shell=True, 
                    stdout=subprocess.PIPE,
            )
            if cmd.returncode == 0:
                logger.info('Created SQL table')
            else:
                logger.info('Failed creating SQL table. Raising error handler.')
                self.error_handler()



    def run_query(self, sql, incremental_field=None):
        """This will run the DB query and save that to the BQ table.
        It will also tack-on the incremental WHERE clause, if needed."""
        #return # for debugging!!!
        TABLE_NAME = self.script_name
        
        # if an incremental field has been supplied, query to get the max value and add in a WHERE clause to the query
        if incremental_field is not None:
            cmd = subprocess.run("bq query --use_legacy_sql=False --format=csv 'SELECT MAX(%s) FROM `%s.%s`'" % ( 
                incremental_field, BQ_DATASET_ID, TABLE_NAME), 
                shell=True, stdout=subprocess.PIPE
            )
            incremental_field_max_value = cmd.stdout.decode().strip().split('\n')[1].replace('""','') or None
            if incremental_field_max_value is not None:
                sql = """SELECT * FROM (%s) _tmp WHERE %s > '%s'""" % (sql, incremental_field, incremental_field_max_value)
        
        # Run the query against our own db and save it to a local csv file in data/ (we use sed for some csv tab-escaping)
        # Note: this is pretty crude if we have a lot of bad string/varchar fields, should probably use a proper csv parser.
        logger.info('Running SQL query...\n%s\n' % sql)
        filepath = os.path.join(os.path.dirname(__file__), 'data', '%s.csv' % self.integrity_field)
        logger.debug('Saving SQL results to file %s' % filepath)
        cmd = subprocess.run('''mysql -b -u %s -h %s -p%s %s -e "%s" | sed -e 's/"/'"'/g" -e "s/''//g" > %s''' % (
               os.environ['DB_USER'], os.environ['DB_HOST'], os.environ['DB_PASS'], os.environ['DB_NAME'], self.formatted_sql, filepath),
               shell=True,
        )
        if cmd.returncode == 0:
            logger.info('Saved SQL results to csv file %s' %  filepath)
        else:
            logger.info('Saving SQL results failed. Raising error handler.')
            self.error_handler()



    def save_to_bq(self, partition_on_field=None, csv_filepath=None):
        """Read the local csv file and save to BQ"""
        
        TABLE_NAME = self.script_name
        csv_filepath = csv_filepath or os.path.join(os.path.dirname(__file__), 'data', '%s.csv' % self.integrity_field)

        logger.info('Saving CSV file to BigQuery...')
        partition_arg = '' if not partition_on_field else '--time_partitioning_field=%s' % partition_on_field
        cmd = subprocess.run(f'''bq load --null_marker="NULL" --field_delimiter=tab --skip_leading_rows=1 --max_bad_records=5 ''' +
                             f'''--source_format=CSV {partition_arg} {BQ_DATASET_ID}.{TABLE_NAME} {csv_filepath} {self.schema}'''
                             , shell=True)
        if cmd.returncode == 0:
            logger.info('Saved SQL results to BQ')
        else:
            logger.info('Failed BQ command:\n%s' % cmd.args)
            logger.info('Saving SQL results to BQ failed. Raising error handler.')
            self.error_handler()



    def teardown(self, from_error=False):
        """What to do at the end of the script, for example, to send
        an email or remove one of the data or process files."""
        # Remove data file?
        pass # keep for now -- easier for debugging to start

        # Remove running pid file?
        if self._lockfile: os.remove(self._lockfile)

        # Add a completed file?
        if not from_error:
            completed_filepath = f'{self._base_folder}/proc/completed/{self.integrity_field}'
            Path(f'{self._base_folder}/proc/completed/{self.integrity_field}').touch()

        # Send an email or post to an endpoint?
        pass





