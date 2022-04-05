#!/usr/bin/python
# -*- coding:utf-8 -*-
"""
Subject: Common Classes
Date:   Sept 13, 2021
Version: V2.0
Description: Common Classes, functions for Python ETL jobs
"""

import sys
import os
import io
import datetime as dt
import time as t
import pandas as pd
import pandas_gbq as pgbq
import threading
import inspect
import numpy as np
import zipfile
import logging
import logging.config
import logging.handlers

import pysftp
import requests
from pymongo import MongoClient

pd.set_option("display.max_columns", 1000)  # show all columns
pd.set_option("display.max_colwidth", 1000)  # show each column entirely
pd.set_option("display.max_rows", 300)  # show all rows

import sqlalchemy

from sshtunnel import SSHTunnelForwarder
import configparser
import json
from google.oauth2 import service_account
import google.cloud.bigquery as bq
import google.cloud.storage as gcs
import boto3
from dateutil import tz

# initial the environment
project_path = os.path.abspath("..")
root_path = os.path.abspath("../..")
config = configparser.ConfigParser()
config.read("/".join([project_path, "conf", "CommonModules.conf"]))

config_project = configparser.ConfigParser()
config_project.read("/".join([project_path, "conf", "project.conf"]))

slack_token = config_project.get('DEFAULT', 'SLACK_TOKEN')
slack_channel = config_project.get('DEFAULT', 'SLACK_CHANNEL')

fiscal_calendar_all = config.get('DEFAULT', 'FISCAL_CALENDAR_ALL')
fiscal_calendar = config.get('DEFAULT', 'FISCAL_CALENDAR')

# initial the logging function
## get job name - the name of the main python module
stack_main = inspect.stack()[6][1]
jobname = os.path.basename(stack_main)
log_datetime = dt.datetime.strftime(dt.datetime.now(), '%Y%m%d-%H%M%S')
log_file = '/'.join([project_path, 'logs', jobname.replace('.py', f'_{log_datetime}.log')])

## create logging message format and datetime format
formatter = logging.Formatter(fmt='%(asctime)s [%(levelname)s] : <%(module)s.%(funcName)s> : %(message)s',
                              datefmt='%Y-%m-%d %H:%M:%S')
## create logger
logger = logging.getLogger(jobname)
logger.setLevel(logging.DEBUG)

## create console handler and set level to debug
ch = logging.StreamHandler()
ch.setLevel(logging.DEBUG)
ch.setFormatter(formatter)
logger.addHandler(ch)

## create file handler and set level to debug
fh = logging.FileHandler(log_file, mode='w', encoding=None, delay=False)
fh.setLevel(logging.DEBUG)
fh.setFormatter(formatter)
logger.addHandler(fh)

gcp_cred_file = '/'.join([root_path, '.ssh', config.get('.ssh', 'GCP_TOKEN')])
gcp_cred = service_account.Credentials.from_service_account_file(gcp_cred_file)


def retry3(func):
    """
    decorate functions, retry function if failed for 3 times
    :param func: function to execute and retry
    :return: return value of the function, False if failed all 3 times
    """

    def inner(*args, **kwargs):
        # print("decorate functions!")
        count, max = 0, 3
        while True:
            if count >= max:
                break

            res = func(*args, **kwargs)
            try:
                if res:
                    break
                else:
                    count += 1
                    logger.warning(f"operation failed {count} time(s), retry...")
            except:
                if not res.empty:
                    break
                else:
                    count += 1
                    logger.warning(f"operation failed {count} time(s), retry...")

        return res

    return inner


def get_bum(project_id='data-cube-migration', dataset='Integration', tablename='bum'):
    """
    query store_id and brand short name from Bigquery integration Bum table
    :return: Dataframe
    """
    query = f"""SELECT SourceLocationID as locationId, BrandShortName as brand 
                FROM `{project_id}.{dataset}.{tablename}`"""
    df = pgbq.read_gbq(query, project_id=project_id, credentials=gcp_cred)

    return df


def get_bum_store_timezone(project_id='data-cube-migration', dataset='Integration', tablename='bum'):
    """
    query store_id and timwzone from Bigquery integration Bum table
    :return: Dataframe
    """
    query = f"""SELECT SourceLocationID as store_number, TimeZone as tz_short, TimeZone2 as tz 
                FROM `{project_id}.{dataset}.{tablename}`"""
    df = pgbq.read_gbq(query, project_id=project_id, credentials=gcp_cred)

    return df


def zipcsvfile(fullfilename, zipfilename):
    """compress data file to zip
    fullfilename: original data file name
    zipfilename: filename of the zip file
    return: True/False"""
    logger.info(f"compressing {fullfilename} to zip...")
    try:
        mode = zipfile.ZIP_DEFLATED
        zf = zipfile.ZipFile(zipfilename, 'w', mode)
        zf.write(fullfilename, os.path.basename(fullfilename))
        zf.close()
        return True
    except Exception as e:
        logger.exception(str(e))
        return False


def remove_local_file(filename):
    """
    remove files from os
    """
    logger.info(f'removeing file: {filename}')
    try:
        os.remove(filename)
        return True
    except Exception as e:
        logger.exception(str(e))
        return False


class CustomDateTime:
    """
    get date time objects:
    1. today
    2. yesterday
    3. past or future date by interval days
    3. start and end date from interval days
    4. start and end time from interval hour/minutes
    5. number of days from 2 date
    6. convert betweem epoch and normal datetime

    default timezone: Eastern Time

    """

    @staticmethod
    def today(fmt='%Y-%m-%d'):
        """get today's date in date and string format 'YYYY-MM-DD' """
        t = dt.datetime.today()
        return t, dt.datetime.strftime(t, fmt)

    @staticmethod
    def yesterday(fmt='%Y-%m-%d'):
        """get yesterday's date in date and string format 'YYYY-MM-DD' """
        t = dt.datetime.today() - dt.timedelta(days=1)
        return t, dt.datetime.strftime(t, fmt)

    @staticmethod
    def date_by_interval_days(number_of_days, fmt='%Y-%m-%d'):
        """get a date by the interval days parameter. return value in date and string format 'YYYY-MM-DD' """
        t = dt.datetime.now().date() + dt.timedelta(days=number_of_days)
        return t, dt.datetime.strftime(t, fmt)

    @staticmethod
    def rang_of_date(days_to_start, rollback_days_from_start, fmt='%Y-%m-%d'):

        last_date = dt.datetime.now().date() + dt.timedelta(days=days_to_start)
        first_date = last_date - dt.timedelta(days=rollback_days_from_start)

        return dict(lastdate=last_date, str_lastdate=dt.datetime.strftime(last_date, fmt),
                    firstdate=first_date, str_firstdate=dt.datetime.strftime(first_date, fmt))

    @staticmethod
    def number_of_days(first_date, last_date, fmt='%Y-%m-%d'):
        """get a interval of 2 date, date variables can be date or string, retrun integer """
        if type(first_date).__name__ == "str":
            first_date = dt.datetime.strptime(first_date, fmt)
        if type(last_date).__name__ == "str":
            last_date = dt.datetime.strptime(last_date, fmt)

        ndays = last_date - first_date

        return ndays.days

    @staticmethod
    def date_list_by_range(first_date, last_date, fmt='%Y-%m-%d'):
        """
        get a list of date by given date range,
        return in datetime and string format
        """
        if type(first_date).__name__ == "str":
            first_date = dt.datetime.strptime(first_date, fmt)
        if type(last_date).__name__ == "str":
            last_date = dt.datetime.strptime(last_date, fmt)

        if first_date == last_date:
            return [first_date], [first_date.strftime(fmt)]

        t = first_date
        dtl, strl = [t], [t.strftime(fmt)]
        while True:
            t = t + dt.timedelta(days=1)
            dtl.append(t)
            strl.append(t.strftime(fmt))
            if t == last_date:
                break

        return dtl, strl

    @staticmethod
    def epoch_and_datetime(epoch_or_datetime_value, mode=1):
        """
        convert epoch to datetime or datetime to epoch
        :param epoch_or_datetime_value: epoch (10 or 13 digits integer) or datetime (YYYY-MM-DD <HH:MM:SS>) value
        :param mode: mode=1: convert from epoch to datetime; mode=2: datetime to epoxh
        :return: datetime value if mode=2; epoch value if mode=1; false of error
        """
        if mode == 1:
            try:
                if len(str(epoch_or_datetime_value)) == 10:
                    return dt.datetime.fromtimestamp(epoch_or_datetime_value)
                elif len(str(epoch_or_datetime_value)) == 13:
                    return dt.datetime.fromtimestamp(epoch_or_datetime_value / 1000)
            except Exception as e:
                logger.exception(str(e))
                return False
        else:
            try:
                if type(epoch_or_datetime_value).__name__ == 'datetime':
                    return int(epoch_or_datetime_value.timestamp())
                elif type(epoch_or_datetime_value).__name__ == 'date':
                    cdt = dt.datetime.strptime(epoch_or_datetime_value.strftime('%Y%m%d'), '%Y%m%d')
                    return int(cdt.timestamp())
                elif type(epoch_or_datetime_value).__name__ == 'str':
                    try:
                        return int(dt.datetime.strptime(epoch_or_datetime_value, '%Y-%m-%d').timestamp())
                    except:
                        return int(dt.datetime.strptime(epoch_or_datetime_value, '%Y-%m-%d %H:%M:%S').timestamp())
            except Exception as e:
                logger.exception(str(e))
                return False

    @staticmethod
    @retry3
    def get_fiscal_calendar_raw(p_date='all'):
        """
        get fiscal calendar
        :param p_date: calendar date to lookup or all(optional)
        :return: fiscal calendar (year week period and quarter) of the given value,
                    5 years fiscal calendar if requests all
                    data in dict and dataframe format
        """
        try:
            if p_date == 'all':
                response = requests.get(fiscal_calendar_all)
                fcal = response.json()["data"]['fiscalCalendar']
            else:
                response = requests.get(fiscal_calendar.format(p_date))
                fcal = response.json()["data"]['fiscalCalendarDate']
        except Exception as e:
            logger.error(str(e))
            return False

        return fcal, pd.DataFrame.from_dict(fcal)

    @staticmethod
    @retry3
    def get_fiscal_calendar_range(start_date, end_date):
        """
        get calender from RDS table
        """
        try:
            date_response = requests.get(fiscal_calendar_all)
        except Exception as e:
            logger.error(str(e))
            return False

        tmpdate, calendar = start_date, []
        while tmpdate <= end_date:
            dayobjs = list(filter(lambda x: x['weekStart'][0:10] <= tmpdate <= x['weekEnd'][0:10],
                                  date_response.json()['data']['fiscalCalendar']))
            dayobj, doc = dayobjs[0], {}

            doc['business_date'] = str(dt.datetime.strptime(tmpdate, '%Y-%m-%d').date())
            doc['fiscal_year'] = dayobj['year']
            doc['fiscal_week'] = dayobj['week']
            doc['fiscal_quarter'] = dayobj['quarter']
            doc['fiscal_period'] = dayobj['period']

            calendar.append(doc)
            tmpdate = dt.datetime.strftime(dt.datetime.strptime(tmpdate, '%Y-%m-%d') + dt.timedelta(days=1), '%Y-%m-%d')

        df_calendar = pd.DataFrame(calendar)
        return df_calendar

    @staticmethod
    def convert_timezone(ts, new_timezone, old_timezone='UTC'):
        """convet timestamp by timezone"""
        if ts is None or ts == '':
            return None

        if new_timezone is None or new_timezone == '':
            new_timezone = old_timezone

        # METHOD 1: Hardcode zones:
        from_zone = tz.gettz(old_timezone)
        to_zone = tz.gettz(new_timezone)

        try:
            if type(ts).__name__ == 'str':
                ts = ts + " " + '00:00:00'
                old_ts = dt.datetime.strptime(ts[0:19], '%Y-%m-%d %H:%M:%S')
            else:
                old_ts = ts
            # Tell the datetime object that it's in UTC time zone since
            # datetime objects are 'naive' by default
            old_ts = old_ts.replace(tzinfo=from_zone)
            # Convert time zone
            new_ts = old_ts.astimezone(to_zone)

        except Exception as e:
            logger.exception(str(e))
            return False

        return new_ts.replace(tzinfo=None)


class ConnectionBuilder:
    """
    a group of static methods to build standard connection to data sources
    db connection type:
    1. connection: sftp, constructor: sftp_conn()
    2. connection: mongoDB, constructor: mongodb_conn()
    3. connection: mysql, constructor: mysql_conn()
    4. connection: sqlserver, constructor: sqlserver_conn()

    """

    def __init__(self, connctiontype=""):
        pass

    @staticmethod
    @retry3
    def sftp_conn(conf_section):
        """
        :param conf_section: CommonModules.conf section name
        :return: pysftp.Connection
        """

        logger.debug(f"connecting to sftp {conf_section} ......")
        cnopts = pysftp.CnOpts()
        cnopts.hostkeys = None
        arg = config[conf_section]
        ftp_URL = arg['HOST']
        username = os.environ[arg['USERNAME']]
        password = os.environ[arg['PASSWORD']]
        try:
            sftp = pysftp.Connection(ftp_URL, username=username, password=password, cnopts=cnopts)
            logger.debug(f"connected to sftp {conf_section}")
            return sftp
        except Exception as e:
            logger.exception(str(e))
            return False

    @staticmethod
    @retry3
    def ssh_tunnel(conf_section):
        """
        :param conf_section: CommonModules.conf section name
        :return: sshtunnel.SSHTunnelForwarder
        """
        logger.debug(f"connecting to ssh tunneral: {conf_section} ......")
        arg = config[conf_section]
        try:
            ssh_username = os.environ[arg['SSHUSERNAME']]
            ssh_host = arg['SSHTUNNEL']
            ssh_pkey = "/".join([root_path, ".ssh", arg['SSHKEY']])
            db_host = arg['HOST']
            port = arg.get('PORT', '3306')
            server = SSHTunnelForwarder((ssh_host, 22), ssh_username=ssh_username, ssh_pkey=ssh_pkey,
                                        remote_bind_address=(db_host, int(port)))
            logger.debug(f"connected to ssh tunneral: {conf_section}")
            return server
        except Exception as e:
            logger.exception(str(e))
            return False

    @staticmethod
    @retry3
    def mysql_conn(conf_section, locat_port=0):
        """
        :param conf_section: CommonModules.conf section name
        :param locat_port: ssh local port if connection type is ssh tunnel
        :return: sqlalchemy.engine.base.Engine
        """
        logger.debug(f"connecting to MySQL: {conf_section} ......")
        arg = config[conf_section]
        try:
            server = arg['HOST']
            database = arg['DATABASE']
            user = os.environ[arg['USERNAME']]
            password = os.environ[arg['PASSWORD']]
            conn_str = "##Undefined"
            if arg.get('TYPE', 'flat') == "flat":
                port = int(arg.get('PORT', '3306'))
                conn_str = f"mysql+pymysql://{user}:{password}@{server}:{port}/{database}"
            elif arg.get('TYPE', 'flat') == "ssh":
                conn_str = f"mysql+pymysql://{user}:{password}@127.0.0.1:{locat_port}/{database}"
            engine = sqlalchemy.create_engine(conn_str, encoding='latin1', echo=False,
                                              connect_args={'connect_timeout': 900})
            c = engine.connect()
            c.close()
            logger.debug(f"connected to MySQL: {conf_section}")
            return engine
        except Exception as e:
            logger.exception(str(e))
            return False

    @staticmethod
    @retry3
    def sqlserver_conn(conf_section):
        """
        :param conf_section: CommonModules.conf section name
        :return: sqlalchemy.engine.base.Engine
        """
        logger.debug(f"connecting to SQL Server: {conf_section} ......")
        arg = config[conf_section]
        try:
            server = arg['HOST']
            database = arg['DATABASE']
            username = os.environ[arg['USERNAME']]
            password = os.environ[arg['PASSWORD']]
            port = arg['PORT']

            conn_str = f'mssql+pyodbc://{username}:{password}@{server}:{port}/{database}?driver=FreeTDS'
            engine = sqlalchemy.create_engine(conn_str, encoding='latin1', echo=False,
                                              connect_args={'connect_timeout': 900})
            c = engine.connect()
            c.close()
            logger.debug(f"connected to SQL Server: {conf_section}")
            return engine
        except Exception as e:
            logger.exception(str(e))
            return False

    @staticmethod
    @retry3
    def mongodb_conn(conf_section):
        """
        :param conf_section: CommonModules.conf section name
        :return: pymongo.mongo_client.MongoClient
        :connect to database: db = client[database name]
        """
        logger.debug(f"connecting to MongoDB: {conf_section} ......")
        arg = config[conf_section]
        try:
            server = arg['HOST']
            database = arg['DATABASE']
            username = os.environ[arg['USERNAME']]
            password = os.environ[arg['PASSWORD']]

            conn_str = f'mongodb+srv://{username}:{password}@{server}/{database}?retryWrites=true&w=majority'
            client = MongoClient(conn_str)
            logger.debug(f"connected to MongoDB: {conf_section}")
            return client
        except Exception as e:
            logger.exception(str(e))
            return False


class queryOperations:
    """
    Data operations by given queries

    """

    def __init__(self):
        pass

    @staticmethod
    @retry3
    def read_from_sql(query_text, db_engine, msg="Querying SQl DataBase ......"):
        """
        ready data from SQL databases with the SQL query
        :param query_text: SQL query
        :param db_engine: SQL Alchemy data engion, generated by the ConnectionBuilder functions
        :param msg: (Optional)
        :return: pandas dataframe
        """
        logger.info(msg)
        try:
            result = pd.read_sql_query(query_text, con=db_engine)
            return result
        except Exception as e:
            logger.exception(str(e))
            return False

    @staticmethod
    @retry3
    def read_from_mongodb(client, database, collection, filter={}, project={},
                          sort={'_id': 1}, limit=0, msg="Querying MongoDB ......."):
        """
        Read data from MongoDB by given criteria
        :param client: MongoDB client, crated by ConnectionBuilder.mongodb_conn
        :param database: database name
        :param collection: collection name
        :param filter: Optional. The query predicate. If unspecified, then all documents in the collection will match the predicate.
        :param project: Optional. The projection specification to determine which fields to include in the returned documents. See Projection and Projection Operators.
        :param sort: Optional. The sort specification for the ordering of the results.
        :param limit: Optional. The maximum number of documents to return. If unspecified, then defaults to no limit. A limit of 0 is equivalent to setting no limit.
        :param msg: (optional) message to show when executing
        :return: a list of JSON documents
        """
        logger.info(msg)
        sort_list = [(k, v) for k, v in sort.items()]
        try:
            conn = client[database]
            result = conn[collection].find(filter, project, no_cursor_timeout=True).sort(sort_list).limit(limit)
            return list(result)
        except Exception as e:
            logger.exception(str(e))
            return False

    @staticmethod
    @retry3
    def readOne_from_mongodb(client, database, collection, filter={}, project={}, msg="Querying MongoDB ......."):
        """
        Read data from MongoDB by given criteria
        :param client: MongoDB client, crated by ConnectionBuilder.mongodb_conn
        :param database: database name
        :param collection: collection name
        :param filter: Optional. The query predicate. If unspecified, then all documents in the collection will match the predicate.
        :param project: Optional. The projection specification to determine which fields to include in the returned documents. See Projection and Projection Operators.
        :param msg: (optional) message to show when executing
        :return: a list of JSON documents
        """
        print(msg, end='\r')
        try:
            conn = client[database]
            result = conn[collection].find_one(filter, project, no_cursor_timeout=True)
            return list(result)
        except Exception as e:
            logger.exception(str(e))
            return False

    @staticmethod
    @retry3
    def update_mongodb(client, database, collection, filter={}, set_query={}, unset_query={},
                       msg="Updating MongoDB ......."):
        """
        Update (many) mongoDB
        :param client: MongoDB client, crated by ConnectionBuilder.mongodb_conn
        :param database: database name
        :param collection: collection name
        :param filter: Optional. The query predicate. If unspecified, then all documents in the collection will match the predicate.
        :param set_query: query to update fields
        :param unset_query: query to remove fields
        :param msg: (optional) message to show when executing
        :return: a list of JSON documents
        """
        logger.info(msg)
        try:
            conn = client[database]
            result = conn[collection].update_many(filter, {'$set': set_query, '$unset': unset_query})
            return result
        except Exception as e:
            logger.exception(str(e))
            return False

    @staticmethod
    @retry3
    def updateOne_mongodb(client, database, collection, filter={}, set_query={}, unset_query={},
                          msg="Updating MongoDB ......."):
        """
        Update (many) mongoDB
        :param client: MongoDB client, crated by ConnectionBuilder.mongodb_conn
        :param database: database name
        :param collection: collection name
        :param filter: Optional. The query predicate. If unspecified, then all documents in the collection will match the predicate.
        :param set_query: query to update fields
        :param unset_query: query to remove fields
        :param msg: (optional) message to show when executing
        :return: a list of JSON documents
        """
        if filter is None:
            filter = {}
        logger.info(msg)
        try:
            conn = client[database]
            result = conn[collection].update_one(filter, {'$set': set_query, '$unset': unset_query})
            return result
        except Exception as e:
            logger.exception(str(e))
            return False

    @staticmethod
    @retry3
    def insert_mongodb(client, database, collection, bdocs, msg="Updating MongoDB ......."):
        """
        Update (many) mongoDB
        :param client: MongoDB client, crated by ConnectionBuilder.mongodb_conn
        :param database: database name
        :param collection: collection name
        :param filter: Optional. The query predicate. If unspecified, then all documents in the collection will match the predicate.
        :param set_query: query to update fields
        :param unset_query: query to remove fields
        :param msg: (optional) message to show when executing
        :return: a list of JSON documents
        """
        print(msg, end='\r')
        try:
            conn = client[database]
            result = conn[collection].insert_many(bdocs)
            return result
        except Exception as e:
            logger.exception(str(e))
            return False


class LoggingToSlack:

    @staticmethod
    def slackapp(text, token="SLACK_BOT_TOKEN", channel='#gcp_notification', jobname='', switch=True):

        def slackmsg_ignore(text):
            try:
                file = '/'.join([project_path, 'conf', 'slack_skip_msg.txt'])
                with open(file, "r") as f:
                    filters = f.readlines()
            except:
                return False

            for ft in filters:
                found = True
                for f in ft.split(","):
                    if f.replace('\n', '') not in text:
                        found = False
                        break
                if found:
                    return found

            return False

        # jobname = inspect.stack()[9][1]
        if not switch:
            return False

        from slack_sdk import WebClient
        from slack_sdk.errors import SlackRequestError

        if slackmsg_ignore(text):
            return True

        try:
            client = WebClient(token=os.environ[token])
            # format the messager with header (job name) and block quotes
            text = f"*`[{jobname}]`*\n>" + text.replace("\n", "\n>")
            response = client.chat_postMessage(channel=channel, text=text)
            assert response["message"]["text"] == text
        except SlackRequestError as e:
            # You will get a SlackApiError if "ok" is False
            assert e.response["ok"] is False
            assert e.response["error"]  # str like 'invalid_auth', 'channel_not_found'
            print(f"Got an error: {e.response['error']}")
        finally:
            return response['ok']

    @staticmethod
    def slack_logging(token, channel, jobname=''):
        """
        send error, warning and cretical message to SLACK channel
        :param token: slack access token
        :param channel: slack channel name
        :return: True when finished
        """

        def fetch_error(str):
            if 'ERROR' in str or 'WARNING' in str or 'CRITICAL' in str:
                return True
            else:
                return False

        with open(log_file, 'r') as lf:
            lines = lf.readlines()
            logs = list(filter(lambda x: fetch_error(x), lines))
            if len(logs) > 0:
                logtext = ''.join(logs)
                LoggingToSlack.slackapp(logtext, token, channel, jobname=jobname, switch=True)

        return True


class GCPOperations:
    """GCP operations
    1. get Bigquery/Storage clients
    2. reformat dataframe by BQ schema
    3. create and upload csv file to gcp storge
    4. load csv data file from storage to bigquery"""

    @staticmethod
    @retry3
    def get_gcp_client(c, credential=gcp_cred_file):
        """get google cloud client"""
        try:
            if c == "storage":
                return gcs.Client.from_service_account_json(credential)
            elif c == "bigquery":
                return bq.Client.from_service_account_json(credential)
            else:
                raise Exception("""Not a valid client type. options:
                1. storage
                2. bigquery""")
        except Exception as e:
            logger.exception(str(e))
            return False

    @staticmethod
    @retry3
    def create_csv_to_gcs(df, filename, project_path, credential, bucket_name='data-cube-migration', folder='',
                          delimiter=',', quotechar='"'):
        """
        store queried data in csv file and upload to gcp storage if the BQ uploading job is failed
        df: dataframe
        filename: csv filename
        """
        local_file = "/".join([project_path, "data-out", filename])
        df.to_csv(local_file, index=False, delimiter=delimiter, quotechar=quotechar)

        try:
            storage_client = gcs.Client.from_service_account_json(credential)
            # Upload the local file to Cloud Storage.
            bucket = storage_client.get_bucket(bucket_name)
            blob = bucket.blob(folder + filename)

            blob.upload_from_filename(local_file)
            logger.info(f"uploaded to Google Storage bucket: {filename}")
            remove_local_file(local_file)
            return True

        except Exception as e:
            logger.exception(str(e))
            return False

    @staticmethod
    def get_gbq_schema(full_file_path):
        """load schema from json file"""
        with open(full_file_path, "r") as f:
            j = f.read()

        return json.loads(j)

    @staticmethod
    def reformat_data_by_type(df_source, tbl_schma):
        """
        reformat values in dataframe according the column data type
        :param df_source:
        :param tbl_schma: BQ table schema
        :return:
        """
        for col in tbl_schma:
            try:
                if col["type"] == "INTEGER":
                    df_source[col["name"]] = pd.to_numeric(df_source[col["name"]], errors='coerce')
                    df_source[col["name"]] = df_source[col["name"]].astype('Int64')
                elif col["type"] == "STRING":
                    df_source[col["name"]] = df_source[col["name"]].apply(lambda x: x.encode('utf8').decode())
                        df_source[col["name"]] = df_source[col["name"]].apply(lambda x: x.strip())
                elif col["type"] == "TIMESTAMP":
                    df_source[col["name"]] = df_source[col["name"]].apply(lambda x: x.strftime('%Y-%m-%d %H:%M:%S'))
            except Exception as e:
                # logger.exception(str(e))
                pass

        return df_source

    @staticmethod
    @retry3
    def get_table_schema(project, dataset, tablename):
        """
        Description: get table schema by given project id, dataset and table name

        """
        # Construct a BigQuery client object.
        client = bq.Client(project, gcp_cred)

        # Set table_id to the ID of the model to fetch.
        table_id = f'{project}.{dataset}.{tablename}'

        table = client.get_table(table_id)  # Make an API request.

        # View table properties
        logger.info("Got table '{}.{}.{}'.".format(table.project, table.dataset_id, table.table_id))
        logger.info("Table description: {}".format(table.description))
        logger.info("Table has {} rows".format(table.num_rows))

        return table.schema

    @staticmethod
    @retry3
    def create_table(project, dataset, schema, tablename,
                     partition_by=None, partition_type=None, partition_field=None, partition_range=None,
                     cluster_field=None):
        try:
            # Construct a BigQuery client object.
            client = bq.Client(project, gcp_cred)

            # Set table_id to the ID of the table to create.
            table_id = f'{project}.{dataset}.{tablename}'

            table = bq.Table(table_id, schema=schema)
            if cluster_field is not None:
                table.clustering_fields = [cluster_field]

            if partition_by == 'time-unit':
                if partition_type is not None and partition_type == 'day':
                    table.time_partitioning = bq.TimePartitioning(
                        type_=bq.TimePartitioningType.DAY,
                        field=partition_field  # name of column to use for partitioning
                    )
                elif partition_type is not None and partition_type == 'month':
                    table.time_partitioning = bq.TimePartitioning(
                        type_=bq.TimePartitioningType.MONTH,
                        field=partition_field  # name of column to use for partitioning
                    )
                elif partition_type is not None and partition_type == 'year':
                    table.time_partitioning = bq.TimePartitioning(
                        type_=bq.TimePartitioningType.YEAR,
                        field=partition_field  # name of column to use for partitioning
                    )
            elif partition_by == 'ingestion-time':
                pass
            elif partition_by == 'integer-range':
                table.range_partitioning = bq.RangePartitioning(
                    # To use integer range partitioning, select a top-level REQUIRED /
                    # NULLABLE column with INTEGER / INT64 data type.
                    field=partition_field,
                    range_=bq.PartitionRange(start=partition_range['start'],
                                             end=partition_range['end'],
                                             interval=partition_range['interval'])
                )
            elif partition_by is None:
                pass

            table = client.create_table(table, exists_ok=True)  # Make an API request.
            logger.info(
                "Created partitioned and clustered table {}.{}.{}".format(
                    table.project, table.dataset_id, table.table_id
                )
            )
            return True
        except Exception as e:
            logger.exception(str(e))
            return False

    @staticmethod
    def export_to_gcs(project, dataset, tablename, bucket, filename):

        logger.info(f'exporting data from : {project}:{dataset}.{tablename} ......')
        try:
            # from google.cloud import bigquery
            client = bq.Client(project, gcp_cred)

            destination_uri = "gs://{}/{}".format(bucket, filename)
            dataset_ref = bq.DatasetReference(project, dataset)
            table_ref = dataset_ref.table(tablename)

            extract_job = client.extract_table(
                table_ref,
                destination_uri,
                # Location must match that of the source table.
                location="US"
            )  # API request
            extract_job.result()  # Waits for job to complete.

            logger.info(
                "Exported {}:{}.{} to {}".format(project, dataset, tablename, destination_uri)
            )
            return True
        except Exception as e:
            logger.exception(str(e))
            return False

    @staticmethod
    def import_from_gcs(project, dataset, tablename, schema, bucket, filename, ff='csv', if_exists='', max_errors=0):
        """
        import data from storage file(s) to BigQuery table
        :param project:
        :param dataset:
        :param tablename:
        :param schema:
        :param bucket:
        :param filename:
        :param ff:
        :param if_exists:
        :return:
        """
        logger.info(f'Importing data to : {project}:{dataset}.{tablename} ......')
        try:
            # Construct a BigQuery client object.
            client = bq.Client(project, gcp_cred)

            # Set table_id to the ID of the table to create.
            table_id = f'{project}.{dataset}.{tablename}'
            job_config = bq.LoadJobConfig(schema=schema)

            if ff == 'csv':
                job_config.source_format = bq.SourceFormat.CSV
                job_config.skip_leading_rows = 1
                job_config.max_bad_records = max_errors
            elif ff == 'avro':
                job_config.source_format = bq.SourceFormat.AVRO
                job_config.max_bad_records = max_errors
            elif ff == 'json':
                pass

            if if_exists == "append":
                job_config.write_disposition = bq.job.WriteDisposition.WRITE_APPEND
            if if_exists == "replace":
                job_config.write_disposition = bq.job.WriteDisposition.WRITE_TRUNCATE

            uri = "gs://{}/{}".format(bucket, filename)
            load_job = client.load_table_from_uri(uri, table_id, job_config=job_config)  # Make an API request.
            load_job.result()  # Waits for the job to complete.
            destination_table = client.get_table(table_id)  # Make an API request.
            logger.info("Loaded {} rows.".format(destination_table.num_rows))
            return True

        except Exception as e:
            logger.exception(str(e))
            return False

    @staticmethod
    @retry3
    def delete_blob(project, bucket, blob_name):
        """Deletes a blob from the bucket."""
        logger.info(f'Deleting object : {blob_name} ......')
        try:
            storage_client = gcs.Client(project, gcp_cred)

            blob_prefix = blob_name.split('*')[0]
            bucket = storage_client.bucket(bucket)
            blobs = bucket.list_blobs(prefix=blob_prefix)

            for b in blobs:
                blob = bucket.blob(b.name)
                blob.delete()

            logger.info("Blob {} deleted.".format(blob_name))
            return True

        except Exception as e:
            logger.exception(str(e))
            return False

    @staticmethod
    @retry3
    def copy_table(project, source_dataset, source_table, destination_dataset, destination_table):
        """
        copy table from source dataset to destination dataset
        :param project:
        :param source_bucket:
        :param source_table:
        :param destination_bucket:
        :param destination_table:
        :return: True/False
        """
        logger.info(
            f'Copying table from {source_dataset}.{source_table} to {destination_dataset}.{destination_table}......')
        try:
            # Construct a BigQuery client object.
            client = bq.Client(project, gcp_cred)

            # Set source_table_id to the ID of the original table.
            source_table_id = f'{project}.{source_dataset}.{source_table}'

            # Set destination_table_id to the ID of the destination table.
            destination_table_id = f'{project}.{destination_dataset}.{destination_table}'

            job = client.copy_table(source_table_id, destination_table_id)
            job.result()  # Wait for the job to complete.

            logger.info("A copy of the table created.")
            return True

        except Exception as e:
            logger.exception(str(e))
            return False

    @staticmethod
    @retry3
    def delete_table(project, dataset, tablename):
        """
        delete table from project.dataset
        :param project:
        :param dataset:
        :param table:
        :return:
        """
        logger.info(f'Deleting table {dataset}.{tablename} ......')
        try:
            # Construct a BigQuery client object.
            client = bq.Client(project, gcp_cred)

            # Set table_id to the ID of the table to fetch.
            table_id = f'{project}.{dataset}.{tablename}'

            # If the table does not exist, delete_table raises
            # google.api_core.exceptions.NotFound unless not_found_ok is True.
            client.delete_table(table_id, not_found_ok=True)  # Make an API request.
            logger.info(f"Deleted table '{dataset}.{tablename}'.")
            return True

        except Exception as e:
            logger.exception(str(e))
            return False

    @staticmethod
    @retry3
    def move_blob(bucket_name, blob_name, destination_bucket_name, destination_blob_name, move=True):
        """Moves a blob from one bucket to another with a new name."""
        logger.info(f'Moving object from {bucket_name}.{blob_name} to {destination_bucket_name}.{destination_blob_name} ......')
        try:
            storage_client = gcs.Client()
            source_bucket = storage_client.bucket(bucket_name)
            destination_bucket = storage_client.bucket(destination_bucket_name)
            blob_prefix = blob_name.split('*')[0]
            blobs = source_bucket.list_blobs(prefix=blob_prefix)
            for b in blobs:
                source_blob = source_bucket.blob(b.name)
                blob_copy = source_bucket.copy_blob(source_blob, destination_bucket, destination_blob_name)
                if move:
                    source_bucket.delete_blob(b.name)

            logger.info(f'object {blob_name} moved from {bucket_name} to {destination_bucket_name}')
            return True

        except Exception as e:
            logger.exception(str(e))
            return False


class SFTPOperations:
    """SFTP Operations
    1. upload to sftp
    2. download from sftp
    3. list sftp file(s)"""

    @staticmethod
    @retry3
    def push_to_sftp(sftp_site, local_filename, remote_foler, remote_filename=None):
        """
        push file(s) to sftp site with assigned remote folder
        :param sftp_site: SFTP site (config) name, refer config file
        :param sftp_folder: remote folder to upload file
        :param local_filename: local filename
        :param remote_filename: remote filename
        :return:
        """
        if remote_filename is None:
            remote_filename = os.path.basename(local_filename)

        try:
            with ConnectionBuilder.sftp_conn(sftp_site) as sftp:
                # with sftp.cd(sftp_folder):
                #     sftp.put(filename)
                sftp.put(local_filename, '/'.join([remote_foler, remote_filename]))

            logger.info(f'{local_filename} uploaded to {sftp_site}')
            return True
        except Exception as e:
            logger.exception(str(e))
            return False


class AWSOperations:
    """
    AWS operations
    """

    @staticmethod
    @retry3
    def get_S3files_list(access_key_id, secret_access_key, Bucket='', Prefix='', Suffix='', WithinLastXHours=-1):
        """
        Find [recently modified] files in an S3 bucket, with given Prefix and Suffix.
        INPUT:
            Bucket: Name of the S3 bucket.
            Prefix: Only fetch keys that start with this prefix (optional). Can be used to filted by brand/date/store.
            Suffix: Only fetch keys that end with this suffix (optional). Tuple of suffixes to look for. Can be used to filted by file type (csv, pq).
            WithinLastXHours: Search for files modified within this number of hours. Only used if positive.

        OUTPUT:
            List of S3 metadata for found files..
            List of S3 filename (Key)

        For example:
                {'ETag': '"c217e671a7ee8f8238204ad1c311b126"',
                  'Key': 'Trans_1143_20180410.csv',
         'LastModified': datetime.datetime(2018, 4, 11, 7, 0, 6, tzinfo=tzutc()),
                 'Size': 3221559,
         'StorageClass': 'STANDARD'}
        """

        kwargs = {'Bucket': Bucket, 'Prefix': Prefix}  # create list of args for list_objects_v2
        s3_client = boto3.client('s3', aws_access_key_id=access_key_id, aws_secret_access_key=secret_access_key)
        obj_list = s3_client.list_objects_v2(**kwargs)
        obj_list_recent = []
        if obj_list['KeyCount'] > 0:
            if WithinLastXHours > 0:  # if requested recent files only, set the cutoff datetime
                s3_tzinfo = obj_list['Contents'][0]['LastModified'].tzinfo  # get timezone used in S3
                LastModCutOff = (dt.datetime.now(s3_tzinfo) - dt.timedelta(
                    hours=WithinLastXHours))  # generate cut-off datetime to find recently modified files in S3

            while True:  # get files while there are more files to retrieve
                if WithinLastXHours > 0:  # if requested recent files only, filter by LastModified
                    obj_list_recent = obj_list_recent + [obj for obj in obj_list['Contents'] if
                                                         (obj['LastModified'] > LastModCutOff) & (obj['Size'] > 0) & (
                                                             obj['Key'].endswith(Suffix))]  # get all files, that a
                else:  # otherwise do not filter by LastModified
                    obj_list_recent = obj_list_recent + [obj for obj in obj_list['Contents'] if (obj['Size'] > 0) & (
                        obj['Key'].endswith(Suffix))]  # get all files, that have non-zero size, and end with Suffix

                if 'NextContinuationToken' in obj_list.keys():
                    # Read more if 'NextContinuationToken' is present, otherwise exit the loop
                    kwargs['ContinuationToken'] = obj_list['NextContinuationToken']
                    obj_list = s3_client.list_objects_v2(**kwargs)
                else:
                    break

        else:
            logger.warning(f'No objects in S3 with Prefix={Prefix}.')
            return [], []

        return obj_list_recent, [o.get('Key', '') for o in obj_list_recent]

    @staticmethod
    @retry3
    def awss3_read_csv(access_key_id, secret_access_key, srcbucketname, source_file_name, colnames=None, encoding=None,
                       compression="infer",
                       quote='"'):
        """This is a fancy version of awss3_get_csv that allows additional arguments for pd.read_csv,
        most relevantly the quotechar can be specified as single-quote for Micros data """
        s3 = boto3.client('s3', aws_access_key_id=access_key_id, aws_secret_access_key=secret_access_key)
        try:
            obj = s3.get_object(Bucket=srcbucketname, Key=source_file_name)
            try:
                df_csv = pd.read_csv(io.BytesIO(obj['Body'].read()), quotechar=quote, encoding=encoding,
                                     names=colnames, compression=compression)
            except:
                df_csv = pd.read_csv(io.BytesIO(obj['Body'].read()), encoding=encoding,
                                     names=colnames, compression=compression)
        except Exception as e:
            logger.exception(str(e))
            return False

        return df_csv

    @staticmethod
    @retry3
    def awss3_write_csv(s3, df, bucketname, file_name):
        """Write Pandas DataFrame csv to S3"""
        try:
            df_csv = df.to_csv(index=False).encode('utf-8')
            status = s3.put_object(Body=df_csv, Bucket=bucketname, Key=file_name)

        except Exception as e:
            logger.exception(str(e))
            return False

        if status['ResponseMetadata']['HTTPStatusCode'] != 200:
            logger.error(f"Upload cav file to S3 failed, error code: {status['ResponseMetadata']['HTTPStatusCode']}")
            return False

        return status

    @staticmethod
    @retry3
    def awss3_read_json(s3, srcbucketname, source_file_name):
        """This is a fancy version of awss3_get_csv that allows additional arguments for pd.read_csv,
        most relevantly the quotechar can be specified as single-quote for Micros data """
        try:
            obj = s3.get_object(Bucket=srcbucketname, Key=source_file_name)
            doc = json.load(io.BytesIO(obj['Body'].read()))

        except Exception as e:
            logger.exception(str(e))
            return False

        return doc

    @staticmethod
    @retry3
    def awss3_write_json(s3, json_data, bucketname, file_name):
        """Write Pandas DataFrame csv to S3"""

        try:
            status = s3.put_object(Body=(bytes(json.dumps(json_data).encode('UTF-8'))), Bucket=bucketname,
                                   Key=file_name)
        except Exception as e:
            logger.exception(str(e))
            return False

        if status['ResponseMetadata']['HTTPStatusCode'] != 200:
            logger.error(f"Upload JSON file to S3 failed, error code: {status['ResponseMetadata']['HTTPStatusCode']}")
            return False

        return status

    @staticmethod
    @retry3
    def awss3_move_object(access_key_id, secret_access_key, src_bucketname, src_filename, arc_bucketname, arc_file_name=None, move=True):

        sfn, afn = src_filename, arc_file_name
        if afn is None:
            afn = src_bucketname
        try:
            session = boto3.Session(aws_access_key_id=access_key_id, aws_secret_access_key=secret_access_key)
            # Creating S3 Resource From the Session.
            s3 = session.resource('s3')
            # Create a Soucre Dictionary That Specifies Bucket Name and Key Name of the Object to Be Copied
            copy_source = {'Bucket': src_bucketname, 'Key': src_filename}
            # Creating Destination Bucket
            destbucket = s3.Bucket(arc_bucketname)
            # Copying the Object to the Target Directory
            destbucket.copy(copy_source, arc_file_name)
            # To Delete the File After Copying It to the Target Directory
            if move:
                s3.Object(src_bucketname, src_filename).delete()
            # Printing the File Moved Information
            logger.info(f'Archived {sfn} to bucket {arc_bucketname}')
            return True
        except Exception as e:
            logger.exception(str(e))
            return False


class DataFrameOperations:
    @staticmethod
    def format_dataframe_columns(df, columns, match=False):
        """
        sort the dataframe with cloumn list, add column(s) with value=None if the column is not in the dataframe
        :param df: dataframe
        :param columns: column list
        :param match: True=export dataframe with columns in column list only, False: export all columns in dataframe
        :return:
        """
        for c in columns:
            if c not in df:
                df[c] = None

        return df

    @staticmethod
    def fillna_none(df):
        df = df.replace([np.nan], [None])
        return df


if __name__ == '__main__':
    """main code here"""
    pass
