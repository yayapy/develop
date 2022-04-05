#!/usr/local/bin/python3.6
# -*- coding:utf-8 -*-

"""
Subject:
Date:
Version:
Description:
"""
import pyspark.sql

from CommonModules import *

root_path = os.path.abspath("..")
sys.path.append('/'.join([root_path, 'bin']))
from CommonModules import ConnectionBuilder as cnb

import findspark
findspark.init()

import pyspark
from pyspark.sql import SparkSession


def func():
    """
    Description:
    
    """
    pass


if __name__ == '__main__':
    """
    use logger instance to record log: info, error, warning, critical and debug
    send error, warning and critical messsage to slack at then end of the job 
    """
    # default header
    logger.info(f"Start of the job <{sys.argv[0]}>")

    # # Slack token name and channel of this project
    # slack_token = ''  # e.g.'SLACK_BOT_TOKEN_CRM'
    # slack_channel = ''  # e.g. '#crm_notification'

    # add code here:
    # from pyspark.sql import SparkSession
    # spark = SparkSession.builder.getOrCreate()
    spark = pyspark.sql.SparkSession.builder.getOrCreate()

    # # end
    # logger.info(f"End of the job <{sys.argv[0]}>")
    # LoggingToSlack.slack_logging(slack_token, slack_channel, jobname=sys.argv[0])
