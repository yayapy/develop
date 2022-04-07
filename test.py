#!/usr/local/bin/python3.6
# -*- coding:utf-8 -*-

"""
Subject:
Date:
Version:
Description:
"""

# from CommonModules import *
#
# root_path = os.path.abspath("..")
# sys.path.append('/'.join([root_path, 'bin']))
# from CommonModules import ConnectionBuilder as cnb


def func():
    """
    Description:
    
    """
    print('test message none')
    return


if __name__ == '__main__':
    """
    use logger instance to record log: info, error, warning, critical and debug
    send error, warning and critical messsage to slack at then end of the job 
    """
    # from hdfs3 import HDFileSystem
    # hdfs = HDFileSystem(host='192.168.2.222', port=8020)
    # hdfs.ls('/')
    # local_filename = 'sample.txt'
    # rpath = input()
    # hdfs.put(local_filename, rpath + 'remote_' + local_filename)
    # hdfs.cp('/user/data/file.txt', '/user2/data')

    # # default header
    # logger.info(f"Start of the job <{sys.argv[0]}>")
    #
    # # Slack token name and channel of this project
    # slack_token = ''  # e.g.'SLACK_BOT_TOKEN_CRM'
    # slack_channel = ''  # e.g. '#crm_notification'
    #
    # # add code here:
    #
    # # end
    # logger.info(f"End of the job <{sys.argv[0]}>")
    # LoggingToSlack.slack_logging(slack_token, slack_channel, jobname=sys.argv[0])
    # func()
    INPUTS = [(1,1),
              (2,2),
              (3,3),
              (1,11),
              (3,33)]

    OUTPUT = {}

    for k, v in INPUTS:
        values = OUTPUT.get(k, []) + [v]
        print(OUTPUT)
        OUTPUT = {**OUTPUT, k: values}

    print(OUTPUT)