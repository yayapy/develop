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


def sum_array(arr: list) -> int:
    if len(arr) > 0:
        return arr[0] + sum_array(arr[1:])
    else:
        return 0



import pandas as pd

pd.set_option("display.max_columns", 1000)  # show all columns
pd.set_option("display.max_colwidth", 1000)  # show each column entirely
pd.set_option("display.max_rows", 300)  # show all rows

def process_data():
    # Do not alter this line.
    biopics = pd.read_csv("biopics.csv", encoding='latin-1')
    # Write your code here.
    ## 1
    biopics.drop_duplicates(inplace=True)
    ## 2
    biopics.rename(columns={'box_office': 'earnings'}, inplace=True)
    ## 3
    biopics.dropna(subset=["earnings"], inplace=True)
    ## 4
    biopics = biopics.loc[biopics['year_release'] >= 1990]
    ## 5
    biopics['type_of_subject'] = biopics['type_of_subject'].astype("category")
    biopics['country'] = biopics['country'].astype("category")
    ## 6
    # biopics['lead_actor_actress'].fillna(value="", inplace=True)
    # biopics['lead_actor_actress_known'] = biopics['lead_actor_actress'].apply(lambda x: False if x.isnull() else True)
    biopics['lead_actor_actress_known'] = biopics['lead_actor_actress'].isnull()
    biopics['lead_actor_actress_known'] = biopics['lead_actor_actress_known'].apply(lambda x: not x)
    ## 7
    biopics['earnings'] = biopics['earnings'].apply(lambda x: round(x/1000000, 2))
    ## 8
    biopics = biopics[['title', 'year_release', 'earnings', 'country', 'type_of_subject', 'lead_actor_actress', 'lead_actor_actress_known']]
    ## 9
    biopics.sort_values(by=['earnings'], ascending=False, inplace=True)
    # Remember to return the right object.
    # return biopics.reset_index(drop=True)
    return biopics.reset_index(drop=True)


if __name__ == '__main__':


    """
    use logger instance to record log: info, error, warning, critical and debug
    send error, warning and critical messsage to slack at then end of the job 
    """
    df = process_data()
    print(df.head())
    df.to_csv('biopics_output.csv', index=False)
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
    # INPUTS = [(1,1),
    #           (2,2),
    #           (3,3),
    #           (1,11),
    #           (3,33)]
    #
    # OUTPUT = {}
    #
    # for k, v in INPUTS:
    #     values = OUTPUT.get(k, []) + [v]
    #     print(OUTPUT)
    #     OUTPUT = {**OUTPUT, k: values}
    #
    # print(OUTPUT)