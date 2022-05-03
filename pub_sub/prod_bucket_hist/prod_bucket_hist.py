#!/usr/local/bin/python3.6
# -*- coding:utf-8 -*-

"""
Subject:
Date:
Version:
Description:
"""

# from CommonModules import *
import datetime
import sys, os, io
import json

import numpy as np
import pandas as pd
import pandas_gbq as pgbq
root_path = os.path.abspath("..")
sys.path.append('/'.join([root_path, 'bin']))
# from CommonModules import ConnectionBuilder as cnb

from concurrent.futures import TimeoutError
from google.cloud import pubsub_v1
from google.oauth2 import service_account
import google.cloud.bigquery as bq

# TODO(developer)
project_id = "data-cube-migration"
topic_id = "storage_data-cube-migration"
subscription_id = "demo_sub"
bq_dataset = "Dev"
bq_table = "bucket_hist"

# Number of seconds the subscriber should listen for messages
timeout = 0.01

gcp_cred = service_account.Credentials.from_service_account_file('/Users/xfguo/workspace/.ssh/cloud_function_default.json')
subscriber = pubsub_v1.SubscriberClient(credentials=gcp_cred)
bq_client = bq.Client.from_service_account_json('/Users/xfguo/workspace/.ssh/cloud_function_default.json')


# The `subscription_path` method creates a fully qualified identifier
# in the form `projects/{project_id}/subscriptions/{subscription_id}`
subscription_path = subscriber.subscription_path(project_id, subscription_id)

msgs = []


def callback(message: pubsub_v1.subscriber.message.Message) -> None:

    print(f"Received {message.data!r}.")
    d = json.loads(message.data)
    if message.attributes:
        d = {**d, **message.attributes}

    msgs.append(d)
    message.ack()


def update_bigquery(data):
    """
    upload datafram to BQ table
    """

    full_tbl_name = ".".join([project_id, bq_dataset, bq_table])
    def test_extract_schema():
        # project = 'bigquery-public-data'
        # dataset_id = 'samples'
        # table_id = 'shakespeare'
        # dataset_ref = bq_client.dataset(bq_dataset, project=project_id)
        # table_ref = dataset_ref.table(bq_table)
        table = bq_client.get_table(full_tbl_name)  # API Request

        # View table properties
        f = io.StringIO("")
        bq_client.schema_to_json(table.schema, f)
        return json.loads(f.getvalue())

    print(f'Updating Bigquery table ......')

    try:
        """clear existing data by datetime"""

        # query = f"""delete from `{full_tbl_name}` where business_date = '{qdate}';"""
        # bq_client = GCPOperations.get_gcp_client('bigquery')
        # query_job = bq_client.query(query)
        # query_job.result()
        # if query_job.state == "DONE":
        """upload data"""
        destination_name = ".".join([bq_dataset, bq_table])
        # schema = test_extract_schema()

        schema = [{'mode': 'NULLABLE', 'name': 'kind', 'type': 'STRING'}, {'mode': 'NULLABLE', 'name': 'id', 'type': 'STRING'}, {'mode': 'NULLABLE', 'name': 'selfLink', 'type': 'STRING'}, {'mode': 'NULLABLE', 'name': 'name', 'type': 'STRING'}, {'mode': 'NULLABLE', 'name': 'bucket', 'type': 'STRING'}, {'mode': 'NULLABLE', 'name': 'generation', 'type': 'INTEGER'}, {'mode': 'NULLABLE', 'name': 'metageneration', 'type': 'INTEGER'}, {'mode': 'NULLABLE', 'name': 'contentType', 'type': 'STRING'}, {'mode': 'NULLABLE', 'name': 'timeCreated', 'type': 'TIMESTAMP'}, {'mode': 'NULLABLE', 'name': 'updated', 'type': 'TIMESTAMP'}, {'mode': 'NULLABLE', 'name': 'storageClass', 'type': 'STRING'}, {'mode': 'NULLABLE', 'name': 'timeStorageClassUpdated', 'type': 'TIMESTAMP'}, {'mode': 'NULLABLE', 'name': 'size', 'type': 'INTEGER'}, {'mode': 'NULLABLE', 'name': 'md5Hash', 'type': 'STRING'}, {'mode': 'NULLABLE', 'name': 'mediaLink', 'type': 'STRING'}, {'mode': 'NULLABLE', 'name': 'crc32c', 'type': 'STRING'}, {'mode': 'NULLABLE', 'name': 'etag', 'type': 'STRING'}, {'mode': 'NULLABLE', 'name': 'objectGeneration', 'type': 'INTEGER'}, {'mode': 'NULLABLE', 'name': 'objectId', 'type': 'STRING'}, {'mode': 'NULLABLE', 'name': 'eventType', 'type': 'STRING'}, {'mode': 'NULLABLE', 'name': 'payloadFormat', 'type': 'STRING'}, {'mode': 'NULLABLE', 'name': 'notificationConfig', 'type': 'STRING'}, {'mode': 'NULLABLE', 'name': 'eventTime', 'type': 'TIMESTAMP'}, {'mode': 'NULLABLE', 'name': 'overwroteGeneration', 'type': 'INTEGER'}, {'mode': 'NULLABLE', 'name': 'bucketId', 'type': 'STRING'}, {'mode': 'NULLABLE', 'name': 'overwrittenByGeneration', 'type': 'INTEGER'}]
        # data.to_csv("./data.csv", index=False)
        format_dataframe(data, schema)
        data.fillna(None, inplace=True)
        pgbq.to_gbq(dataframe=data,
                    destination_table=destination_name,
                    project_id=project_id,
                    credentials=gcp_cred,
                    table_schema=schema,
                    progress_bar=True,
                    if_exists="replace")
        print(f'Bigquery table has been updated')

    except Exception as e:
        print(f"Error:{str(e)}")
        return False

    return True


def format_dataframe(d: pd.DataFrame, table_schema:list) -> None:
    for col in table_schema:
        try:
            if col["type"] == "INTEGER":
                d[col["name"]] = d[col["name"]].astype('Int64').fillna
            elif col["type"] == "STRING":
                d[col["name"]] = d[col["name"]].astype(str)
            elif col["type"] == "TIMESTAMP":
                d[col["name"]] = d[col["name"]].astype(datetime.datetime)
        except Exception as e:
            print(str(e))
            pass

    return


if __name__ == '__main__':
    """
    use logger instance to record log: info, error, warning, critical and debug
    send error, warning and critical messsage to slack at then end of the job 
    """
    # default header
    print(f"Start of the job <{sys.argv[0]}>")

    # Slack token name and channel of this project
    slack_token = ''  # e.g.'SLACK_BOT_TOKEN_CRM'
    slack_channel = ''  # e.g. '#crm_notification'

    # add code here:
    flow_control = pubsub_v1.types.FlowControl(max_messages=10)
    streaming_pull_future = subscriber.subscribe(subscription_path,
                                                 callback=callback,
                                                 flow_control=flow_control)

    print(f"Listening for messages on {subscription_path}..\n")

    # Wrap subscriber in a 'with' block to automatically call close() when done.
    with subscriber:
        try:
            # When `timeout` is not set, result() will block indefinitely,
            # unless an exception is encountered first.
            streaming_pull_future.result(timeout=timeout)
        except TimeoutError:
            streaming_pull_future.cancel()  # Trigger the shutdown.
            streaming_pull_future.result()  # Block until the shutdown is complete.

    print(f"{len(msgs)} messages")
    # with open(f'./bukect_hist.json', 'w', encoding='utf-8') as f:
    #     json.dump(msgs, f, ensure_ascii=False, indent=4)
    #
    # with open(f'./bukect_hist_short.json', 'w', encoding='utf-8') as f2:
    #     json.dump(msgs[0:10], f2, ensure_ascii=False, indent=4)
    df = pd.DataFrame(msgs)
    # df = df.astype("string")
    # df = df.fillna('')
    # df = df.fillna(np.nan).replace([np.nan], [None])
    # df = df.where(pd.notnull(df), None)
    # df.to_csv('./bukect_hist.csv', index=False)
    update_bigquery(df)

    print("End of job")
    # end
    # logger.info(f"End of the job <{sys.argv[0]}>")
    # LoggingToSlack.slack_logging(slack_token, slack_channel, jobname=sys.argv[0])
