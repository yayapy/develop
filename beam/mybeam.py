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
import logging
import re
import argparse
import apache_beam as beam
from apache_beam.io import ReadFromText
from apache_beam.io import WriteToText
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions


def run(argv=None, save_main_session=True):
    """
    Description:

    """
    parser = argparse.ArgumentParser()
    parser.add_argument('--input', dest='input', default='./test_src.txt')
    parser.add_argument('--output', dest='output', default='./test_tgt.txt')
    known_args, pipeline_args = parser.parse_known_args(argv)
    pipeline_args.extend([
        # CHANGE 2/6: (OPTIONAL) Change this to DataflowRunner to
        # run your pipeline on the Google Cloud Dataflow Service.
        '--runner=DirectRunner',
        # CHANGE 3/6: (OPTIONAL) Your project ID is required in order to
        # run your pipeline on the Google Cloud Dataflow Service.
        # '--project=SET_YOUR_PROJECT_ID_HERE',
        # CHANGE 4/6: (OPTIONAL) The Google Cloud region (e.g. us-central1)
        # is required in order to run your pipeline on the Google Cloud
        # Dataflow Service.
        # '--region=SET_REGION_HERE',
        # CHANGE 5/6: Your Google Cloud Storage path is required for staging local
        # files.
        # '--staging_location=gs://YOUR_BUCKET_NAME/AND_STAGING_DIRECTORY',
        # CHANGE 6/6: Your Google Cloud Storage path is required for temporary
        # files.
        # '--temp_location=gs://YOUR_BUCKET_NAME/AND_TEMP_DIRECTORY',
        '--job_name=mybeam',
    ])

    pipeline_options = PipelineOptions(pipeline_args)
    pipeline_options.view_as(SetupOptions).save_main_session = save_main_session
    with beam.Pipeline(options=pipeline_options) as pipeline:
        lines = pipeline | ReadFromText(known_args.input, skip_header_lines=True)

        def format_output(input_pc):
            logging.debug(input_pc)
            ll = input_pc.split(',')
            return 'id: %s, name: %s' % (ll[0], ll[1])

        output = lines | 'Format' >> beam.Map(format_output)
        output | WriteToText(known_args.output)


if __name__ == '__main__':
    """
    use logger instance to record log: info, error, warning, critical and debug
    send error, warning and critical messsage to slack at then end of the job 
    """
    # default header
    # logger.info(f"Start of the job <{sys.argv[0]}>")

    # Slack token name and channel of this project
    # slack_token = ''  # e.g.'SLACK_BOT_TOKEN_CRM'
    # slack_channel = ''  # e.g. '#crm_notification'

    # add code here:
    logging.getLogger().setLevel(logging.DEBUG)
    run()
    # end
    # logger.info(f"End of the job <{sys.argv[0]}>")
    # LoggingToSlack.slack_logging(slack_token, slack_channel, jobname=sys.argv[0])
