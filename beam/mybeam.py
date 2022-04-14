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
from apache_beam.dataframe.io import read_csv
from apache_beam.io import ReadFromText
from apache_beam.io import WriteToText
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions
from apache_beam.io.filesystems import FileSystems as beam_fs
import codecs
import csv
from typing import Dict, Iterable, List


class Tsplit(beam.DoFn):
    def process(self, element):
        id, name, cost = element.split(",")
        return [{
            'id': int(id),
            'name': str(name),
            'cost': float(cost)
        }]


class TcollectCost(beam.DoFn):
    def process(self, element):
        return [(int(element['id']), float(element['cost']) * 1.13)]


class TcollectCost2(beam.DoFn):
    def process(self, element):
        return [{'id': int(element['id']),
                 'cost': float(element['cost'])}]


@beam.ptransform_fn
@beam.typehints.with_input_types(beam.pvalue.PBegin)
@beam.typehints.with_output_types(Dict[str, str])
def ReadCsvFiles(pbegin: beam.pvalue.PBegin, file_patterns: List[str]) -> beam.PCollection[Dict[str, str]]:
    def expand_pattern(pattern: str) -> Iterable[str]:
        for match_result in beam_fs.match([pattern])[0].metadata_list:
          yield match_result.path

    def read_csv_lines(file_name: str) -> Iterable[Dict[str, str]]:
        with beam_fs.open(file_name) as f:
            # Beam reads files as bytes, but csv expects strings,
            # so we need to decode the bytes into utf-8 strings.
            for row in csv.DictReader(codecs.iterdecode(f, 'utf-8')):
                yield dict(row)

    if isinstance(file_patterns, str):
        file_patterns = [file_patterns]

    return (
        pbegin
        | 'Create file patterns' >> beam.Create(file_patterns)
        | 'Expand file patterns' >> beam.FlatMap(expand_pattern)
        | 'Read CSV lines' >> beam.FlatMap(read_csv_lines)
    )


def run1(argv=None, save_main_session=True):
    """
    Description:

    """
    parser = argparse.ArgumentParser()
    parser.add_argument('--input', dest='input', default='./test_src.csv')
    parser.add_argument('--output', dest='output', default='./test_tgt.txt')
    parser.add_argument('--output2', dest='output2', default='./test_tgt2.txt')
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
    # input_patterns = ['./test_src.csv']
    pipeline_options = PipelineOptions(pipeline_args)
    # option = PipelineOptions(flag=[], type_check_additional='all')
    # with beam.Pipeline(options=option) as pipeline:
    # pipeline_options.view_as(SetupOptions).save_main_session = save_main_session
    with beam.Pipeline(options=pipeline_options) as pipeline:
        # lines = pipeline | ReadFromText(known_args.input, skip_header_lines=True)
        # def format_output(input_pc):
        #     logging.debug(input_pc)
        #     ll = input_pc.split(',')
        #     return 'id: %s, name: %s' % (ll[0], ll[1])
        #
        # output = lines | 'Format' >> beam.Map(format_output)
        # output | WriteToText(known_args.output)

        # read_lines = (pipeline |
        #               ReadFromText(known_args.input, skip_header_lines=True)
        #               )
        #
        # read_lines | "Print raw" >> beam.Map(print)
        #
        # split_lines = read_lines | "split" >> beam.ParDo(Tsplit())
        # split_lines | "Print split" >> beam.Map(print)

        # after_tax = split_lines | "Tax" >> beam.Map(lambda x: {x['id'], x['name'], x['cost'] * 1.13})
        # after_tax | "Print tax" >> beam.Map(print)

        split_lines = (pipeline | "Read CSV files" >> ReadCsvFiles(known_args.input))
        split_lines | "Print split" >> beam.Map(print)

        grouped_lines = (
                split_lines |
                "Tuple columns" >> beam.ParDo(TcollectCost()) |
                "group by 1" >> beam.GroupByKey()
        )

        grouped_lines | "Print Grouped" >> beam.Map(print)
        trans_lines = (
                grouped_lines |
                "combine mean" >> beam.CombineValues(beam.combiners.MeanCombineFn())
        )
        trans_lines | "write file 1" >> WriteToText(known_args.output)


        # lines2 = (
        #         read_lines |
        #         "fetch columns" >> beam.ParDo(TcollectCost2()) |
        #         "write file 2" >> WriteToText(known_args.output2)
        # )


def run2():
    # input_patterns = ['./test_src*.csv']
    input_patterns = './test_src*.csv'
    option = PipelineOptions(flag=[], type_check_additional='all')
    with beam.Pipeline(options=option) as pipeline:
        csv_lines = (pipeline
                     | "Read CSV files" >> ReadCsvFiles(input_patterns)
                     | "Print elements" >> beam.Map(print))


def run3():
    def count(n: int) -> Iterable[int]:
        return [i for i in range(n)]

    with beam.Pipeline() as pipeline:
        (
            pipeline
            | "create pipeline" >> beam.Create([5])
            | "Generate elements" >> beam.FlatMap(count)
            | "Print element" >> beam.Map(print)
        )


def run_count():
    @beam.ptransform_fn
    @beam.typehints.with_input_types(beam.pvalue.PBegin)
    @beam.typehints.with_output_types(int)
    def Count(pbegin: beam.pvalue.PBegin, n: int) -> beam.PCollection[int]:
        def count(n: int) -> Iterable[int]:
            for i in range(n):
                yield i
        return (
            pbegin
            | "Create inputs" >> beam.Create([n])
            | "Generate elements" >> beam.FlatMap(count)
        )

    n = 5
    option = PipelineOptions(flag=[], type_check_additional='all')
    with beam.Pipeline(options=option) as pipeline:
        (
            pipeline
            | f'Count to {n}' >> Count(n)
            | "Print elements" >> beam.Map(print)
        )


def run_df():
    import apache_beam as beam
    from apache_beam.dataframe import convert
    # import apache_beam.runners.interactive.interactive_beam as ib
    # from apache_beam.runners.interactive.interactive_runner import InteractiveRunner


    input_patterns = './test_src.csv'
    option = PipelineOptions(flag=[], type_check_additional='all')
    with beam.Pipeline(options=option) as pipeline:
        df = pipeline | 'Read_csv' >> beam.dataframe.io.read_csv(input_patterns)

        (
            convert.to_pcollection(df)
            | "To Dict" >> beam.Map(lambda x: dict(x._asdict()))
            | "Print" >> beam.Map(print)
        )


def run_gcp(argv=None):
    parser = argparse.ArgumentParser()
    parser.add_argument('--input', dest='input', default='./test_src.csv')
    parser.add_argument('--output', dest='output', default='./test_tgt.txt')
    parser.add_argument('--output2', dest='output2', default='./test_tgt2.txt')
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
    # input_patterns = ['./test_src.csv']
    pipeline_options = PipelineOptions(pipeline_args)
    with beam.Pipeline(options=pipeline_options) as pipeline:
        lines = (pipeline |
                 'Read GCS File' >> beam.io.ReadFromText('gs://rguo_dev/native-land_language.csv') |
                 "Print" >> beam.Map(print) |
                 "Write" >> beam.io.WriteToText('gs://rguo_dev/native-land_language_write.txt'))




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
    logging.getLogger().setLevel(logging.INFO)
    # run1()
    # run2()
    # run3()
    # run_count()
    # run_df()
    # end
    run_gcp()
    # logger.info(f"End of the job <{sys.argv[0]}>")
    # LoggingToSlack.slack_logging(slack_token, slack_channel, jobname=sys.argv[0])
