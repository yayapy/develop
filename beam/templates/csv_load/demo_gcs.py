import csv
import json
import apache_beam as beam
from io import StringIO
from google.cloud import bigquery
from apache_beam.coders.coders import Coder
from apache_beam.io import ReadFromText
from apache_beam.options.pipeline_options import (
    GoogleCloudOptions,
    PipelineOptions,
    SetupOptions,
    WorkerOptions
)


class TemplateOptions(PipelineOptions):
    @classmethod
    def _add_argparse_args(cls, parser):
        parser.add_value_provider_argument(
            "--input_file_path", type=str, help="File path to a local or stored in a GCS bucket"
        )
        parser.add_value_provider_argument(
            "--fq_table_name", type=str, help="Destination table"
        )


class ParseCsv(beam.DoFn):
    def __init__(self, column_names: list):
        self.column_names = column_names

    def process(self, element, *args, **kwargs):
        reader = csv.DictReader(
            element.splitlines(),
            quotechar='"',
            quoting=csv.QUOTE_ALL,
            skipinitialspace=True,
            fieldnames=self.column_names,
            delimiter=","
        )
        for row in reader:
            yield row


class CustomCoder(Coder):
    """A custom coder used for reading and writing strings as UTF-8."""

    def encode(self, value):
        return value.encode("us-ascii", "replace")

    def decode(self, value):
        return value.decode("us-ascii", "ignore")

    def is_deterministic(self):
        return True


def add_prefix(element):
    return "Demo >>" + element


class add_prefix(beam.DoFn):
    def process(self, element):
        # print(element)
        yield "Demo >> " + element


def run():
    pipeline_options = PipelineOptions()
    options = pipeline_options.view_as(TemplateOptions)
    gcp_options = pipeline_options.view_as(GoogleCloudOptions)

    pipeline_options.view_as(SetupOptions).save_main_session = True
    pipeline_options.view_as(WorkerOptions).num_workers = 3
    pipeline_options.view_as(WorkerOptions).max_num_workers = 5
    with beam.Pipeline(options=pipeline_options) as pipeline:
        lines = (pipeline |
                 'Read GCS File' >> beam.io.ReadFromText('../../test_src.csv') |
                 # "Print before" >> beam.Map(print) |
                 "Add prefix" >> beam.ParDo(add_prefix()) |
                 # "Print after" >> beam.Map(print) |
                 "Write" >> beam.io.WriteToText('./test_src_write.txt')
                 )


if __name__ == '__main__':
    run()
