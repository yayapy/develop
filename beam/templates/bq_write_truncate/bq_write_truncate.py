import csv
from datetime import datetime

import apache_beam as beam
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
            "--filter_date", type=str, help="Date for filtering input file"
        )
        parser.add_value_provider_argument(
            "--fq_table_filter_date_partition", type=str, help="Start date partition table"
        )


class ParseCsvFn(beam.DoFn):
    """
    A transform to split a line based on two individual dates
    This transform will have 2 outputs:
        - one containing the elements having start_date as business date
        - one containing the elements having end_date as business date
    """

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


class DateFilterFn(beam.DoFn):
    def __init__(self, filter_date: str):
        self.filter_date = filter_date

    def process(self, element, *args, **kwargs):

        business_date = datetime.strptime(element['businessDate'], '%Y-%m-%d %H:%M:%S')
        business_date = datetime.strftime(business_date, '%Y-%m-%d')

        if business_date == self.filter_date:
            yield element


class ElementFilterFn(beam.DoFn):
    def __init__(self, element_key: str, element_value: str):
        self.element_key = element_key
        self.element_value = element_value

    def process(self, element):
        if element[self.element_key] == self.element_value:
            yield element


class CustomCoder(Coder):
    """A custom coder used for reading and writing strings as UTF-8."""

    def encode(self, value):
        return value.encode("us-ascii", "replace")

    def decode(self, value):
        return value.decode("us-ascii", "ignore")

    def is_deterministic(self):
        return True


def filter_start_date(input_data, start_date):
    business_date = datetime.strptime(input_data['businessDate'], '%Y-%m-%d %H:%M:%S')
    business_date = datetime.strftime(business_date, '%Y-%m-%d')

    return business_date == start_date


def run():
    pipeline_options = PipelineOptions()
    options = pipeline_options.view_as(TemplateOptions)
    gcp_options = pipeline_options.view_as(GoogleCloudOptions)

    pipeline_options.view_as(SetupOptions).save_main_session = True
    pipeline_options.view_as(WorkerOptions).num_workers = 3
    pipeline_options.view_as(WorkerOptions).max_num_workers = 5

    tableschema = [
              {
                "name": "brand",
                "type": "STRING"
              },
              {
                "name": "customer_id",
                "type": "INTEGER"
              },
              {
                "name": "order_id",
                "type": "INTEGER"
              },
              {
                "name": "order_date",
                "type": "DATE"
              },
              {
                "name": "channel_id",
                "type": "INTEGER"
              },
              {
                "name": "ORDER_SUBTOTAL_BEFORE_DDDCT",
                "type": "FLOAT"
              }
            ]

    columns = [s['name'] for s in tableschema]

    with beam.Pipeline(options=pipeline_options) as p:

        schema = {
            'fields': tableschema
        }

        (
            p
            | 'Read csv file' >> ReadFromText(options.input_file_path, coder=CustomCoder())
            | 'Parse csv file to dict' >> beam.ParDo(ParseCsvFn(columns))
            # | 'Filter by date' >> beam.Filter(filter_start_date, options.filter_date)
            | 'Write output to BigQuery' >> beam.io.WriteToBigQuery(
                options.fq_table_filter_date_partition,
                schema=schema,
                create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
                write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE)
            # | 'Write start date to text' >> beam.io.WriteToText('start-date-rows')
        )


if __name__ == '__main__':
    run()
