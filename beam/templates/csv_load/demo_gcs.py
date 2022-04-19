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

class add_tax(beam.DoFn):
    def process(self, element):
        brand,customer_id,order_id,order_date,channel_id,ORDER_SUBTOTAL_BEFORE_DDDCT = element.split(",")
        return [{
                "brand": str(brand),
                "customer_id": int(customer_id),
                "order_id": int(order_id),
                "order_date": str(order_date),
                "ORDER_SUBTOTAL_BEFORE_DDDCT": float(ORDER_SUBTOTAL_BEFORE_DDDCT),
                "ORDER_SUBTOTAL_AFTER_TAX": float(ORDER_SUBTOTAL_BEFORE_DDDCT) * 1.13
                }]

def run():
    pipeline_options = PipelineOptions()
    options = pipeline_options.view_as(TemplateOptions)
    gcp_options = pipeline_options.view_as(GoogleCloudOptions)

    pipeline_options.view_as(SetupOptions).save_main_session = True
    pipeline_options.view_as(WorkerOptions).num_workers = 3
    pipeline_options.view_as(WorkerOptions).max_num_workers = 5
    bar = 20
    columns = ['brand', 'customer_id', 'order_id', 'order_date', 'channel_id', 'ORDER_SUBTOTAL_BEFORE_DDDCT']
    with beam.Pipeline(options=pipeline_options) as pipeline:
        lines = (pipeline | 'Read GCS File' >> beam.io.ReadFromText('gs://rguo_dev/sample_orders',
                                                                    skip_header_lines=True))

        Parse = lines | "Parse" >> beam.ParDo(ParseCsv(columns)) | "Parse output" >> beam.io.WriteToText('gs://rguo_dev/sample_orders_parse.csv')
        taxed = (lines
                 | "Add prefix" >> beam.ParDo(add_tax())
                 # | "Write" >> beam.io.WriteToText('gs://rguo_dev/test_src_output.csv')
                 | "Tax output" >> beam.io.WriteToText('gs://rguo_dev/sample_orders_tax.csv')
                 )

        # over20 = (taxed | "filter cost over 20" >> beam.ParDo(filter_by_total_over(bar)) |
        #           # "Print over20" >> beam.Map(print) |
        #           "Output over" >> beam.io.WriteToText('gs://rguo_dev/test_src_output_over.csv'))
        #
        # below20 = (taxed | "filter cost below 20" >> beam.ParDo(filter_by_total_below(bar)) |
        #            # "Print over20" >> beam.Map(print) |
        #            "Output below" >> beam.io.WriteToText('gs://rguo_dev/test_src_output_below.csv'))

    # runner = DataflowRunner()
    # runner.run_pipeline(pipeline, options=options)


if __name__ == '__main__':
    run()
