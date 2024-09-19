import json
import datetime
import argparse
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, GoogleCloudOptions, StandardOptions
from apache_beam.transforms.window import FixedWindows

class ParsePubSubMessage(beam.DoFn):
    def process(self, element):
        element = json.loads(element.decode('utf-8'))
        yield element['customer_id'], element['purchase_amount'], element['time_stamp']

class CustomTimeStamp(beam.DoFn):
    def process(self, elements):
        if len(elements) > 2:
            try:
                unix_timestamp = float(elements[2])  # timestamp in seconds
                
                if unix_timestamp < 0 or unix_timestamp > 2**31:
                    raise ValueError("Timestamp out of range")

                yield beam.window.TimestampedValue(elements, unix_timestamp)
            except (ValueError, TypeError) as e:
                print(f"Invalid timestamp {elements[2]}: {e}")
                default_timestamp = float(datetime.datetime.now().timestamp())
                yield beam.window.TimestampedValue(elements, default_timestamp)
        else:
            print(f"Element does not have enough items: {elements}")
            yield None

class ParseFn(beam.DoFn):
    def process(self, element):
        yield (element[0], float(element[1]))  # customer_id and purchase_amount

class FormatOutputFn(beam.DoFn):
    def process(self, element, window=beam.DoFn.WindowParam):
        customer_id, total_purchase = element
        try:
            start = window.start.to_utc_datetime()
            end = window.end.to_utc_datetime()
            output_str = f"{customer_id}: ${total_purchase}, Window: {start} - {end}"
            print(output_str)
            yield output_str.encode('utf-8')  # Encode as bytes
        except OverflowError:
            yield f"{customer_id}: ${total_purchase}, Invalid timestamp".encode('utf-8')  # Encode as bytes

def tocsv(elements):
    elements = list(elements)
    elements[2] = str(elements[2]).split("{")[1].split('}')[0]
    return ",".join(str(x) for x in elements)

def customer_data_aggregation(argv=None):
    parser = argparse.ArgumentParser()

    # Add necessary arguments for the pipeline
    parser.add_argument(
        '--input_subscription',
        dest='input_subscription',
        required=True,
        help='Pub/Sub subscription to read from'
    )
    parser.add_argument(
        '--output_topic',
        dest='output_topic',
        required=True,
        help='Pub/Sub topic to write to'
    )
    parser.add_argument(
        '--runner',
        dest='runner',
        default='DirectRunner',  # Default to DirectRunner (local execution)
        help='Runner type (DirectRunner or DataflowRunner)'
    )
    parser.add_argument(
        '--project',
        dest='project',
        required=False,  # Only required for DataflowRunner
        help='Google Cloud Project ID'
    )
    parser.add_argument(
        '--temp_location',
        dest='temp_location',
        required=False,  # Only required for DataflowRunner
        help='Temporary storage location for Dataflow (e.g., gs://bucket/temp)'
    )
    parser.add_argument(
        '--region',
        dest='region',
        required=False,  # Only required for DataflowRunner
        help='Google Cloud region (e.g., us-central1)'
    )
    parser.add_argument(
        '--streaming',
        dest='streaming',
        action='store_true',
        help='Specify this flag to run the pipeline in streaming mode'
    )
    parser.add_argument(
        '--job_name',
        dest='job_name',
        required=False,
        help='Dataflow job name'
    )

    # Parse the arguments
    known_args, pipeline_args = parser.parse_known_args(argv)

    # Set pipeline options based on parsed arguments
    options = PipelineOptions(pipeline_args)

    if known_args.runner == 'DataflowRunner':
        print("Running with DataflowRunner")
    else:
        print("Running with DirectRunner")

    
    # For DataflowRunner, set additional options using GoogleCloudOptions
    if known_args.runner == 'DataflowRunner':
        google_cloud_options = options.view_as(GoogleCloudOptions)
        google_cloud_options.project = known_args.project
        google_cloud_options.temp_location = known_args.temp_location
        google_cloud_options.region = known_args.region
        google_cloud_options.job_name = known_args.job_name  # Set the job name

    # Ensure the pipeline is in streaming mode if Pub/Sub is used
    if known_args.streaming:
        options.view_as(StandardOptions).streaming = True

    # Define the pipeline
    with beam.Pipeline(options=options) as p:
        purchases = (
            p
            | 'Read from PubSub' >> beam.io.ReadFromPubSub(subscription=known_args.input_subscription)
            | "Parse Pubsub Data" >> beam.ParDo(ParsePubSubMessage())
            | 'Custom Timestamp' >> beam.ParDo(CustomTimeStamp())
            | 'Filter None' >> beam.Filter(lambda x: x is not None)  # Filter out None values
        )

        windowed_purchases = (
            purchases
            | 'Apply Fixed Window' >> beam.WindowInto(FixedWindows(30))  # Fixed window of 30 seconds
            | 'Parse CSV' >> beam.ParDo(ParseFn())
            | 'Sum Purchases' >> beam.CombinePerKey(sum)  # Sum purchases by customer_id
        )

        formatted_output = (
            windowed_purchases
            | 'Format Output' >> beam.ParDo(FormatOutputFn())
            # | "Print Output" >> beam.Map(print)  # Print output to console
            | "Write To PubSub" >> beam.io.WriteToPubSub(topic=known_args.output_topic)
        )

if __name__ == '__main__':
    customer_data_aggregation()


