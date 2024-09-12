import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions

# Define custom options by subclassing PipelineOptions
class CustomOptions(PipelineOptions):
    @classmethod
    def _add_argparse_args(cls, parser):
        parser.add_argument('--input',           
                            dest='input',
                            required=True,
                            help='Input file to process.')
        parser.add_argument('--output',
                            dest='output',
                            required=True,
                            help='Output file to write results to.')

# Parse the arguments and create a pipeline
options = PipelineOptions()
custom_options = options.view_as(CustomOptions)

inputs_pattern = custom_options.input
outputs_prefix = custom_options.output

# Define the Beam pipeline
with beam.Pipeline(options=options) as p:
    attendance_count = (
        p
        | 'Read lines' >> beam.io.ReadFromText(inputs_pattern)
        | 'Split row' >> beam.Map(lambda record: record.split(','))
        | 'Get all Accounts Dept Persons' >> beam.Filter(lambda record: record[3] == 'Accounts')
        | 'Pair each employee with 1' >> beam.Map(lambda record: (record[1], 1))
        | 'Group and sum' >> beam.CombinePerKey(sum)
        | 'Format results' >> beam.Map(lambda employee_count: str(employee_count))
        | 'Write results' >> beam.io.WriteToText(outputs_prefix)
    )
