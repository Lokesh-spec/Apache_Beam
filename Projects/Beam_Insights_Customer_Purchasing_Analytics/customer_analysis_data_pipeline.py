import csv
import json
import argparse
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions

## Parsing Input elements
def parser_args():
    parser = argparse.ArgumentParser(description="Apache Beam DirectRunner Example")

    parser.add_argument("--input", type=str, required=True)
    parser.add_argument("--output", type=str, required=True)
    parser.add_argument("--runner", type=str, default="DirectRunner")
    parser.add_argument("--temp_location", type=str)
    
    args, pipeline_args = parser.parse_known_args()

    return args, pipeline_args

## Aggregate Purchase Amount / Frequency Per Region
def sum_values_per_key(elements):
    converting_to_number = [float(element) for element in elements[1]]

    return elements[0], sum(converting_to_number)

## Format the Region vs Purchase Amount and Frequency
def format_results_region(elements):
    # Ensure to handle the correct types
    return (
        elements[0],
        elements[1]["purchase_amount"][0]["purchase_amount"][0],
        elements[1]["purchase_frequency"][0]["purchase_frequency"][0]
    )
    

def format_results_age(elements):
    return (
        elements[0],
        elements[1]["Income"][0],
        elements[1]["Purchase Amount"][0]
    )

# Convert tuple to CSV format
def format_as_csv(element):
    return ",".join(map(str, element))
    

## Customer Analysis Data Pipeline
def customer_analysis_data_pipeline(input_file_path, output_file_path, pipeline_args):
    
    ## Creating the Pipeline Options
    options = PipelineOptions(pipeline_args)
    
    with beam.Pipeline(options=options) as pipeline:
        customer_analysis_data = (
            pipeline
            | "Read From CSV File" >> beam.io.ReadFromText(input_file_path, skip_header_lines=1)
            | "Split the Records" >> beam.Map(lambda element: element.split(","))
            # Column_Details = ["user_id", "age", "annual_income", "purchase_amount", "loyalty_score", "region", "purchase_frequency"]
        )

        ### Region vs Pucharse Amount / Purchase Frequency
        purchase_amount_per_region = (
            customer_analysis_data
            | "Map Region and Purchase Amount" >> beam.Map(lambda element: (element[5], element[3])) 
            | "Group by Region with Purchase Amount" >> beam.GroupByKey()
            | "Aggregate Region Per Purchase Amount" >> beam.Map(sum_values_per_key)
            | "Convert Amount to Dict" >> beam.Map(lambda elements: (elements[0], {"purchase_amount": [elements[1]]}))
        )
        
        purchase_power_per_region = (
            customer_analysis_data
            | "Map Region and Purchase Frequency" >> beam.Map(lambda element: (element[5], element[6])) 
            | "Group by Region with Purchase Frequency" >> beam.GroupByKey()
            | "Aggregate Region Per Purchase Frequency" >> beam.Map(sum_values_per_key)
            | "Convert Frequency to Dict" >> beam.Map(lambda elements: (elements[0], {"purchase_frequency": [elements[1]]}))
        )

        merged_results_region = (
            {"purchase_amount": purchase_amount_per_region, "purchase_frequency": purchase_power_per_region}
            | "Merge Region" >> beam.CoGroupByKey()
            | "Format Results Region" >> beam.Map(format_results_region)
        )

        region_pcollection = (
            pipeline
            | "Create New Tuple PCollection for Region" >> beam.Create([("Region", "Purchase Amount", "Purchase Frequency")])
        )

        combined_purchase_and_write_region = (
            (region_pcollection, merged_results_region)
            | "Flatten PCollections Region" >> beam.Flatten()
            | "Format as CSV Region" >> beam.Map(format_as_csv)
            | "Write to Output Region File" >> beam.io.WriteToText(f"{output_file_path}/RegionVsPurchaseAmount&Frequency", file_name_suffix=".csv")
        )

        ### Age Vs Income
        age_vs_income = (
            customer_analysis_data 
            | "Map Age and Income" >> beam.Map(lambda element: (element[1], element[2]))
            | "Group by Age with Amount Income" >> beam.GroupByKey()
            | "Aggregate Age Per Annual Income" >> beam.Map(sum_values_per_key)
            # | "Print and Debug Income" >> beam.Map(print)
        )

        ## Age vs Purchase Power
        age_vs_purchase_amount = (
            customer_analysis_data 
            | "Map Age and Purchase Amount" >> beam.Map(lambda element: (element[1], element[3]))
            | "Group by Age with Purchase Amount" >> beam.GroupByKey()
            | "Aggregate Age Per Purchase Amount" >> beam.Map(sum_values_per_key)
            # | "Print and Debug Annual Income" >> beam.Map(print)
        )

        merged_results_region = (
            {"Income": age_vs_income, "Purchase Amount": age_vs_purchase_amount}
            | "Merge Age" >> beam.CoGroupByKey()
            | "Format Results Age" >> beam.Map(format_results_age)
            
        )

        age_pcollection = (
            pipeline
            | "Create New Tuple PCollection for Age" >> beam.Create([("Age", "Income", "Purchase Amount")])
        )

        combined_purchase_and_write_age = (
            (age_pcollection, merged_results_region)
            | "Flatten PCollections Age" >> beam.Flatten()
            | "Format as CSV Age" >> beam.Map(format_as_csv)
            | "Write to Output Age File" >> beam.io.WriteToText(f"{output_file_path}/AgeVsIncome&PurchaseAmount", file_name_suffix=".csv")
        )

        age_vs_Loyalty = (
            customer_analysis_data 
            | "Map Age and Loyalty Data" >> beam.Map(lambda element: (element[1], element[4]))
            | "Group by Age with Loyalty" >> beam.GroupByKey()
            | "Aggregate Age Per Loyalty" >> beam.Map(sum_values_per_key)
        )

        age_Loyalty_pcollection = (
            pipeline
            | "Create New Tuple PCollection for Age Loyalty" >> beam.Create([("Age", "Loyalty")])
        )

        combined_purchase_and_write_age_Loyalty = (
            (age_Loyalty_pcollection, age_vs_Loyalty)
            | "Flatten PCollections Age Loyalty" >> beam.Flatten()
            | "Format as CSV Age Loyalty" >> beam.Map(format_as_csv)
            | "Write to Output Age Loyalty File" >> beam.io.WriteToText(f"{output_file_path}/AgeVsLoyalty", file_name_suffix=".csv")
        )


if __name__ == "__main__":
    ## Parsing the Arguments
    args, pipeline_args = parser_args()
    
    ## Run the Pipeline
    customer_analysis_data_pipeline(args.input, args.output, pipeline_args)
