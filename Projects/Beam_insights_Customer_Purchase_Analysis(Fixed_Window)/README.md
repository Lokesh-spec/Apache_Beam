# Customer Data Processing Pipeline

## Overview

This project consists of two main components:
1. **Customer Data Generation**: A script to generate random customer purchase data and publish it to a Google Cloud Pub/Sub topic.
2. **Customer Data Processing Pipeline**: An Apache Beam pipeline that processes the published data, applies windowing and aggregation, and prints the results.

## Components

### 1. Customer Data Generation

**Script**: `generate_customer_data.py`

- **Purpose**: Generates random customer data and publishes it to Google Cloud Pub/Sub. Saves the data to a CSV file for local reference.
- **Features**:
  - Generates random purchase data.
  - Publishes data to Pub/Sub.
  - Saves data to `customer_data.csv`.

### 2. Customer Data Processing Pipeline

**Script**: `fixed_window.py`

- **Purpose**: Processes customer purchase data from Google Cloud Pub/Sub using Apache Beam. Aggregates data in fixed time windows and prints the results.
- **Features**:
  - Reads data from Pub/Sub.
  - Applies fixed windowing.
  - Aggregates purchase amounts by customer.
  - Formats and prints the results.

## Requirements

- Python 3.7 or later
- Google Cloud SDK
- Apache Beam SDK for Python (`apache-beam`)
- Google Cloud Pub/Sub library for Python (`google-cloud-pubsub`)
- Pandas library (`pandas`)

## Installation

1. **Clone the repository**:

    ```bash
    git clone <repository-url>
    cd <repository-directory>
    ```

2. **Set up a virtual environment**:

    ```bash
    python -m venv venv
    source venv/bin/activate  # On Windows use `venv\Scripts\activate`
    ```

3. **Install required packages**:

    ```bash
    pip install apache-beam[gcp] google-cloud-pubsub pandas
    ```

## Configuration

### Google Cloud Authentication

Ensure that the `GOOGLE_APPLICATION_CREDENTIALS` environment variable is set to the path of your Google Cloud service account key file:

```bash
export GOOGLE_APPLICATION_CREDENTIALS="/path/to/your-service-account-file.json"
```

1. **Customer Data Generation**:

    ###### Command-Line Arguments: 
    <ul>
      <li> <mark>--project</mark> : Google Cloud Project ID. (Required)</li>
      <li> <mark>--pubsub_topic</mark>: Pub/Sub topic path in the format projects/YOUR_PROJECT/topics/YOUR_TOPIC. (Required)</li>
    </ul>
    <br />

    ```bash
       python generate_customer_data.py --project YOUR_PROJECT_ID --pubsub_topic projects/YOUR_PROJECT_ID/topics/YOUR_TOPIC
    ```
1. **Customer Data Processing Pipeline**:

    ###### Command-Line Arguments:: 
    <ul>
      <li> <mark>--input-subscription</mark> : Google Cloud Pub/Sub input subscription path. (Required)</li>
      <li> <mark>--output_topic</mark> : Google Cloud Pub/Sub output topic path. (Required)</li>
      <li> <mark>--runner</mark>: Apache Beam runner to use (DirectRunner or DataflowRunner). (Default: DirectRunner)</li>
      <li> <mark>--project</mark>: Google Cloud Project ID. (Required for DataflowRunner)</li>
      <li> <mark>--temp_location</mark>: Temporary storage location for Dataflow. (Required for DataflowRunner)</li>
      <li> <mark>--region</mark>: Google Cloud region for Dataflow. (Required for DataflowRunner)</li>
      <li> <mark>--streaming</mark>: Flag to run the pipeline in streaming mode. (Required for Pub/Sub)</li>
    </ul>
    <br />

   ###### For local execution using DirectRunner:
    ```bash
       python fixed_window.py --subscription projects/YOUR_PROJECT_ID/subscriptions/YOUR_SUBSCRIPTION --input_subscription projects/YOUR_PROJECT_ID/subscriptions/YOUR_SUBSCRIPTION --output_topic=projects/YOUR_PROJECT_ID/topic/TOPIC_NAME --runner DirectRunner --streaming
    ```
   
   ##### For execution on Google Cloud Dataflow:
   ```bash
      python fixed_window.py --input_subscription projects/YOUR_PROJECT_ID/subscriptions/YOUR_SUBSCRIPTION --output_topic=projects/YOUR_PROJECT_ID/topic/TOPIC_NAME --runner DataflowRunner --project YOUR_PROJECT_ID --temp_location gs://YOUR_BUCKET/temp/ --region YOUR_REGION --streaming
   ```

###### **Data**
  ###### Input
```
Customer ID,Purchase Amount,Time Stamp
CO26,66.9,1726644363.1514711
CO81,54.49,1726644364.1664236
CO37,75.47,1726644365.1678798
CO84,39.73,1726644366.1779315
CO78,17.57,1726644367.1802464
CO31,82.59,1726644368.1809685
CO01,29.57,1726644369.1812363
CO53,30.95,1726644370.1966197
CO65,73.99,1726644371.1966197
CO45,28.08,1726644372.1971228
CO00,51.99,1726644373.1971333
CO16,98.79,1726644374.1973805
CO44,11.5,1726644375.2130084
CO42,18.88,1726644376.2134213
CO33,68.15,1726644377.214424
CO85,64.25,1726644378.2290642
CO17,54.87,1726644379.2406187
CO59,80.46,1726644380.2461483
CO94,57.44,1726644381.2463632
CO89,19.77,1726644382.2463639
CO89,78.48,1726644383.2463684
CO26,89.17,1726644384.2617674
CO90,46.67,1726644385.2622404
CO89,17.45,1726644386.277645
CO71,55.78,1726644387.2932718
CO04,54.1,1726644388.30912
CO33,19.94,1726644389.3245234
CO56,17.64,1726644390.3247645
CO92,40.76,1726644391.3249803
CO74,33.12,1726644392.3408446
CO57,31.5,1726644393.3564737
CO22,47.07,1726644394.3718336
CO90,28.43,1726644395.3721552
CO90,21.46,1726644396.3729024
CO23,49.78,1726644397.3739028
CO21,54.55,1726644398.3741174
CO82,65.37,1726644399.3898368
CO63,48.86,1726644400.4053965
CO66,97.46,1726644401.4209983
CO31,46.93,1726644402.4366226
CO12,39.67,1726644403.4366417
CO60,26.21,1726644404.4520283
CO58,55.78,1726644405.4522567
CO84,80.57,1726644406.4523158
CO55,80.52,1726644407.4678383
CO38,66.92,1726644408.4678547
CO07,21.84,1726644409.484921
CO97,43.82,1726644410.4850368
CO29,58.49,1726644411.5005472
CO42,47.13,1726644412.5008678
CO25,55.84,1726644413.5011356
CO96,30.52,1726644414.516755
CO70,14.16,1726644415.5326915
CO33,77.16,1726644416.5482442
CO03,68.67,1726644417.5636263
CO44,87.9,1726644418.563865
CO40,57.68,1726644419.5652342
CO24,54.14,1726644420.5654032
CO43,19.7,1726644421.5654037
CO21,35.87,1726644422.5811017
CO92,47.12,1726644423.596659
CO90,20.02,1726644424.6120763
CO67,84.97,1726644425.6122487
CO91,40.25,1726644426.6124156
CO49,63.83,1726644427.613802
CO11,78.77,1726644428.615341
CO83,54.92,1726644429.6165397
CO74,17.41,1726644430.6165485
CO45,10.99,1726644431.6169877
CO26,66.7,1726644432.6182427
CO15,73.74,1726644433.6182473
CO43,98.72,1726644434.6210315
CO88,54.33,1726644435.624316
CO39,64.61,1726644436.6271715
CO52,53.96,1726644437.6419291
CO50,36.61,1726644438.6538546
CO86,18.8,1726644439.6622036
CO63,28.71,1726644440.6695502
CO05,85.88,1726644441.6762135
CO19,59.83,1726644442.677015
CO85,61.04,1726644443.6782691
CO74,21.97,1726644444.686151
CO65,89.73,1726644445.6881766
CO14,73.28,1726644446.6896672
CO62,13.32,1726644447.7046678
CO05,50.25,1726644448.7069428
CO24,97.38,1726644449.7225478
CO50,77.52,1726644450.7379067
CO21,49.05,1726644451.7383108
CO90,31.26,1726644452.7538562
CO81,30.71,1726644453.7696834
CO86,48.2,1726644454.7851942
CO50,33.26,1726644455.8005733
CO11,44.25,1726644456.8007567
CO61,50.92,1726644457.8030522
CO89,28.05,1726644458.8033297
CO16,34.54,1726644459.818723
CO15,31.24,1726644460.818732
CO09,89.2,1726644461.8204947
CO77,82.92,1726644462.8209548
100,CO67,85.73,1726644463.8209743
CO39,34.1,1726644464.8209915
CO26,20.24,1726644465.8363688
CO94,90.93,1726644466.8366199
CO28,19.91,1726644467.8522193
CO73,56.82,1726644468.8522503
CO10,14.02,1726644469.8531067
CO20,49.71,1726644470.8532302
CO39,82.11,1726644471.8538203
CO45,55.7,1726644472.8694358
CO73,17.68,1726644473.8696907
CO29,33.47,1726644474.8705425
CO33,26.13,1726644475.87145
CO44,52.83,1726644476.8870566
CO25,45.53,1726644477.887063
CO06,83.86,1726644478.9026575
CO67,30.63,1726644479.9026806
CO62,68.8,1726644480.9026895
CO14,81.65,1726644481.9183512
CO12,32.01,1726644482.9339404
CO48,51.93,1726644483.9493241
CO00,69.61,1726644484.950881
CO58,81.95,1726644485.952732
CO56,30.81,1726644486.9539545
CO08,40.74,1726644487.9551446
CO10,88.38,1726644488.9708126
CO82,74.85,1726644489.9863565
CO10,91.33,1726644490.986788
CO31,57.52,1726644492.0023854
CO77,42.26,1726644493.0040529
CO64,47.45,1726644494.0194404
CO44,53.04,1726644495.0201366
CO01,10.28,1726644496.0357697
CO38,40.54,1726644497.0450194
CO23,89.37,1726644498.0604358
CO79,59.12,1726644499.06197
CO43,53.54,1726644500.063256
CO15,53.94,1726644501.0636086
CO88,39.6,1726644502.06398
CO37,98.98,1726644503.0748
CO39,66.21,1726644504.080629
CO98,59.58,1726644505.0815835
CO24,62.49,1726644506.0852103
CO90,70.95,1726644507.100029
CO87,16.73,1726644508.1153462
CO05,72.94,1726644509.1178675
CO47,50.5,1726644510.133457
CO64,29.78,1726644511.134033
CO77,61.18,1726644512.1346438
CO34,42.38,1726644513.1352623

```

  ###### output of CO39:

```
Customer ID:, Purchase Amount, Window Information
CO39:, $64.61, Window: 2024-09-18 07:27:00 - 2024-09-18 07:27:30
CO39:, $116.21000000000001, Window: 2024-09-18 07:27:30 - 2024-09-18 07:28
CO39:, $66.21, Window: 2024-09-18 07:28:00 - 2024-09-18 07:28:30
```



