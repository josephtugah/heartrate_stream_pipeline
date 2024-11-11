# heartrate_stream_pipeline
An end-to-end ELT pipeline to store simulated heart rate data inside a data warehouse; uses Kafka for real-time processing, Airbyte for data integration, Apache Spark for transformation, Dagster for orchestration, and Power BI for visualization

# Heart Rate Stream to Data Warehouse

![heartbeat](https://github.com/user-attachments/assets/836f7668-b288-4913-95b4-038b9f55ffed)


## Table of Contents
- [Overview](#overview)
- [Project Components](#project-components)
  - [Synthetic OLTP Data Generation](#1-synthetic-oltp-data-generation)
  - [Simulated Heart Rate Stream](#2-simulated-heart-rate-stream)
  - [Streaming to Kafka](#3-streaming-to-kafka)
  - [Sinking to GCS](#4-sinking-to-GCS)
  - [Ingestion with Airbyte](#5-ingestion-with-airbyte)
  - [Transformation with Apache Spark](#6-tranformation-with-spark)
  - [Orchestration with Dagster](#7-orchestration-with-dagster)
  - [Visualization](#8-visualization)
- [Implementation Screenshots](#implementation-screenshots)
- [Limitations and Next Steps](#limitations-and-next-steps)
  - [Known Issues](#known-issues)



## Overview
This project implements a data pipeline for historical analysis of heart rate data, primarily catering to medical and research specialists studying cardiovascular health. 

The pipeline begins with the generation of synthetic heart rate data, which is streamed to a Kafka topic for real-time processing and sinked to a cloud data lake for persistant storage. 

The data is then incrementally ingested into a data warehouse where it is transformed and stored in a final state tailored for efficient querying and analysis.

The following are examples of research questions that could be answered with the warehoused data:

- How does heart rate vary across different activities for users of different demographics and/or regions?

- Can historical heart rate data provide insights into the impact of seasonal variations on heart rate patterns among different users?

- Can historical heart rate data be used to predict the likelihood of specific activities occurring at certain times of the day within different regions?


## Project Components:
<img width="935" alt="Architecture" src="https://github.com/user-attachments/assets/e5593b78-1fe7-439e-8526-cb6b5bcf8d13">


### 1. Synthetic OLTP Data Generation 

Synthetic operational data is generated (users data, activities data) that will be used to provide further context to each heart rate stream record. 

The users and activities data were generated using the Faker library inside custom python scripts that can be adjusted and re-used when needed. Each script generates a pandas DataFrame object and uploads the object as a static CSV file. The CSV data is stored in a GCP bucket.

Further information with regards to the data may be found inside the `mock-data/static` directory.

To run the python scripts and re-create the CSV files:

`python -m mock-data.static.scripts.generate-activities`

`python -m mock-data.static.scripts.generate-users`

### 2. Simulated Heart Rate Stream

Synthetic heart rate data is generated to simulate real-world scenarios, such as heart rate data measured by an Apple Watch device. 

A `producer.py` script has been developed to act as a Kafka producer, leveraging the `confluent-kafka` Python library to generate the heart rate data. This script reads the already generated users and activities CSV files to pick out a random activity ID and user ID, sending it along with a randomly-generated heart rate to the Kafka topic. 

The script has been containerized inside a Docker image using a `Dockerfile` located in the root folder of the project, which is then pushed into Amazon ECR and ran using a continuously-running ECS task.

The Faker library is again used inside the `producer.py` script to generate a random latitude and longitude coordinates based on the user's address country. This as well as a timestamp column (indicating the event time) are added to the heart rate data record and sent as a JSON object to the Kafka topic for processing. 

Heart rate data format: 

```
{
 user_id: 10001, 
 heart_rate: 128, 
 timestamp: 1711943972, 
 meta: {
  activity_id: 20890,
  location: {latitude: 37.416834, longitude: -121.975002}
 }
}
```

Both JSON and AVRO formats were considered for sending heart rate records, however JSON was chosen as it incurred lower storage costs compared to AVRO when transmitting heart rate data over the network.

To run a custom made script to generate JSON and AVRO files with mock heart rate data:  

`python -m docs.performance-analysis.stream.compare-avro-json`


### 3. Streaming to Kafka

A Kafka cluster and topic were established for real-time data ingestion using GCP Managed Kafka Service. The topic was divided into 6 partitions, enabling Kafka to parallelize the stream processing and allow for scalability (not truly necessary as data was written synchronously inside the `producer.py` script). The default value of '1 Week' was selected as the retention period for each message. 

GCP's Datastream API faciilated the efficient data transfer from the kakfka topic to a GCS bucket as a sink. An IAM role was configured to granted the compute engine default service account the necessary permissions to write data to the GCS bucket. 


Kafka topic data lineage: 


### 4. Sinking to GCS

The GCS bucket partitions and stores the streamed data by its event time (YYYY/MM/DD/HH directory format). Then, within each directory, a separate JSON file is created for each partition which stores the stream data relevant to that specific partition. 

The decision to use a cloud data lake to persistently store the raw streaming data enables reusability of the data for subsequent workflows, such as machine learning pipelines operating on raw data. Additionally, if data is corrupted somewhere within the load or transformation part of the ELT pipeline, it is always possible revert to the original, unaltered data for reprocessing or troubleshooting purposes.

<img width="939" alt="GCS lake" src="https://github.com/user-attachments/assets/927db965-9472-459e-925d-08b4ccaac099">

### 5. Ingestion with Airbyte into BigQuery

An Airbyte instance is used to ingest the static activity and user data of patients which was stored in a GCS bucket and the unbounded S3 stream data into the data warehouse hosted on GCS.

For the GCS to Bigquery sync, first a GCS source is established with the following configurations:

```
| Config Name                      | Config Value     |
|----------------------------------|------------------|
| Bucket                           | <name-of-bucket> |
| Format                           | Jsonl format     |
| Name                             | heart_rate_stream|
| Globs                            | **               |
| Days to sync if history is full  | 3                |
| Input schema                     | N/A              |
| Schemaless                       | Unselected       |
| Validation policy                | Emit record      |
```

The selected replication sync mode for the GCS -> Bigquery connection is `Incremental | Append` with the Airbyte-generated `_ab_source_file_last_modified` column used as the cursor field. Inside GCS's Identity Access Management, a separate *airbyte-stream-reader* user was created with read access to the GCS bucket. 

<img width="954" alt="Airbyte" src="https://github.com/user-attachments/assets/9afbb07c-227c-46c2-a0b6-825b0db76487">


For the static user and activity data to Bigquery sync, datasets were created in BigQuery warehouse and the user_activity_lake was set up as a course and loaded into tables in the Bronze layer of the Bigquery warehouse

Manual replication mode is used for the airbyte ingestion as the decision of when to trigger the sync is left to the selected data orchestration tool (Dagster).

Below is all the ingested data in the BigQuery Warehouse bronze layer.

<img width="915" alt="bronze layer" src="https://github.com/user-attachments/assets/6a481eca-7b17-427f-a8ac-aa7bcc43cad8">

### 6. Transformation with Apache Spark
The data loaded from Airbyte into Bigquery forms the Bronze layer. This data is then read and undergoes the following transformations before forming the Silver layer:
- Flatten JSON fields
- Deduplication
- Field rename
- Data type casting

Before the data is loaded into the Silver layer, data quality checks were performed using Great Expectations, ensuring that:
- Primary keys and foreign keys are not null
- Values are within the expected range (for example, latitude and longitude are between [-90, 90] and [-180, 180], respectfully)
- Values are within the expected set (for example, data includes races for years between 1950 and 2023)

When loading data into the Silver layer, it is important to note that:
- Surrogate keys are created

Lastly, the Gold layer is created by reading data from the Silver layer and performing additional transformations such as `joins`, `field renaming` (to follow business definitions or requirements), `calculations`, and so on.

![erd (2)](https://github.com/user-attachments/assets/ef8cfbd3-a50b-4f88-99b2-6561d71231a4)
