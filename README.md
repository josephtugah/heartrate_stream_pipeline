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
  - [Sinking to S3](#4-sinking-to-s3)
  - [Ingestion with Airbyte](#5-ingestion-with-airbyte)
  - [Transformation with Apache Spark](#6-tranformation-with-spark)
  - [Orchestration with Dagster](#7-orchestration-with-dagster)
  - [Visualization](#8-visualization)
- [Implementation Screenshots](#implementation-screenshots)
- [Limitations and Next Steps](#limitations-and-next-steps)
  - [Known Issues](#known-issues)
