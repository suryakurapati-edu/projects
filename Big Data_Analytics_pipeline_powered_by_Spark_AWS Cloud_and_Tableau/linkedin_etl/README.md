README.txt
===========

LinkedIn Job Analytics - Big Data ETL Pipeline on AWS
-----------------------------------------------------

Overview:
---------
This project implements a layered data lake architecture using Apache Spark on Amazon EMR to ingest, transform, and store LinkedIn job postings data from Kaggle. The pipeline processes data through raw, processed, and consumption zones using PySpark and stores results in Amazon RDS for downstream consumption in Tableau dashboards.

Directory Structure:
--------------------
linkedin_etl/
├── main.py                    # Main ETL execution script
├── job_config/
│   └── linkedin_postings.conf  # Configuration file for job-specific parameters
├── sql/
│   └── linkedin_postings.sql   # SQL transformation logic
├── logs/                      # Auto-generated logs directory
└── README.txt                 # This file

Technologies Used:
------------------
- Apache Spark 3.5.5 on Amazon EMR 7.9.0
- PySpark for ETL logic
- Amazon S3 for data lake storage
- Amazon RDS (MySQL) for final consumption layer
- Tableau for BI and dashboarding
- MySQL Connector for table creation
- ConfigParser and Logging modules in Python

Data Flow:
----------
1. Raw Zone: Kaggle datasets (CSV format) ingested into S3 (e.g., s3a://raw-zone-layer/postings/).
2. Processed Zone: PySpark transformations applied on EMR and results written as Parquet to S3 (e.g., s3a://processed-zone-layer/).
3. Consumption Zone: Final curated data written to MySQL (Amazon RDS) and used by Tableau dashboards.

Setup Instructions:
-------------------
1. Launch AWS EMR cluster (v7.9.0) with Spark, Hadoop, Hive, and Hue.
2. Configure inbound rules to allow SSH access from your IP.
3. SSH into the EMR master node:
   ssh -i project-linkedin-emr.pem hadoop@ec2-16-16-122-117.eu-north-1.compute.amazonaws.com
4. SCP project files:
   scp -i project-linkedin-emr.pem -r linkedin_etl hadoop@ec2-16-16-122-117.eu-north-1.compute.amazonaws.com:/home/hadoop/
5. Set up Amazon RDS (MySQL) instance and note host, username, password, and database info.
6. Modify the config file `linkedin_postings.conf` with correct paths and credentials.

Execution:
----------
1. SSH into EMR and navigate to the project folder:
   cd /home/hadoop/linkedin_etl/
2. Submit the Spark job:
   spark-submit main.py linkedin_postings

This will:
- Read raw CSVs from S3
- Apply SQL-based joins and transformations
- Validate and deduplicate based on config
- Write Parquet to S3 (Processed Zone)
- Create table in RDS if not exists
- Write final dataset to RDS table

Key Features:
-------------
- Modular and reusable ETL logic with class-based architecture
- Config-driven pipeline (e.g., file paths, keys, SQL)
- SQL logic decoupled from code via external .sql files
- Logging mechanism with time-stamped logs per run
- Automatic schema-based MySQL table creation
- Structured data ready for BI tools like Tableau

Dependencies:
-------------
- Python 3.x
- PySpark
- MySQL Connector (`pip install mysql-connector-python`)
- AWS EMR Cluster
- Amazon RDS MySQL Instance

Credits:
--------
Dataset Source: 
- Kaggle (LinkedIn Job Postings) - https://www.kaggle.com/datasets/arshkon/linkedin-job-postings?resource=download
- United States Zip State Mapping - https://rowzero.io/workbook/29B539E0CC92758496152C9C/74
Author: Surya Chandra Raju Kurapati
