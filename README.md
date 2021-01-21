# Data Pipeline with Apache Airflow

This Project creates a Data Pipeline  using Apache Airflow.
The source data consists of log files from a song streaming platform and is located in an AWS S3 bucket. The goal is to store the relevant information in an Amazon Redshift datawarehouse.
The final dimensional architecture follows the star schema and consist of one Fact Table (songplays) and four Dimension Tables (users,songs,artists and time).
The design is used to optimize for analytical queries and allows to efficiently store the data in Redshift.

## Project Files
The files in the repository are used as described below:

* **[ETL_Dag.py](ETL_Dag.py)**: Contains the main data pipeline and the airflow dag.
* **[create_tables.sql](create_tables.sql)**: SQL commands to create the dimensional model.
* **[test_staging_dag.py](test_staging_dag.py)**: Test file for debugging purposes.

## Final Dimensional Database Schema:

<img src="./databasedesign.PNG" width="500" height="500">

## Datasets:

The used datasets are located on an S3 bucket and are stored in json format. 
The song_data is a subset from the [Million Song Dataset](http://millionsongdataset.com/) and the log_data contains streaming information of a music streaming platform.

## Airflow Pipeline:
<img src="./airflow_pipeline.PNG" width="800" height="300">


## Setup and Execution

The following setup is required:
* Apache Airflow
* Amazon Redshift Data Warehouse (AWS) and the corresponding connection keys
* The pipeline can be launched in Airflow by starting the DAG **S3_to_Redshift_ETL** which is defined in [ETL_Dag.py](ETL_Dag.py)