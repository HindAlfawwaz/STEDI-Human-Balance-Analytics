#  Data Pipelines with Airflow

## Project Introduction
In this project, As a data engineer for the STEDI team, data lakehouse solution will be built for sensor data that trains a machine learning model.


## Project Details

A music streaming company, Sparkify, has decided that it is time to introduce more automation and monitoring to their data warehouse ETL pipelines and come to the conclusion that the best tool to achieve this is Apache Airflow.

They have decided to bring you into the project and expect you to create high grade data pipelines that are dynamic and built from reusable tasks, can be monitored, and allow easy backfills. They have also noted that the data quality plays a big part when analyses are executed on top the data warehouse and want to run tests against their datasets after the ETL steps have been executed to catch any discrepancies in the datasets.

The source data resides in S3 and needs to be processed in Sparkify's data warehouse in Amazon Redshift. The source datasets consist of JSON logs that tell about user activity in the application and JSON metadata about the songs the users listen to.




## Prerequisites:
Create an IAM User in AWS.
Follow the steps on the page Create an IAM User in AWS in the lesson Data Pipelines.
Configure Redshift Serverless in AWS.
Follow the steps on the page Configure Redshift Serverless in the lesson Airflow and AWS.

## Setting up Connections
Connect Airflow and AWS
Follow the steps on the page Connections - AWS Credentials in the lesson Airflow and AWS.
Use the workspace provided on the page Project Workspace in this lesson.
Connect Airflow to AWS Redshift Serverless
Follow the steps on the page Add Airflow Connections to AWS Redshift in the lesson Airflow and AWS.




## Project Data 
 1. Log data `s3://udacity-dend/log_data`
 2. Song Data  `s3://udacity-dend/song_data`


## Steps 
1. create your own S3 bucket using the AWS Cloudshell :
    ```sh
    aws s3 mb s3://dend-hind/
    ```


3. Copy the data from the udacity bucket to the home cloudshell directory:

    ```
    aws s3 cp s3://udacity-dend/log-data/ ~/log-data/ --recursive
    aws s3 cp s3://udacity-dend/song-data/ ~/song-data/ --recursive
    ```

Copy the data from the home cloudshell directory to  my own bucket 


    ```
    aws s3 cp ~/log-data/ s3://dend-hind/log-data/ --recursive
    aws s3 cp ~/song-data/ s3://dend-hind/song-data/ --recursive
    ```
    
List the data in your own bucket to be sure it copied over:

    ```
    aws s3 ls s3://dend-hind/log-data/
    aws s3 ls s3://dend-hind/song-data/
    ```



## Operators
* Begin_execution & Stop_execution 

Dummy operators representing DAG start and end point

* Stage_events & Stage_songs

Extract and Load data from S3 to Amazon Redshift

* Load_songplays_fact_table & Load_*_dim_table

Load and Transform data from staging to fact and dimension tables

* Run_data_quality_checks

Run data quality checks to ensure no empty tables


## How to Run
Create S3 bucket and copy data from source
Set Up AWS and Airflow Configurations
Run create_tables DAG to create tables in Redshift
Run finaly_project DAG to trigger the ETL data pipeline




## Create an IAM User in AWS
with below access:

* Administrator Access
* AmazonRedshiftFullAccess
* AmazonS3Full Access
