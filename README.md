# Sparkify Data Warehouse on Amazon Redshift

Sparkify, a music streaming startup, has grown their user base and song database and want to move their processes and data onto the cloud. Their data resides in S3, in a directory of JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs in their app.

In this project, we will create an ETL pipeline to build a data warehouses hosted on Redshift. 

## Project Datasets

We will be working with two datasets that reside in S3:

* Song data: s3://udacity-dend/song_data
* Log data: s3://udacity-dend/log_data

#### Song Dataset

A subset of real data from [Million Song Dataset](http://millionsongdataset.com/). Each file is in JSON format and contains metadata about a song and the artist of that song. The files are partitioned by the first three letters of each song's track ID.

Sample Data:

    {"num_songs": 1, "artist_id": "ARJIE2Y1187B994AB7", "artist_latitude": null, "artist_longitude": null, "artist_location": "", "artist_name": "Line Renaud", "song_id": "SOUPIRU12A6D4FA1E1", "title": "Der Kleine Dompfaff", "duration": 152.92036, "year": 0}

#### Log Dataset

This dataset consists of log files in JSON format generated by [this event simulator](https://github.com/Interana/eventsim) based on the songs in the dataset above. These simulate app activity logs from an imaginary music streaming app based on configuration settings.

The log files in the dataset are partitioned by year and month.

Sample Data:

    {"artist":null,"auth":"Logged In","firstName":"Celeste","gender":"F","itemInSession":0,"lastName":"Williams","length":null,"level":"free","location":"Klamath Falls, OR","method":"GET","page":"Home","registration":1541078e+12,"sessionId":438,"song":null,"status":200,"ts":1541990217796,"userAgent":"\"Mozilla\/5.0 (Macintosh; Intel Mac OS X 10_9_4) AppleWebKit\/537.36 (KHTML, like Gecko) Chrome\/36.0.1985.143 Safari\/537.36\"","userId":"53"}


## Schema for song play analysis

![database_schema](/database_schema.png)

#### Fact Table

*Songplay_fact* - records in event data associated with song plays.

    songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent

#### Dimension Tables
 
*User_dim*
 
    user_id, first_name, last_name, gender, level

*Song_dim*

    song_id, title, artist_id, year, duration

*Artist_dim* - records in event data associated with song plays.

    artist_id, name, location, lattitude, longitude
 
*time_dim* - records in event data associated with song plays.

    start_time, hour, day, week, month, year, weekday


## Project Structure

1. sql_queries.py - contains all the SQL queries used to `CREATE` and `DROP` the database tables, `COPY` data from `S3`, and `INSERT` into data into the tables.
2. create_tables.py - connects to the database, creates the staging tables and the fact and dimension tables.
3. etl.py - extracts `JSON` data from the `S3 bucket` and ingest it into `Redshift`. 
4. dwh_example.cfg - contains the configurations about the `CLUSTER`, `IAM ROLE` and `S3`.
5. queries.ipynb - 


## How to run


#### Prerequisites

**python3** is required to run this project.

**jupyter** 

**ipython-sql** - to make Jupyter Notebook and SQL queries to AWS Redshift work together

    $ pip3 install ipython-sql


#### Cluster setup 

* Create a new IAM user in your AWS account
* Attach the AmazonS3ReadOnlyAccess policy to the role
* Create a new security group, choose the default VPC 
* Add a rule to the security group to allow incoming connections to the redshift cluster on port 5439 from anywhere in the world
* Create a Redshift Cluster:
    - Recommended type: dc2.large with 2 nodes
    - Region: US-West-2
* Use the cluster endpoint and the Iam role ARN to fill up the configurations in dwh.cfg 


To create the Staging and Analytic tables, run:

    $ python3 create_tables.py

To load the data into the tables, run:

    $ python3 etl.py
    
To run analytical queries on the data:

    $ pip3 install ipython-sql
    $ jupyter notebooks 