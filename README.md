## Project Overview

A music streaming startup, Sparkify, has grown their user base and song database and want to move their processes and data onto the cloud. Their data resides in S3, in a directory of JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs in their app.

In this project, we will create an ETL pipeline to build a data warehouses hosted on Redshift. 
<div align="center">
<img src=images/redshift_target.png >
<p>Target Redshift AWS Architecture</p>
</div>

## Summary
* [Project structure](#Structure)
* [Datasets](#Datasets)
* [Analytics](#analytics)
* [Schema](#Schema)


#### Structure

* <b> /images </b> - some screenshots.
* <b> Analytics.ipynb </b> - It is a notebook containing basic analytics on the datawarehouse.
* <b> create_cluster.ipynb </b> - It is a notebook containing code to create a redshift cluster.
* <b> create_cluster.py </b> - A script to create a redshift cluster.
* <b> create_tables.py </b> - A script to drop and create tables.
* <b> etl.py </b> - A script to load data from s3 to stagging tables and then to fact and dim tables using the given dataset on S3.
* <b> sql_queries.py </b> - A script containing sql queries.
* <b> dwh.cfg </b> - Configuration file to add AWS credentials.
* <b> delete_cluster.py </b> - A script to delete the redshift cluster.

## Datasets

You'll be working with two datasets that reside in S3. Here are the S3 links for each:

* <b> Song data </b> - s3://udacity-dend/song_data
* <b> Log data </b> - s3://udacity-dend/log_data
* <b> Log data json path </b> - s3://udacity-dend/log_json_path.json

## Schema

#### Fact Table
songplays - records in event data associated with song plays. Columns for the table:

    songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent

#### Dimension Tables 
##### users

    user_id, first_name, last_name, gender, level
##### songs

    song_id, title, artist_id, year, duration

##### artists

    artist_id, name, location, lattitude, longitude

##### time

    start_time, hour, day, week, month, year, weekday


