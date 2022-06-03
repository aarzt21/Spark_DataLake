### Introduction
A music streaming startup, Sparkify, has grown their user base and song database even more and want to move their data warehouse to a data lake. 
Their data resides in S3, in a directory of JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs in their app.

The task is to
1. Build ETL pipeline that extracts the data from S3
2. Processes the data using Spark (deployed on an AWS cluster)
3. Loads the data back into S3 as a set of dimensional Tables.
 
 This will allow their analytics team to continue finding insights in what songs their users are listening to.


### Schema for Song Play Analysis
Using the song and log datasets, a star schema is created, optimized for queries on song play analysis.

#### Fact Table

**songplays** - records in log data associated with song plays i.e. records with page NextSong
        songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent

#### Dimension Tables

**users** - users in the app
        user_id, first_name, last_name, gender, level

**songs** - songs in music database
        song_id, title, artist_id, year, duration

**artists** - artists in music database
        artist_id, name, location, lattitude, longitude

**time** - timestamps of records in songplays broken down into specific units
        start_time, hour, day, week, month, year, weekday

### Project Structure
#### etl.py
As the name suggests, the purpose of this script is to implement the ETL task. 
Precisely, it loads the JSON files into staging tables before
it inserts it into the into the fact and dimension tables. 

The functions found in the script are:

- ```create_spark_session()```: creates the Spark Session that performs the operations on the data
- ```process_log_data(spark, input_data, output_data)```: reads the log data from S3 and process the data to create the tables *users*, *times* and *songplays*. 
                                                            Further, it also saves the tables (in parquet format) back to S3.
- ```process_song_data(spark, input_data, output_data)```: reads the song data from S3 bucket and transforms it in order to create the *songs* and *artists* tables. Later the tables are 
                                                            uploaded back to S3 (in parquet format).
- ```main()```: is the function that defines how the process occurs.