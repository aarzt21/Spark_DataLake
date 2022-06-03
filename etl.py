import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.types import DoubleType, IntegerType, LongType, StringType, StructType, StructField, TimestampType
from pyspark.sql.functions import udf, col, monotonically_increasing_id
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear



def access():
    '''Setup AWS Access with your credentials'''

    # parse config file
    config = configparser.ConfigParser()
    config.read('dlf.cfg')
    # set environment variables
    os.environ['AWS_ACCESS_KEY_ID']=config['AWS']['KEY']
    os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS']['SECRET']


def create_spark_session():
    '''Create/Override Spark Session
        The SparkContext is the main entry point for Spark functionality and connects the cluster with the application.
    '''

    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.5") \
        .getOrCreate()
    return spark


def create_song_df(spark, song_files):
    """This function
        I. defines schema for songs df
        II. sources raw song data from S3
        III. returns song data as a Spark_DF
    Params:
        spark       : Spark session
        song_files  : path to song data
    """
    # schema
    """If we are reading a text file and want to convert it into a dataframe, we will be required to create a schema for that. 
    By schema it means to provide the column names, datatype of columns and other information."""

    song_schema = StructType([
        StructField('artist_id', StringType(), False),
        StructField('artist_latitude', StringType(), True),
        StructField('artist_longitude', StringType(), True),
        StructField('artist_location', StringType(), True),
        StructField('artist_name', StringType(), False),
        StructField('song_id', StringType(), False),
        StructField('title', StringType(), False),
        StructField('duration', DoubleType(), True),
        StructField('year', IntegerType(), True)
    ])
    # read song data file
    song_df = spark.read.json(song_files, schema=song_schema)
    return song_df


def create_log_df(spark, log_files):
    """This function
        I. defines schema for log data df
        II. sources raw data from S3 bucket
        III. returns log data as a Spark_DF
    Params:
        spark      : Spark session
        log_files  : path to log files containing input data
    """
    # schema
    log_schema = StructType([
        StructField('artist', StringType(), False),
        StructField('auth', StringType(), True),
        StructField('firstName', StringType(), True),
        StructField('gender', StringType(), True),
        StructField('itemInSession', LongType(), True),
        StructField('lastName', StringType(), True),
        StructField('length', DoubleType(), True),
        StructField('level', StringType(), True),
        StructField('location', StringType(), True),
        StructField('method', StringType(), True),
        StructField('page', StringType(), False),
        StructField('registration', DoubleType(), True),
        StructField('sessionId', LongType(), True),
        StructField('song', StringType(), False),
        StructField('status', LongType(), True),
        StructField('ts', LongType(), False),
        StructField('userAgent', StringType(), True),
        StructField('userId', StringType(), False),
    ])
    # read log data file
    log_df = spark.read.json(log_files, schema=log_schema)
    return log_df


def process_song_data(spark, song_df, output_path):
    """This function creates song and artist tables
        and writes them to an S3 bucket
    Params:
        spark            : Spark session
        song_df          : Spark DF
        output_data_dir  : path for outfile
    """
    # get cols to create songs table
    song_fields = ['song_id','title','artist_id','year','duration']
    songs_table = song_df.select(song_fields).dropDuplicates(['song_id'])

    # write songs table to parquet files
    songs_out_path = str(output_path + 'songs/' + 'songs.parquet')
    songs_table.write.parquet(songs_out_path, mode='overwrite', partitionBy=['year', 'artist_id'])

    # get cols to create artists table
    artist_fields = ['artist_id','artist_name','artist_location','artist_latitude','artist_longitude']
    artists_table = song_df.select(artist_fields) \
                    .withColumnRenamed('artist_name','artist') \
                    .withColumnRenamed('artist_location','location') \
                    .withColumnRenamed('artist_latitude','latitude') \
                    .withColumnRenamed('artist_longitude','longitude') \
                    .dropDuplicates(['artist_id'])

    # write artists table to parquet files
    artists_out_path = str(output_path + 'artists/' + 'artists.parquet')
    artists_table.write.parquet(artists_out_path, mode='overwrite')


def process_log_data(spark, song_df, log_df, output_path):
    """ ETL process for Sparkify log data
    This function:
    1. Creates users, time, and songplays tables
    2. Writes new tables to S3
    Parameters:
        spark            : Spark session
        song_df          : Spark dataframe containing song source data
        log_df           : Spark dataframe containing log source data
        output_data_dir  : output path for newly created tables (parquet format)
    """

    print("Processing log data...\n")

    # filter by actions 
    df = log_df.filter(log_df.page == 'NextSong')

    # get cols for users table
    user_fields = ['userId', 'firstName', 'lastName', 'gender', 'level']
    users_table = df.select(user_fields).dropDuplicates(['userId'])

    # write USERS table to parquet files
    users_out_path = str(output_path + 'users/' + 'users.parquet')
    users_table.write.parquet(users_out_path, mode='overwrite')

    # create timestamp column from original ts column
    get_timestamp = udf(lambda x: datetime.fromtimestamp(x/1000), TimestampType())
    df = df.withColumn('timestamp', get_timestamp(df.ts))

    # get cols to create time table
    time_table = df.select(
        col('timestamp').alias('start_time'),
        hour(col('timestamp')).alias('hour'),
        dayofmonth(col('timestamp')).alias('day'),
        weekofyear(col('timestamp')).alias('week'),
        month(col('timestamp')).alias('month'),
        year(col('timestamp')).alias('year')
    ).dropDuplicates(['start_time'])

    # write TIME table to parquet files 
    time_out_path = str(output_path + 'time/' + 'time.parquet')
    time_table.write.parquet(time_out_path, mode='overwrite', partitionBy=['year', 'month'])

    # join song and log datasets
    df = df.join(song_df, (song_df.title==df.song)&(song_df.artist_name==df.artist)) \
        .withColumn('songplay_id', monotonically_increasing_id())

    # get cols from joined song and log datasets to do songplays table
    songplays = df.select(
        col('timestamp').alias('start_time'),
        col('userId'),
        col('level'),
        col('song_id'),
        col('artist_id'),
        col('sessionId'),
        col('location'),
        col('userAgent'),
        year('timestamp').alias('year'),
        month('timestamp').alias('month')
    )

    # write SONGPLAYS table to parquet files partitioned by year and month
    songplays_out_path = str(output_path + 'songplays/' + 'songplays.parquet')
    songplays.write.parquet(songplays_out_path, mode='overwrite', partitionBy=['year', 'month'])


def main():
    '''Executes the entire ETL process'''
    # paths
    song_files = 's3a://udacity-dend/song_data/*/*/*/*.json'
    log_files = 's3a://udacity-dend/log_data/*/*/*.json'
    output_path = 's3a://project4-alex'

    # parse config file and set AWS keys
    access()

    # create Spark session
    spark = create_spark_session()

    # create dfs
    song_df = create_song_df(spark, song_files)
    log_df = create_log_df(spark, log_files)
    #process 
    process_song_data(spark, song_df, output_path)
    process_log_data(spark, song_df, log_df, output_path)


if __name__ == "__main__":
    main()
    print('JOB DONE ........')