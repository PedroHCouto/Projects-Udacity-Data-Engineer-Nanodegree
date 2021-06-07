import findspark
findspark.init()

import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format
import pyspark.sql.functions as F
from pyspark.sql.types import StructType, StructField, IntegerType,\
        StringType, DateType, FloatType, DoubleType
from pyspark.sql.window import Window

#config = configparser.ConfigParser()
#config.read('dl.cfg')
#
#os.environ['AWS_ACCESS_KEY_ID']=config['AWS_ACCESS_KEY_ID']
#os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS_SECRET_ACCESS_KEY']


def create_spark_session():
    """This functions simply creates a spark session 
    with to work local for prototyping and testing.

    Returns:
        spark: an object containing a SparkSession. 
    """
    spark = SparkSession \
        .builder \
        .appName('test')\
        .getOrCreate()
        #.config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        #.getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    """Function to read raw data, about the songs, in json format from S3 putting
    the data in the right format (indicated schema).
    Then, from this first table, the dimensions tables songs_table
    and artists_table are created and saved back into the specified local folder.

    Args:
        spark: SparkSession to handle data using Spark;
        input_data: general path where the data resides locally;
        output_data: folder's path where the new tables will be stored.
    """

    # get filepath to song data file
    song_data = os.path.join(input_data, 'song_data')
    
    # creating a schema for song data
    schema = StructType([StructField('num_songs', IntegerType()),
                         StructField('artist_id', StringType()),
                         StructField('artist_latitude', FloatType()),
                         StructField('artist_longitude', FloatType()),
                         StructField('artist_location', StringType()),
                         StructField('artist_name', StringType()),
                         StructField('song_id', StringType()),
                         StructField('title', StringType()),
                         StructField('duration', DoubleType()),
                         StructField('year', IntegerType())
                         ])
    # read song data file
    df = spark.read.option("recursiveFileLookup", "true").json(song_data, schema = schema)

    # extract columns to create songs table
    columns_songs = ['song_id',
                     'title',
                     'artist_id',
                     'year',
                     'duration']
    
    # create songs_table and drop duplicated rows 
    songs_table = df.selectExpr(columns_songs).dropDuplicates()
    
    # write songs table to parquet files partitioned by year and artist
    songs_table.write.csv(os.path.join(output_data, 'songs_table'), 
                          mode = 'overwrite',
                          header = True)

    # extract columns to create artists table
    columns_artists = ['artist_id',
                       'artist_name as name',
                       'artist_location as location',
                       'artist_latitude as latitude',
                       'artist_longitude as longitude']

    # create artists_table and drop duplicated rows 
    artists_table = df.selectExpr(columns_artists).dropDuplicates()
    
    # write artists table to parquet files
    artists_table.write.csv(os.path.join(output_data, 'artists_table'),
                            mode = 'overwrite',
                            header = True)


def process_log_data(spark, input_data, output_data):
    """Function to read raw data, about logs and songs, in json format from S3 putting
    the data in the right format (indicated schema).
    Then, from this first table, the dimensions tables users_table
    and time_table as well as the fact table songplays_table are created 
    and saved into the specified local folder.

    Args:
        spark: SparkSession to handle data using Spark;
        input_data: general path where the data resides locally;
        output_data: folder's path where the new tables will be stored.
    """

    # get filepath to log data file
    log_data = os.path.join(input_data, 'log_data')

    # read log data file
    df = spark.read.json(log_data)
    
    # filter by actions for song plays
    df = df.where(df.page == 'NextSong')
    # extract columns for users table 
    columns_users = ['userId as user_id',
                     'firstName as first_name',
                     'lastName as last_name',
                     'gender',
                     'level']   

    # create users_table and drop duplicated rows                      
    users_table = df.selectExpr(columns_users).dropDuplicates()
    
    # write users table to parquet files
    users_table.write.csv(os.path.join(output_data, 'users_table'), 
                          mode = 'overwrite',
                          header = True)

    # create timestamp column from original timestamp column
    df = df.withColumn('start_time', F.to_timestamp(df.ts / 1000))
    
    # extract columns to create time table
    columns_time = ['start_time',
                    F.hour('start_time').alias('hour'),
                    F.dayofmonth('start_time').alias('day'),
                    F.weekofyear('start_time').alias('week'),
                    F.month('start_time').alias('month'),
                    F.year('start_time').alias('year'),
                    F.dayofweek('start_time').alias('weekday')]

    # create time table and drop duplicated rows                
    time_table = df.select(columns_time).dropDuplicates()

    # write time table to parquet files partitioned by year and month
    time_table.write.csv(os.path.join(output_data, 'time_table'), 
                         mode = 'overwrite',
                         header = True)


    # read in song data to use for songplays table
    song_data = os.path.join(input_data, 'song_data')
    schema = StructType([StructField('num_songs', IntegerType()),
                         StructField('artist_id', StringType()),
                         StructField('artist_latitude', FloatType()),
                         StructField('artist_longitude', FloatType()),
                         StructField('artist_location', StringType()),
                         StructField('artist_name', StringType()),
                         StructField('song_id', StringType()),
                         StructField('title', StringType()),
                         StructField('duration', DoubleType()),
                         StructField('year', IntegerType())
                         ])
    song_df = spark.read.option("recursiveFileLookup", "true").json(song_data, schema = schema)

    # extract columns from joined song and log datasets to create songplays table
    columns_songplay = ['start_time', 
                        'userId as user_id', 
                        'level', 
                        'song_id', 
                        'artist_id',
                        'sessionId as session_id', 
                        'location',
                        'userAgent as user_agent'] 

    condition = [df.song == song_df.title, df.length == song_df.duration, df.artist == song_df.artist_name]
    songplays_table = df.join(song_df, on = condition, how = 'left_outer').selectExpr(columns_songplay)

    # create songplay_id column
    songplays_table = songplays_table.withColumn('songplay_id',
        F.row_number().over(Window.orderBy(F.monotonically_increasing_id())))

    # write songplays table to parquet files partitioned by year and month
    songplays_table.write.csv(os.path.join(output_data, 'songplays_table'), 
                              mode = 'overwrite',
                              header = True)


def main():
    """Function to create a SparkSession a parses it with input and output paths
    into the functions to process the raw data from the desired local path and 
    save it into the chosen destination.
    """

    spark = create_spark_session()
    input_data = "./data"
    output_data = "./created_tables/"
    
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)

    print('job finished')
    
if __name__ == "__main__":
    main()
