import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format
import pyspark.sql.functions as F
from pyspark.sql.types import StructType, StructField, IntegerType,\
        StringType, DateType, FloatType, DoubleType, LongType
from pyspark.sql.window import Window

config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config['AWS']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS']['AWS_SECRET_ACCESS_KEY']


def create_spark_session():
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    # get filepath to song data file
    song_data = os.path.join(input_data, 'song_data')
    
    # creating a schema for song data
    schema_song = StructType([StructField('num_songs', IntegerType()),
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
    df = spark.read.option("recursiveFileLookup", "true").json(song_data, schema = schema_song)

    # extract columns to create songs table
    columns_songs = ['song_id',
                     'title',
                     'artist_id',
                     'year',
                     'duration']

    songs_table = df.selectExpr(columns_songs)
    
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

    artists_table = df.selectExpr(columns_artists)
    
    # write artists table to parquet files
    artists_table.write.csv(os.path.join(output_data, 'artists_table'),
                            mode = 'overwrite',
                            header = True)


def process_log_data(spark, input_data, output_data):
    # get filepath to log data file
    log_data = os.path.join(input_data, 'log_data')

    # creating a schema for song data
    schema_log = StructType([StructField('artist', StringType()),
                             StructField('auth', StringType()),
                             StructField('firstName', StringType()),
                             StructField('gender', StringType()),
                             StructField('itemInSession', LongType()),
                             StructField('lastName', StringType()),
                             StructField('length', DoubleType()),
                             StructField('level', StringType()),
                             StructField('location', StringType()),
                             StructField('method', StringType()),
                             StructField('page', StringType()),
                             StructField('registration', DoubleType()),
                             StructField('sessionId', LongType()),
                             StructField('song', StringType()),
                             StructField('status', LongType()),
                             StructField('ts', LongType()),
                             StructField('userAgent', StringType()),
                             StructField('userId', StringType()),
                             ])

    # read log data file
    df = spark.read.json(log_data, schema = schema_log)

    # filter by actions for song plays
    df = df.where(df.page == 'NextSong')
    # extract columns for users table 
    columns_users = ['userId as user_id',
                     'firstName as first_name',
                     'lastName as last_name',
                     'gender',
                     'level']   
    users_table = df.selectExpr(columns_users)
    
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
    time_table = df.select(columns_time)

    # write time table to parquet files partitioned by year and month
    time_table.write.csv(os.path.join(output_data, 'time_table'), 
                         mode = 'overwrite',
                         header = True)


    # read in song data to use for songplays table
    song_data = os.path.join(input_data, 'song_data')

     # creating a schema for song data
    schema_song = StructType([StructField('num_songs', IntegerType()),
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
    song_df = spark.read.option("recursiveFileLookup", "true").json(song_data, schema = schema_song)

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
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    output_data = "s3://data-lakes-spark-project/created_tables_S3/"
    
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)

        
if __name__ == "__main__":
    main()