import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col, when
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format
from pyspark.sql.types import StringType, DateType, FloatType
from sparkify_udfs import *

# not really sure what this is for...
# config = configparser.ConfigParser()

# config.read('dl.cfg')

# os.environ['AWS_ACCESS_KEY_ID']=config['Secrets']['aws_access_key_id']
# os.environ['AWS_SECRET_ACCESS_KEY']=config['Secrets']['AWS_SECRET_ACCESS_KEY']


def create_spark_session():
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    # get filepath to song data file
    song_data = input_data + "song_data/*/*/*/*.json"

    # read song data file
    df = spark.read.json(song_data)

    # extract columns to create songs table
    # song_id, title, artist_id, year, duration
    songs_table = df.select(['song_id', 'title', 'artist_id', 'year', 'duration']).distinct()

    song_table_yearTyped = songs_table.withColumn('year', when(songs_table['year'] != 0, year(songs_table['year'].cast('string'))).\
                                                                otherwise(0)
                                                    )
        
    # write songs table to parquet files partitioned by year and artist
    song_table_yearTyped.write.partitionBy("year", "artist_id").mode('overwrite').parquet('s3://dgump-spark-bucket/analytics/song_table.parquet')

    # extract columns to create artists table
    # artist_id, name, location, lattitude, longitude
    artists_table = df.select(['artist_id', 'artist_name','num_songs', 'artist_location', 'artist_latitude', 'artist_longitude'])

    # drop dupes, found by investigating artist_id
    artists_table_deduped = artists_table.distinct()
    
    # write artists table to parquet files
    artists_table_deduped.write.partitionBy('artist_id').mode('overwrite').parquet('s3://dgump-spark-bucket/analytics/artists_table.parquet')


def process_log_data(spark, input_data, output_data):
    # get filepath to log data file
    log_data = input_data + "log-data/*.json"

    # read log data file
    df = spark.read.json(log_data)
    
    # filter by actions for song plays
    df_song_plays = df.select('*').where(col('page') == 'NextSong')

    # extract columns for users table
    # user_id, first_name, last_name, gender, level    
    users_table = df_song_plays.select(['userID', 'firstName', 'lastName', 'gender', 'level']).distinct()
    
    # write users table to parquet files
    users_table.write.partitionBy('userID').mode('overwrite').parquet('s3://dgump-spark-bucket/analytics/users_table.parquet')

    # create timestamp column from original timestamp column
    get_timestamp = udf()
    df = 
    
    # create datetime column from original timestamp column
    get_datetime = udf()
    df = 
    
    # extract columns to create time table
    time_table = 
    
    # write time table to parquet files partitioned by year and month
    # start_time, hour, day, week, month, year, weekday
    time_table

    # read in song data to use for songplays table
    song_df = 

    # extract columns from joined song and log datasets to create songplays table
    # songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent 
    songplays_table = 

    # write songplays table to parquet files partitioned by year and month
    songplays_table 


def main():
    spark = create_spark_session()
    input_data = "s3a://dgump-spark-bucket/project_data_small/"
    
    output_data = ""
    
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
