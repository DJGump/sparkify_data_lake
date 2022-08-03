import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col, when
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format
from pyspark.sql.types import StringType, DateType, FloatType
import sparkify_udfs

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
    songs_table = df.select(['song_id', 'title', 'year', 'duration']).distinct()

    song_table_yearTyped = songs_table.withColumn('year', when(songs_table['year'] != 0, year(songs_table['year'].cast('string'))).\
                                                                otherwise(0)
                                                    )
        
    # write songs table to parquet files partitioned by year and artist
    song_table_yearTyped.write.partitionBy("year").mode('overwrite').parquet('s3://dgump-spark-bucket/analytics/song_table.parquet')

    #s3://dgump-spark-bucket/analytics/

    # extract columns to create artists table
    artists_table = 
    
    # write artists table to parquet files
    artists_table


def process_log_data(spark, input_data, output_data):
    # get filepath to log data file
    log_data =

    # read log data file
    df = 
    
    # filter by actions for song plays
    df = 

    # extract columns for users table    
    users_table = 
    
    # write users table to parquet files
    users_table

    # create timestamp column from original timestamp column
    get_timestamp = udf()
    df = 
    
    # create datetime column from original timestamp column
    get_datetime = udf()
    df = 
    
    # extract columns to create time table
    time_table = 
    
    # write time table to parquet files partitioned by year and month
    time_table

    # read in song data to use for songplays table
    song_df = 

    # extract columns from joined song and log datasets to create songplays table 
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
