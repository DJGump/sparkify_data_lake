import configparser
from datetime import datetime
import os
# from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col, when, concat_ws, countDistinct
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, dayofweek, date_format, to_date, from_unixtime
from pyspark.sql.types import StringType, DateType, FloatType

config = configparser.ConfigParser()

config.read('dl.cfg')

def create_spark_session():
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
        #.addFile("sparkify_udfs.py")
    
    # potential solution for slow s3 write, issue with older versions of hadoop
    # https://stackoverflow.com/questions/42822483/extremely-slow-s3-write-times-from-emr-spark
    # may not work, trouble accessing sc, and may not affect the running sc
    #sc._jsc.hadoopConfiguration().set("mapreduce.fileoutputcommitter.algorithm.version", "2")
    spark.conf.set("spark.hadoop.mapred.output.committer.class","com.appsflyer.spark.DirectOutputCommitter")
    spark.conf.set("mapreduce.fileoutputcommitter.marksuccessfuljobs", "false")
    return spark


spark = create_spark_session()

input_data = config['Paths']['INPUT_PATH']
output_data = config['Paths']['OUTPUT_PATH']

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

song_table_yearTyped_yearRenamed = song_table_yearTyped.withColumnRenamed('year', 'song_year')

song_table_partitions = song_table_yearTyped_yearRenamed.withColumn('artist_part1', col('artist_id').substr(1,3))

# write songs table to parquet files partitioned by year and artist
song_table_partitions.write.partitionBy("song_year", 'artist_part1').mode('overwrite').parquet(output_data + 'song_table/song_table.parquet')