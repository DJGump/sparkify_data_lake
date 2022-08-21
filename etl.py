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

os.environ['AWS_ACCESS_KEY_ID']=config['Secrets']['aws_access_key_id']
os.environ['AWS_SECRET_ACCESS_KEY']=config['Secrets']['AWS_SECRET_ACCESS_KEY']

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
    return spark


def get_timestamp(ts):
    # """
    # converts timestamp from miliseconds to seconds, then to a datetime.
    # Assumes input is of type int, and is a timestamp in miliseconds.
    # """
    ts_seconds = ts // 1000
    ts_seconds_str = str(ts_seconds)
    return ts_seconds_str


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
    
    song_table_yearTyped_yearRenamed = song_table_yearTyped.withColumnRenamed('year', 'song_year')

    song_table_partitions = song_table_yearTyped_yearRenamed.withColumn('artist_part1', col('artist_id').substr(1,3))

    # write songs table to parquet files partitioned by year and artist
    song_table_partitions.write.partitionBy("song_year", 'artist_part1').mode('overwrite').parquet(output_data + 'song_table/song_table.parquet')

    # extract columns to create artists table
    # artist_id, name, location, lattitude, longitude
    artists_table = df.select(['artist_id', 'artist_name','num_songs', 'artist_location', 'artist_latitude', 'artist_longitude'])

    # drop dupes, found by investigating artist_id
    artists_table_deduped = artists_table.distinct()

    artists_table_partitions = artists_table_deduped.withColumn('artist_part1', col('artist_id').substr(1,3))\
                                                    .withColumn('artist_part2', col('artist_id').substr(4,1))
    
    # write artists table to parquet files
    artists_table_partitions.write.partitionBy('artist_part1', 'artist_part2').mode('overwrite').parquet(output_data + 'artists_table/artists_table.parquet')

    return


def process_log_data(spark, input_data, output_data):
    # get filepath to log data file
    log_data = input_data + "log-data/*.json"

    # read log data file
    log_df = spark.read.json(log_data)
    
    # filter by actions for song plays
    df_song_plays = log_df.select('*').where(col('page') == 'NextSong')

    # extract columns for users table
    # user_id, first_name, last_name, gender, level    
    users_table = df_song_plays.select([
                                    col('userID').alias('user_id'), 
                                    col('firstName').alias('first_name'), 
                                    col('lastName').alias('last_name'), 
                                    'gender', 
                                    'level']).distinct()
    
    users_table_partitions = users_table.withColumn('user_part1', col('first_name').substr(1,1))\
                                        .withColumn('user_part2', col('last_name').substr(1,1))

    # write users table to parquet files
    users_table_partitions.write.partitionBy('user_part1', 'user_part2').mode('overwrite').parquet(output_data + 'users_table/users_table.parquet')

    

    # create datetime column from original timestamp column
    get_timestampUDF = udf(lambda x: get_timestamp(x), StringType())
    df_song_plays_datetime = df_song_plays.withColumn('datetime', from_unixtime(get_timestampUDF(col('ts'))))
    
    

    # extract columns to create time table
    # start_time, hour, day, week, month, year, weekday
    time_table = df_song_plays_datetime.select(
                                            'datetime',
                                            col('ts').alias('unix_timestamp'),
                                            date_format('datetime', 'HH:mm:ss').alias('start_time'),
                                            hour('datetime').alias('hour'),
                                            dayofmonth('datetime').alias('day'),
                                            weekofyear('datetime').alias('week'),
                                            month('datetime').alias('month'),
                                            year('datetime').alias('year'),
                                            dayofweek('datetime').alias('weekday')
    ).distinct()

    # write time table to parquet files partitioned by year and month
    # this table seems silly...
    #...but what do i know? maybe it saves processing? joins are cheaper than parsing?
    time_table.write.partitionBy(['year', 'month']).mode('overwrite').parquet(output_data + 'time_table/time_table.parquet')

    # read in song data to use for songplays table
    song_data = output_data + "/song_table/song_table.parquet"
    song_df = spark.read.parquet(song_data)

    # extract columns from joined song and log datasets to create songplays table
    # songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent 

    # join song_table.title on log.song
    songplays_table_songs = df_song_plays.join(song_df, df_song_plays.song == song_df.title, how='left')

    songplays_table_songs_time = songplays_table_songs.join(time_table, time_table.unix_timestamp == songplays_table_songs.ts, how='left')

    songplays_table_songs_time_playID = songplays_table_songs_time.withColumn('songplay_id', concat_ws("+", col('sessionID').cast(StringType()), col('itemInSession').cast(StringType())))
    #+ 'itemInSession')

    songplays_table = songplays_table_songs_time_playID.select(
                                                        'songplay_id',
                                                        'start_time',
                                                        'year',
                                                        'month',
                                                        col('userId').alias('user_id'),
                                                        'level',
                                                        'song_id',
                                                        'artist_id',
                                                        col('sessionId').alias('session_id'),
                                                        'location',
                                                        col('userAgent').alias('user_agent')
    )

    # write songplays table to parquet files partitioned by year and month
    songplays_table.write.partitionBy(['year', 'month']).mode('overwrite').parquet(output_data + 'songplays_table/songplays_table.parquet')


def main():
    spark = create_spark_session()
    input_data = config['Paths']['INPUT_PATH']

    
    output_data = config['Paths']['OUTPUT_PATH']
    
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
