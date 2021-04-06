import configparser
import os

from datetime import datetime

import pyspark.sql.functions as F
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format, to_date, dayofweek, monotonically_increasing_id

from pyspark.sql.types import StructType as R, StructField as fld, \
                                DoubleType as Dbl, StringType as Str, \
                                IntegerType as Int, DateType as Dat, \
                                TimestampType


config = configparser.ConfigParser()
config.read('dl.cfg')

#os.environ['AWS_ACCESS_KEY_ID']=config['AWS_ACCESS_KEY_ID']
#os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS_SECRET_ACCESS_KEY']


KEY = config.get('AWS', 'KEY')
SECRET = config.get('AWS', 'SECRET')


def create_spark_session():
    """
    Creates a Spark Session with packages
    :return: Spark Session
    """
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    """
    Process the song dataset and creates the song table and artist table\
    :param spark: Spark Session
    :param input_data: path to files to process
    :param output_data: path/to/files to write the results dataset
    """
    # get filepath to song data file
    print('Reading Song_data')
    song_data = input_data + 'song_data/*/*/*/*.json'
    
    #define the song schema
    songSchema = R([
        fld("artist_id", Str()),
        fld("artist_latitude", Dbl()),
        fld("artist_location", Str()),
        fld("artist_longitude", Dbl()),
        fld("artist_name", Str()),
        fld("duration", Dbl()),
        fld("num_songs", Int()),
        fld("song_id", Str()),
        fld("title", Str()),
        fld("year", Int())
    ])
    
    # read song data file
    df = spark.read.json(song_data, schema=songSchema).dropDuplicates().cache()
    
    # Rename fields
    fields=[("artist_id", "artist_id"),
              ("artist_latitude", "latitude"),
              ("artist_location", "location"),
              ("artist_longitude", "longitude"),
              ("artist_name", "name"),
              ("duration", "duration"),
              ("num_songs", "num_songs"),
              ("song_id", "song_id"),
              ("title", "title"),
              ("year",  "year")]
    exprs = ["{} as {}".format(field[0], field[1]) for field in fields]
    dfNamed = df.selectExpr(*exprs)

    # extract columns to create songs table
    print('Creating Songs Table')
    songs_columns = ['title', 'artist_id', 'year', 'duration']
    
    songs_table = dfNamed.select(songs_columns).withColumn('song_id', monotonically_increasing_id()).distinct()
    
    # write songs table to parquet files partitioned by year and artist
    print('Writing to songs table')
    
    songs_data_path = os.path.join(output_data, "songs")    
    songs_table.write.partitionBy("year", "artist_id").parquet(songs_data_path, mode="overwrite")

    # extract columns to create artists table
    print('Creating artists Table')
    artist_columns = ['artist_id', 'name', 'location', 'latitude', 'longitude']
    
    artists_table = dfNamed.select(artist_columns).distinct()
    
    # write artists table to parquet files
    print('Writing to artists table')
    
    artists_data_path = os.path.join(output_data, "artists/")    
    artists_table.write.parquet(artists_data_path, mode="overwrite")

    
    #df.createOrReplaceTempView("song_df_table")


def process_log_data(spark, input_data, output_data):
    """
    Process the log dataset and create user table, time table and songsplat table
    :param spark: SparkSession
    :param input_data: path/to/files to process
    :param output_data: path/to/files to write the results Datasets
    
    """
    # get filepath to log data file
    print('Reading Log Data')
    log_data = input_data + 'log_data/*.json'

    # read log data file
    df = spark.read.json(log_data).dropDuplicates()
    
    # filter by actions for song plays
    df = df.filter(df.page == 'NextSong').cache()
    
    # rename fields
    fields = [("artist", "artist"),
          ("auth", "auth"),
          ("firstName", "first_name"),
          ("gender", "gender"),
          ("itemInSession", "itemInSession"),
          ("lastName", "last_name"),
          ("length", "length"),
          ("level", "level"),
          ("location", "location"),
          ("method", "method"),
          ("page", "page"),
          ("registration", "registration"),
          ("sessionId", "session_id"),
          ("song", "song"),
          ("status", "status"),
          ("ts", "ts"),
          ("userAgent", "user_agent"),
          ("userId", "user_id")
          ]
    exprs = [ "{} as {}".format(field[0],field[1]) for field in fields]
    df = df.selectExpr(*exprs)

    # extract columns for users table    
    print('Creating Users Table')
    users_columns = ['user_id', 'first_name', 'last_name', 'gender', 'level']
    users_table =  df.selectExpr(users_columns).distinct()
    
    # write users table to parquet files
    print('Writing to artists table')
    
    user_data_path = os.path.join(output_data, "users/")    
    users_table.write.parquet(user_data_path, mode="overwrite")
    

    # create timestamp column from original timestamp column
    get_timestamp = udf(lambda x: datetime.fromtimestamp(x/1000).strftime('%Y-%m-%d %H:%M:%S'))
    df = df.withColumn('timestamp', get_timestamp('ts'))

    #df = df.withColumn('start_time', get_datetime('ts'))
    
    # extract columns to create time table
    print('Creating Time Table')
    time_table = df.select(
        col('timestamp').alias('start_time'),
        hour('timestamp').alias('hour'),
        dayofmonth('timestamp').alias('day'),
        weekofyear('timestamp').alias('week'),
        month('timestamp').alias('month'),
        year('timestamp').alias('year'),
        date_format('timestamp', 'u').alias('weekday')).orderBy("start_time").drop_duplicates() 
                                        
                            
    # write time table to parquet files partitioned by year and month
    print('Writing to time table')
    
    time_data_path = os.path.join(output_data, "time/")    
    time_table.write.partitionBy("year", "month").parquet(time_data_path, mode="overwrite")
   

    # read in song data to use for songplays table
    print('Reading Songs Table')
    #song_df = spark.read.parquet(output_data + 'songs/*/*/*.parquet')
    
    song_df = spark.read.json(os.path.join(input_data, 
                                           'song_data/*/*/*/*.json')).selectExpr("song_id",
                                                                                 "title", 
                                                                                 "artist_id", 
                                                                                 "artist_name", 
                                                                                 "year", 
                                                                                 "duration").drop_duplicates()
    
    '''
    print('Creating Songs Play Table')
    songs_logs=df.join(song_df, (df.song == song_df.title))
    songplays_table = songs_logs.join(time_table, 
                                      songs_logs.timestamp == time_table.start_time)\
                                      .drop(songs_logs.year)\
                                      .drop(songs_logs.start_time)\
                                      .withColumn("songplay_id", monotonically_increasing_id())
    '''
    songplays_table = df.join(song_df, 
                           (df.song == song_df.title) & 
                           (df.artist == song_df.artist_name) & 
                           (df.length == song_df.duration) & 
                           (year(df.timestamp) == song_df.year), 'left_outer').select(df.timestamp.alias("start_time"), 
                                                                                    df.user_id,
                                                                                    df.level,
                                                                                    song_df.song_id,
                                                                                    song_df.artist_id,
                                                                                    df.session_id, 
                                                                                    df.location, 
                                                                                    df.user_agent,
                                                                                    year(df.timestamp).alias('year'),
                                                                                    month(df.timestamp).alias('month')).orderBy("start_time", "user_id").withColumn("songplay_id", F.monotonically_increasing_id())


    # extract columns from joined song and log datasets to create songplays table 
    songplays_table_col = ['songplay_id', 'start_time', 'user_id', 'level', 'song_id', 
                           'artist_id', 'session_id', 'location', 'user_agent', 'year', 'month']
    
    songplays_table = songplays_table.select(songplays_table_col).repartition("year", "month")

    # write songplays table to parquet files partitioned by year and month
    print('Writing to songs plays table')
    
    songplay_data_path = os.path.join(output_data, "songplay/")    
    songplays_table.write.partitionBy("year", "month").parquet(songplay_data_path, mode="overwrite")

    print('Done')


def main():
    print('Creating spark session on AWS')
    spark = create_spark_session()
    print('Reading Input Data')
    
    #input_data = ""
    #output_data = "out/"
    
    input_data = "s3a://udacity-dend/"
    output_data = "s3a://deudacity/"
    
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
