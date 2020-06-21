import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col, monotonically_increasing_id
from pyspark.sql.functions import year, month, dayofmonth, dayofweek,hour, weekofyear, date_format
from pyspark.sql.types import StructType as R, StructField as Fld, DoubleType as Dbl, StringType as Str, IntegerType as Int, DateType as Dat, TimestampType



try:
    config = configparser.ConfigParser()
    config.read('dl.cfg')
except Exception as e:
    print(e)


try:
    os.environ['AWS_ACCESS_KEY_ID']=config['AWS']['AWS_ACCESS_KEY_ID']
    os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS']['AWS_SECRET_ACCESS_KEY']
except Exception as e:
    print(e)


def create_spark_session():
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    """This function loads song_data from S3 and processes it by extracting the songs and artist tables
        and then again loaded back to S3
    Args:
        spark(:obj:`pyspark.sql.session.SparkSession`): SparkSession
        input_data (str): S3 bucket where song files are stored
        output (str): S3 bucket file path to store resulting files

    Returns:
        None
    """
    print("**** Starting to process song data *****")
    # get filepath to song data file
    song_data = input_data+'song_data/*/*/*/*.json'
    
    # read song data file
    
    songSchema = R([
        Fld("artist_id",Str()),
        Fld("artist_latitude",Dbl()),
        Fld("artist_location",Str()),
        Fld("artist_longitude",Dbl()),
        Fld("artist_name",Str()),
        Fld("song_id",Str()),
        Fld("duration",Dbl()),
        Fld("num_songs",Int()),
        Fld("title",Str()),
        Fld("year",Int()),
    ])
    
    
    try:
        df = spark.read.json(song_data, schema=songSchema)
    except Exception as e:
        print(e)
        
    # extract columns to create songs table
    songs_fields = ["song_id", "title", "artist_id", "year", "duration"]
    songs_table = df.select(songs_fields).dropDuplicates(["song_id"])
    
    # write songs table to parquet files partitioned by year and artist
    try:
        songs_table.write.parquet(output_data + "songs.parquet", partitionBy=("year", "artist_id"), mode="overwrite")
    except Exception as e:
        print(e)
    
    print("**** songs table data load is complete *****")
    
    # extract columns to create artists table
    artists_fields = ["artist_id", "artist_name as name", "artist_location as location", "artist_latitude as lattitude", "artist_longitude as longitude"]
    artists_table = df.selectExpr(artists_fields).dropDuplicates(["artist_id"])
    
    # write artists table to parquet files
    try:
        artists_table.write.parquet(output_data + "artists.parquet",  mode="overwrite")
    except Exception as e:
        print(e)
    print("**** artists table data load is complete *****")
    
    print("**** song data processing is finished *****")


def process_log_data(spark, input_data, output_data):
    """This function loads log_data from S3 and processes it by extracting the users and time dimension tables
        and songplays fact table then again loaded back to S3
    Args:
        spark(:obj:`pyspark.sql.session.SparkSession`): SparkSession
        input_data (str): S3 bucket where song files are stored
        output (str): S3 bucket file path to store resulting files

    Returns:
        None
    """
    print("**** Starting to process log data *****")
    # get filepath to log data file
    log_data = input_data+'log_data/*/*/*.json'

    # read log data file
    try:
        df =spark.read.json(log_data)
    except Exception as e:
        print(e)
    
    # filter by actions for song plays
    df = df.filter(df.page == "NextSong")

    # extract columns for users table    
    users_fields = ["userId as user_id", "firstName as first_name", "lastName as last_name", "gender", "level","ts"]
    users_table = df.selectExpr(users_fields).orderBy("ts",ascending=False).dropDuplicates(["userId"]).drop("ts")
    
    # write users table to parquet files
    try:
        users_table.write.parquet(output_data + "users.parquet",  mode="overwrite")
    except Exception as e:
        print(e)
        
    print("**** users table data load is complete *****")

    # create timestamp column from original timestamp column
    #get_timestamp = udf(date_convert, TimestampType())
    #df = df.withColumn("datetime",get_timestamp(df.ts))
    
    # create datetime column from original timestamp column
    get_datetime = udf(lambda ms: datetime.fromtimestamp(ms // 1000), TimestampType())
    df = df.withColumn("datetime",get_timestamp(df.ts))
       
    # extract columns to create time table
    time_fields = ["datetime as start_time", "hour(datetime) as hour", "dayofmonth(datetime) as day",
                   "weekofyear(datetime) as week", "month(datetime) as month", "year(datetime) as year", 
                   "dayofweek(datetime) as weekday"]
    time_table = df.select(time_fields).dropDuplicates(["start_time"])
    
    # write time table to parquet files partitioned by year and month
    try:
        time_table.write.parquet(output_data + "time.parquet", partitionBy=("year", "month"), mode="overwrite")
    except Exception as e:
        print(e)
    
    print("**** time table data load is complete *****")

    # read in song data to use for songplays table
    songs_df = spark.read.parquet(output_data + "songs.parquet")
    
    artists_df = spark.read.parquet(output_data + "artists.parquet")
    
    song_df = songs_df.join(artists_df.aslias("artists"), 
                            songs_df.artist_id == artists_df.artist_id , 
                            "inner" ).select("title", "name", "duration", "song_id", "artists.artist_id")

    # extract columns from joined song and log datasets to create songplays table 
    songplays_table = df.join(song_df , (df.song == song_df.title) & (df.artist ==song_df.name) & (df.length == song_df.duration), "inner")
    songplays_table = songplays_table.withColumn("songplay_id",monotonically_increasing_id())
    songplays_table = songplays_table.selectExpr("songplay_id", "datetime as start_time", "userId as user_id",
                                                 "month(datetime) as month", "year(datetime) as year",
                                                 "level", "song_id", "artist_id","sessionId as session_id",
                                                 "location", "userAgent as user_agent").dropDuplicates()
    
    # write songplays table to parquet files partitioned by year and month
    try:
        songplays_table.write.parquet(output_data + "songplays.parquet", partitionBy=("year", "month"), mode="overwrite")
    except Exception as e:
        print(e)

    print("**** songplays table data load is complete *****")
    
    print("**** log data processing is finished *****")


def main():
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    output_data = "s3a://sparkify-dend/"
    
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
