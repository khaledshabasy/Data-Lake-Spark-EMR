import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
import pyspark.sql.types as T
#from pyspark.sql.functions import udf, col
#from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format


config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config['AWS']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS']['AWS_SECRET_ACCESS_KEY']


def create_spark_session():
    """
        Instantiates the Spark Session with specific configuration. 
        
    """
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    """
        Loads song_data from S3 and processes it by extracting the songs and artists tables
        and then loaded back to S3
        
        Parameters:
            spark       : this is the Spark Session
            input_data  : the path of ingested song_data
            output_data : the path where results are stored
            
    """
    # get filepath to song data file
    song_data = input_data + 'song_data/**/**/**/*.json'
    
    # read song data file
    df = spark.read.json(song_data)

    # extract columns to create songs table
    songs_table = df.select('song_id', 'title', 'artist_id', 'year', 'duration').filter('song_id is not Null').dropDuplicates(['song_id'])
    
    # write songs table to parquet files partitioned by year and artist
    songs_table.write.mode('overwrite').partitionBy("year", "artist_id").parquet(output_data + "songs")

    # extract columns to create artists table
    artists_table = df.select('artist_id',\
                              F.col('artist_name').alias('name'),\
                              F.col('artist_location').alias('location'),\
                              F.col('artist_latitude').alias('latitude'),\
                              F.col('artist_longitude').alias('longitude')).filter('artist_id is not Null').dropDuplicates(['artist_id'])
    
    # write artists table to parquet files
    artists_table.write.mode('overwrite').parquet(output_data + "artists")


def process_log_data(spark, input_data, output_data):
    """
        Loads log_data from S3 and processes it by extracting the users and time tables
        and then loaded back to S3. Loaded data are read again to extract songplays table.
        
        Parameters:
            spark       : this is the Spark Session
            input_data  : the path of ingested log_data
            output_data : the path where results are stored
            
    """
    # get filepath to log data file
    log_data = input_data + 'log_data/**/**/*.json'

    # read log data file
    df = spark.read.json(log_data)
    
    # filter by actions for song plays
    df = df.filter(df.page == 'NextSong')

    # extract columns for users table    
    users_table = df.select(F.col('userId').alias('user_id'),\
                        F.col('firstName').alias('first_name'),\
                        F.col('lastName').alias('last_name'),\
                        F.col('gender'),\
                        F.col('level')).dropDuplicates(['user_id', 'level']).filter('user_id != ""').withColumn('user_id', F.expr('cast(user_id as int)'))
    
    # write users table to parquet files
    users_table.write.mode('overwrite').parquet(output_data + "users")

    # create timestamp column from original timestamp column
    get_timestamp = F.udf(lambda x: datetime.fromtimestamp(x/1000), T.TimestampType())
    df = df.withColumn('timestamp', get_timestamp("ts"))
                       
    # create datetime column from original timestamp column
    get_datetime = F.udf(lambda x: datetime.fromtimestamp(x/1000), T.DateType())
    df = df.withColumn('datetime', get_datetime("ts"))
    
    # extract columns to create time table
    time_table = df.select(F.col('timestamp').alias('start_time'),\
                            F.hour('timestamp').alias('hour'),\
                            F.dayofmonth('timestamp').alias('day'),\
                            F.weekofyear('timestamp').alias('week'),\
                            F.month('timestamp').alias('month'),\
                            F.year('timestamp').alias('year'),\
                            F.dayofweek('timestamp').alias('weekday')).filter('start_time is not Null').dropDuplicates(['start_time'])
    
    # write time table to parquet files partitioned by year and month
    time_table.write.mode('overwrite').partitionBy("year", "month").parquet(output_data + "time")

    # read in song data to use for songplays table
    song_df = spark.read.parquet(output_data + "songs")

    
    # write out log and songs data to TempView
    df.createOrReplaceTempView('log_data_table')
    song_df.createOrReplaceTempView('song_data_table')
    
    # extract columns from joined song and log datasets to create songplays table 
    songplays_table = spark.sql("""
                                SELECT  monotonically_increasing_id() as songplay_id,
                                        logT.timestamp as start_time,
                                        logT.userId as user_id,
                                        logT.level as level,
                                        songT.song_id as song_id,
                                        songT.artist_id as artist_id,
                                        logT.sessionId as session_id,
                                        logT.location as location,
                                        logT.userAgent as user_agent,
                                        month(logT.timestamp) as month,
                                        year(logT.timestamp) as year
                                FROM    log_data_table logT
                                JOIN    song_data_table songT
                                ON      logT.length = songT.duration AND logT.song = songT.title
                            """)

    # write songplays table to parquet files partitioned by year and month
    songplays_table.write.mode('overwrite').partitionBy("year", "month").parquet(output_data + "songplays")


def main():
    '''
    - Establishes connection to the Spark Session.
    - Prepares the S3 paths of input and output data.
    - Executes the processing of song and log data.
    
    '''
    spark = create_spark_session()
    input_data = "s3n://udacity-dend/"
    #input_data = "/home/workspace/data/"
    output_data = "s3n://myspark-output/output/"
    #output_data = "/home/workspace/output/"

    
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
