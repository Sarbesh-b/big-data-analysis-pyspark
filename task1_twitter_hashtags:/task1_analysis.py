from pyspark.sql import functions as F
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    to_timestamp, date_format, col, dayofweek, hour, when, 
    concat, lit, sum as _sum, avg
)

if __name__ == "__main__":
    # Initialize Spark session
    spark = SparkSession.builder.appName("twitterdata_full").getOrCreate()

    # Fetch S3 credentials and endpoint configurations from environment variables
    s3_data_repository_bucket = os.environ['DATA_REPOSITORY_BUCKET']
    s3_endpoint_url = os.environ['S3_ENDPOINT_URL'] + ':' + os.environ['BUCKET_PORT']
    s3_access_key_id = os.environ['AWS_ACCESS_KEY_ID']
    s3_secret_access_key = os.environ['AWS_SECRET_ACCESS_KEY']
    s3_bucket = os.environ['BUCKET_NAME']

    # Set Hadoop configuration to access S3
    hadoopConf = spark.sparkContext._jsc.hadoopConfiguration()
    hadoopConf.set("fs.s3a.endpoint", s3_endpoint_url)
    hadoopConf.set("fs.s3a.access.key", s3_access_key_id)
    hadoopConf.set("fs.s3a.secret.key", s3_secret_access_key)
    hadoopConf.set("fs.s3a.path.style.access", "true")
    hadoopConf.set("fs.s3a.connection.ssl.enabled", "false")

    # Load the dataset
    input_path = f"s3a://{s3_data_repository_bucket}/ECS765/Twitter/twitter.csv"
    df = spark.read.options(header=True, inferSchema=True).csv(input_path)

    # QUESTION 1: Load and display data
    df.show(15)
    total_entries = df.count()
    print("Total entries in the dataset:", total_entries)

    # QUESTION 2: Convert timestamp and extract weekdays
    df = df.withColumn("tweet_time", to_timestamp(col("timestamp").cast("string"), "yyyyMMddHHmmss"))
    df_filtered = df.filter((dayofweek(col("tweet_time")) >= 2) & (dayofweek(col("tweet_time")) <= 6))
    df_filtered = df_filtered.withColumn("tweet_date", date_format(col("tweet_time"), "yyyy-MM-dd"))
    df_filtered = df_filtered.orderBy("tweet_date")

    # QUESTION 3: Geographical distribution using filtered data
    df_geo = df_filtered.withColumn("location", concat(col("longitude"), lit(","), col("latitude")))
    location_counts = df_geo.groupBy("location").count()
    location_counts.orderBy("location").show(10, truncate=False)
    output_path_q3 = f"s3a://{s3_bucket}/task3_q3_geographical_distribution.csv"
    location_counts.coalesce(1).write.mode("overwrite").option("header", True).csv(output_path_q3)

    # QUESTION 4: Time of day analysis
    df_time = df_filtered.withColumn("hour", hour(col("tweet_time")))
    df_time = df_time.withColumn("day_name", date_format(col("tweet_time"), "EEEE"))
    df_time = df_time.withColumn("time_of_day",
                                  when((col("hour") >= 5) & (col("hour") <= 11), "Morning")
                                  .when((col("hour") >= 12) & (col("hour") <= 16), "Afternoon")
                                  .when((col("hour") >= 17) & (col("hour") <= 21), "Evening")
                                  .otherwise("Night"))
    time_counts = df_time.groupBy("time_of_day").count()
    output_path_q4 = f"s3a://{s3_bucket}/task4_q4_time_of_day_distribution.csv"
    time_counts.coalesce(1).write.mode("overwrite").option("header", True).csv(output_path_q4)

    # QUESTION 5: Group by day of week using full dataset
    df_day = df.withColumn("tweet_time", to_timestamp(col("timestamp").cast("string"), "yyyyMMddHHmmss"))
    df_day = df_day.withColumn("day_name", date_format(col("tweet_time"), "EEEE"))
    day_counts = df_day.groupBy("day_name").count()
    total_count = day_counts.agg(_sum("count")).collect()[0][0]
    day_counts = day_counts.withColumn("percentage", (col("count") / total_count) * 100)
    output_path_q5 = f"s3a://{s3_bucket}/task5_q5_day_of_week_distribution.csv"
    day_counts.coalesce(1).write.mode("overwrite").option("header", True).csv(output_path_q5)

    # QUESTION 6: Filter days with unusually high tweet counts
    mean_count = day_counts.agg(avg("count")).collect()[0][0]
    high_tweet_days = day_counts.filter(col("count") > mean_count)
    high_tweet_days.orderBy("day_name").show(truncate=False)
    output_path_q6 = f"s3a://{s3_bucket}/task6_q6_unusually_high_tweets.csv"
    high_tweet_days.coalesce(1).write.mode("overwrite").option("header", True).csv(output_path_q6)

    # QUESTION 7: Top 10 tweet locations using unfiltered data
    df_location = df.withColumn("location", concat(col("longitude"), lit(","), col("latitude")))
    top_locations = df_location.groupBy("location").count().orderBy(col("count").desc()).limit(10)
    top_locations.show(truncate=False)
    output_path_q7 = f"s3a://{s3_bucket}/task7_q7_top_locations.csv"
    top_locations.coalesce(1).write.mode("overwrite").option("header", True).csv(output_path_q7)

    # Stop Spark session
    spark.stop()