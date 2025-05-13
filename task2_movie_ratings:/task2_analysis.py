import sys, string
import os
import socket
from pyspark.sql import SparkSession
from datetime import datetime
from pyspark.sql.functions import (
    to_timestamp, from_unixtime, date_format, col, countDistinct, when, concat, lit,
    split, explode, sum as _sum, avg, count
)
from pyspark.sql.types import FloatType, IntegerType

# 
# Flag Variables
# 
# Q1 & Q2 are required as base, others depend on them but can be run independently.
runQ1 = True   # Load ratings data
runQ2 = False  # Filter data & convert timestamp
runQ3 = False   # Rating Distribution (categorize as Low, Medium, High)
runQ4 = False   # Add Columns: Extract Year, Month and categorize as Early/Late Year
runQ5 = False   # Genre Analysis
runQ6 = False   # Temporal Analysis (Group by Year)
runQ7 = False   # Top Movies (Highest Average Ratings)
runQ8 = True   # User Analysis (Frequent vs Infrequent Raters)

if __name__ == "__main__":
    # 
    # REQUIRED BLOCK: Spark and S3 Configuration
    # 
    spark = SparkSession.builder.appName("MovieRatingsAnalysis").getOrCreate()
    # Optionally, adjust the log level:
    # spark.sparkContext.setLogLevel("WARN")
    
    s3_data_repository_bucket = os.environ['DATA_REPOSITORY_BUCKET']  # Read-only bucket (e.g., data-repository-bkt)
    s3_bucket = os.environ['BUCKET_NAME']  # Your output bucket
    s3_endpoint_url = os.environ['S3_ENDPOINT_URL'] + ':' + os.environ['BUCKET_PORT']
    s3_access_key_id = os.environ['AWS_ACCESS_KEY_ID']
    s3_secret_access_key = os.environ['AWS_SECRET_ACCESS_KEY']
    
    hadoopConf = spark.sparkContext._jsc.hadoopConfiguration()
    hadoopConf.set("fs.s3a.endpoint", s3_endpoint_url)
    hadoopConf.set("fs.s3a.access.key", s3_access_key_id)
    hadoopConf.set("fs.s3a.secret.key", s3_secret_access_key)
    hadoopConf.set("fs.s3a.path.style.access", "true")
    hadoopConf.set("fs.s3a.connection.ssl.enabled", "false")
    
    # 
    # Q1: LOAD DATA
    # 
    if runQ1:
        ratings_path = f"s3a://{s3_data_repository_bucket}/ECS765/MovieLens/ratings.csv"
        ratings_df = spark.read.options(header=True, inferSchema=True).csv(ratings_path)
        unique_users = ratings_df.select(countDistinct(col("userId")).alias("unique_users")).collect()[0]["unique_users"]
        print("Q1 -> Number of unique users who have rated movies:", unique_users)
    else:
        ratings_path = f"s3a://{s3_data_repository_bucket}/ECS765/MovieLens/ratings.csv"
        ratings_df = spark.read.options(header=True, inferSchema=True).csv(ratings_path)
    
    # 
    # Q2: FILTER DATA & CONVERT TIMESTAMP
    # 
    if runQ2:
        # Use from_unixtime() to convert Unix epoch seconds to a readable date.
        ratings_q2 = ratings_df.withColumn("rating_date", date_format(from_unixtime(col("timestamp")), "yyyy-MM-dd"))
        ratings_q2 = ratings_q2.sort("rating_date")
        print("Q2 -> Sample of data with 'rating_date' in 'YYYY-MM-DD' format (sorted by date):")
        ratings_q2.show(10, truncate=False)
    else:
        ratings_q2 = None
    
    # 
    # Q3: RATING DISTRIBUTION
    # 
    if runQ3:
        # Filter the ratings to include only valid ratings between 1 and 5.
        ratings_valid = ratings_df.filter((col("rating") >= 1) & (col("rating") <= 5))
        # Create a new column "rating_category" that categorizes ratings as:
        # "Low": [1, 2] ; "Medium": [3, 4] ; "High": (5)
        ratings_q3 = ratings_valid.withColumn(
            "rating_category",
            when(col("rating") < 3, "Low")
            .when((col("rating") >= 3) & (col("rating") < 5), "Medium")
            .when(col("rating") == 5, "High")
        )
        # Group by rating_category and count the number of ratings per category.
        rating_distribution = ratings_q3.groupBy("rating_category").count()
        print("Q3 -> Rating distribution by category:")
        rating_distribution.orderBy("rating_category").show(truncate=False)
        
        # Save Q3 output to S3 for later visualization.
        output_path_q3 = f"s3a://{s3_bucket}/task2_q3_rating_distribution.csv"
        rating_distribution.coalesce(1).write.mode("overwrite").option("header", True).csv(output_path_q3)
        print(f"Q3 Output written to: {output_path_q3}")
    
    # 
    # Q4: ADD COLUMNS (YEAR, MONTH, & TIME OF YEAR)
    # 
    if runQ4:
        # Convert the timestamp to a proper timestamp and extract year and month.
        ratings_q4 = ratings_df.withColumn("tweet_time", from_unixtime(col("timestamp")))
        ratings_q4 = ratings_q4.withColumn("year", date_format(col("tweet_time"), "yyyy")) \
                               .withColumn("month", date_format(col("tweet_time"), "MM")) \
                               .withColumn("time_of_year", when(col("month").cast("int") <= 6, "Early Year").otherwise("Late Year"))
        print("Q4 -> Sample data with 'year', 'month', and 'time_of_year':")
        ratings_q4.select("year", "month", "time_of_year").show(10, truncate=False)
        
        # Aggregate by time_of_year: count the number of ratings for each category.
        time_distribution = ratings_q4.groupBy("time_of_year").count()
        print("Q4 -> Time distribution (Early Year vs Late Year):")
        time_distribution.orderBy("time_of_year").show(truncate=False)
        
        # Save Q4 output to S3.
        output_path_q4 = f"s3a://{s3_bucket}/task2_q4_time_of_year_distribution.csv"
        time_distribution.coalesce(1).write.mode("overwrite").option("header", True).csv(output_path_q4)
        print(f" Q4 Output written to: {output_path_q4}")
    
    # 
    # Q5: GENRE ANALYSIS
    # 
    if runQ5:
        # Load the movies dataset.
        movies_path = f"s3a://{s3_data_repository_bucket}/ECS765/MovieLens/movies.csv"
        movies_df = spark.read.options(header=True, inferSchema=True).csv(movies_path)
        
        # Join movies with ratings on movieId.
        movies_ratings_df = movies_df.join(ratings_df, on="movieId", how="inner")
        
        # Create a new column by splitting the 'genres' column by the pipe ('|') character,
        # then explode it so that each genre appears in a separate row.
        movies_ratings_df = movies_ratings_df.withColumn("genre", explode(split(col("genres"), "\\|")))
        
        # Define the list of valid genres.
        valid_genres = ["Action", "Adventure", "Animation", "Children", "Comedy", "Crime",
                        "Documentary", "Drama", "Fantasy", "Film-Noir", "Horror", "Musical",
                        "Mystery", "Romance", "Sci-Fi", "Thriller", "War", "Western"]
        
        # Filter out rows with invalid genres.
        movies_ratings_df = movies_ratings_df.filter(col("genre").isin(valid_genres))
        
        # Group by genre and count the number of ratings per genre.
        genre_distribution = movies_ratings_df.groupBy("genre").count()
        print("Q5 -> Genre distribution (number of ratings per valid genre):")
        genre_distribution.orderBy("genre").show(truncate=False)
        
        # Save Q5 output to S3.
        output_path_q5 = f"s3a://{s3_bucket}/task2_q5_genre_distribution.csv"
        genre_distribution.coalesce(1).write.mode("overwrite").option("header", True).csv(output_path_q5)
        print(f" Q5 Output written to: {output_path_q5}")
    
    # 
    # Q6: TEMPORAL ANALYSIS (GROUP BY YEAR)
    # 
    if runQ6:
        # Extract the year from the timestamp using from_unixtime.
        ratings_by_year = ratings_df.withColumn("year", date_format(from_unixtime(col("timestamp")), "yyyy"))
        # Group by 'year' and count the total number of ratings per year.
        year_counts = ratings_by_year.groupBy("year").count().orderBy("year")
        
        # Calculate the overall total rating count and then the percentage per year.
        total_rating_count = year_counts.agg(_sum("count")).collect()[0][0]
        year_counts = year_counts.withColumn("percentage", (col("count") / total_rating_count) * 100)
        
        print("Q6 -> Aggregated rating counts by year and their percentage share:")
        year_counts.show(10, truncate=False)
        
        # Save Q6 output to S3.
        output_path_q6 = f"s3a://{s3_bucket}/task2_q6_temporal_analysis.csv"
        year_counts.coalesce(1).write.mode("overwrite").option("header", True).csv(output_path_q6)
        print(f" Q6 Output written to: {output_path_q6}")
    
    # 
    # Q7: TOP MOVIES (Highest Average Ratings)
    # 
    if runQ7:
        # Load the movies dataset.
        movies_path = f"s3a://{s3_data_repository_bucket}/ECS765/MovieLens/movies.csv"
        movies_df = spark.read.options(header=True, inferSchema=True).csv(movies_path)
        
        # Join movies with ratings on 'movieId'.
        movies_ratings_df = movies_df.join(ratings_df, on="movieId", how="inner")
        
        # Group by movieId, title, and genres to compute the average rating and count of ratings.
        movies_avg = movies_ratings_df.groupBy("movieId", "title", "genres").agg(
            avg(col("rating")).alias("avg_rating"),
            count(col("rating")).alias("rating_count")
        )
        
        # Order the results by average rating in descending order and select the top 10 movies.
        top_movies = movies_avg.orderBy(col("avg_rating").desc()).limit(10)
        
        print("Q7 -> Top 10 movies with the highest average ratings (including genres):")
        top_movies.show(truncate=False)
        
        # Save Q7 output to S3.
        output_path_q7 = f"s3a://{s3_bucket}/task2_q7_top_movies.csv"
        top_movies.coalesce(1).write.mode("overwrite").option("header", True).csv(output_path_q7)
        print(f" Q7 Output written to: {output_path_q7}")
    
    # 
    # Q8: USER ANALYSIS (Frequent vs Infrequent Raters)
    # 
    if runQ8:
        # Group ratings by userId and count the number of ratings per user.
        user_counts = ratings_df.groupBy("userId").count().withColumnRenamed("count", "rating_count")
        
        # Create a column 'rater_category' categorizing users:
        # "Frequent Raters" (if count > 50) and "Infrequent Raters" otherwise.
        user_counts = user_counts.withColumn(
            "rater_category",
            when(col("rating_count") > 50, "Frequent Raters").otherwise("Infrequent Raters")
        )
        
        # Find the top 10 users by rating count.
        top_users = user_counts.orderBy(col("rating_count").desc()).limit(10)
        print("Q8 -> Top 10 users by number of ratings:")
        top_users.show(truncate=False)
        print("Column headers:", top_users.columns) 
        # Aggregate the distribution of rater categories.
        rater_distribution = user_counts.groupBy("rater_category").count()
        #print("Q8 -> Distribution of Frequent vs Infrequent Raters:")
        #rater_distribution.orderBy("rater_category").show(truncate=False)
        
        # Save Q8 output to S3.
        output_path_q8 = f"s3a://{s3_bucket}/task2_q8_user_distribution.csv"
        rater_distribution.coalesce(1).write.mode("overwrite").option("header", True).csv(output_path_q8)
        print(f" Q8 Output written to: {output_path_q8}")
    
    # Stop the Spark session.
    spark.stop()
