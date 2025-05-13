import sys, string, os, socket, time, operator, boto3, json
from datetime import datetime
from pyspark.sql import Row, SparkSession
from pyspark.sql.streaming import DataStreamWriter, DataStreamReader
from pyspark.sql.functions import (
    explode, split, window, col, count, regexp_extract, sum, avg, max, when
)
from pyspark.sql.types import IntegerType, DateType, StringType, StructType, TimestampType

if __name__ == "__main__":
    # Initialize Spark Session
    spark = (SparkSession.builder
             .appName("HDFSLogSparkStreaming_Q2_Q4")
             .getOrCreate())
    spark.sparkContext.setLogLevel("ERROR")

    # Environment Variables (Socket)
    hdfs_host = os.getenv("STREAMING_SERVER_HDFS", "default_host")
    port_str = os.getenv("STREAMING_SERVER_HDFS_PORT", "9999")

    try:
        hdfs_port = int(port_str)
    except ValueError:
        raise ValueError(f"Invalid port number: {port_str}")

    # Read the streaming data from socket
    rawDF = (spark.readStream
                  .format("socket")
                  .option("host", hdfs_host)
                  .option("port", hdfs_port)
                  .option("includeTimestamp", "true")
                  .load())

    # Q1: Load and print the dataset
    print("Q1: Loading the Dataset in append mode.")
    q1_query = (rawDF.writeStream
                      .format("console")
                      .outputMode("append")
                      .option("truncate", "false")
                      .start())
    time.sleep(60)
    q1_query.stop()

    # Q2: Add a watermark
    print("Q2: Defining a 5-second Watermark on 'timestamp'...")
    df_with_watermark = rawDF.withWatermark("timestamp", "5 seconds")

    # Q3: Print watermarked data
    print("Q3: Displaying watermarked data to analyze patterns.")
    q3_query = (df_with_watermark.writeStream
                           .format("console")
                           .outputMode("append")
                           .option("truncate", "false")
                           .start())
    time.sleep(60)
    q3_query.stop()

    # Q4: Analyze DataNode activity
    print("Q4: Analyzing DataNode logs in 60s windows with 30s slide.")
    datanodeCounts = (df_with_watermark
                      .filter(col("value").contains("DataNode"))
                      .groupBy(window(col("timestamp"), "60 seconds", "30 seconds"))
                      .agg(count("*").alias("datanode_count")))

    q4_query = (datanodeCounts.writeStream
                            .format("console")
                            .outputMode("update")
                            .option("truncate", "false")
                            .start())
    time.sleep(60)
    q4_query.stop()

    # Q5: Aggregate bytes per host
    print("Q5: Aggregating bytes per host.")
    bytesPerHost = (df_with_watermark
                    .withColumn("hostname", regexp_extract(col("value"), r'src:\s*/([\d\.]+):', 1))
                    .withColumn("bytes", regexp_extract(col("value"), r'(\d+)\s*$', 1).cast("long"))
                    .groupBy("hostname")
                    .agg(sum("bytes").alias("total_bytes"))
                    .orderBy(col("total_bytes").desc()))

    q5_query = (bytesPerHost.writeStream
                          .format("console")
                          .outputMode("complete")
                          .option("truncate", "false")
                          .start())
    time.sleep(150)
    q5_query.stop()

    # Q6: Filter logs with specific patterns and trigger
    print("Q6: Filtering logs (INFO & blk_) + 15s trigger.")
    df_q6 = (df_with_watermark
             .select(
                 regexp_extract(col("value"), r'src:\s*/([\d\.]+):', 1).alias("hostname"),
                 col("value"),
                 col("timestamp")
             )
             .filter(col("value").contains("INFO") & col("value").contains("blk_")))

    q6_counts = df_q6.groupBy("hostname").count()

    q6_query = (q6_counts.writeStream
                         .format("console")
                         .trigger(processingTime="15 seconds")
                         .outputMode("complete")
                         .option("truncate", "false")
                         .start())
    time.sleep(90)
    q6_query.stop()

    spark.stop()