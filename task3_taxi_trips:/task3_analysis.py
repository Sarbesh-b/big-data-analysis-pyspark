#!/usr/bin/env python

import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, sum as spark_sum, avg as spark_avg, first as spark_first, min as spark_min,
    lit, when, hour
)
from pyspark.sql.types import (
    StructType, StructField, StringType,
    IntegerType, DoubleType
)
from graphframes import GraphFrame

if __name__ == "__main__":

    # 
    # Spark Session & S3 Config
    # 
    spark = SparkSession.builder.appName("ChicagoTaxiAnalysis_T3").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    # Environment variables (adjust as needed)
    s3_repo_bucket = os.environ.get("DATA_REPOSITORY_BUCKET", "")
    s3_endpoint    = os.environ.get("S3_ENDPOINT_URL", "")
    s3_port        = os.environ.get("BUCKET_PORT", "80")
    s3_access_id   = os.environ.get("AWS_ACCESS_KEY_ID", "")
    s3_secret_key  = os.environ.get("AWS_SECRET_ACCESS_KEY", "")
    s3_bucket_out  = os.environ.get("BUCKET_NAME", "")

    # Build full endpoint
    s3_endpoint_full = f"{s3_endpoint}:{s3_port}"

    # Configure Hadoop for S3
    hadoopConf = spark.sparkContext._jsc.hadoopConfiguration()
    hadoopConf.set("fs.s3a.endpoint", s3_endpoint_full)
    hadoopConf.set("fs.s3a.access.key", s3_access_id)
    hadoopConf.set("fs.s3a.secret.key", s3_secret_key)
    hadoopConf.set("fs.s3a.path.style.access", "true")
    hadoopConf.set("fs.s3a.connection.ssl.enabled", "false")

    # 
    # Q1: Load the Data (ACTIVE)
    # 
    print(" Q1: Load Data ")
    taxi_data_path = f"s3a://{s3_repo_bucket}/ECS765/Chicago_Taxitrips/chicago_taxi_trips.csv"
    taxiDF = (
        spark.read
             .option("header", True)
             .option("inferSchema", True)
             .option("timestampFormat", "MM/dd/yyyy hh:mm:ss a")
             .csv(taxi_data_path)
    )

    record_count = taxiDF.count()
    print(f"Total records in taxiDF = {record_count}")
    taxiDF.printSchema()

    # 
    # Q2: Build Vertices & Edges (ACTIVE)
    # 
    print("\n Q2: Constructing Vertices and Edges ")

    # Example schemas (not strictly required in PySpark, but provided for clarity)
    vertexSchema = StructType([
        StructField("id", StringType(), True),
        StructField("latitude", DoubleType(), True),
        StructField("longitude", DoubleType(), True),
        StructField("CensusTract", StringType(), True)
    ])

    edgeSchema = StructType([
        StructField("src", StringType(), True),
        StructField("dst", StringType(), True),
        StructField("TripMiles", DoubleType(), True),
        StructField("TripSeconds", IntegerType(), True),
        StructField("Fare", DoubleType(), True)
    ])

    pickupVertices = (
        taxiDF.select(
            col("Pickup Community Area").alias("id"),
            col("Pickup Centroid Latitude").alias("latitude"),
            col("Pickup Centroid Longitude").alias("longitude"),
            col("Pickup Census Tract").alias("CensusTract")
        )
        .where("id is not null and latitude is not null and longitude is not null and CensusTract is not null")
    )

    dropVertices = (
        taxiDF.select(
            col("Dropoff Community Area").alias("id"),
            col("Dropoff Centroid Latitude").alias("latitude"),
            col("Dropoff Centroid Longitude").alias("longitude"),
            col("Dropoff Census Tract").alias("CensusTract")
        )
        .where("id is not null and latitude is not null and longitude is not null and CensusTract is not null")
    )

    vertexDF = pickupVertices.union(dropVertices).distinct()

    edgeDF = (
        taxiDF.select(
            col("Pickup Community Area").alias("src"),
            col("Dropoff Community Area").alias("dst"),
            col("Trip Miles").alias("TripMiles"),
            col("Trip Seconds").alias("TripSeconds"),
            col("Fare").alias("Fare")
        )
        .where("src is not null and dst is not null and TripMiles is not null and TripSeconds is not null and Fare is not null")
    )

    # Optionally show sample data
    # vertexDF.show(5, truncate=False)
    # edgeDF.show(5, truncate=False)

    # 
    # Q3: Create Graph & Display (COMMENTED OUT)
    # 
    
    print("\n=== Q3: Graph + 10 Sample Joins ===")
    from graphframes import GraphFrame
    srcRen = vertexDF.withColumnRenamed("id","src") \
                     .withColumnRenamed("latitude","srcLat") \
                     .withColumnRenamed("longitude","srcLon") \
                     .withColumnRenamed("CensusTract","srcTract")
    dstRen = vertexDF.withColumnRenamed("id","dst") \
                     .withColumnRenamed("latitude","dstLat") \
                     .withColumnRenamed("longitude","dstLon") \
                     .withColumnRenamed("CensusTract","dstTract")

    taxiGraph = GraphFrame(vertexDF, edgeDF)
    combinedDF = edgeDF.join(srcRen, "src", "left").join(dstRen, "dst", "left")

    combinedDF.select(
        "src", "dst", "TripMiles", "TripSeconds", "Fare",
        "srcLat", "srcLon", "srcTract",
        "dstLat", "dstLon", "dstTract"
    ).show(10, truncate=False)
    

    # 
    # Q4: Connected (src==dst) (COMMENTED OUT)
    # 
    
    print("\n=== Q4: Same-Community Trips ===")
    same_area = edgeDF.filter(col("src") == col("dst"))
    count_same = same_area.count()
    print(f"Number of edges where src=dst: {count_same}")
    same_area.show(10, truncate=False)
    

    # 
    # Q5: BFS (COMMENTED OUT)
    #
    
    print("\n=== Q5: BFS to community area '49' ===")
    from graphframes import GraphFrame
    BFSgraph = GraphFrame(vertexDF, edgeDF)
    bfs_data = BFSgraph.bfs(
        fromExpr="id != '49'",
        toExpr="id = '49'",
        maxPathLength=10
    )

    summarized_bfs = (
        bfs_data
        .select(
            col("from.id").alias("startComm"),
            col("to.id").alias("endComm"),
            col("e0.TripMiles").alias("edgeMiles")
        )
        .filter(col("endComm") == "49")
        .groupBy("startComm")
        .agg(
            spark_first("endComm").alias("landmark"),
            spark_min("edgeMiles").alias("shortestMiles")
        )
        .orderBy("startComm")
    )
    summarized_bfs.show(10, truncate=False)
    

    # 
    # Q6: PageRank (COMMENTED OUT)
    # 
    
    print("\n=== Q6: PageRank (Standard & Weighted) ===")
    from graphframes import GraphFrame

    # Deduplicate vertices
    uniqueVerts = (
        pickupVertices.union(dropVertices).distinct()
                     .filter(col("id").isNotNull())
                     .groupBy("id")
                     .agg(
                         spark_first("latitude").alias("latitude"),
                         spark_first("longitude").alias("longitude"),
                         spark_first("CensusTract").alias("CensusTract")
                     )
    )

    # (a) Standard PageRank
    prGraph = GraphFrame(uniqueVerts, edgeDF)
    standardPR = prGraph.pageRank(resetProbability=0.15, tol=0.01)

    stdRanked = (
        standardPR.vertices
                  .select("id", "pagerank")
                  .orderBy(col("pagerank").desc())
    )
    stdRanked.show(5, truncate=False)

    # (b) Weighted PageRank
    weightedEdges = (
        taxiDF.groupBy(
            col("Pickup Community Area").cast("int").alias("src"),
            col("Dropoff Community Area").cast("int").alias("dst")
        )
        .agg(spark_sum("Trip Miles").alias("weight"))
        .na.drop("any")
    )

    outWeights = (
        weightedEdges.groupBy("src")
                     .agg(spark_sum("weight").alias("sumOut"))
    )

    normalizedEdges = (
        weightedEdges.join(outWeights, "src", "left")
                     .withColumn(
                         "norm_weight",
                         when(col("sumOut") == 0, 0).otherwise(col("weight") / col("sumOut"))
                     )
    )

    damping = 0.85
    iterationCount = 10
    vertCount = uniqueVerts.count()
    initRank = 1.0 / vertCount

    rankDF = uniqueVerts.select("id").withColumn("pagerank", lit(initRank))

    for _ in range(iterationCount):
        contrib = (
            normalizedEdges.join(rankDF, rankDF.id == normalizedEdges.src, "inner")
            .select(
                normalizedEdges.dst.alias("id"),
                (rankDF.pagerank * normalizedEdges.norm_weight).alias("contrib")
            )
        )
        summed = contrib.groupBy("id").agg(spark_sum("contrib").alias("total_contrib"))
        rankDF = (
            uniqueVerts.join(summed, "id", "left")
            .na.fill({"total_contrib": 0})
            .withColumn(
                "pagerank",
                ((1 - damping)/vertCount) + damping * col("total_contrib")
            )
        )

    print("Top 5 - Weighted PageRank (id, pagerank):")
    rankDF.select("id", "pagerank").orderBy(col("pagerank").desc()).show(5, truncate=False)
    

    # 
    # Q7: Fare Analysis (ACTIVE)
    # 
    print("\n Question 7:")

    from pyspark.sql.types import DoubleType
    fareAnalysisDF = taxiDF.select(
        hour(col("Trip Start Timestamp")).alias("start_hour"),
        col("Trip Miles").cast(DoubleType()).alias("distance"),
        col("Trip Seconds").cast(DoubleType()).alias("duration"),
        col("Fare").cast(DoubleType()).alias("fare_amount")
    ).na.drop(subset=["start_hour","distance","duration","fare_amount"])

    # 1) Average fare by distance
    distAvgFare = (
        fareAnalysisDF.groupBy("distance")
                      .agg(spark_avg("fare_amount").alias("avg_fare"))
                      .orderBy("distance")
    )
    distAvgFare.show(10, truncate=False)

    # 2) Average fare by duration
    durAvgFare = (
        fareAnalysisDF.groupBy("duration")
                      .agg(spark_avg("fare_amount").alias("avg_fare"))
                      .orderBy("duration")
    )
    durAvgFare.show(10, truncate=False)

    # 3) Average fare by hour
    hrAvgFare = (
        fareAnalysisDF.groupBy("start_hour")
                      .agg(spark_avg("fare_amount").alias("avg_fare"))
                      .orderBy("start_hour")
    )
    hrAvgFare.show(10, truncate=False)

    # Correlations
    corr_dist = fareAnalysisDF.corr("distance", "fare_amount")
    corr_dur  = fareAnalysisDF.corr("duration", "fare_amount")
    corr_hr   = fareAnalysisDF.corr("start_hour", "fare_amount")

    print(f"\nCorrelation (distance, fare_amount) = {corr_dist}")
    print(f"Correlation (duration, fare_amount)  = {corr_dur}")
    print(f"Correlation (start_hour, fare_amount)= {corr_hr}")

    # Example: Save to S3 (CSV)
    fare_out_path = f"s3a://{s3_bucket_out}/task_3/q7_mod.csv"
    (
        fareAnalysisDF.coalesce(1)
                      .write
                      .mode("overwrite")
                      .option("header", True)
                      .csv(fare_out_path)
    )

    
    
    
    spark.stop()
    print("END")