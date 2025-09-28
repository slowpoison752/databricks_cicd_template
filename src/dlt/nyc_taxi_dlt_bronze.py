"""
NYC Taxi DLT Bronze Pipeline
DLT implementation of the bronze ingestion layer with enhanced capabilities

Key DLT Benefits:
- Parallel execution with existing job workflow
- Built-in data lineage and quality monitoring
- Automatic schema evolution and conflict resolution
- Resource isolation and auto-scaling
- Built-in retry and error handling
"""

import dlt
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    current_timestamp,
    lit,
    col,
    when,
    to_timestamp,
    year,
    month,
    dayofmonth,
    regexp_replace
)
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType

# DLT Configuration - get from pipeline configuration
source_path = spark.conf.get("source_path",
                             "/databricks-datasets/nyctaxi/tripdata/yellow/yellow_tripdata_2019-01.csv.gz")
environment_suffix = spark.conf.get("environment_suffix", "")


@dlt.table(
    name="nyc_taxi_raw",
    comment="Raw NYC taxi data ingested from source files",
    table_properties={
        "quality": "bronze",
        "layer": "raw",
        "pipelines.autoOptimize.managed": "true"
    }
)
def raw_taxi_data():
    """
    Raw data ingestion - exact copy of source data
    DLT automatically handles schema inference and evolution
    """
    return (
        spark.read
        .option("header", "true")
        .option("inferSchema", "true")
        .csv(source_path)
        .withColumn("ingestion_timestamp", current_timestamp())
        .withColumn("data_source", lit("nyc_taxi_public"))
        .withColumn("environment", lit(environment_suffix))
    )


@dlt.table(
    name="nyc_taxi_bronze",
    comment="Cleaned and validated NYC taxi data with quality metrics",
    table_properties={
        "quality": "bronze",
        "layer": "cleaned",
        "pipelines.autoOptimize.managed": "true"
    }
)
@dlt.expect_all({
    "valid_pickup_datetime": "pickup_datetime IS NOT NULL",
    "valid_total_amount": "total_amount IS NOT NULL AND total_amount >= 0",
    "valid_trip_distance": "trip_distance IS NOT NULL AND trip_distance >= 0",
    "valid_passenger_count": "passenger_count_clean IS NOT NULL AND passenger_count_clean > 0"
})
def bronze_taxi_data():
    """
    Bronze layer with data cleaning and quality expectations
    DLT automatically tracks data quality and lineage
    """
    raw_df = dlt.read("nyc_taxi_raw")

    return (
        raw_df
        # Convert string timestamps to proper timestamp type
        .withColumn(
            "pickup_datetime",
            to_timestamp(col("tpep_pickup_datetime"), "yyyy-MM-dd HH:mm:ss")
        )
        .withColumn(
            "dropoff_datetime",
            to_timestamp(col("tpep_dropoff_datetime"), "yyyy-MM-dd HH:mm:ss")
        )

        # Add date partitioning columns
        .withColumn("pickup_year", year(col("pickup_datetime")))
        .withColumn("pickup_month", month(col("pickup_datetime")))
        .withColumn("pickup_day", dayofmonth(col("pickup_datetime")))

        # Clean passenger count
        .withColumn(
            "passenger_count_clean",
            when(
                col("passenger_count").isNull() | (col("passenger_count") <= 0), 1
            ).otherwise(col("passenger_count"))
        )

        # Data quality flags
        .withColumn(
            "data_quality_flag",
            when(
                (col("trip_distance") <= 0) |
                (col("total_amount") <= 0) |
                (col("pickup_datetime").isNull()),
                "SUSPICIOUS"
            ).otherwise("CLEAN")
        )

        # Add DLT-specific metadata
        .withColumn("pipeline_stage", lit("bronze"))
        .withColumn("data_classification", lit("public"))
        .withColumn("business_domain", lit("transportation"))
        .withColumn("dlt_pipeline_id", lit("${resources.pipelines.nyc_taxi_dlt_pipeline.id}"))
        .withColumn("processing_date", col("ingestion_timestamp").cast("date"))
    )


# DLT Data Quality Monitoring Table
@dlt.table(
    name="nyc_taxi_bronze_quality_metrics",
    comment="Data quality metrics and monitoring for bronze layer"
)
def bronze_quality_metrics():
    """
    Data quality monitoring table
    Tracks quality metrics over time for monitoring and alerting
    """
    bronze_df = dlt.read("nyc_taxi_bronze")

    return (
        bronze_df
        .groupBy("processing_date", "data_quality_flag")
        .agg(
            count("*").alias("record_count"),
            avg("total_amount").alias("avg_fare"),
            avg("trip_distance").alias("avg_distance"),
            max("ingestion_timestamp").alias("latest_ingestion")
        )
        .withColumn("quality_check_timestamp", current_timestamp())
        .withColumn("environment", lit(environment_suffix))
    )


# OPTIONAL: Quarantine table for suspicious records
@dlt.table(
    name="nyc_taxi_bronze_quarantine",
    comment="Quarantined records that failed quality checks"
)
def bronze_quarantine():
    """
    Quarantine suspicious records for investigation
    Keeps bad data for analysis without affecting downstream processing
    """
    return (
        dlt.read("nyc_taxi_bronze")
        .filter(col("data_quality_flag") == "SUSPICIOUS")
        .withColumn("quarantine_reason", lit("Failed bronze quality checks"))
        .withColumn("quarantine_timestamp", current_timestamp())
    )