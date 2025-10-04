"""
NYC Taxi DLT Pipeline - Bronze Layer Transformation Functions
Pure transformation logic without DLT decorators

These functions contain only the transformation logic and return DataFrames.
They are called by the main dlt_pipeline.py which handles the @dlt decorators.
"""
from pyspark.sql import DataFrame, functions as F
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType, TimestampType

# Schema definition for NYC taxi data
TAXI_SCHEMA = StructType([
    StructField("VendorID", IntegerType(), True),
    StructField("tpep_pickup_datetime", TimestampType(), True),
    StructField("tpep_dropoff_datetime", TimestampType(), True),
    StructField("passenger_count", IntegerType(), True),
    StructField("trip_distance", DoubleType(), True),
    StructField("RatecodeID", IntegerType(), True),
    StructField("store_and_fwd_flag", StringType(), True),
    StructField("PULocationID", IntegerType(), True),
    StructField("DOLocationID", IntegerType(), True),
    StructField("payment_type", IntegerType(), True),
    StructField("fare_amount", DoubleType(), True),
    StructField("extra", DoubleType(), True),
    StructField("mta_tax", DoubleType(), True),
    StructField("tip_amount", DoubleType(), True),
    StructField("tolls_amount", DoubleType(), True),
    StructField("improvement_surcharge", DoubleType(), True),
    StructField("total_amount", DoubleType(), True)
])


def add_ingestion_metadata(df: DataFrame, environment: str = "") -> DataFrame:
    """
    Add ingestion metadata to raw data.

    Args:
        df: Input DataFrame from CSV read
        environment: Environment suffix for tracking

    Returns:
        DataFrame with metadata columns added
    """
    return (
        df
        .withColumn("ingestion_timestamp", F.current_timestamp())
        .withColumn("source_file", F.col("_metadata.file_path"))
        .withColumn("environment", F.lit(environment))
    )


def enrich_with_computed_fields(df: DataFrame) -> DataFrame:
    """
    Enrich bronze data with computed columns and quality flags.

    Args:
        df: Raw taxi data DataFrame

    Returns:
        DataFrame with additional computed columns
    """
    return (
        df
        # Calculate trip duration in minutes
        .withColumn(
            "trip_duration_minutes",
            (F.col("tpep_dropoff_datetime").cast("long") -
             F.col("tpep_pickup_datetime").cast("long")) / 60
        )
        # Calculate average speed in mph
        .withColumn(
            "avg_speed_mph",
            F.when(
                F.col("trip_duration_minutes") > 0,
                F.col("trip_distance") / (F.col("trip_duration_minutes") / 60)
            ).otherwise(0)
        )
        # Extract date and time components
        .withColumn("pickup_date", F.to_date("tpep_pickup_datetime"))
        .withColumn("pickup_hour", F.hour("tpep_pickup_datetime"))
        .withColumn("pickup_day_of_week", F.dayofweek("tpep_pickup_datetime"))
        # Add data quality flag for suspicious records
        .withColumn(
            "is_suspicious",
            F.when(
                (F.col("trip_distance") > 100) |
                (F.col("trip_duration_minutes") > 180) |
                (F.col("avg_speed_mph") > 100) |
                (F.col("passenger_count") > 6),
                F.lit(True)
            ).otherwise(F.lit(False))
        )
        # Add processing timestamp
        .withColumn("bronze_processed_timestamp", F.current_timestamp())
    )


def calculate_bronze_quality_metrics(df: DataFrame) -> DataFrame:
    """
    Calculate data quality metrics for bronze layer monitoring.

    Args:
        df: Bronze enriched DataFrame

    Returns:
        DataFrame with aggregated quality metrics by date
    """
    return (
        df
        .groupBy("pickup_date")
        .agg(
            F.count("*").alias("total_records"),
            F.sum(F.when(F.col("is_suspicious"), 1).otherwise(0)).alias("suspicious_records"),
            F.avg("trip_distance").alias("avg_trip_distance"),
            F.avg("trip_duration_minutes").alias("avg_trip_duration"),
            F.avg("fare_amount").alias("avg_fare_amount"),
            F.min("tpep_pickup_datetime").alias("earliest_pickup"),
            F.max("tpep_pickup_datetime").alias("latest_pickup"),
            F.current_timestamp().alias("metric_timestamp")
        )
        .withColumn(
            "data_quality_score",
            F.round(
                (1 - (F.col("suspicious_records") / F.col("total_records"))) * 100,
                2
            )
        )
    )


# Helper function for configuration
def get_bronze_table_path(catalog: str, schema: str, table_name: str) -> str:
    """
    Get full Unity Catalog path for bronze tables.

    Args:
        catalog: Catalog name
        schema: Schema name
        table_name: Table name

    Returns:
        Full table path in format: catalog.schema.table
    """
    return f"{catalog}.{schema}.{table_name}"