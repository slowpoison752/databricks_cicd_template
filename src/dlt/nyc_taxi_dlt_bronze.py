"""
NYC Taxi DLT Pipeline - Bronze Layer
Unity Catalog Compatible with Serverless Compute
"""
import dlt
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType, TimestampType

# Get configuration from DLT pipeline settings
def get_config(key, default=None):
    """Safely get configuration from Spark conf"""
    try:
        return spark.conf.get(key, default)
    except Exception:
        return default

# Configuration
SOURCE_PATH = get_config("source_path", "/databricks-datasets/nyctaxi/tripdata/yellow/yellow_tripdata_2019-01.csv.gz")
CATALOG = get_config("catalog_name", "dev_catalog")
SCHEMA = get_config("schema_name", "nyc_taxi_dev")
ENVIRONMENT = get_config("environment_suffix", "")

# Table name suffix for feature branch isolation
#TABLE_SUFFIX = ENVIRONMENT.replace("-", "_").replace("/", "_")  # Sanitize suffix

# Define schema for NYC taxi data
taxi_schema = StructType([
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

# ============================================================================
# BRONZE LAYER - Raw Data Ingestion
# ============================================================================

@dlt.table(
    name=f"{CATALOG}.{SCHEMA}.bronze_taxi_raw",
    comment="Raw NYC taxi trip data ingested from CSV - Bronze layer",
    table_properties={
        "quality": "bronze",
        "pipelines.autoOptimize.managed": "true",
        "delta.enableChangeDataFeed": "true"
    }
)
@dlt.expect_all_or_drop({
    "valid_vendor": "VendorID IS NOT NULL",
    "valid_pickup_time": "tpep_pickup_datetime IS NOT NULL",
    "valid_dropoff_time": "tpep_dropoff_datetime IS NOT NULL"
})
def bronze_taxi_raw():
    """
    Ingest raw taxi data from CSV with data quality expectations.
    Drops rows that don't meet basic quality standards.

    Target: {CATALOG}.{SCHEMA}.bronze_taxi_raw
    """
    return (
        spark.read
        .format("csv")
        .option("header", "true")
        .option("inferSchema", "true")
        .option("timestampFormat", "yyyy-MM-dd HH:mm:ss")
        .schema(taxi_schema)
        .load(SOURCE_PATH)
        .withColumn("ingestion_timestamp", F.current_timestamp())
        .withColumn("source_file", F.input_file_name())
        .withColumn("environment", F.lit(ENVIRONMENT))
    )

# ============================================================================
# BRONZE LAYER - Enriched with Metadata
# ============================================================================

@dlt.table(
    name=f"{CATALOG}.{SCHEMA}.bronze_taxi_enriched",
    comment="Bronze taxi data enriched with computed columns and metadata",
    table_properties={
        "quality": "bronze",
        "pipelines.autoOptimize.managed": "true"
    }
)
@dlt.expect_all({
    "positive_trip_distance": "trip_distance > 0",
    "positive_passenger_count": "passenger_count > 0",
    "valid_fare": "fare_amount >= 0",
    "valid_trip_duration": "trip_duration_minutes > 0"
})
def bronze_taxi_enriched():
    """
    Enriched bronze layer with additional computed columns.
    Adds data quality warnings but keeps all records.

    Target: {CATALOG}.{SCHEMA}.bronze_taxi_enriched
    """
    # Read from bronze_taxi_raw using the suffixed name
    df = dlt.read(f"bronze_taxi_raw")

    return (
        df
        # Calculate trip duration
        .withColumn(
            "trip_duration_minutes",
            (F.col("tpep_dropoff_datetime").cast("long") -
             F.col("tpep_pickup_datetime").cast("long")) / 60
        )
        # Calculate average speed
        .withColumn(
            "avg_speed_mph",
            F.when(
                F.col("trip_duration_minutes") > 0,
                F.col("trip_distance") / (F.col("trip_duration_minutes") / 60)
            ).otherwise(0)
        )
        # Extract date components
        .withColumn("pickup_date", F.to_date("tpep_pickup_datetime"))
        .withColumn("pickup_hour", F.hour("tpep_pickup_datetime"))
        .withColumn("pickup_day_of_week", F.dayofweek("tpep_pickup_datetime"))
        # Add data quality flags
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
        # Add processing metadata
        .withColumn("bronze_processed_timestamp", F.current_timestamp())
    )

# ============================================================================
# BRONZE LAYER - Streaming View (Optional)
# ============================================================================

@dlt.view(
    name="bronze_taxi_streaming",
    comment="Streaming view of bronze taxi data for real-time processing"
)
def bronze_taxi_streaming():
    """
    Streaming view for continuous ingestion scenarios.
    Can be used for real-time dashboards or downstream streaming tables.
    """
    return (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("header", "true")
        .option("inferSchema", "true")
        .schema(taxi_schema)
        .load(SOURCE_PATH.replace(".csv.gz", ""))  # Point to directory for streaming
        .withColumn("ingestion_timestamp", F.current_timestamp())
    )

# ============================================================================
# DATA QUALITY METRICS TABLE
# ============================================================================

@dlt.table(
    name=f"{CATALOG}.{SCHEMA}.bronze_data_quality_metrics",
    comment="Data quality metrics for bronze layer monitoring",
    table_properties={
        "quality": "metrics"
    }
)
def bronze_data_quality_metrics():
    """
    Aggregate data quality metrics for monitoring and alerting.

    Target: {CATALOG}.{SCHEMA}.bronze_data_quality_metrics
    """
    df = dlt.read(f"bronze_taxi_enriched")

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

# ============================================================================
# HELPER FUNCTIONS
# ============================================================================

def get_bronze_table_path(table_name):
    """Helper function to get full Unity Catalog path for bronze tables"""
    return f"{CATALOG}.{SCHEMA}.{table_name}"

# Print configuration for debugging (visible in DLT logs)
print(f"DLT Bronze Layer Configuration:")
print(f"  Catalog: {CATALOG}")
print(f"  Schema: {SCHEMA}")
print(f"  Source Path: {SOURCE_PATH}")
print(f"  Environment: {ENVIRONMENT}")
print(f"  Full table path example: {get_bronze_table_path('bronze_taxi_raw')}")