"""
NYC Taxi DLT Pipeline - Silver Layer
Unity Catalog Compatible with Serverless Compute
"""
import dlt
from pyspark.sql import functions as F
from pyspark.sql.window import Window

# Get configuration from DLT pipeline settings
def get_config(key, default=None):
    """Safely get configuration from Spark conf"""
    try:
        return spark.conf.get(key, default)
    except Exception:
        return default

# Configuration
CATALOG = get_config("catalog_name", "dev_catalog")
SCHEMA = get_config("schema_name", "nyc_taxi_dev")
ENVIRONMENT = get_config("environment_suffix", "")


# ============================================================================
# SILVER LAYER - Cleaned and Validated Data
# ============================================================================

@dlt.table(
    name="silver_taxi_trips",
    comment="Cleaned and validated taxi trip data - Silver layer",
    table_properties={
        "quality": "silver",
        "pipelines.autoOptimize.managed": "true",
        "delta.enableChangeDataFeed": "true"
    },
    partition_cols=["pickup_date"]
)
@dlt.expect_all_or_drop({
    "valid_trip_distance": "trip_distance > 0 AND trip_distance < 100",
    "valid_passenger_count": "passenger_count > 0 AND passenger_count <= 6",
    "valid_fare": "fare_amount > 0 AND fare_amount < 500",
    "valid_duration": "trip_duration_minutes > 0 AND trip_duration_minutes < 180",
    "valid_speed": "avg_speed_mph > 0 AND avg_speed_mph < 80",
    "logical_timestamps": "tpep_pickup_datetime < tpep_dropoff_datetime"
})
def silver_taxi_trips():
    """
    Cleaned taxi trip data with strict quality expectations.
    Drops records that don't meet silver layer standards.

    Target: {CATALOG}.{SCHEMA}.silver_taxi_trips
    """
    # Read from bronze enriched table
    df = dlt.read("bronze_taxi_enriched")

    return (
        df
        # Filter out suspicious records
        .filter(~F.col("is_suspicious"))
        # Remove duplicates based on trip characteristics
        .dropDuplicates([
            "tpep_pickup_datetime",
            "tpep_dropoff_datetime",
            "PULocationID",
            "DOLocationID",
            "passenger_count"
        ])
        # Standardize and clean data
        .withColumn(
            "payment_type_name",
            F.when(F.col("payment_type") == 1, "Credit Card")
            .when(F.col("payment_type") == 2, "Cash")
            .when(F.col("payment_type") == 3, "No Charge")
            .when(F.col("payment_type") == 4, "Dispute")
            .otherwise("Unknown")
        )
        .withColumn(
            "rate_code_name",
            F.when(F.col("RatecodeID") == 1, "Standard")
            .when(F.col("RatecodeID") == 2, "JFK")
            .when(F.col("RatecodeID") == 3, "Newark")
            .when(F.col("RatecodeID") == 4, "Nassau or Westchester")
            .when(F.col("RatecodeID") == 5, "Negotiated")
            .when(F.col("RatecodeID") == 6, "Group Ride")
            .otherwise("Unknown")
        )
        # Calculate fare per mile
        .withColumn(
            "fare_per_mile",
            F.round(F.col("fare_amount") / F.col("trip_distance"), 2)
        )
        # Calculate tip percentage
        .withColumn(
            "tip_percentage",
            F.round((F.col("tip_amount") / F.col("fare_amount")) * 100, 2)
        )
        # Add time-based features
        .withColumn(
            "is_weekend",
            F.when(F.col("pickup_day_of_week").isin([1, 7]), True).otherwise(False)
        )
        .withColumn(
            "time_of_day",
            F.when((F.col("pickup_hour") >= 6) & (F.col("pickup_hour") < 12), "Morning")
            .when((F.col("pickup_hour") >= 12) & (F.col("pickup_hour") < 18), "Afternoon")
            .when((F.col("pickup_hour") >= 18) & (F.col("pickup_hour") < 22), "Evening")
            .otherwise("Night")
        )
        # Add processing metadata
        .withColumn("silver_processed_timestamp", F.current_timestamp())
        # Select relevant columns
        .select(
            "VendorID",
            "tpep_pickup_datetime",
            "tpep_dropoff_datetime",
            "pickup_date",
            "pickup_hour",
            "pickup_day_of_week",
            "is_weekend",
            "time_of_day",
            "passenger_count",
            "trip_distance",
            "trip_duration_minutes",
            "avg_speed_mph",
            "PULocationID",
            "DOLocationID",
            "RatecodeID",
            "rate_code_name",
            "payment_type",
            "payment_type_name",
            "fare_amount",
            "extra",
            "mta_tax",
            "tip_amount",
            "tip_percentage",
            "tolls_amount",
            "improvement_surcharge",
            "total_amount",
            "fare_per_mile",
            "ingestion_timestamp",
            "silver_processed_timestamp"
        )
    )

# ============================================================================
# SILVER LAYER - Aggregated Daily Stats
# ============================================================================

@dlt.table(
    name="silver_daily_trip_stats",
    comment="Daily aggregated trip statistics - Silver layer",
    table_properties={
        "quality": "silver",
        "pipelines.autoOptimize.managed": "true"
    },
    partition_cols=["pickup_date"]
)
def silver_daily_trip_stats():
    """
    Daily aggregated statistics for taxi trips.
    Useful for trend analysis and reporting.

    Target: {CATALOG}.{SCHEMA}.silver_daily_trip_stats
    """
    df = dlt.read("silver_taxi_trips")

    return (
        df
        .groupBy("pickup_date")
        .agg(
            # Trip counts
            F.count("*").alias("total_trips"),
            F.countDistinct("VendorID").alias("unique_vendors"),

            # Distance metrics
            F.avg("trip_distance").alias("avg_trip_distance"),
            F.sum("trip_distance").alias("total_trip_distance"),
            F.min("trip_distance").alias("min_trip_distance"),
            F.max("trip_distance").alias("max_trip_distance"),

            # Duration metrics
            F.avg("trip_duration_minutes").alias("avg_trip_duration"),
            F.min("trip_duration_minutes").alias("min_trip_duration"),
            F.max("trip_duration_minutes").alias("max_trip_duration"),

            # Fare metrics
            F.avg("fare_amount").alias("avg_fare_amount"),
            F.sum("fare_amount").alias("total_fare_amount"),
            F.avg("tip_amount").alias("avg_tip_amount"),
            F.avg("tip_percentage").alias("avg_tip_percentage"),
            F.sum("total_amount").alias("total_revenue"),

            # Passenger metrics
            F.avg("passenger_count").alias("avg_passengers"),
            F.sum("passenger_count").alias("total_passengers"),

            # Speed metrics
            F.avg("avg_speed_mph").alias("avg_speed"),

            # Payment distribution
            F.sum(F.when(F.col("payment_type") == 1, 1).otherwise(0)).alias("credit_card_trips"),
            F.sum(F.when(F.col("payment_type") == 2, 1).otherwise(0)).alias("cash_trips"),

            # Time distribution
            F.sum(F.when(F.col("is_weekend"), 1).otherwise(0)).alias("weekend_trips"),

            # Metadata
            F.current_timestamp().alias("aggregation_timestamp")
        )
        .withColumn(
            "avg_fare_per_mile",
            F.round(F.col("total_fare_amount") / F.col("total_trip_distance"), 2)
        )
        .withColumn(
            "credit_card_percentage",
            F.round((F.col("credit_card_trips") / F.col("total_trips")) * 100, 2)
        )
    )

# ============================================================================
# SILVER LAYER - Hourly Stats by Location
# ============================================================================

@dlt.table(
    name="silver_hourly_location_stats",
    comment="Hourly trip statistics by pickup location - Silver layer",
    table_properties={
        "quality": "silver",
        "pipelines.autoOptimize.managed": "true"
    },
    partition_cols=["pickup_date"]
)
def silver_hourly_location_stats():
    """
    Hourly statistics aggregated by pickup location.
    Useful for location-based analysis and demand forecasting.

    Target: {CATALOG}.{SCHEMA}.silver_hourly_location_stats
    """
    df = dlt.read("silver_taxi_trips")

    return (
        df
        .groupBy("pickup_date", "pickup_hour", "PULocationID")
        .agg(
            F.count("*").alias("trip_count"),
            F.avg("trip_distance").alias("avg_distance"),
            F.avg("trip_duration_minutes").alias("avg_duration"),
            F.avg("fare_amount").alias("avg_fare"),
            F.avg("passenger_count").alias("avg_passengers"),
            F.countDistinct("DOLocationID").alias("unique_destinations"),
            F.current_timestamp().alias("aggregation_timestamp")
        )
        .withColumn(
            "trips_rank",
            F.dense_rank().over(
                Window
                .partitionBy("pickup_date", "pickup_hour")
                .orderBy(F.desc("trip_count"))
            )
        )
    )

# ============================================================================
# SILVER LAYER - Payment Analysis
# ============================================================================

@dlt.table(
    name="silver_payment_analysis",
    comment="Payment method analysis with tipping patterns - Silver layer",
    table_properties={
        "quality": "silver"
    }
)
def silver_payment_analysis():
    """
    Analysis of payment methods and tipping behavior.

    Target: {CATALOG}.{SCHEMA}.silver_payment_analysis
    """
    df = dlt.read("silver_taxi_trips")

    return (
        df
        .groupBy("pickup_date", "payment_type_name", "time_of_day")
        .agg(
            F.count("*").alias("trip_count"),
            F.avg("tip_amount").alias("avg_tip_amount"),
            F.avg("tip_percentage").alias("avg_tip_percentage"),
            F.avg("fare_amount").alias("avg_fare_amount"),
            F.avg("total_amount").alias("avg_total_amount"),
            F.sum("total_amount").alias("total_revenue"),
            F.current_timestamp().alias("aggregation_timestamp")
        )
        .withColumn(
            "revenue_percentage",
            F.round(
                (F.col("total_revenue") / F.sum("total_revenue").over(Window.partitionBy("pickup_date"))) * 100,
                2
            )
        )
    )

# ============================================================================
# SILVER LAYER - Data Quality Checks
# ============================================================================

@dlt.table(
    name="silver_data_quality",
    comment="Data quality metrics for silver layer",
    table_properties={
        "quality": "metrics"
    }
)
def silver_data_quality():
    bronze_df = dlt.read(f"bronze_taxi_enriched")
    silver_df = dlt.read(f"silver_taxi_trips")

    bronze_count = bronze_df.count()
    silver_count = silver_df.count()
    pass_rate = round((silver_count / bronze_count) * 100, 2) if bronze_count > 0 else 0

    # Use SQL to create the dataframe instead
    return spark.sql(f"""
        SELECT 
            current_date() as date,
            {bronze_count} as bronze_records,
            {silver_count} as silver_records,
            {pass_rate} as pass_rate_percentage,
            {bronze_count - silver_count} as records_dropped,
            current_timestamp() as check_timestamp
    """)
# ============================================================================
# HELPER FUNCTIONS
# ============================================================================

def get_silver_table_path(table_name):
    """Helper function to get full Unity Catalog path for silver tables"""
    return f"{CATALOG}.{SCHEMA}.{table_name}"

# Print configuration for debugging
print(f"DLT Silver Layer Configuration:")
print(f"  Catalog: {CATALOG}")
print(f"  Schema: {SCHEMA}")
print(f"  Full table path example: {get_silver_table_path('silver_taxi_trips')}")