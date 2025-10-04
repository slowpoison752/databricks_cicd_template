"""
NYC Taxi DLT Pipeline - Silver Layer Transformation Functions
Pure transformation logic without DLT decorators

These functions contain only the transformation logic and return DataFrames.
They are called by the main dlt_pipeline.py which handles the @dlt decorators.
"""
from pyspark.sql import DataFrame, functions as F
from pyspark.sql.window import Window


def clean_and_validate_trips(df: DataFrame) -> DataFrame:
    """
    Clean and validate taxi trip data with strict quality standards.

    Args:
        df: Bronze enriched DataFrame

    Returns:
        Cleaned and validated DataFrame ready for silver layer
    """
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
        # Standardize payment type
        .withColumn(
            "payment_type_name",
            F.when(F.col("payment_type") == 1, "Credit Card")
            .when(F.col("payment_type") == 2, "Cash")
            .when(F.col("payment_type") == 3, "No Charge")
            .when(F.col("payment_type") == 4, "Dispute")
            .otherwise("Unknown")
        )
        # Standardize rate code
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


def aggregate_daily_stats(df: DataFrame) -> DataFrame:
    """
    Calculate daily aggregated trip statistics.

    Args:
        df: Silver taxi trips DataFrame

    Returns:
        DataFrame with daily aggregated metrics
    """
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


def aggregate_hourly_location_stats(df: DataFrame) -> DataFrame:
    """
    Calculate hourly statistics by pickup location.

    Args:
        df: Silver taxi trips DataFrame

    Returns:
        DataFrame with hourly location-based metrics
    """
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


def analyze_payment_patterns(df: DataFrame) -> DataFrame:
    """
    Analyze payment methods and tipping behavior.

    Args:
        df: Silver taxi trips DataFrame

    Returns:
        DataFrame with payment analysis metrics
    """
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


def calculate_silver_quality_metrics(bronze_df: DataFrame, silver_df: DataFrame) -> DataFrame:
    """
    Calculate data quality metrics comparing bronze and silver layers.

    Args:
        bronze_df: Bronze enriched DataFrame
        silver_df: Silver taxi trips DataFrame

    Returns:
        DataFrame with quality metrics
    """
    from pyspark.sql import SparkSession
    spark = SparkSession.builder.getOrCreate()

    bronze_count = bronze_df.count()
    silver_count = silver_df.count()
    pass_rate = round((silver_count / bronze_count) * 100, 2) if bronze_count > 0 else 0

    return spark.sql(f"""
        SELECT 
            current_date() as date,
            {bronze_count} as bronze_records,
            {silver_count} as silver_records,
            {pass_rate} as pass_rate_percentage,
            {bronze_count - silver_count} as records_dropped,
            current_timestamp() as check_timestamp
    """)


def get_silver_table_path(catalog: str, schema: str, table_name: str) -> str:
    """
    Get full Unity Catalog path for silver tables.

    Args:
        catalog: Catalog name
        schema: Schema name
        table_name: Table name

    Returns:
        Full table path in format: catalog.schema.table
    """
    return f"{catalog}.{schema}.{table_name}"