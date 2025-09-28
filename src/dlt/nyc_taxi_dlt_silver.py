"""
NYC Taxi DLT Silver Pipeline
Business logic and aggregations on cleaned bronze data

Silver Layer Features:
- Business rule enforcement
- Data enrichment
- Aggregations and metrics
- Dimension modeling
"""

import dlt
from pyspark.sql.functions import (
    col, when, lit, round as spark_round,
    date_format, hour, dayofweek,
    avg, sum as spark_sum, count, max as spark_max, min as spark_min,
    current_timestamp, window
)


@dlt.table(
    name="nyc_taxi_silver",
    comment="Business-ready NYC taxi data with enrichments and business rules",
    table_properties={
        "quality": "silver",
        "layer": "curated",
        "pipelines.autoOptimize.managed": "true"
    }
)
@dlt.expect_all({
    "business_valid_fare": "fare_amount_clean > 0",
    "business_valid_distance": "trip_distance_clean > 0",
    "reasonable_speed": "trip_speed_mph <= 200",  # No trips faster than 200 mph
    "positive_duration": "trip_duration_minutes > 0"
})
def silver_taxi_data():
    """
    Silver layer with business logic and enrichments
    Only processes CLEAN records from bronze
    """
    return (
        dlt.read("nyc_taxi_bronze")
        .filter(col("data_quality_flag") == "CLEAN")  # Only clean records

        # Business rule: Clean fare amounts
        .withColumn(
            "fare_amount_clean",
            when(col("fare_amount") < 0, 0).otherwise(col("fare_amount"))
        )
        .withColumn(
            "trip_distance_clean",
            when(col("trip_distance") < 0, 0).otherwise(col("trip_distance"))
        )

        # Calculate business metrics
        .withColumn(
            "trip_duration_minutes",
            (col("dropoff_datetime").cast("long") - col("pickup_datetime").cast("long")) / 60
        )
        .withColumn(
            "trip_speed_mph",
            when(
                (col("trip_duration_minutes") > 0) & (col("trip_distance_clean") > 0),
                spark_round((col("trip_distance_clean") / col("trip_duration_minutes")) * 60, 2)
            ).otherwise(0)
        )

        # Time-based dimensions
        .withColumn("pickup_hour", hour(col("pickup_datetime")))
        .withColumn("pickup_day_of_week", dayofweek(col("pickup_datetime")))
        .withColumn("pickup_date", col("pickup_datetime").cast("date"))

        # Business categorizations
        .withColumn(
            "time_of_day_category",
            when(col("pickup_hour").between(6, 11), "Morning")
            .when(col("pickup_hour").between(12, 17), "Afternoon")
            .when(col("pickup_hour").between(18, 21), "Evening")
            .otherwise("Night")
        )
        .withColumn(
            "trip_distance_category",
            when(col("trip_distance_clean") <= 1, "Short")
            .when(col("trip_distance_clean") <= 5, "Medium")
            .when(col("trip_distance_clean") <= 15, "Long")
            .otherwise("Very Long")
        )
        .withColumn(
            "fare_category",
            when(col("fare_amount_clean") <= 10, "Budget")
            .when(col("fare_amount_clean") <= 25, "Standard")
            .when(col("fare_amount_clean") <= 50, "Premium")
            .otherwise("Luxury")
        )

        # Revenue calculations
        .withColumn(
            "revenue_per_mile",
            when(
                col("trip_distance_clean") > 0,
                spark_round(col("total_amount") / col("trip_distance_clean"), 2)
            ).otherwise(0)
        )

        # Add silver layer metadata
        .withColumn("silver_processing_timestamp", current_timestamp())
        .withColumn("pipeline_stage", lit("silver"))
    )


@dlt.table(
    name="nyc_taxi_daily_metrics",
    comment="Daily aggregated metrics for business reporting"
)
def daily_metrics():
    """
    Daily aggregated business metrics
    Perfect for dashboards and reporting
    """
    return (
        dlt.read("nyc_taxi_silver")
        .groupBy("pickup_date")
        .agg(
            count("*").alias("total_trips"),
            spark_sum("fare_amount_clean").alias("total_fare_revenue"),
            spark_sum("total_amount").alias("total_revenue"),
            avg("fare_amount_clean").alias("avg_fare"),
            avg("trip_distance_clean").alias("avg_distance"),
            avg("trip_duration_minutes").alias("avg_duration_minutes"),
            avg("trip_speed_mph").alias("avg_speed_mph"),
            spark_max("total_amount").alias("max_fare"),
            spark_min("total_amount").alias("min_fare")
        )
        .withColumn("metrics_date", current_timestamp())
        .withColumn("environment", lit(spark.conf.get("environment_suffix", "")))
    )


@dlt.table(
    name="nyc_taxi_hourly_patterns",
    comment="Hourly demand patterns for operational insights"
)
def hourly_patterns():
    """
    Hourly demand patterns
    Useful for fleet management and pricing optimization
    """
    return (
        dlt.read("nyc_taxi_silver")
        .groupBy("pickup_hour", "pickup_day_of_week", "time_of_day_category")
        .agg(
            count("*").alias("trip_count"),
            avg("fare_amount_clean").alias("avg_fare"),
            avg("trip_distance_clean").alias("avg_distance"),
            avg("passenger_count_clean").alias("avg_passengers")
        )
        .withColumn("pattern_analysis_timestamp", current_timestamp())
    )


# STREAMING TABLE EXAMPLE (if you want real-time processing)
# Uncomment if you have streaming data sources
"""
@dlt.table(
    name="nyc_taxi_silver_streaming",
    comment="Real-time taxi data processing"
)
def silver_streaming():
    return (
        dlt.read_stream("nyc_taxi_bronze")
        .filter(col("data_quality_flag") == "CLEAN")
        # Add your streaming transformations here
        .withColumn("stream_processing_time", current_timestamp())
    )
"""