"""
NYC Taxi DLT Pipeline - Main Orchestrator
Unity Catalog Compatible with Serverless Compute

This is the main DLT pipeline file containing all @dlt decorators and I/O operations.
All transformation logic is in the helpers/ modules for reusability and testing.
"""
import dlt
from pyspark.sql import functions as F

# Import transformation functions from helpers
from helpers.bronze_transforms import (
    TAXI_SCHEMA,
    add_ingestion_metadata,
    enrich_with_computed_fields,
    calculate_bronze_quality_metrics
)

from helpers.silver_transforms import (
    clean_and_validate_trips,
    aggregate_daily_stats,
    aggregate_hourly_location_stats,
    analyze_payment_patterns,
    calculate_silver_quality_metrics
)

from helpers.gold_transforms import (
    calculate_daily_kpis,
    analyze_peak_hours,
    analyze_route_performance,
    segment_customers,
    calculate_weekly_trends,
    breakdown_revenue,
    create_executive_summary
)


# ============================================================================
# CONFIGURATION
# ============================================================================

def get_config(key, default=None):
    """Safely get configuration from Spark conf"""
    try:
        return spark.conf.get(key, default)
    except Exception:
        return default


# Pipeline configuration
CATALOG = get_config("catalog_name", "dev_catalog")
SCHEMA = get_config("schema_name", "nyc_taxi_dev")
SOURCE_PATH = get_config("source_path", "/databricks-datasets/nyctaxi/tripdata/yellow/yellow_tripdata_2019-01.csv.gz")
ENVIRONMENT = get_config("environment_suffix", "")


# ============================================================================
# BRONZE LAYER - RAW DATA INGESTION
# ============================================================================

@dlt.table(
    name="bronze_taxi_raw",
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
    """
    # Read from CSV source
    df = (
        spark.read
        .format("csv")
        .option("header", "true")
        .option("inferSchema", "true")
        .option("timestampFormat", "yyyy-MM-dd HH:mm:ss")
        .schema(TAXI_SCHEMA)
        .load(SOURCE_PATH)
    )

    # Apply transformation from helper
    return add_ingestion_metadata(df, ENVIRONMENT)


@dlt.table(
    name="bronze_taxi_enriched",
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
    """
    # Read from previous bronze table
    df = dlt.read("bronze_taxi_raw")

    # Apply transformation from helper
    return enrich_with_computed_fields(df)


@dlt.view(
    name="bronze_taxi_streaming",
    comment="Streaming view of bronze taxi data for real-time processing"
)
def bronze_taxi_streaming():
    """
    Streaming view for continuous ingestion scenarios.
    Can be used for real-time dashboards or downstream streaming tables.
    """
    df = (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("header", "true")
        .option("inferSchema", "true")
        .schema(TAXI_SCHEMA)
        .load(SOURCE_PATH.replace(".csv.gz", ""))
        .withColumn("ingestion_timestamp", F.current_timestamp())
    )

    return df


@dlt.table(
    name="bronze_data_quality_metrics",
    comment="Data quality metrics for bronze layer monitoring",
    table_properties={
        "quality": "metrics"
    }
)
def bronze_data_quality_metrics():
    """
    Aggregate data quality metrics for monitoring and alerting.
    """
    # Read from bronze enriched table
    df = dlt.read("bronze_taxi_enriched")

    # Apply transformation from helper
    return calculate_bronze_quality_metrics(df)


# ============================================================================
# SILVER LAYER - CLEANED AND VALIDATED DATA
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
    """
    # Read from bronze enriched table
    df = dlt.read("bronze_taxi_enriched")

    # Apply transformation from helper
    return clean_and_validate_trips(df)


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
    """
    # Read from silver trips table
    df = dlt.read("silver_taxi_trips")

    # Apply transformation from helper
    return aggregate_daily_stats(df)


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
    """
    # Read from silver trips table
    df = dlt.read("silver_taxi_trips")

    # Apply transformation from helper
    return aggregate_hourly_location_stats(df)


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
    """
    # Read from silver trips table
    df = dlt.read("silver_taxi_trips")

    # Apply transformation from helper
    return analyze_payment_patterns(df)


@dlt.table(
    name="silver_data_quality",
    comment="Data quality metrics for silver layer",
    table_properties={
        "quality": "metrics"
    }
)
def silver_data_quality():
    """
    Monitor data quality by comparing bronze and silver record counts.
    """
    # Read from both bronze and silver tables
    bronze_df = dlt.read("bronze_taxi_enriched")
    silver_df = dlt.read("silver_taxi_trips")

    # Apply transformation from helper
    return calculate_silver_quality_metrics(bronze_df, silver_df)


# ============================================================================
# GOLD LAYER - BUSINESS ANALYTICS
# ============================================================================

@dlt.table(
    name="gold_daily_kpis",
    comment="Daily KPI metrics for executive dashboards - Gold layer",
    table_properties={
        "quality": "gold",
        "pipelines.autoOptimize.managed": "true",
        "business_owner": "analytics_team",
        "refresh_schedule": "daily"
    }
)
def gold_daily_kpis():
    """
    Key Performance Indicators aggregated daily.
    Business-ready table for executive dashboards.
    """
    # Read from silver trips table
    df = dlt.read("silver_taxi_trips")

    # Apply transformation from helper
    return calculate_daily_kpis(df)


@dlt.table(
    name="gold_peak_hours_analysis",
    comment="Peak hour demand patterns for operations planning - Gold layer",
    table_properties={
        "quality": "gold",
        "business_owner": "operations_team"
    }
)
def gold_peak_hours_analysis():
    """
    Identify peak hours and demand patterns for resource allocation.
    """
    # Read from silver trips table
    df = dlt.read("silver_taxi_trips")

    # Apply transformation from helper
    return analyze_peak_hours(df)


@dlt.table(
    name="gold_route_performance",
    comment="Popular routes and their performance metrics - Gold layer",
    table_properties={
        "quality": "gold",
        "business_owner": "strategy_team"
    }
)
def gold_route_performance():
    """
    Analyze performance of popular routes (pickup to dropoff location pairs).
    """
    # Read from silver trips table
    df = dlt.read("silver_taxi_trips")

    # Apply transformation from helper
    return analyze_route_performance(df)


@dlt.table(
    name="gold_customer_segments",
    comment="Customer behavior segmentation based on trip patterns - Gold layer",
    table_properties={
        "quality": "gold",
        "business_owner": "marketing_team"
    }
)
def gold_customer_segments():
    """
    Segment customers based on behavior patterns.
    """
    # Read from silver trips table
    df = dlt.read("silver_taxi_trips")

    # Apply transformation from helper
    return segment_customers(df)


@dlt.table(
    name="gold_weekly_trends",
    comment="Week-over-week trend analysis - Gold layer",
    table_properties={
        "quality": "gold",
        "business_owner": "analytics_team"
    }
)
def gold_weekly_trends():
    """
    Weekly aggregated trends for longer-term analysis.
    """
    # Read from silver trips table
    df = dlt.read("silver_taxi_trips")

    # Apply transformation from helper
    return calculate_weekly_trends(df)


@dlt.table(
    name="gold_revenue_breakdown",
    comment="Detailed revenue breakdown for financial reporting - Gold layer",
    table_properties={
        "quality": "gold",
        "business_owner": "finance_team"
    }
)
def gold_revenue_breakdown():
    """
    Comprehensive revenue breakdown by various dimensions.
    """
    # Read from silver trips table
    df = dlt.read("silver_taxi_trips")

    # Apply transformation from helper
    return breakdown_revenue(df)


@dlt.table(
    name="gold_executive_summary",
    comment="High-level executive summary metrics - Gold layer",
    table_properties={
        "quality": "gold",
        "business_owner": "executive_team",
        "sla": "1_hour"
    }
)
def gold_executive_summary():
    """
    Single-row executive summary with key metrics.
    Updated with latest available data.
    """
    # Read from silver trips and daily stats tables
    df = dlt.read("silver_taxi_trips")
    daily_stats = dlt.read("silver_daily_trip_stats")

    # Apply transformation from helper
    return create_executive_summary(df, daily_stats)


# ============================================================================
# PIPELINE CONFIGURATION LOGGING
# ============================================================================

print("=" * 80)
print("NYC TAXI DLT PIPELINE - CONFIGURATION")
print("=" * 80)
print(f"Catalog:        {CATALOG}")
print(f"Schema:         {SCHEMA}")
print(f"Source Path:    {SOURCE_PATH}")
print(f"Environment:    {ENVIRONMENT}")
print(f"Full Path:      {CATALOG}.{SCHEMA}")
print("=" * 80)
print("\nPipeline Layers:")
print("  ✓ Bronze Layer (4 tables/views)")
print("    - bronze_taxi_raw")
print("    - bronze_taxi_enriched")
print("    - bronze_taxi_streaming (view)")
print("    - bronze_data_quality_metrics")
print("\n  ✓ Silver Layer (5 tables)")
print("    - silver_taxi_trips")
print("    - silver_daily_trip_stats")
print("    - silver_hourly_location_stats")
print("    - silver_payment_analysis")
print("    - silver_data_quality")
print("\n  ✓ Gold Layer (7 tables)")
print("    - gold_daily_kpis")
print("    - gold_peak_hours_analysis")
print("    - gold_route_performance")
print("    - gold_customer_segments")
print("    - gold_weekly_trends")
print("    - gold_revenue_breakdown")
print("    - gold_executive_summary")
print("=" * 80)