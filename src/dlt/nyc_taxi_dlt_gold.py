"""
NYC Taxi DLT Pipeline - Gold Layer
Unity Catalog Compatible with Serverless Compute
Business-ready aggregated tables for analytics and reporting
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


# ============================================================================
# GOLD LAYER - Business KPIs Dashboard
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

    Target: {CATALOG}.{SCHEMA}.gold_daily_kpis
    """
    df = dlt.read("silver_taxi_trips")

    # Calculate daily KPIs
    daily_kpis = (
        df
        .groupBy("pickup_date")
        .agg(
            # Volume Metrics
            F.count("*").alias("total_trips"),
            F.sum("passenger_count").alias("total_passengers"),
            F.sum("trip_distance").alias("total_miles"),

            # Revenue Metrics
            F.sum("total_amount").alias("total_revenue"),
            F.sum("fare_amount").alias("total_fare_revenue"),
            F.sum("tip_amount").alias("total_tip_revenue"),
            F.avg("total_amount").alias("avg_revenue_per_trip"),

            # Operational Metrics
            F.avg("trip_duration_minutes").alias("avg_trip_duration"),
            F.avg("trip_distance").alias("avg_trip_distance"),
            F.avg("avg_speed_mph").alias("avg_speed"),

            # Customer Experience
            F.avg("tip_percentage").alias("avg_tip_percentage"),
            F.avg("passenger_count").alias("avg_passengers_per_trip")
        )
    )

    # Calculate day-over-day changes using window functions
    window_spec = Window.orderBy("pickup_date")

    return (
        daily_kpis
        # Calculate previous day metrics
        .withColumn("prev_day_trips", F.lag("total_trips", 1).over(window_spec))
        .withColumn("prev_day_revenue", F.lag("total_revenue", 1).over(window_spec))

        # Calculate percentage changes
        .withColumn(
            "trips_change_pct",
            F.round(
                ((F.col("total_trips") - F.col("prev_day_trips")) / F.col("prev_day_trips")) * 100,
                2
            )
        )
        .withColumn(
            "revenue_change_pct",
            F.round(
                ((F.col("total_revenue") - F.col("prev_day_revenue")) / F.col("prev_day_revenue")) * 100,
                2
            )
        )

        # Calculate derived business metrics
        .withColumn(
            "revenue_per_mile",
            F.round(F.col("total_revenue") / F.col("total_miles"), 2)
        )
        .withColumn(
            "utilization_rate",
            F.round((F.col("total_miles") / (F.col("total_trips") * 10)) * 100, 2)  # Assuming 10 miles max per trip
        )

        # Add metadata
        .withColumn("last_updated", F.current_timestamp())
        .withColumn("data_freshness_hours", F.lit(24))

        # Select and order columns
        .select(
            "pickup_date",
            "total_trips",
            "trips_change_pct",
            "total_passengers",
            "total_miles",
            "total_revenue",
            "revenue_change_pct",
            "total_fare_revenue",
            "total_tip_revenue",
            "avg_revenue_per_trip",
            "revenue_per_mile",
            "avg_trip_duration",
            "avg_trip_distance",
            "avg_speed",
            "avg_tip_percentage",
            "avg_passengers_per_trip",
            "utilization_rate",
            "last_updated",
            "data_freshness_hours"
        )
        .orderBy("pickup_date")
    )


# ============================================================================
# GOLD LAYER - Peak Hours Analysis
# ============================================================================

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

    Target: {CATALOG}.{SCHEMA}.gold_peak_hours_analysis
    """
    df = dlt.read("silver_taxi_trips")

    return (
        df
        .groupBy("pickup_hour", "is_weekend", "time_of_day")
        .agg(
            F.count("*").alias("total_trips"),
            F.avg("fare_amount").alias("avg_fare"),
            F.avg("trip_distance").alias("avg_distance"),
            F.avg("trip_duration_minutes").alias("avg_duration"),
            F.avg("passenger_count").alias("avg_passengers"),
            F.sum("total_amount").alias("total_revenue"),
            F.countDistinct("pickup_date").alias("days_observed")
        )
        # Calculate average trips per day for this hour
        .withColumn(
            "avg_trips_per_day",
            F.round(F.col("total_trips") / F.col("days_observed"), 0)
        )
        # Calculate revenue per hour
        .withColumn(
            "avg_revenue_per_hour",
            F.round(F.col("total_revenue") / F.col("days_observed"), 2)
        )
        # Rank hours by demand
        .withColumn(
            "demand_rank",
            F.dense_rank().over(
                Window
                .partitionBy("is_weekend")
                .orderBy(F.desc("avg_trips_per_day"))
            )
        )
        .withColumn("last_updated", F.current_timestamp())
        .orderBy("is_weekend", "pickup_hour")
    )


# ============================================================================
# GOLD LAYER - Route Performance Analysis
# ============================================================================

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

    Target: {CATALOG}.{SCHEMA}.gold_route_performance
    """
    df = dlt.read("silver_taxi_trips")

    return (
        df
        .groupBy("PULocationID", "DOLocationID")
        .agg(
            F.count("*").alias("trip_count"),
            F.avg("trip_distance").alias("avg_distance"),
            F.avg("trip_duration_minutes").alias("avg_duration"),
            F.avg("fare_amount").alias("avg_fare"),
            F.avg("tip_amount").alias("avg_tip"),
            F.avg("total_amount").alias("avg_total_revenue"),
            F.sum("total_amount").alias("total_revenue"),
            F.avg("avg_speed_mph").alias("avg_speed")
        )
        # Filter for statistically significant routes (at least 10 trips)
        .filter(F.col("trip_count") >= 10)
        # Calculate profitability metrics
        .withColumn(
            "revenue_per_mile",
            F.round(F.col("total_revenue") / (F.col("avg_distance") * F.col("trip_count")), 2)
        )
        .withColumn(
            "revenue_per_minute",
            F.round(F.col("total_revenue") / (F.col("avg_duration") * F.col("trip_count")), 2)
        )
        # Rank routes by profitability
        .withColumn(
            "profitability_rank",
            F.dense_rank().over(Window.orderBy(F.desc("revenue_per_minute")))
        )
        # Classify route type
        .withColumn(
            "route_type",
            F.when(F.col("avg_distance") < 2, "Short Distance")
            .when(F.col("avg_distance") < 5, "Medium Distance")
            .when(F.col("avg_distance") < 10, "Long Distance")
            .otherwise("Extra Long Distance")
        )
        .withColumn("last_updated", F.current_timestamp())
        .orderBy(F.desc("trip_count"))
    )


# ============================================================================
# GOLD LAYER - Customer Segmentation
# ============================================================================

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

    Target: {CATALOG}.{SCHEMA}.gold_customer_segments
    """
    df = dlt.read("silver_taxi_trips")

    return (
        df
        .groupBy("payment_type_name", "time_of_day", "is_weekend")
        .agg(
            F.count("*").alias("trip_count"),
            F.avg("trip_distance").alias("avg_distance"),
            F.avg("total_amount").alias("avg_spend"),
            F.avg("tip_percentage").alias("avg_tip_pct"),
            F.sum("total_amount").alias("total_revenue"),
            F.avg("passenger_count").alias("avg_passengers")
        )
        # Calculate customer value metrics
        .withColumn(
            "revenue_contribution_pct",
            F.round(
                (F.col("total_revenue") / F.sum("total_revenue").over(Window.partitionBy())) * 100,
                2
            )
        )
        # Segment classification
        .withColumn(
            "customer_segment",
            F.when(
                (F.col("avg_spend") > 30) & (F.col("avg_tip_pct") > 15),
                "Premium"
            )
            .when(
                (F.col("avg_spend") > 20) & (F.col("trip_count") > 100),
                "High Value"
            )
            .when(F.col("avg_spend") < 10, "Budget")
            .otherwise("Standard")
        )
        .withColumn("last_updated", F.current_timestamp())
        .orderBy(F.desc("total_revenue"))
    )


# ============================================================================
# GOLD LAYER - Weekly Trends
# ============================================================================

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

    Target: {CATALOG}.{SCHEMA}.gold_weekly_trends
    """
    df = dlt.read("silver_taxi_trips")

    # Get week start date (Monday)
    weekly_df = (
        df
        .withColumn("week_start", F.date_trunc("week", "pickup_date"))
        .groupBy("week_start")
        .agg(
            F.count("*").alias("total_trips"),
            F.sum("total_amount").alias("total_revenue"),
            F.avg("total_amount").alias("avg_revenue_per_trip"),
            F.sum("trip_distance").alias("total_miles"),
            F.avg("trip_distance").alias("avg_miles_per_trip"),
            F.avg("tip_percentage").alias("avg_tip_pct"),
            F.countDistinct("pickup_date").alias("active_days")
        )
    )

    # Calculate week-over-week changes
    window_spec = Window.orderBy("week_start")

    return (
        weekly_df
        .withColumn("prev_week_trips", F.lag("total_trips", 1).over(window_spec))
        .withColumn("prev_week_revenue", F.lag("total_revenue", 1).over(window_spec))
        .withColumn(
            "trips_wow_change_pct",
            F.round(
                ((F.col("total_trips") - F.col("prev_week_trips")) / F.col("prev_week_trips")) * 100,
                2
            )
        )
        .withColumn(
            "revenue_wow_change_pct",
            F.round(
                ((F.col("total_revenue") - F.col("prev_week_revenue")) / F.col("prev_week_revenue")) * 100,
                2
            )
        )
        # Calculate moving averages
        .withColumn(
            "trips_4week_avg",
            F.round(
                F.avg("total_trips").over(
                    Window.orderBy("week_start").rowsBetween(-3, 0)
                ),
                0
            )
        )
        .withColumn(
            "revenue_4week_avg",
            F.round(
                F.avg("total_revenue").over(
                    Window.orderBy("week_start").rowsBetween(-3, 0)
                ),
                2
            )
        )
        .withColumn("last_updated", F.current_timestamp())
        .select(
            "week_start",
            "total_trips",
            "trips_wow_change_pct",
            "trips_4week_avg",
            "total_revenue",
            "revenue_wow_change_pct",
            "revenue_4week_avg",
            "avg_revenue_per_trip",
            "total_miles",
            "avg_miles_per_trip",
            "avg_tip_pct",
            "active_days",
            "last_updated"
        )
    )


# ============================================================================
# GOLD LAYER - Revenue by Time Period
# ============================================================================

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

    Target: {CATALOG}.{SCHEMA}.gold_revenue_breakdown
    """
    df = dlt.read("silver_taxi_trips")

    return (
        df
        .groupBy("pickup_date", "time_of_day", "payment_type_name")
        .agg(
            # Revenue components
            F.sum("fare_amount").alias("base_fare_revenue"),
            F.sum("extra").alias("extra_charges_revenue"),
            F.sum("mta_tax").alias("mta_tax_revenue"),
            F.sum("tip_amount").alias("tip_revenue"),
            F.sum("tolls_amount").alias("tolls_revenue"),
            F.sum("improvement_surcharge").alias("surcharge_revenue"),
            F.sum("total_amount").alias("total_revenue"),

            # Volume metrics
            F.count("*").alias("trip_count"),
            F.sum("trip_distance").alias("total_miles")
        )
        # Calculate percentages
        .withColumn(
            "base_fare_pct",
            F.round((F.col("base_fare_revenue") / F.col("total_revenue")) * 100, 2)
        )
        .withColumn(
            "tip_pct",
            F.round((F.col("tip_revenue") / F.col("total_revenue")) * 100, 2)
        )
        # Calculate per-trip metrics
        .withColumn(
            "avg_revenue_per_trip",
            F.round(F.col("total_revenue") / F.col("trip_count"), 2)
        )
        .withColumn(
            "avg_revenue_per_mile",
            F.round(F.col("total_revenue") / F.col("total_miles"), 2)
        )
        .withColumn("last_updated", F.current_timestamp())
        .orderBy("pickup_date", "time_of_day", "payment_type_name")
    )


# ============================================================================
# GOLD LAYER - Executive Summary
# ============================================================================

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

    Target: {CATALOG}.{SCHEMA}.gold_executive_summary
    """
    df = dlt.read("silver_taxi_trips")
    daily_stats = dlt.read("silver_daily_trip_stats")

    # Get latest date using window function instead of collect
    latest_metrics = (
        df
        .withColumn("max_date", F.max("pickup_date").over(Window.partitionBy()))
        .filter(F.col("pickup_date") == F.col("max_date"))
    )

    # Get 7-day and 30-day averages using date arithmetic
    summary = (
        latest_metrics
        .agg(
            F.max("pickup_date").alias("report_date"),
            F.count("*").alias("trips_today"),
            F.sum("total_amount").alias("revenue_today"),
            F.avg("total_amount").alias("avg_revenue_per_trip_today")
        )
        .crossJoin(
            daily_stats
            .join(
                latest_metrics.select(F.max("pickup_date").alias("latest_date")),
                on=[F.col("pickup_date") >= F.date_sub(F.col("latest_date"), 7)]
            )
            .agg(
                F.avg("total_trips").alias("avg_trips_7d"),
                F.avg("total_revenue").alias("avg_revenue_7d")
            )
        )
        .crossJoin(
            daily_stats
            .join(
                latest_metrics.select(F.max("pickup_date").alias("latest_date")),
                on=[F.col("pickup_date") >= F.date_sub(F.col("latest_date"), 30)]
            )
            .agg(
                F.avg("total_trips").alias("avg_trips_30d"),
                F.avg("total_revenue").alias("avg_revenue_30d")
            )
        )
    )

    return (
        summary
        .withColumn(
            "trips_vs_7d_avg_pct",
            F.round(((F.col("trips_today") - F.col("avg_trips_7d")) / F.col("avg_trips_7d")) * 100, 2)
        )
        .withColumn(
            "revenue_vs_7d_avg_pct",
            F.round(((F.col("revenue_today") - F.col("avg_revenue_7d")) / F.col("avg_revenue_7d")) * 100, 2)
        )
        .withColumn("last_updated", F.current_timestamp())
        .withColumn("data_quality_status", F.lit("Good"))
    )


# ============================================================================
# HELPER FUNCTIONS
# ============================================================================

def get_gold_table_path(table_name):
    """Helper function to get full Unity Catalog path for gold tables"""
    return f"{CATALOG}.{SCHEMA}.{table_name}"


# Print configuration for debugging
print(f"DLT Gold Layer Configuration:")
print(f"  Catalog: {CATALOG}")
print(f"  Schema: {SCHEMA}")
print(f"  Full table path example: {get_gold_table_path('gold_daily_kpis')}")