# Databricks notebook source
# MAGIC %md
# MAGIC # Executive KPI Dashboard
# MAGIC
# MAGIC Real-time business metrics from Gold layer tables
# MAGIC
# MAGIC **Configuration:**
# MAGIC - Catalog: `dev_catalog`
# MAGIC - Schema: `nyc_taxi`
# MAGIC - Refresh: Auto-refresh every 5 minutes

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuration

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.window import Window
import datetime

# Widgets for dynamic configuration
dbutils.widgets.text("catalog", "dev_catalog", "Catalog")
dbutils.widgets.text("schema", "nyc_taxi", "Schema")
dbutils.widgets.text("suffix", "", "Table Suffix")
dbutils.widgets.dropdown("days_back", "30", ["7", "14", "30", "60", "90"], "Days Back")

# Get widget values
CATALOG = dbutils.widgets.get("catalog")
SCHEMA = dbutils.widgets.get("schema")
SUFFIX = dbutils.widgets.get("suffix")
DAYS_BACK = int(dbutils.widgets.get("days_back"))


# Full table paths
def get_table(name):
    return f"{CATALOG}.{SCHEMA}.{name}{SUFFIX}"


print(f"Dashboard Configuration:")
print(f"  Catalog: {CATALOG}")
print(f"  Schema: {SCHEMA}")
print(f"  Suffix: {SUFFIX}")
print(f"  Days Back: {DAYS_BACK}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Executive Summary

# COMMAND ----------

# DBTITLE 1,📊 Executive Summary - Current Performance
exec_summary = spark.table(get_table("gold_executive_summary"))

display(
    exec_summary
    .select(
        "report_date",
        F.format_number("trips_today", 0).alias("Trips Today"),
        F.concat(F.lit("$"), F.format_number("revenue_today", 2)).alias("Revenue Today"),
        F.concat(F.lit("$"), F.format_number("avg_revenue_per_trip_today", 2)).alias("Avg Revenue/Trip"),
        F.concat(F.round("trips_vs_7d_avg_pct", 1), F.lit("%")).alias("Trips vs 7d Avg"),
        F.concat(F.round("revenue_vs_7d_avg_pct", 1), F.lit("%")).alias("Revenue vs 7d Avg"),
        F.format_number("avg_trips_7d", 0).alias("7-Day Avg Trips"),
        F.concat(F.lit("$"), F.format_number("avg_revenue_7d", 2)).alias("7-Day Avg Revenue"),
        "data_quality_status",
        "last_updated"
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Daily KPIs Overview

# COMMAND ----------

# DBTITLE 1,📈 Key Performance Indicators - Last {DAYS_BACK} Days
daily_kpis_df = spark.table(get_table("gold_daily_kpis"))

# Filter for selected time period
daily_kpis_filtered = (
    daily_kpis_df
    .orderBy(F.desc("pickup_date"))
    .limit(DAYS_BACK)
)

display(
    daily_kpis_filtered
    .orderBy(F.desc("pickup_date"))
    .select(
        F.date_format("pickup_date", "MM/dd/yyyy").alias("Date"),
        F.format_number("total_trips", 0).alias("Total Trips"),
        F.concat(F.round("trips_change_pct", 1), F.lit("%")).alias("Trips Change %"),
        F.concat(F.lit("$"), F.format_number("total_revenue", 2)).alias("Revenue"),
        F.concat(F.round("revenue_change_pct", 1), F.lit("%")).alias("Revenue Change %"),
        F.concat(F.lit("$"), F.format_number("avg_revenue_per_trip", 2)).alias("Avg Revenue/Trip"),
        F.concat(F.round("avg_tip_percentage", 1), F.lit("%")).alias("Avg Tip %"),
        F.format_number("total_miles", 0).alias("Total Miles"),
        F.concat(F.round("utilization_rate", 1), F.lit("%")).alias("Utilization %")
    )
)

# COMMAND ----------

# DBTITLE 1,💰 Revenue Trend - Last {DAYS_BACK} Days
revenue_trend = (
    daily_kpis_filtered
    .orderBy("pickup_date")
    .select(
        "pickup_date",
        F.round("total_revenue", 2).alias("total_revenue"),
        "revenue_change_pct"
    )
)

display(revenue_trend)

# COMMAND ----------

# DBTITLE 1,🚕 Trip Volume Trend with Moving Average
trip_trend = (
    daily_kpis_filtered
    .orderBy("pickup_date")
    .select("pickup_date", "total_trips", "trips_change_pct")
    .withColumn(
        "7_day_moving_avg",
        F.round(
            F.avg("total_trips").over(
                Window.orderBy("pickup_date").rowsBetween(-6, 0)
            ),
            0
        )
    )
)

display(trip_trend)

# COMMAND ----------

# DBTITLE 1,📊 Key Metrics Comparison
metrics_comparison = (
    daily_kpis_filtered
    .orderBy("pickup_date")
    .select(
        "pickup_date",
        F.round("avg_revenue_per_trip", 2).alias("Avg Revenue/Trip"),
        F.round("avg_trip_distance", 2).alias("Avg Trip Distance (miles)"),
        F.round("avg_trip_duration", 2).alias("Avg Duration (min)"),
        F.round("avg_tip_percentage", 1).alias("Avg Tip %")
    )
)

display(metrics_comparison)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Hourly Demand Patterns

# COMMAND ----------

# DBTITLE 1,⏰ Peak Hours Analysis - Weekday vs Weekend
hourly_patterns = spark.table(get_table("gold_peak_hours_analysis"))

display(
    hourly_patterns
    .select(
        "pickup_hour",
        "time_of_day",
        F.when(F.col("is_weekend"), "Weekend").otherwise("Weekday").alias("day_type"),
        F.format_number("avg_trips_per_day", 0).alias("Avg Trips/Day"),
        F.concat(F.lit("$"), F.format_number("avg_revenue_per_hour", 2)).alias("Avg Revenue/Hour"),
        F.concat(F.lit("$"), F.format_number("avg_fare", 2)).alias("Avg Fare"),
        F.round("avg_distance", 2).alias("Avg Distance"),
        "demand_rank"
    )
    .orderBy("day_type", "pickup_hour")
)

# COMMAND ----------

# DBTITLE 1,🔥 Top 10 Peak Hours - Weekday
display(
    hourly_patterns
    .filter(F.col("is_weekend") == False)
    .orderBy(F.desc("avg_trips_per_day"))
    .limit(10)
    .select(
        "pickup_hour",
        "time_of_day",
        F.format_number("avg_trips_per_day", 0).alias("Avg Trips/Day"),
        F.concat(F.lit("$"), F.format_number("avg_revenue_per_hour", 2)).alias("Revenue/Hour"),
        "demand_rank"
    )
)

# COMMAND ----------

# DBTITLE 1,📊 Hourly Demand Heatmap Data
heatmap_data = (
    hourly_patterns
    .select(
        "pickup_hour",
        F.when(F.col("is_weekend"), "Weekend").otherwise("Weekday").alias("day_type"),
        "avg_trips_per_day"
    )
    .orderBy("day_type", "pickup_hour")
)

display(heatmap_data)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Route Performance Analysis

# COMMAND ----------

# DBTITLE 1,🗺️ Top 20 Most Profitable Routes
route_performance = spark.table(get_table("gold_route_performance"))

display(
    route_performance
    .orderBy(F.desc("revenue_per_minute"))
    .limit(20)
    .select(
        "PULocationID",
        "DOLocationID",
        "route_type",
        F.format_number("trip_count", 0).alias("Total Trips"),
        F.round("avg_distance", 2).alias("Avg Distance (mi)"),
        F.round("avg_duration", 2).alias("Avg Duration (min)"),
        F.concat(F.lit("$"), F.format_number("avg_total_revenue", 2)).alias("Avg Revenue"),
        F.concat(F.lit("$"), F.format_number("revenue_per_mile", 2)).alias("Revenue/Mile"),
        F.concat(F.lit("$"), F.format_number("revenue_per_minute", 2)).alias("Revenue/Min"),
        "profitability_rank"
    )
)

# COMMAND ----------

# DBTITLE 1,🚗 Route Type Distribution
route_type_summary = (
    route_performance
    .groupBy("route_type")
    .agg(
        F.count("*").alias("route_count"),
        F.sum("trip_count").alias("total_trips"),
        F.sum("total_revenue").alias("total_revenue"),
        F.avg("avg_distance").alias("avg_distance"),
        F.avg("revenue_per_mile").alias("avg_revenue_per_mile")
    )
    .orderBy(F.desc("total_revenue"))
)

display(
    route_type_summary
    .select(
        "route_type",
        F.format_number("route_count", 0).alias("# Routes"),
        F.format_number("total_trips", 0).alias("Total Trips"),
        F.concat(F.lit("$"), F.format_number("total_revenue", 2)).alias("Total Revenue"),
        F.round("avg_distance", 2).alias("Avg Distance"),
        F.concat(F.lit("$"), F.format_number("avg_revenue_per_mile", 2)).alias("Avg Revenue/Mile")
    )
)

# COMMAND ----------

# DBTITLE 1,📍 Top Pickup Locations by Volume
top_pickups = (
    route_performance
    .groupBy("PULocationID")
    .agg(
        F.sum("trip_count").alias("total_trips"),
        F.sum("total_revenue").alias("total_revenue"),
        F.avg("avg_distance").alias("avg_distance")
    )
    .orderBy(F.desc("total_trips"))
    .limit(15)
)

display(
    top_pickups
    .select(
        "PULocationID",
        F.format_number("total_trips", 0).alias("Total Trips"),
        F.concat(F.lit("$"), F.format_number("total_revenue", 2)).alias("Total Revenue"),
        F.round("avg_distance", 2).alias("Avg Trip Distance")
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Customer Segmentation Analysis

# COMMAND ----------

# DBTITLE 1,👥 Customer Segments Overview
customer_segments = spark.table(get_table("gold_customer_segments"))

display(
    customer_segments
    .orderBy(F.desc("total_revenue"))
    .select(
        "customer_segment",
        "payment_type_name",
        "time_of_day",
        F.when(F.col("is_weekend"), "Weekend").otherwise("Weekday").alias("day_type"),
        F.format_number("trip_count", 0).alias("Trip Count"),
        F.concat(F.lit("$"), F.format_number("avg_spend", 2)).alias("Avg Spend"),
        F.concat(F.round("avg_tip_pct", 1), F.lit("%")).alias("Avg Tip %"),
        F.concat(F.lit("$"), F.format_number("total_revenue", 2)).alias("Total Revenue"),
        F.concat(F.round("revenue_contribution_pct", 2), F.lit("%")).alias("Revenue %")
    )
)

# COMMAND ----------

# DBTITLE 1,💎 Segment Performance Summary
segment_summary = (
    customer_segments
    .groupBy("customer_segment")
    .agg(
        F.sum("trip_count").alias("total_trips"),
        F.sum("total_revenue").alias("total_revenue"),
        F.avg("avg_spend").alias("avg_spend"),
        F.avg("avg_tip_pct").alias("avg_tip_pct")
    )
    .orderBy(F.desc("total_revenue"))
)

display(
    segment_summary
    .select(
        "customer_segment",
        F.format_number("total_trips", 0).alias("Total Trips"),
        F.concat(F.lit("$"), F.format_number("total_revenue", 2)).alias("Total Revenue"),
        F.concat(F.lit("$"), F.format_number("avg_spend", 2)).alias("Avg Spend/Trip"),
        F.concat(F.round("avg_tip_pct", 1), F.lit("%")).alias("Avg Tip %")
    )
)

# COMMAND ----------

# DBTITLE 1,💳 Payment Type Analysis
payment_analysis = (
    customer_segments
    .groupBy("payment_type_name")
    .agg(
        F.sum("trip_count").alias("total_trips"),
        F.sum("total_revenue").alias("total_revenue"),
        F.avg("avg_spend").alias("avg_spend"),
        F.avg("avg_tip_pct").alias("avg_tip_pct")
    )
    .withColumn(
        "trip_share_pct",
        F.round((F.col("total_trips") / F.sum("total_trips").over(Window.partitionBy())) * 100, 2)
    )
    .orderBy(F.desc("total_trips"))
)

display(
    payment_analysis
    .select(
        "payment_type_name",
        F.format_number("total_trips", 0).alias("Total Trips"),
        F.concat(F.round("trip_share_pct", 1), F.lit("%")).alias("Trip Share %"),
        F.concat(F.lit("$"), F.format_number("total_revenue", 2)).alias("Total Revenue"),
        F.concat(F.lit("$"), F.format_number("avg_spend", 2)).alias("Avg Spend"),
        F.concat(F.round("avg_tip_pct", 1), F.lit("%")).alias("Avg Tip %")
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Weekly Trends Analysis

# COMMAND ----------

# DBTITLE 1,📅 Week-over-Week Trends
weekly_trends = spark.table(get_table("gold_weekly_trends"))

display(
    weekly_trends
    .orderBy(F.desc("week_start"))
    .limit(12)
    .select(
        F.date_format("week_start", "MM/dd/yyyy").alias("Week Starting"),
        F.format_number("total_trips", 0).alias("Total Trips"),
        F.concat(F.round("trips_wow_change_pct", 1), F.lit("%")).alias("WoW Change %"),
        F.format_number("trips_4week_avg", 0).alias("4-Week Avg Trips"),
        F.concat(F.lit("$"), F.format_number("total_revenue", 2)).alias("Total Revenue"),
        F.concat(F.round("revenue_wow_change_pct", 1), F.lit("%")).alias("Revenue WoW %"),
        F.concat(F.lit("$"), F.format_number("revenue_4week_avg", 2)).alias("4-Week Avg Revenue"),
        F.concat(F.lit("$"), F.format_number("avg_revenue_per_trip", 2)).alias("Avg Revenue/Trip"),
        "active_days"
    )
)

# COMMAND ----------

# DBTITLE 1,📊 Weekly Trends Visualization
weekly_viz = (
    weekly_trends
    .orderBy("week_start")
    .select(
        "week_start",
        "total_trips",
        "trips_4week_avg",
        "total_revenue",
        "revenue_4week_avg"
    )
)

display(weekly_viz)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Revenue Breakdown Analysis

# COMMAND ----------

# DBTITLE 1,💵 Revenue Components Breakdown
revenue_breakdown = spark.table(get_table("gold_revenue_breakdown"))

# Get last 7 days
recent_breakdown = (
    revenue_breakdown
    .orderBy(F.desc("pickup_date"))
    .filter(F.col("pickup_date") >= F.date_sub(F.current_date(), 7))
)

display(
    recent_breakdown
    .select(
        F.date_format("pickup_date", "MM/dd/yyyy").alias("Date"),
        "payment_type_name",
        "time_of_day",
        F.format_number("trip_count", 0).alias("Trips"),
        F.concat(F.lit("$"), F.format_number("base_fare_revenue", 2)).alias("Base Fare"),
        F.concat(F.round("base_fare_pct", 1), F.lit("%")).alias("Base %"),
        F.concat(F.lit("$"), F.format_number("tip_revenue", 2)).alias("Tips"),
        F.concat(F.round("tip_pct", 1), F.lit("%")).alias("Tip %"),
        F.concat(F.lit("$"), F.format_number("total_revenue", 2)).alias("Total Revenue"),
        F.concat(F.lit("$"), F.format_number("avg_revenue_per_trip", 2)).alias("Avg/Trip")
    )
    .orderBy(F.desc("Date"), "time_of_day")
)

# COMMAND ----------

# DBTITLE 1,🕐 Revenue by Time of Day
time_of_day_revenue = (
    recent_breakdown
    .groupBy("time_of_day")
    .agg(
        F.sum("trip_count").alias("total_trips"),
        F.sum("total_revenue").alias("total_revenue"),
        F.sum("tip_revenue").alias("total_tips"),
        F.avg("avg_revenue_per_trip").alias("avg_revenue_per_trip")
    )
    .withColumn(
        "revenue_share_pct",
        F.round((F.col("total_revenue") / F.sum("total_revenue").over(Window.partitionBy())) * 100, 2)
    )
    .orderBy(F.desc("total_revenue"))
)

display(
    time_of_day_revenue
    .select(
        "time_of_day",
        F.format_number("total_trips", 0).alias("Total Trips"),
        F.concat(F.lit("$"), F.format_number("total_revenue", 2)).alias("Total Revenue"),
        F.concat(F.round("revenue_share_pct", 1), F.lit("%")).alias("Revenue Share %"),
        F.concat(F.lit("$"), F.format_number("avg_revenue_per_trip", 2)).alias("Avg Revenue/Trip"),
        F.concat(F.lit("$"), F.format_number("total_tips", 2)).alias("Total Tips")
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Operational Insights

# COMMAND ----------

# DBTITLE 1,⚡ Operational Efficiency Metrics
efficiency_metrics = (
    daily_kpis_filtered
    .orderBy(F.desc("pickup_date"))
    .limit(30)
    .select(
        "pickup_date",
        "avg_speed",
        "avg_trip_duration",
        "avg_trip_distance",
        "utilization_rate",
        "revenue_per_mile"
    )
)

display(
    efficiency_metrics
    .select(
        F.date_format("pickup_date", "MM/dd/yyyy").alias("Date"),
        F.round("avg_speed", 2).alias("Avg Speed (mph)"),
        F.round("avg_trip_duration", 2).alias("Avg Duration (min)"),
        F.round("avg_trip_distance", 2).alias("Avg Distance (mi)"),
        F.concat(F.round("utilization_rate", 1), F.lit("%")).alias("Utilization %"),
        F.concat(F.lit("$"), F.format_number("revenue_per_mile", 2)).alias("Revenue/Mile")
    )
)

# COMMAND ----------

# DBTITLE 1,📈 Speed and Distance Correlation
correlation_data = (
    efficiency_metrics
    .select(
        "pickup_date",
        F.round("avg_speed", 2).alias("avg_speed"),
        F.round("avg_trip_distance", 2).alias("avg_distance")
    )
    .orderBy("pickup_date")
)

display(correlation_data)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary Statistics

# COMMAND ----------

# DBTITLE 1,📊 Overall Performance Summary - Last {DAYS_BACK} Days
summary_stats = daily_kpis_filtered.agg(
    F.sum("total_trips").alias("total_trips"),
    F.sum("total_revenue").alias("total_revenue"),
    F.sum("total_miles").alias("total_miles"),
    F.avg("avg_revenue_per_trip").alias("avg_revenue_per_trip"),
    F.avg("avg_tip_percentage").alias("avg_tip_percentage"),
    F.avg("avg_trip_duration").alias("avg_trip_duration"),
    F.avg("utilization_rate").alias("avg_utilization_rate"),
    F.count("*").alias("days_analyzed")
)

display(
    summary_stats
    .select(
        F.lit(DAYS_BACK).alias("Days Analyzed"),
        F.format_number("total_trips", 0).alias("Total Trips"),
        F.concat(F.lit("$"), F.format_number("total_revenue", 2)).alias("Total Revenue"),
        F.format_number("total_miles", 0).alias("Total Miles"),
        F.concat(F.lit("$"), F.format_number("avg_revenue_per_trip", 2)).alias("Avg Revenue/Trip"),
        F.concat(F.round("avg_tip_percentage", 1), F.lit("%")).alias("Avg Tip %"),
        F.round("avg_trip_duration", 2).alias("Avg Duration (min)"),
        F.concat(F.round("avg_utilization_rate", 1), F.lit("%")).alias("Avg Utilization %")
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Data Quality Check

# COMMAND ----------

# DBTITLE 1,✅ Data Quality Status
data_quality = {
    "Table": [],
    "Last Updated": [],
    "Row Count": [],
    "Status": []
}

tables = [
    "gold_daily_kpis",
    "gold_peak_hours_analysis",
    "gold_route_performance",
    "gold_customer_segments",
    "gold_weekly_trends",
    "gold_revenue_breakdown",
    "gold_executive_summary"
]

for table in tables:
    try:
        df = spark.table(get_table(table))
        count = df.count()
        last_updated = df.select(F.max("last_updated")).collect()[0][0]

        data_quality["Table"].append(table)
        data_quality["Last Updated"].append(str(last_updated))
        data_quality["Row Count"].append(count)
        data_quality["Status"].append("✅ OK" if count > 0 else "⚠️ Empty")
    except Exception as e:
        data_quality["Table"].append(table)
        data_quality["Last Updated"].append("N/A")
        data_quality["Row Count"].append(0)
        data_quality["Status"].append(f"❌ Error: {str(e)[:50]}")

quality_df = spark.createDataFrame(
    list(zip(data_quality["Table"], data_quality["Last Updated"], data_quality["Row Count"], data_quality["Status"])),
    ["Table", "Last Updated", "Row Count", "Status"]
)

display(quality_df)

# COMMAND ----------

print("✅ Dashboard completed successfully!")
print(f"📊 Analyzed data for the last {DAYS_BACK} days")
print(f"🕐 Generated at: {datetime.datetime.now()}")