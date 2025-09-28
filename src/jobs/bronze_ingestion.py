"""
NYC Taxi Data Bronze Ingestion
Business-agnostic template for any data ingestion use case

This job demonstrates:
- Public data ingestion (no API keys needed)
- CSV to Delta Lake conversion
- Data quality validation
- Error handling and logging
- CI/CD friendly patterns
"""

import logging
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
)
from pyspark.sql.types import StructType, StructField, StringType, DoubleType

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class NYCTaxiBronzeIngestion:
    """
    NYC Taxi data ingestion to Bronze layer

    Template pattern that can be adapted for any business use case:
    - Replace NYC taxi with your data source
    - Modify schema for your data structure
    - Keep the CI/CD patterns intact
    """

    def __init__(self):
        self.spark = self._create_spark_session()

    def _create_spark_session(self):
        """Create Spark session optimized for Delta Lake"""
        return (
            SparkSession.builder.appName("NYC-Taxi-Bronze-Ingestion")
            .config("spark.sql.adaptive.enabled", "true")
            .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
            .getOrCreate()
        )

    def _get_nyc_taxi_schema(self):
        """
        Define schema for NYC taxi data

        Template pattern: Replace with your business data schema
        """
        return StructType(
            [
                StructField("VendorID", StringType(), True),
                StructField("tpep_pickup_datetime", StringType(), True),
                StructField("tpep_dropoff_datetime", StringType(), True),
                StructField("passenger_count", DoubleType(), True),
                StructField("trip_distance", DoubleType(), True),
                StructField("pickup_longitude", DoubleType(), True),
                StructField("pickup_latitude", DoubleType(), True),
                StructField("RatecodeID", StringType(), True),
                StructField("store_and_fwd_flag", StringType(), True),
                StructField("dropoff_longitude", DoubleType(), True),
                StructField("dropoff_latitude", DoubleType(), True),
                StructField("payment_type", StringType(), True),
                StructField("fare_amount", DoubleType(), True),
                StructField("extra", DoubleType(), True),
                StructField("mta_tax", DoubleType(), True),
                StructField("tip_amount", DoubleType(), True),
                StructField("tolls_amount", DoubleType(), True),
                StructField("total_amount", DoubleType(), True),
            ]
        )

    def _validate_data_quality(self, df):
        """
        Perform data quality validation

        Template pattern: Add your business-specific validations
        """
        logger.info("Performing data quality validation...")

        total_records = df.count()

        # Check for null values in critical columns
        critical_columns = ["tpep_pickup_datetime", "total_amount"]
        quality_issues = {}

        for col_name in critical_columns:
            null_count = df.filter(col(col_name).isNull()).count()
            null_percentage = (
                (null_count / total_records) * 100 if total_records > 0 else 0
            )
            quality_issues[f"{col_name}_null_percentage"] = null_percentage

        # Check for negative amounts (business logic validation)
        negative_amounts = df.filter(col("total_amount") < 0).count()
        quality_issues["negative_amounts_count"] = negative_amounts

        # Calculate overall quality score
        quality_score = 100.0
        for key, value in quality_issues.items():
            if "percentage" in key:
                quality_score -= value * 2  # 2% penalty per 1% null values

        logger.info(f"Data quality metrics: {quality_issues}")
        logger.info(f"Quality score: {quality_score:.2f}%")

        return quality_score, quality_issues

    def _add_metadata_columns(self, df):
        """
        Add metadata columns for lineage and processing tracking

        Template pattern: Standard metadata for any data pipeline
        """
        return (
            df.withColumn("ingestion_timestamp", current_timestamp())
            .withColumn("data_source", lit("nyc_taxi_public"))
            .withColumn("pipeline_stage", lit("bronze"))
            .withColumn("data_classification", lit("public"))
            .withColumn("business_domain", lit("transportation"))
            .withColumn("ingestion_job", lit("bronze_taxi_ingestion"))
            .withColumn("processing_date", col("ingestion_timestamp").cast("date"))
        )

    def _clean_basic_issues(self, df):
        """
        Basic data cleaning - handle common data issues

        Template pattern: Common cleaning steps for any dataset
        """
        logger.info("Performing basic data cleaning...")

        # Convert string timestamps to proper timestamp type
        df = df.withColumn(
            "pickup_datetime",
            to_timestamp(col("tpep_pickup_datetime"), "yyyy-MM-dd HH:mm:ss"),
        ).withColumn(
            "dropoff_datetime",
            to_timestamp(col("tpep_dropoff_datetime"), "yyyy-MM-dd HH:mm:ss"),
        )

        # Add date partitioning columns (useful for performance)
        df = (
            df.withColumn("pickup_year", year(col("pickup_datetime")))
            .withColumn("pickup_month", month(col("pickup_datetime")))
            .withColumn("pickup_day", dayofmonth(col("pickup_datetime")))
        )

        # Handle invalid passenger counts
        df = df.withColumn(
            "passenger_count_clean",
            when(
                col("passenger_count").isNull() | (col("passenger_count") <= 0), 1
            ).otherwise(col("passenger_count")),
        )

        # Flag suspicious records for downstream investigation
        df = df.withColumn(
            "data_quality_flag",
            when(
                (col("trip_distance") <= 0)
                | (col("total_amount") <= 0)
                | (col("pickup_datetime").isNull()),
                "SUSPICIOUS",
            ).otherwise("CLEAN"),
        )

        return df

    def ingest_nyc_taxi_data(self, source_path, target_path):
        logger.info(f"Starting NYC taxi data ingestion from {source_path}")

        try:
            # Read source data
            logger.info("Reading source data...")
            df = self.spark.read \
                .option("header", "true") \
                .option("inferSchema", "true") \
                .csv(source_path)

            initial_count = df.count()
            logger.info(f"Read {initial_count} records from source")

            # Basic data cleaning (add this back)
            df_cleaned = self._clean_basic_issues(df)

            # Data quality validation (add this back)
            quality_score, quality_issues = self._validate_data_quality(df_cleaned)

            # Add metadata columns
            df_final = self._add_metadata_columns(df_cleaned)

            # Write to table (simplified)
            logger.info(f"Writing to table: {target_path}")

            if target_path.count('.') == 2:
                # Three-level namespace (works for both hive_metastore and Unity Catalog)
                df_final.write \
                    .format("delta") \
                    .mode("overwrite") \
                    .option("overwriteSchema", "true") \
                    .saveAsTable(target_path)
            else:
                # DBFS path
                df_final.write \
                    .format("delta") \
                    .mode("overwrite") \
                    .option("overwriteSchema", "true") \
                    .save(target_path)

            final_count = df_final.count()

            logger.info("=== Bronze Ingestion Completed Successfully ===")
            logger.info(f"Records processed: {initial_count}")
            logger.info(f"Records written: {final_count}")
            logger.info(f"Quality score: {quality_score:.2f}%")
            logger.info(f"Data location: {target_path}")

            return {
                "status": "success",
                "records_processed": initial_count,
                "records_written": final_count,
                "quality_score": quality_score,
                "target_path": target_path
            }

        except Exception as e:
            logger.error(f"Bronze ingestion failed: {str(e)}")
            raise


def main():
    """Main execution function for NYC Taxi Bronze ingestion"""
    logger.info("=== NYC Taxi Bronze Ingestion Job Started ===")

    # Use Databricks sample data
    source_path = "/databricks-datasets/nyctaxi/tripdata/yellow/yellow_tripdata_2019-01.csv.gz"

    # Write to Unity Catalog instead of DBFS mount
    target_path = "hive_metastore.default.nyc_taxi_bronze"  # catalog.schema.table format

    # Initialize and run ingestion
    ingestion = NYCTaxiBronzeIngestion()

    try:
        result = ingestion.ingest_nyc_taxi_data(source_path, target_path)

        logger.info("=== Job Summary ===")
        logger.info(f"Status: {result['status']}")
        logger.info(f"Quality Score: {result['quality_score']:.2f}%")
        logger.info("Bronze layer ready for downstream processing!")

    except Exception as e:
        logger.error(f"Job failed: {str(e)}")
        raise

if __name__ == "__main__":
    main()
