"""
Minimal Databricks job for testing deployment
"""


def main():
    """Simple test function"""
    print("🎉 Hello from Databricks!")
    print("✅ Deployment successful!")
    print("🚀 Ready for databricks project!")

    # Test basic Spark functionality
    try:
        from pyspark.sql import SparkSession

        spark = SparkSession.builder.appName("HelloTest").getOrCreate()

        # Create simple DataFrame
        data = [("Deployment", "Success"), ("Pipeline", "Working")]
        df = spark.createDataFrame(data, ["Status", "Result"])

        print("📊 Test DataFrame:")
        df.show()

        print(f"✅ Spark context working: {spark.sparkContext.applicationId}")
        return True

    except Exception as e:
        print(f"❌ Error: {e}")
        return False


if __name__ == "__main__":
    success = main()
    if success:
        print("🎯 All tests passed - ready for engine project!")
    else:
        print("⚠️ Tests failed - check configuration")
