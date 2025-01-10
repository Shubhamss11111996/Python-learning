from pyspark.sql import SparkSession

def get_spark_session(app_name="Spark_ETL_Project"):
    return SparkSession.builder \
        .appName(app_name) \
        .config("spark.jars", "file:///C:/Users/Lenovo/Downloads/ojdbc8-12.2.0.1.jar") \
        .config("spark.driver.extraClassPath", "file:///C:/Users/Lenovo/Downloads/ojdbc8-12.2.0.1.jar") \
        .getOrCreate()

