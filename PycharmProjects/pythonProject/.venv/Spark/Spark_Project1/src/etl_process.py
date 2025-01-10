from pyspark.sql.functions import sum
from config.spark_config import get_spark_session
import json

def load_credentials():
    with open("data/db_credentials.json", "r") as file:
        return json.load(file)

def perform_etl():
    credentials = load_credentials()
    jdbc_url = f"jdbc:oracle:thin:@{credentials['ORACLE_HOST']}:{credentials['ORACLE_PORT']}/{credentials['ORACLE_SERVICE_NAME']}"

    connection_properties = {
        "user": credentials["ORACLE_USER"],
        "password": credentials["ORACLE_PASSWORD"],
        "driver": "oracle.jdbc.driver.OracleDriver"
    }

    spark = get_spark_session()

    # Extract
    sales_df = spark.read.jdbc(url=jdbc_url, table="sales_data", properties=connection_properties)
    print("Extracted Data:")
    sales_df.show()

    # Transform
    transformed_df = sales_df.groupBy("region").agg(sum("sales_amount").alias("total_sales"))
    print("Transformed Data:")
    transformed_df.show()

    # Load
    transformed_df.write.jdbc(
        url=jdbc_url,
        table="transformed_sales_data",
        mode="overwrite",
        properties=connection_properties
    )
    print("Transformed data loaded.")
