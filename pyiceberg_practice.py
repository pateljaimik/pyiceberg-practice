from aws_secret_manager import get_secret
from pyiceberg.catalog import load_catalog
from dotenv import load_dotenv
from ast import literal_eval

from pyiceberg.schema import Schema, NestedField
from pyiceberg.partitioning import PartitionSpec, IdentityTransform, PartitionField
from pyiceberg.types import StringType, DoubleType, LongType, TimestampType, DateType

from pyspark.sql import SparkSession
from pyspark.sql.functions import lit
from datetime import datetime
import requests
import os

load_dotenv()

def create_iceberg_table():

    catalog_name = get_secret('CATALOG_NAME')
    credential = get_secret('TABULAR_CREDENTIAL')

    spark = (
        SparkSession.builder
        .config("spark.sql.defaultCatalog", f"{catalog_name}")
        .config(f"spark.sql.catalog.{catalog_name}", "org.apache.iceberg.spark.SparkCatalog")
        .config(f"spark.sql.catalog.{catalog_name}.credential", credential)
        .config("spark.sql.defaultCatalog", catalog_name)
        .config(f"spark.sql.catalog.{catalog_name}.catalog-impl", "org.apache.iceberg.rest.RESTCatalog")
        .config(f"spark.sql.catalog.{catalog_name}.warehouse", f"{catalog_name}")
        .config(f"spark.sql.catalog.{catalog_name}.uri", "https://api.tabular.io/ws/")
        .config("spark.sql.shuffle.partitions", "50")
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
        .config("spark.jars.packages", "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.5.2,org.apache.iceberg:iceberg-aws-bundle:1.5.2,org.apache.hadoop:hadoop-aws:3.3.6")
        .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.AnonymousAWSCredentialsProvider")
        .getOrCreate()
    )

    catalog = load_catalog(
        'academy',
        type="rest",  # Explicitly define the catalog type as REST
        uri="https://api.tabular.io/ws",  # Your REST catalog endpoint
        warehouse=catalog_name,
        credential=credential,
    )

    # my table with username schema
    table_identifier = "jaimikdpatel.maang_stock_prices"

    # Define Iceberg schema
    iceberg_schema = Schema(
        fields=[
            NestedField(1, "ticker", StringType(), required=True),
            NestedField(2, "open", DoubleType(), required=True),
            NestedField(3, "close", DoubleType(), required=True),
            NestedField(4, "high", DoubleType(), required=True),
            NestedField(5, "low", DoubleType(), required=True),
            NestedField(6, "volume", LongType(), required=True),
            NestedField(7, "timestamp", TimestampType(), required=True),
            NestedField(8, "date", DateType(), required=True),
        ]
    )

    partition_spec = PartitionSpec(
        fields=[
            PartitionField(source_id=8, field_id=1000, name="date", transform=IdentityTransform())
        ]
    )

    ## Todo create your partitioned Iceberg table
    ## catalog.create_table()

    # Create table if it doesn't exist
    if not catalog.table_exists(table_identifier):
        catalog.create_table(
            identifier=table_identifier,
            schema=iceberg_schema,
            partition_spec=partition_spec,
            properties={
                "format-version": "2",
                "write.wap.enabled": "true"
            }
        )
        print(f"Created table: {table_identifier}")
    else:
        print(f"Table already exists: {table_identifier}")
        my_table = catalog.load_table(table_identifier)
        # print(my_table)
    
    ## TODO create a branch table.create_branch()
    # Check if the branch exists, otherwise create it
    
    spark.sql(f"""
    ALTER TABLE jaimikdpatel.maang_stock_prices CREATE BRANCH IF NOT EXISTS audit_branch
    """)
    print("Audit branch created successfully.")

    existing_branches = my_table.refs()
    print("Existing branches: ", existing_branches)


    ## TODO read data from the Polygon API and load it into the Iceberg table branch

    start_date = "2025-05-05"
    end_date = "2025-05-06"
    maang_stocks = ['AAPL', 'AMZN', 'NFLX', 'GOOGL', 'META']
    polygon_api_key = literal_eval(get_secret("POLYGON_CREDENTIALS"))['AWS_SECRET_ACCESS_KEY']
    parsed = []
    for stock in maang_stocks:
        
        polygon_url = f"https://api.polygon.io/v2/aggs/ticker/{stock}/range/1/day/{start_date}/{end_date}?adjusted=true&sort=asc&apiKey="
        response = requests.get(polygon_url+polygon_api_key)
    
        if response.status_code == 200:
            data = response.json()
            results = data.get("results", [])

            for row in results:
                parsed.append({
                    "ticker": str(data.get("ticker")),
                    "open": float(row["o"]),
                    "close": float(row["c"]),
                    "high": float(row["h"]),
                    "low": float(row["l"]),
                    "volume": int(row["v"]),
                    "timestamp": datetime.fromtimestamp(row["t"] / 1000),
                    "date": datetime.fromtimestamp(row["t"] / 1000).date()
                })
        else:
            print("Response Error: ", response.status_code)
    
    
    df = spark.createDataFrame(parsed)
    df.show()

    df.write \
        .format("iceberg") \
        .option("branch", "audit_branch") \
        .partitionBy("date") \
        .mode("append") \
        .save(table_identifier)



if __name__ == "__main__":
    create_iceberg_table()
