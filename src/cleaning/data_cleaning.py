import os
from databricks.connect import DatabricksSession
from databricks.sdk.core import Config
from pyspark.sql import functions as F

# -------------------------------------------
#  Create Databricks Spark Session (v15)
# -------------------------------------------
def get_spark_session():
    print("Initializing Databricks Spark session via Databricks Connect v15...")

    cfg = Config(
        host=os.getenv("DATABRICKS_HOST"),
        token=os.getenv("DATABRICKS_TOKEN")
    )

    return DatabricksSession.builder.config(cfg).getOrCreate()


# -------------------------------------------
#  Cleaning Logic
# -------------------------------------------
def clean_dataframe(df):
    print("Starting cleaning steps...")

    # Example cleaning: Drop duplicates
    df = df.dropDuplicates()

    # Example: Remove null rows
    df = df.dropna()

    # Example: Trim string columns
    for col, dtype in df.dtypes:
        if dtype == "string":
            df = df.withColumn(col, F.trim(F.col(col)))

    return df


# -------------------------------------------
#  Main Job
# -------------------------------------------
def main():
    print("Starting data cleaning job...")

    spark = get_spark_session()

    catalog = os.getenv("DATABRICKS_CATALOG", "workspace")
    schema = os.getenv("DATABRICKS_SCHEMA", "feature_store_project")

    source_table = f"{catalog}.{schema}.raw_claims"
    cleaned_table = f"{catalog}.{schema}.cleaned_claims"

    print(f"Reading input table: {source_table}")
    df = spark.table(source_table)

    cleaned_df = clean_dataframe(df)

    print(f"Writing cleaned output table: {cleaned_table}")
    cleaned_df.write.mode("overwrite").saveAsTable(cleaned_table)

    print("Cleaning job completed successfully!")


# -------------------------------------------
#  Entry Point
# -------------------------------------------
if __name__ == "__main__":
    main()
#attempt 5