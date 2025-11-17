import os
import pandas as pd
from pyspark.sql import SparkSession


def get_spark_session():
    print("Initializing Databricks Spark session via Databricks Connect v15...")

    spark = (
        SparkSession.builder
        .appName("DataCleaningJob")
        .config("spark.databricks.service.server.enabled", "true")
        .getOrCreate()
    )

    print("Spark session created!")
    return spark


def read_table(spark, table):
    catalog = os.getenv("DATABRICKS_CATALOG")
    schema = os.getenv("DATABRICKS_SCHEMA")

    full_name = f"{catalog}.{schema}.{table}"
    print(f"Reading table: {full_name}")

    return spark.read.table(full_name)


def clean_dataframe(df):
    print("Performing cleaning operations...")

    # Drop duplicates
    df = df.dropDuplicates()

    # Fill nulls
    df = df.fillna("UNKNOWN")

    return df


def write_output(df):
    print("Converting to Pandas and saving CSV...")

    pdf = df.toPandas()
    output_path = "cleaned_output.csv"
    pdf.to_csv(output_path, index=False)

    print(f"Saved cleaned output → {output_path}")


def main():
    spark = get_spark_session()

    # Read fct_claim
    df = read_table(spark, "fct_claim")

    # Clean
    cleaned_df = clean_dataframe(df)

    # Save
    write_output(cleaned_df)

    print("Data cleaning completed successfully.")


if __name__ == "__main__":
    main()
