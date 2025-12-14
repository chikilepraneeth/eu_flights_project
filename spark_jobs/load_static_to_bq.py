import argparse
from pyspark.sql import SparkSession


def main(gcs_path: str, bq_table: str):
    # Create Spark session
    spark = (
        SparkSession.builder
        .appName("LoadStaticFlightsCSVToBigQuery")
        .getOrCreate()
    )

    # Read CSV from GCS
    df = (
        spark.read
        .option("header", "true")        # first row has column names
        .option("inferSchema", "true")   # let Spark detect data types
        .csv(gcs_path)
    )

    # Print schema to Dataproc logs (optional)
    df.printSchema()

    # Write to BigQuery
    # bq_table must be in form: project_id.dataset.table_name
    (
        df.write
        .format("bigquery")
        .option("table", bq_table)
        .mode("overwrite")  # use "append" if you don't want to replace
        .save()
    )

    spark.stop()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--gcs_path",
        required=True,
        help="GCS path to the CSV file, e.g. gs://eu-flights-static/flights.csv",
    )
    parser.add_argument(
        "--bq_table",
        required=True,
        help="BigQuery table in form project_id.dataset.table_name",
    )
    args = parser.parse_args()

    main(args.gcs_path, args.bq_table)
