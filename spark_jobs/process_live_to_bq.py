from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_timestamp, unix_timestamp, when, to_date, lit

PROJECT_ID = "aviationstack-480614"

GCS_LIVE_PATH = "gs://eu-flights-live/live_flights_raw/*/*"

BQ_CURATED_TABLE = f"{PROJECT_ID}.flights_us.flights_live_curated"

TEMP_GCS_BUCKET = "eu-flights-live"


def main():
    spark = SparkSession.builder.appName("ProcessLiveFlightsToBigQuery").getOrCreate()
    spark.conf.set("temporaryGcsBucket", TEMP_GCS_BUCKET)

    print(f"[INFO] Reading raw live flights from {GCS_LIVE_PATH}")

    
    df = spark.read.json(GCS_LIVE_PATH)
    print(f"[INFO] Loaded {df.count()} raw rows")

    flat = df.select(
        col("flight_date").cast("string").alias("flight_date"),
        col("flight_status").cast("string").alias("status"),

        col("departure.airport").cast("string").alias("dep_airport"),
        col("departure.icao").cast("string").alias("dep_icao"),
        col("departure.iata").cast("string").alias("dep_iata"),
        col("departure.scheduled").cast("string").alias("dep_scheduled"),
        col("departure.actual").cast("string").alias("dep_actual"),

        col("arrival.airport").cast("string").alias("arr_airport"),
        col("arrival.icao").cast("string").alias("arr_icao"),
        col("arrival.iata").cast("string").alias("arr_iata"),
        col("arrival.scheduled").cast("string").alias("arr_scheduled"),
        col("arrival.actual").cast("string").alias("arr_actual"),

        col("airline.name").cast("string").alias("airline_name"),
        col("airline.iata").cast("string").alias("airline_iata"),
        col("flight.number").cast("string").alias("flight_number"),

        col("_source_dep_icao").cast("string").alias("source_dep_icao"),
        col("_ingestion_time_utc").cast("string").alias("ingestion_time_utc"),
    )

    flat = flat.where(col("dep_icao").isNotNull() & col("arr_icao").isNotNull())

    flat = (
        flat.withColumn("dep_sched_ts", to_timestamp("dep_scheduled"))
            .withColumn("dep_actual_ts", to_timestamp("dep_actual"))
    )

    flat = flat.withColumn(
        "dep_delay_min",
        when(
            col("dep_sched_ts").isNotNull() & col("dep_actual_ts").isNotNull(),
            (unix_timestamp("dep_actual_ts") - unix_timestamp("dep_sched_ts")) / 60.0,
        ).otherwise(None),
    )

    flat = flat.withColumn("ingestion_date", to_date(col("ingestion_time_utc")))

    curated = flat.select(
        "flight_date",
        "status",
        "dep_airport",
        "dep_icao",
        "dep_iata",
        "dep_scheduled",
        "dep_actual",
        "dep_delay_min",
        "arr_airport",
        "arr_icao",
        "arr_iata",
        "arr_scheduled",
        "arr_actual",
        "airline_name",
        "airline_iata",
        "flight_number",
        "source_dep_icao",
        "ingestion_time_utc",
        "ingestion_date",
    )

    print("[INFO] Curated schema:")
    curated.printSchema()

    (curated.write
        .format("bigquery")
        .option("table", BQ_CURATED_TABLE)
        .option("temporaryGcsBucket", TEMP_GCS_BUCKET)
        .mode("append")
        .save()
    )

    print(f"[INFO] Written curated live flights to BigQuery table {BQ_CURATED_TABLE}")
    spark.stop()
    print("[INFO] Spark job completed.")


if __name__ == "__main__":
    main()
