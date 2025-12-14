"""
Ingest live flight data from Aviationstack into GCS,
using ONLY airports that exist in the static dataset.

- Reads distinct airports from a CSV in GCS:
    gs://eu-flights-live/reference/airports.csv
- Calls Aviationstack API with dep_icao for each airport
- Writes raw live data as Parquet to GCS:
    gs://eu-flights-live/live_flights/ingestion_date=YYYY-MM-DD/...
"""

import json
import urllib.parse
import urllib.request
from datetime import datetime

from pyspark.sql import SparkSession


GCS_BUCKET = "eu-flights-live"
AIRPORTS_CSV_PATH = f"gs://{GCS_BUCKET}/reference/airports.csv"


API_KEY = "6bf720b2bcfcfc7544f8373fc5b0338d"  


def fetch_flights_for_dep_icao(dep_icao: str, limit: int = 100):

    base_url = "http://api.aviationstack.com/v1/flights"
    params = {
        "access_key": API_KEY,
        "dep_icao": dep_icao,
        "limit": limit,
    }
    url = base_url + "?" + urllib.parse.urlencode(params)

    try:
        with urllib.request.urlopen(url, timeout=20) as resp:
            data = json.loads(resp.read().decode("utf-8"))
        flights = data.get("data", []) or []
        return flights
    except Exception as e:
        print(f"[WARN] Failed to fetch flights for {dep_icao}: {e}")
        return []


def main():
    
    spark = (
        SparkSession.builder
        .appName("IngestAviationstackLiveFromAirportsCSV")
        .getOrCreate()
    )

    print(f"[INFO] Reading airports from {AIRPORTS_CSV_PATH}")

    airports_df = (
        spark.read
        .option("header", "true")
        .csv(AIRPORTS_CSV_PATH)
        .select("APT_ICAO")
        .where("APT_ICAO IS NOT NULL")
        .distinct()
    )

    airport_codes = [row.APT_ICAO for row in airports_df.collect()]
    print(f"[INFO] Found {len(airport_codes)} distinct airports in CSV")

    all_rows = []
    ingestion_time = datetime.utcnow().isoformat()

    for dep_icao in airport_codes:
        flights = fetch_flights_for_dep_icao(dep_icao)
        print(f"[INFO] {dep_icao}: fetched {len(flights)} flights")

        for f in flights:
            dep = f.get("departure") or {}
            arr = f.get("arrival") or {}
            airline = f.get("airline") or {}
            flt = f.get("flight") or {}

            row = {
                "flight_date": f.get("flight_date"),
                "status": f.get("flight_status"),

                "dep_airport": dep.get("airport"),
                "dep_icao": dep.get("icao"),
                "dep_iata": dep.get("iata"),
                "dep_scheduled": dep.get("scheduled"),
                "dep_actual": dep.get("actual"),
                "dep_terminal": dep.get("terminal"),
                "dep_gate": dep.get("gate"),

                "arr_airport": arr.get("airport"),
                "arr_icao": arr.get("icao"),
                "arr_iata": arr.get("iata"),
                "arr_scheduled": arr.get("scheduled"),
                "arr_actual": arr.get("actual"),
                "arr_terminal": arr.get("terminal"),
                "arr_gate": arr.get("gate"),

                "airline_name": airline.get("name"),
                "airline_iata": airline.get("iata"),
                "flight_number": flt.get("number"),
                "codeshared": flt.get("codeshared"),

                "ingestion_time_utc": ingestion_time,
                "source_dep_icao_static": dep_icao,
            }
            all_rows.append(row)

    if not all_rows:
        print("[WARN] No flights fetched from Aviationstack. Exiting.")
        spark.stop()
        return

    df = spark.createDataFrame(all_rows)

    date_str = datetime.utcnow().strftime("%Y-%m-%d")
    gcs_path = f"gs://{GCS_BUCKET}/live_flights/ingestion_date={date_str}"

    (
        df.write
        .mode("append")
        .parquet(gcs_path)
    )

    print(f"[INFO] Written Parquet to {gcs_path}")

    spark.stop()


if __name__ == "__main__":
    main()
