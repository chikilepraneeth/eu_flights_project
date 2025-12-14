import os
from datetime import datetime, timezone

from google.cloud import dataproc_v1


PROJECT_ID = os.environ.get("PROJECT_ID", "aviationstack-480614")
REGION = os.environ.get("REGION", "us-central1")

# Your ETL script in GCS
PYSPARK_URI = os.environ.get(
    "PYSPARK_URI",
    "gs://eu-flights-live/code/process_live_to_bq.py"
)

# Optional: keep using your deps bucket (same as you already use)
DEPS_BUCKET = os.environ.get("DEPS_BUCKET", "eu-flights-live")


def trigger_dataproc_etl(request):
    # Unique batch id per run
    batch_id = f"live-etl-{datetime.now(timezone.utc).strftime('%Y%m%d-%H%M%S')}"

    client = dataproc_v1.BatchControllerClient(
        client_options={"api_endpoint": f"{REGION}-dataproc.googleapis.com:443"}
    )

    parent = f"projects/{PROJECT_ID}/locations/{REGION}"

    batch = dataproc_v1.Batch(
        pyspark_batch=dataproc_v1.PySparkBatch(main_python_file_uri=PYSPARK_URI),
        environment_config=dataproc_v1.EnvironmentConfig(
            execution_config=dataproc_v1.ExecutionConfig(
                service_account=os.environ.get("DATAPROC_SA_EMAIL")  # optional
            )
        ),
    )

    op = client.create_batch(parent=parent, batch=batch, batch_id=batch_id)

    msg = f"Submitted Dataproc batch: {batch_id} (operation={op.operation.name})"
    print(msg)
    return (msg, 200)
