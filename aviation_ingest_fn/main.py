import os
import json
import urllib.parse
import urllib.request
from urllib.error import HTTPError, URLError
from datetime import datetime
from google.cloud import storage

BUCKET_NAME = os.environ.get("BUCKET_NAME", "eu-flights-live")


API_KEYS_RAW = os.environ.get("AVIATIONSTACK_API_KEYS", "")
API_KEYS = [k.strip() for k in API_KEYS_RAW.split(",") if k.strip()]

AIRPORTS = [
    "EDDF","EDDM","EGLL","EGKK","LFPG","LFPO","EHAM","LEMD","LEBL","LIRF"
]

def _mask_key(k: str) -> str:
    if not k:
        return "<empty>"
    if len(k) <= 6:
        return "***"
    return f"{k[:3]}***{k[-3:]}"

def _fetch_flights_for(dep_icao: str, api_key: str, limit: int = 100) -> list[dict]:
    base_url = "http://api.aviationstack.com/v1/flights"
    params = {"access_key": api_key, "dep_icao": dep_icao, "limit": limit}
    url = base_url + "?" + urllib.parse.urlencode(params)

    with urllib.request.urlopen(url, timeout=25) as resp:
        data = json.loads(resp.read().decode("utf-8"))
    return data.get("data", []) or []

def main(request):
    if not API_KEYS:
        return ("Missing AVIATIONSTACK_API_KEYS env var (comma-separated keys)", 500)

    ts = datetime.utcnow().strftime("%Y%m%dT%H%M%S")
    date_str = datetime.utcnow().strftime("%Y-%m-%d")

    all_flights = []
    keys_tried = []
    key_used = None

    
    for key in API_KEYS:
        keys_tried.append(_mask_key(key))
        try:
            tmp = []
            for dep_icao in AIRPORTS:
                flights = _fetch_flights_for(dep_icao, api_key=key, limit=100)
                now_iso = datetime.utcnow().isoformat()
                for f in flights:
                    f["_source_dep_icao"] = dep_icao
                    f["_ingestion_time_utc"] = now_iso
                tmp.extend(flights)

            
            all_flights = tmp
            key_used = key
            break

        except HTTPError as e:
            
            if e.code in (401, 403, 429):
                print(f"[WARN] Key {_mask_key(key)} failed with HTTP {e.code}. Trying next key...")
                continue
            
            raise

        except URLError as e:
            print(f"[WARN] Network error with key {_mask_key(key)}: {e}. Trying next key...")
            continue

    if not key_used:
        return (f"All API keys failed. Tried: {keys_tried}", 429)

    if not all_flights:
        return (f"Key {_mask_key(key_used)} worked but returned 0 flights", 200)

    # Write JSONL to GCS
    client = storage.Client()
    bucket = client.bucket(BUCKET_NAME)

    blob_path = f"live_flights_raw/date={date_str}/aviationstack_{ts}.jsonl"
    blob = bucket.blob(blob_path)

    lines = [json.dumps(rec) for rec in all_flights]
    blob.upload_from_string("\n".join(lines), content_type="application/json")

    msg = f"Wrote {len(all_flights)} flights to gs://{BUCKET_NAME}/{blob_path} using key {_mask_key(key_used)}"
    print(msg)
    return (msg, 200)
