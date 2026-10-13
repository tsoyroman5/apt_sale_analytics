"""
Ingestion script: pulls Korean apartment sale transaction data from
data.go.kr (RTMSDataSvcAptTradeDev — the "detailed" endpoint) and loads
it into BigQuery as a raw landing table.

This is the EL step. dbt picks up from the raw table for the T (transform)
layer — staging -> intermediate -> mart. Keep this script separate from
your dbt project folder; that separation is worth calling out in your
README as a deliberate EL/T split, not an accident.

Setup before running:
1. pip install requests pandas google-cloud-bigquery db-dtypes
2. Auth to GCP locally: `gcloud auth application-default login`
   (simplest for a solo project; a service account key is the
   alternative if you end up running this somewhere other than your
   own machine)
3. Create the target BigQuery dataset first (e.g. `raw`) via console
   or `bq mk raw`
"""

import time
import xml.etree.ElementTree as ET
import os

import pandas as pd
import requests
from google.cloud import bigquery
from dotenv import load_dotenv

load_dotenv()

API_KEY = os.getenv("API_KEY")
GCP_PROJECT_ID = os.getenv("GCP_PROJECT_ID")
BQ_DATASET = os.getenv("BQ_DATASET")
BQ_TABLE = os.getenv("BQ_TABLE")

BASE_URL = "https://apis.data.go.kr/1613000/RTMSDataSvcAptTradeDev/getRTMSDataSvcAptTradeDev"

# Pick your scope deliberately — don't pull all of Korea's full history.
# A handful of districts over 24-36 months is plenty for a portfolio project.
# Example: Jongno-gu, Jung-gu, Yongsan-gu (Seoul) — adjust to whatever
# area makes a better story for you.
DISTRICTS = ["11710", "11680", "11500", "11740", "11620"]
MONTHS = [f"{year}{month:02d}" for year in [2023, 2024, 2025, 2026] for month in range(1, 13)]

CHECKPOINT_FILE = "raw_apt_transactions.csv"
 
MAX_RETRIES = 4
RETRY_BACKOFF_BASE = 2  # seconds; doubles each attempt (2, 4, 8, 16)

class ApiError(Exception):
    """Raised when the API's own resultCode signals a failure (auth,
    rate limit, bad params, etc.) rather than an HTTP-level error."""
 
 
def fetch_page(lawd_cd: str, deal_ymd: str, page_no: int = 1, num_rows: int = 1000) -> str:
    """Retries on transient network failures (timeouts, connection drops,
    SSL handshake issues — the apis.data.go.kr server does this under
    load) with exponential backoff. Re-raises after MAX_RETRIES so a
    genuinely dead connection still surfaces instead of looping forever."""
    params = {
        "serviceKey": API_KEY,
        "LAWD_CD": lawd_cd,
        "DEAL_YMD": deal_ymd,
        "pageNo": page_no,
        "numOfRows": num_rows,
    }
    last_error: Exception | None = None
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            resp = requests.get(BASE_URL, params=params, timeout=30)
            resp.raise_for_status()
            return resp.text
        except (requests.exceptions.Timeout, requests.exceptions.ConnectionError) as e:
            last_error = e
            wait = RETRY_BACKOFF_BASE**attempt
            print(f"    network error ({type(e).__name__}) — retry {attempt}/{MAX_RETRIES} in {wait}s")
            time.sleep(wait)
    raise last_error
 
 
def parse_response(xml_text: str) -> tuple[list[dict], int]:
    """Returns (records, total_count). Raises ApiError if resultCode != 000."""
    root = ET.fromstring(xml_text)
 
    result_code = root.findtext(".//resultCode")
    result_msg = root.findtext(".//resultMsg")
    if result_code is not None and result_code != "000":
        raise ApiError(f"API returned {result_code}: {result_msg}")
 
    items = root.findall(".//item")
    records = [{child.tag: child.text for child in item} for item in items]
    total_count = int(root.findtext(".//totalCount") or 0)
    return records, total_count
 
 
def fetch_district_month(lawd_cd: str, deal_ymd: str) -> list[dict]:
    """Fetches ALL pages for one district/month, not just the first.
    numOfRows=1000 covers most district-months in one call, but this
    loops on totalCount instead of assuming that — a quiet district
    doesn't guarantee every district stays under 1000 transactions."""
    num_rows = 1000
    page_no = 1
    collected: list[dict] = []
 
    while True:
        xml_text = fetch_page(lawd_cd, deal_ymd, page_no=page_no, num_rows=num_rows)
        records, total_count = parse_response(xml_text)
        collected.extend(records)
        if len(collected) >= total_count or not records:
            break
        page_no += 1
        time.sleep(0.2)
 
    return collected
 
 
def load_completed_keys(checkpoint_file: str) -> set[tuple[str, str]]:
    """What's already safely on disk from a previous (possibly crashed) run."""
    if not os.path.exists(checkpoint_file):
        return set()
    done = pd.read_csv(checkpoint_file, usecols=["lawd_cd", "deal_ymd"], dtype=str)
    return set(zip(done["lawd_cd"], done["deal_ymd"]))
 
 
def append_checkpoint(records: list[dict], checkpoint_file: str) -> None:
    """Writes one district/month's records to disk immediately — don't
    wait until the whole run finishes to persist anything."""
    if not records:
        return
    df = pd.DataFrame(records)
    write_header = not os.path.exists(checkpoint_file)
    df.to_csv(checkpoint_file, mode="a", header=write_header, index=False)
 
 
def fetch_all(checkpoint_file: str = CHECKPOINT_FILE) -> pd.DataFrame:
    completed = load_completed_keys(checkpoint_file)
    if completed:
        print(f"Resuming from {checkpoint_file} — {len(completed)} combos already done, skipping those.")
 
    for lawd_cd in DISTRICTS:
        for deal_ymd in MONTHS:
            if (lawd_cd, deal_ymd) in completed:
                continue
 
            try:
                records = fetch_district_month(lawd_cd, deal_ymd)
            except ApiError as e:
                # Don't let one bad district/month silently zero out —
                # surface it and keep going so a key/approval problem
                # shows up immediately instead of looking like "no data".
                print(f"{lawd_cd} / {deal_ymd}: FAILED (API error) — {e}")
                continue
            except requests.exceptions.RequestException as e:
                # Network error survived every retry in fetch_page. Log
                # and move on — do NOT let this kill the whole run; this
                # is exactly the failure that lost ~70 fetched combos
                # before checkpointing existed.
                print(f"{lawd_cd} / {deal_ymd}: FAILED (network, gave up after {MAX_RETRIES} retries) — {e}")
                continue
 
            for r in records:
                r["lawd_cd"] = lawd_cd  # tag source params onto each record
                r["deal_ymd"] = deal_ymd
            append_checkpoint(records, checkpoint_file)
            print(f"{lawd_cd} / {deal_ymd}: {len(records)} records (saved to {checkpoint_file})")
            time.sleep(0.2)  # be polite to the API — avoid rate-limit errors
 
    return pd.read_csv(checkpoint_file, dtype=str)
 
 
def load_to_bigquery(df: pd.DataFrame, project_id: str, dataset_id: str, table_id: str) -> None:
    client = bigquery.Client(project=project_id)
    table_ref = f"{project_id}.{dataset_id}.{table_id}"
    job_config = bigquery.LoadJobConfig(
        write_disposition="WRITE_TRUNCATE",  # swap to WRITE_APPEND once you add incremental loads
        autodetect=True,  # fine for now — tighten the schema explicitly once you know the fields
    )
    job = client.load_table_from_dataframe(df, table_ref, job_config=job_config)
    job.result()
    print(f"Loaded {len(df)} rows into {table_ref}")
 
 
if __name__ == "__main__":
    df = fetch_all()
    load_to_bigquery(df, GCP_PROJECT_ID, BQ_DATASET, BQ_TABLE)