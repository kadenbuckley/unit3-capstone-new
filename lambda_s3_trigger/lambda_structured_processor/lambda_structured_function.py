import os
import re
import json
import datetime
import urllib.parse

import boto3
from botocore.exceptions import ClientError

s3   = boto3.client("s3")
glue = boto3.client("glue")

GLUE_JOB_NAME = os.environ.get("GLUE_JOB_NAME", "capstone-structured-etl")
GLUE_CRAWLER_NAME = os.environ.get("GLUE_CRAWLER_NAME", "")
BUCKET = os.environ["DATA_BUCKET"]
OUTPUT_ROOT = os.environ.get("OUTPUT_ROOT", "processed/structured")
STAGING_ROOT = os.environ.get("STAGING_ROOT", "staging/structured")
ERROR_ROOT = os.environ.get("ERROR_ROOT", "errors/structured")
ENABLE_CRAWLER = os.environ.get("ENABLE_CRAWLER", "true").lower() == "true"

_VALID_EXT = (".csv", ".json")

def _today_yyyy_mm_dd() -> str:
    return datetime.date.today().isoformat()

def _dataset_from_key(key: str) -> str:
    """
    incoming/csv/customers_2025-09-22.csv -> 'customers'
    incoming/json/sales-2024-12.json      -> 'sales'
    incoming/json/Orders.json             -> 'orders'
    """
    base = os.path.basename(key)
    m = re.split(r"[_\-.]", base, maxsplit=1)
    ds = (m[0] if m and m[0] else os.path.splitext(base)[0]).strip().lower()
    ds = re.sub(r"[^a-z0-9_]+", "_", ds)
    return ds or "dataset"

def _copy_then_delete(src_bucket: str, src_key: str, dest_bucket: str, dest_key: str):
    s3.copy_object(
        Bucket=dest_bucket,
        CopySource={"Bucket": src_bucket, "Key": src_key},
        Key=dest_key,
    )
    s3.delete_object(Bucket=src_bucket, Key=src_key)

def _move_to_errors(src_bucket: str, src_key: str, reason: str):
    base = os.path.basename(src_key)
    err_key = f"{ERROR_ROOT}/{_today_yyyy_mm_dd()}/{base}"
    s3.copy_object(
        Bucket=src_bucket,
        CopySource={"Bucket": src_bucket, "Key": src_key},
        Key=err_key,
        MetadataDirective="REPLACE",
        Metadata={"ingest_error": reason[:255]},
    )
    s3.delete_object(Bucket=src_bucket, Key=src_key)
    print(f"[ERROR] moved to {err_key} because: {reason}")

def _start_crawler(dataset: str):
    name = GLUE_CRAWLER_NAME or f"crawler-{dataset}"
    try:
        glue.start_crawler(Name=name)
        print(f"[INFO] started crawler: {name}")
    except glue.exceptions.CrawlerRunningException:
        print(f"[INFO] crawler already running: {name}")
    except glue.exceptions.EntityNotFoundException:
        print(f"[WARN] crawler not found: {name} (skipping)")
    except ClientError as e:
        print(f"[WARN] start_crawler failed: {e}")

def _start_glue_job(dataset: str, staging_prefix: str, output_prefix: str):
    args = {
        "--dataset": dataset,
        "--staging_path": f"s3://{BUCKET}/{staging_prefix}",
        "--output_path":  f"s3://{BUCKET}/{output_prefix}",
        "--redshift_table": dataset,   
        "--load_mode": "append",
    }
    resp = glue.start_job_run(JobName=GLUE_JOB_NAME, Arguments=args)
    print(f"[INFO] started glue job {GLUE_JOB_NAME}, runId={resp['JobRunId']}")
    return resp["JobRunId"]

# Lambda handler
def lambda_handler(event, context):
    """
    S3 event -> move incoming file to staging -> (optional) crawler -> ETL job.
    """
    print("[DEBUG] event:", json.dumps(event))
    for rec in event.get("Records", []):
        try:
            src_bucket = rec["s3"]["bucket"]["name"]
            src_key = urllib.parse.unquote(rec["s3"]["object"]["key"])
            _, ext = os.path.splitext(src_key.lower())
            if ext not in _VALID_EXT:
                print(f"[INFO] ignoring non-csv/json key: {src_key}")
                continue
            if src_bucket != BUCKET:
                print(f"[WARN] event for different bucket {src_bucket}, expected {BUCKET}; skipping")
                continue

            dataset = _dataset_from_key(src_key)
            ingest_date = _today_yyyy_mm_dd()
            dest_key = f"{STAGING_ROOT}/{dataset}/ingest_date={ingest_date}/{os.path.basename(src_key)}"

            print(f"[INFO] staging {src_key} -> {dest_key}")
            _copy_then_delete(BUCKET, src_key, BUCKET, dest_key)


            if ENABLE_CRAWLER:
                _start_crawler(dataset)

            staging_prefix = f"{STAGING_ROOT}/{dataset}/"
            output_prefix  = f"{OUTPUT_ROOT}/{dataset}/"
            run_id = _start_glue_job(dataset, staging_prefix, output_prefix)

            print(json.dumps({
                "ok": True,
                "dataset": dataset,
                "staged_key": dest_key,
                "glue_job": GLUE_JOB_NAME,
                "glue_run_id": run_id
            }))

        except Exception as e:

            try:
                if "src_key" in locals() and src_key:
                    _move_to_errors(BUCKET, src_key, reason=str(e))
            except Exception as move_err:
                print(f"[FATAL] could not move to errors: {move_err}")
            raise 

    return {"ok": True}
