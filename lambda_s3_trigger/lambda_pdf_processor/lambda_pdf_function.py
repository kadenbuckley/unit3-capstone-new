import os, json, time, urllib.parse, logging, boto3, re, os.path
from botocore.exceptions import ClientError
import json as _json
import requests
from requests_aws4auth import AWS4Auth

# ---------- logging ----------
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# ---------- AWS clients & constants ----------
REGION = os.getenv("AWS_REGION", "us-east-1")

s3 = boto3.client("s3", region_name=REGION)
textract = boto3.client("textract", region_name=REGION)

bedrock = boto3.client("bedrock-runtime", region_name=REGION)
MODEL_ID = "amazon.titan-embed-text-v2:0"
EMBED_DIM = 1024  # (for later OpenSearch index dimension)

def _os_auth():
    # Use the Lambda role creds to sign requests (SigV4)
    session = boto3.Session()
    creds = session.get_credentials().get_frozen_credentials()
    return AWS4Auth(creds.access_key, creds.secret_key, REGION, "es", session_token=creds.token)

def _os_endpoint():
    ep = os.environ.get("OS_ENDPOINT", "").rstrip("/")
    return ep if ep.startswith("https://") else (f"https://{ep}" if ep else "")

def _os_ensure_index(index_name: str, dim: int):
    ep = _os_endpoint()
    if not ep:
        return False
    try:
        r = requests.head(f"{ep}/{index_name}", auth=_os_auth(), timeout=5)
        if r.status_code == 200:
            return True
        if r.status_code == 404:
            body = {
                "settings": { "index.knn": True },
                "mappings": {
                    "properties": {
                        "s3_uri": {"type": "keyword"},
                        "doc_id": {"type": "integer"},
                        "chunk_index": {"type": "integer"},
                        "text": {"type": "text"},
                        "embedding": {
                            "type": "knn_vector",
                            "dimension": dim,
                            "method": {
                                "name": "hnsw",
                                "engine": "nmslib",
                                "space_type": "cosinesimil"
                            }
                        }
                    }
                }
            }
            put = requests.put(f"{ep}/{index_name}", auth=_os_auth(), json=body, timeout=30)
            put.raise_for_status()
            logger.info(f"[os] created index {index_name}")
            return True
        # Any other code: treat as not ready yet
        logger.warning(f"[os] HEAD index returned {r.status_code}: {r.text[:200]}")
        return False
    except Exception as e:
        logger.info(f"[os] ensure_index not ready yet: {e}")
        return False

def os_index_chunks(index_name: str, s3_uri: str, doc_id: int, chunks: list[str], vectors: list[list[float]]):
    ep = _os_endpoint()
    if not ep:
        logger.info("[os] OS_ENDPOINT not set; skipping OS indexing")
        return

    if not _os_ensure_index(index_name, EMBED_DIM):
        logger.info("[os] index not ready; skipping OS indexing this run")
        return

    try:
        # Build NDJSON bulk body
        lines = []
        for i, (text, vec) in enumerate(zip(chunks, vectors)):
            lines.append(_json.dumps({"index": {"_index": index_name}}))
            lines.append(_json.dumps({
                "s3_uri": s3_uri,
                "doc_id": doc_id,
                "chunk_index": i,
                "text": text,
                "embedding": vec
            }))
        payload = "\n".join(lines) + "\n"

        r = requests.post(
            f"{ep}/_bulk",
            headers={"Content-Type": "application/x-ndjson"},
            data=payload,
            auth=_os_auth(),
            timeout=60,
        )
        r.raise_for_status()
        resp = r.json()
        logger.info(f"[os] bulk indexed ok={not resp.get('errors', False)} items={len(resp.get('items', []))} index={index_name}")
    except Exception as e:
        logger.exception(f"[os] index error: {e}")

# ---------- DB (pg8000) ----------
from pg8000.native import Connection as PGConnection

def _db_conn():
    return PGConnection(
        user=os.environ["DB_USER"],
        password=os.environ["DB_PASSWORD"],
        host=os.environ["DB_HOST"],
        port=int(os.environ.get("DB_PORT", "5432")),
        database=os.environ.get("DB_NAME", "postgres"),
        ssl_context=True,  # TLS to RDS
    )

def store_document_and_chunks(s3_uri: str, chunks: list[str], title: str | None = None) -> int:
    conn = _db_conn()
    try:
        # Insert the document and get its id
        [row] = conn.run(
            "INSERT INTO documents (s3_uri, title) "
            "VALUES (:s3, :t) "
            "RETURNING id",
            s3=s3_uri,
            t=title or os.path.basename(urllib.parse.urlparse(s3_uri).path),
        )
        doc_id = row[0]

        # Insert chunks
        offset = 0
        for i, c in enumerate(chunks):
            start = offset
            end = start + len(c)
            conn.run(
                "INSERT INTO doc_chunks (document_id, chunk_index, char_start, char_end, text) "
                "VALUES (:d, :i, :s, :e, :t)",
                d=doc_id, i=i, s=start, e=end, t=c,
            )
            offset = end

        return doc_id
    finally:
        try:
            conn.close()
        except Exception:
            pass

# ---------- Embeddings ----------
def embed_text(text: str) -> list[float]:
    payload = {"inputText": text}
    resp = bedrock.invoke_model(
        modelId=MODEL_ID,
        body=json.dumps(payload).encode("utf-8"),
        contentType="application/json",
        accept="application/json",
    )
    body = json.loads(resp["body"].read().decode("utf-8"))
    return body["embedding"]

# ---------- Chunking helpers ----------
def _split_paragraphs(text: str) -> list[str]:
    parts = re.split(r"\n{2,}", text.strip())
    return [p.strip() for p in parts if p.strip()]

def _chunk_text(text: str,
                target_chars: int = 3200,
                max_chars: int = 4000,
                overlap_chars: int = 200) -> list[str]:
    paras = _split_paragraphs(text)
    chunks, cur = [], ""
    for p in paras:
        p = (p + "\n")
        if len(cur) + len(p) <= target_chars:
            cur += p
        elif len(p) > max_chars:
            # hard split long paragraph
            for i in range(0, len(p), target_chars):
                piece = p[i:i+target_chars]
                if cur:
                    chunks.append(cur)
                    cur = ""
                chunks.append(piece)
        else:
            # close current chunk (with overlap)
            if cur:
                chunks.append(cur)
                cur = cur[max(0, len(cur)-overlap_chars):]
            cur += p
    if cur:
        chunks.append(cur[:max_chars])
    return chunks

# ---------- Textract ----------
def _textract_text(bucket: str, key: str) -> str:
    head = s3.head_object(Bucket=bucket, Key=key)
    logger.info(f"[head_object] Bucket={bucket} Key={key} "
                f"ContentType={head.get('ContentType')} Size={head.get('ContentLength')}")

    start = textract.start_document_text_detection(
        DocumentLocation={"S3Object": {"Bucket": bucket, "Name": key}},
    )
    job_id = start["JobId"]

    waited = 0
    max_wait = int(os.getenv("TEXTRACT_MAX_POLL_SECONDS", "180"))
    while True:
        resp = textract.get_document_text_detection(JobId=job_id)
        status = resp["JobStatus"]
        if status == "SUCCEEDED":
            break
        if status == "FAILED":
            raise RuntimeError(f"Textract job failed: {resp}")
        if waited >= max_wait:
            raise TimeoutError(f"Textract timed out after {waited}s (job_id={job_id})")
        time.sleep(2); waited += 2

    blocks = resp.get("Blocks", [])
    next_token = resp.get("NextToken")
    while next_token:
        resp = textract.get_document_text_detection(JobId=job_id, NextToken=next_token)
        blocks.extend(resp.get("Blocks", []))
        next_token = resp.get("NextToken")

    lines = [b["Text"] for b in blocks if b.get("BlockType") == "LINE" and "Text" in b]
    return "\n".join(lines)

# ---------- Handler ----------
def lambda_handler(event, context):
    try:
        rec = event["Records"][0]
        bucket = rec["s3"]["bucket"]["name"]

        raw_key = rec["s3"]["object"]["key"]
        key = urllib.parse.unquote(raw_key)  # avoid '+' -> space issues
        logger.info(f"[event] bucket={bucket} raw_key={raw_key} decoded_key={key}")

        if not key.lower().endswith(".pdf"):
            return {"skip": True, "reason": "not a PDF", "key": key}

        # 1) Extract text
        text = _textract_text(bucket, key)
        logger.info(f"[pdf_ingest] {bucket}/{key} chars={len(text)}")

        # 2) Chunk
        chunks = _chunk_text(text)
        logger.info(f"[chunk] n={len(chunks)} sizes={[len(c) for c in chunks[:5]]}")

        # 3) Embed (kept for later OpenSearch use)
        vectors = []
        for c in chunks:
            vectors.append(embed_text(c))
        logger.info(f"[embed] n={len(vectors)} dim={len(vectors[0]) if vectors else 0}")

        # 4) Store raw text chunks in RDS
        s3_uri = f"s3://{bucket}/{key}"
        doc_id = store_document_and_chunks(s3_uri, chunks)
        logger.info(f"[rds] stored doc_id={doc_id} chunks={len(chunks)}")

        # 5. Send to OpenSearch (no-op until endpoint is ready / env set)
        os_index_chunks(os.environ.get("OS_INDEX", "pdf-chunks"), s3_uri, doc_id, chunks, vectors)

        return {
            "status": "ok",
            "s3": s3_uri,
            "document_id": doc_id,
            "chunks": len(chunks),
        }

    except ClientError as e:
        logger.exception(f"[pdf_ingest] AWS error detail: {getattr(e, 'response', {})}")
        raise
    except Exception as e:
        logger.exception(f"[pdf_ingest] error: {e}")
        raise
