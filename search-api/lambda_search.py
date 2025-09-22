import os, json, boto3
from botocore.awsrequest import AWSRequest
from botocore.auth import SigV4Auth
import urllib3

REGION = os.getenv("AWS_REGION", "us-east-1")
OS_ENDPOINT = os.environ["OS_ENDPOINT"].rstrip("/")
OS_INDEX = os.getenv("OS_INDEX", "pdf-chunks")
MODEL_ID = os.getenv("MODEL_ID", "amazon.titan-embed-text-v2:0")

http = urllib3.PoolManager()
bedrock = boto3.client("bedrock-runtime", region_name=REGION)

def _signed(method: str, url: str, body: bytes | None = None):
    session = boto3.Session(region_name=REGION)
    creds = session.get_credentials().get_frozen_credentials()
    headers = {"Content-Type": "application/json"} if body else {}
    req = AWSRequest(method=method, url=url, data=body, headers=headers)
    SigV4Auth(creds, "es", REGION).add_auth(req)
    return http.request(method, url, body=body, headers=dict(req.headers.items()))

def _embed(text: str) -> list[float]:
    payload = {"inputText": text}
    resp = bedrock.invoke_model(
        modelId=MODEL_ID,
        body=json.dumps(payload).encode("utf-8"),
        contentType="application/json",
        accept="application/json",
    )
    body = json.loads(resp["body"].read().decode("utf-8"))
    return body["embedding"]

def _search_keyword(q: str, size: int = 5):
    query = {"size": size, "query": {"match": {"text": q}}}
    r = _signed("POST", f"{OS_ENDPOINT}/{OS_INDEX}/_search", json.dumps(query).encode("utf-8"))
    return r.status, r.data.decode("utf-8")

def _search_semantic(q: str, size: int = 5, k: int = 5):
    vec = _embed(q)
    query = {
        "size": size,
        "query": {
            "knn": {
                "embedding": {
                    "vector": vec,
                    "k": k
                }
            }
        }
    }
    r = _signed("POST", f"{OS_ENDPOINT}/{OS_INDEX}/_search", json.dumps(query).encode("utf-8"))
    return r.status, r.data.decode("utf-8")

def lambda_handler(event, context):
    params = (event or {}).get("queryStringParameters") or {}
    q = params.get("q") or "test"
    mode = (params.get("mode") or "keyword").lower()

    if mode == "semantic":
        status, body = _search_semantic(q)
    else:
        status, body = _search_keyword(q)

    return {
        "statusCode": status,
        "headers": {"Content-Type": "application/json"},
        "body": body,
    }
