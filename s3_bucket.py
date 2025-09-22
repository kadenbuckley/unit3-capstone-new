# Create S3 Bucket w/ Unique Name

import boto3, botocore, time, re

# ----- Config -----
REGION = "us-east-1"
BUCKET_NAME = "unit3-capstone"

# Initiate the client
s3 = boto3.client('s3', region_name=REGION)
sts = boto3.client("sts", region_name=REGION)

# ---------- S3 ----------
def make_bucket_name(prefix: str) -> str:
    acct = sts.get_caller_identity()["Account"]
    ts = int(time.time())
    base = f"{prefix}-{acct}-{REGION}-{ts}".lower()
    return re.sub(r"[^a-z0-9.-]", "-", base)[:63]

def ensure_bucket(bucket: str) -> str:
    try:
        s3.head_bucket(Bucket=bucket)
        print(f"ℹ️  bucket exists: s3://{bucket}")
        return bucket
    except botocore.exceptions.ClientError:
        pass
    if REGION == "us-east-1":
        s3.create_bucket(Bucket=bucket)
    else:
        s3.create_bucket(Bucket=bucket, CreateBucketConfiguration={"LocationConstraint": REGION})
    print(f"✅ created bucket: s3://{bucket}")
    return bucket

# ---------- main ----------
def main():
    bucket = ensure_bucket(make_bucket_name(BUCKET_NAME))

    print("\n🎉 Bucket Created")

if __name__ == "__main__":
    main()