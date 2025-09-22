"""
Create (or reuse) a PostgreSQL RDS instance suitable for the capstone project,
open inbound PostgreSQL to your current public IP, and create the required tables.

- Instance: db.t4g.micro, Postgres, 20 GB, PubliclyAccessible (dev/demo)
- Tables: documents, doc_chunks

Usage:
  python rds_setup.py
"""

import os
import time
import json
import boto3
import requests
from botocore.exceptions import ClientError, WaiterError

# config
db_identifier = os.getenv("DB_IDENTIFIER", "my-postgres-db")
db_name       = os.getenv("DB_NAME",       "capstone")
db_user       = os.getenv("DB_USER",       "dbadmin")
db_password   = os.getenv("DB_PASSWORD",   "myStrongPassword123!")  # dev only; rotate later
db_class      = os.getenv("DB_CLASS",      "db.t4g.micro")
db_storage_gb = int(os.getenv("DB_STORAGE_GB", "20"))
db_port       = int(os.getenv("DB_PORT", "5432"))
publicly_accessible = True  # dev/demo; prefer private + VPC Lambda in prod
backup_retention_days = 1   # small daily snapshot; set 0 to disable
storage_type = os.getenv("DB_STORAGE_TYPE", "gp3")  # gp2/gp3; gp3 is modern & cost-effective

region = os.getenv("AWS_REGION") or os.getenv("AWS_DEFAULT_REGION")  # rely on profile/ENV

rds = boto3.client("rds", region_name=region)
ec2 = boto3.client("ec2", region_name=region)


def get_my_ip():
    """Return caller public IP (str) or None if lookup fails."""
    try:
        return requests.get("https://ipinfo.io/ip", timeout=5).text.strip()
    except Exception:
        print("⚠️  Could not auto-detect your IP. You can add it manually later in the SG.")
        return None


def ensure_rds_instance():
    """Describe or create the DB instance. Waits until 'available'. Returns (endpoint, vpc_sg_ids)."""
    try:
        print(f"Checking if DB instance '{db_identifier}' already exists...")
        resp = rds.describe_db_instances(DBInstanceIdentifier=db_identifier)
        inst = resp["DBInstances"][0]
        status = inst["DBInstanceStatus"]
        endpoint = inst.get("Endpoint", {}).get("Address")
        print(f"DB instance '{db_identifier}' exists with status: {status}")
        if status != "available" or not endpoint:
            print("Waiting for DB to become available...")
            waiter = rds.get_waiter("db_instance_available")
            waiter.wait(DBInstanceIdentifier=db_identifier, WaiterConfig={"Delay": 30, "MaxAttempts": 60})
            inst = rds.describe_db_instances(DBInstanceIdentifier=db_identifier)["DBInstances"][0]
            endpoint = inst["Endpoint"]["Address"]
        print(f"✅ Endpoint: {endpoint}")
        vpc_sg_ids = [g["VpcSecurityGroupId"] for g in inst.get("VpcSecurityGroups", [])]
        return endpoint, vpc_sg_ids

    except ClientError as e:
        code = e.response["Error"]["Code"]
        if code not in ("DBInstanceNotFoundFault", "DBInstanceNotFound"):
            raise
        print(f"DB instance '{db_identifier}' not found. Creating new instance...")

        create_kwargs = dict(
            DBInstanceIdentifier=db_identifier,
            MasterUsername=db_user,
            MasterUserPassword=db_password,
            DBInstanceClass=db_class,
            Engine="postgres",
            AllocatedStorage=db_storage_gb,
            PubliclyAccessible=publicly_accessible,
            BackupRetentionPeriod=backup_retention_days,
            StorageType=storage_type,
            Port=db_port,
            MultiAZ=False,
            AutoMinorVersionUpgrade=True,
            DeletionProtection=False,
            DBName=db_name,
        )

        try:
            resp = rds.create_db_instance(**create_kwargs)
            status = resp["DBInstance"]["DBInstanceStatus"]
            print(f"Creating RDS instance: class={db_class}, storage={db_storage_gb}GB {storage_type}, public={publicly_accessible}")
            print("This can take several minutes...")
        except ClientError as create_error:
            print(f"❌ Error creating DB instance: {create_error}")
            raise

        print("Waiting for DB to become available...")
        waiter = rds.get_waiter("db_instance_available")
        try:
            waiter.wait(DBInstanceIdentifier=db_identifier, WaiterConfig={"Delay": 30, "MaxAttempts": 60})
        except WaiterError as werr:
            print(f"❌ Waiter error: {werr}")
            raise

        inst = rds.describe_db_instances(DBInstanceIdentifier=db_identifier)["DBInstances"][0]
        endpoint = inst["Endpoint"]["Address"]
        print(f"✅ Endpoint: {endpoint}")
        vpc_sg_ids = [g["VpcSecurityGroupId"] for g in inst.get("VpcSecurityGroups", [])]
        return endpoint, vpc_sg_ids


def allow_postgres_from_my_ip(security_group_ids):
    """Authorize inbound 5432/tcp from your /32 in each provided SG."""
    if not security_group_ids:
        print("ℹ️  No VPC security groups attached to the DB (unexpected). Skipping SG update.")
        return

    my_ip = get_my_ip()
    if not my_ip:
        return
    cidr = f"{my_ip}/32"

    for sg_id in security_group_ids:
        print(f"Updating security group {sg_id} to allow {cidr} on port {db_port}...")
        try:
            ec2.authorize_security_group_ingress(
                GroupId=sg_id,
                IpPermissions=[{
                    "IpProtocol": "tcp",
                    "FromPort": db_port,
                    "ToPort": db_port,
                    "IpRanges": [{"CidrIp": cidr, "Description": "pg access (dev)"}],
                }],
            )
            print(f"✅ SG {sg_id}: added rule for {cidr}")
        except ClientError as e:
            code = e.response["Error"]["Code"]
            if code == "InvalidPermission.Duplicate":
                print(f"ℹ️  SG {sg_id}: rule for {cidr} already exists")
            else:
                print(f"❌ Error updating SG {sg_id}: {e}")


def create_tables_if_missing(endpoint):
    """Create the project tables using pg8000 if available."""
    try:
        import pg8000.native as pg
    except Exception:
        print("ℹ️  Skipping table creation (pg8000 not installed).")
        print("    To install locally:  pip install pg8000")
        print("    Or run these SQL statements manually in your DB client:")
        print("""
CREATE TABLE IF NOT EXISTS documents (
  id BIGSERIAL PRIMARY KEY,
  s3_uri TEXT NOT NULL,
  title TEXT,
  uploaded_at TIMESTAMPTZ DEFAULT now()
);
CREATE TABLE IF NOT EXISTS doc_chunks (
  id BIGSERIAL PRIMARY KEY,
  document_id BIGINT REFERENCES documents(id) ON DELETE CASCADE,
  chunk_index INT NOT NULL,
  char_start INT,
  char_end INT,
  text TEXT NOT NULL
);
""")
        return

    print("Creating tables if needed...")
    conn = pg.Connection(
        user=db_user,
        password=db_password,
        host=endpoint,
        port=db_port,
        database=db_name,
        ssl_context=True,  # TLS required by RDS
    )
    try:
        conn.run("""
        CREATE TABLE IF NOT EXISTS documents (
          id BIGSERIAL PRIMARY KEY,
          s3_uri TEXT NOT NULL,
          title TEXT,
          uploaded_at TIMESTAMPTZ DEFAULT now()
        )""")
        conn.run("""
        CREATE TABLE IF NOT EXISTS doc_chunks (
          id BIGSERIAL PRIMARY KEY,
          document_id BIGINT REFERENCES documents(id) ON DELETE CASCADE,
          chunk_index INT NOT NULL,
          char_start INT,
          char_end INT,
          text TEXT NOT NULL
        )""")
        conn.commit()
        print("✅ Tables ensured.")
    finally:
        conn.close()


def main():
    print(f"Region: {region or '(from AWS profile)'}")
    endpoint, sg_ids = ensure_rds_instance()
    allow_postgres_from_my_ip(sg_ids)

    print("\nConnection info:")
    print(f"  host={endpoint}")
    print(f"  port={db_port}")
    print(f"  dbname={db_name}")
    print(f"  user={db_user}")
    print("  password=******")
    print("\npsql example:")
    print(f'  PGSSLMODE=require psql "host={endpoint} port={db_port} dbname={db_name} user={db_user} password=YOUR_PASS"')

    # Create tables now (optional, requires pg8000)
    create_tables_if_missing(endpoint)

    print("\n🎉 RDS is ready for your Lambda. Set these Lambda env vars:")
    print(f"  DB_HOST={endpoint}")
    print(f"  DB_PORT={db_port}")
    print(f"  DB_NAME={db_name}")
    print(f"  DB_USER={db_user}")
    print(f"  DB_PASSWORD=your-password")


if __name__ == "__main__":
    try:
        main()
    except ClientError as e:
        print(f"❌ AWS error: {e}")
    except Exception as e:
        print(f"❌ Unexpected error: {e}")
