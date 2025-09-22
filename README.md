# Unit 3 Capstone
### by Kaden Buckley
### September 22, 2025

## AWS Data Flow & Infrastructure

- s3 bucket as main repository for all documents
- PDF upload to s3 triggers the lambda_pdf_function, which uses textract to extract text from PDF, store the raw text in RDS, embed the text using bedrock, and store the embeddings in OpenSearch.
- CSV and JSON uploads to s3 trigger the lambda_structured function.
- Next steps include: Finishing glue crawler to catalog and discover shcemas of the ingested files, creating glue transform to run ETL jobs, creating redshift, and connect Quicksight to Redhsift.

## Lambda Functions
### Lambda PDF Ingestion Function

```bash
# 1.) Shell variables
REGION=us-east-1
ACCOUNT_ID=$(aws sts get-caller-identity --query Account --output text)
FUNC=lambda_pdf_function
BUCKET=unit3-capstone-564045267362-us-east-1-1758321354

#2.) Create IAM role for Lambda
aws iam create-role \
  --role-name ${FUNC}-role \
  --assume-role-policy-document '{
    "Version":"2012-10-17",
    "Statement":[{"Effect":"Allow","Principal":{"Service":"lambda.amazonaws.com"},"Action":"sts:AssumeRole"}]
  }' || true

# CloudWatch logs, S3 read, Textract
aws iam attach-role-policy --role-name ${FUNC}-role --policy-arn arn:aws:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole
aws iam attach-role-policy --role-name ${FUNC}-role --policy-arn arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess
aws iam attach-role-policy --role-name ${FUNC}-role --policy-arn arn:aws:iam::aws:policy/AmazonTextractFullAccess

#3.) Package code
zip -q function.zip lambda_pdf_function.py

#4.) Create lambda and ignore if it exists
aws lambda create-function \
  --function-name ${FUNC} \
  --runtime python3.12 \
  --role arn:aws:iam::${ACCOUNT_ID}:role/${FUNC}-role \
  --handler lambda_pdf_function.lambda_handler \
  --zip-file fileb://function.zip \
  --region ${REGION} || true

#5.) Update code if needed
aws lambda update-function-code \
  --function-name ${FUNC} \
  --zip-file fileb://function.zip \
  --region ${REGION}

#6.) Allow s3 to invoke lambda
aws lambda add-permission \
  --function-name ${FUNC} \
  --statement-id s3invoke-pdf \
  --action lambda:InvokeFunction \
  --principal s3.amazonaws.com \
  --source-arn arn:aws:s3:::${BUCKET} \
  --region ${REGION} || true

#7.) Wire the s3 event notification
aws s3api put-bucket-notification-configuration \
  --bucket ${BUCKET} \
  --notification-configuration "{
    \"LambdaFunctionConfigurations\": [
      {
        \"Id\": \"PdfIngest\",
        \"LambdaFunctionArn\": \"arn:aws:lambda:${REGION}:${ACCOUNT_ID}:function:${FUNC}\",
        \"Events\": [\"s3:ObjectCreated:*\"],
        \"Filter\": {\"Key\": {\"FilterRules\": [{\"Name\": \"suffix\", \"Value\": \".pdf\"}]}}
      }
    ]
  }" \
  --region ${REGION}

#8.) Test
aws s3 cp ~/Desktop/hello.pdf "s3://$BUCKET/incoming/hello.pdf" --region "$REGION"

aws logs tail "/aws/lambda/${FUNC}" --region "${REGION}" --follow
```

### Create a AWS RDS to Store Raw Text Using rds_setup.py
```bash
# Create database
PGPASSWORD='myStrongPassword123!' PGSSLMODE=require psql \
  -h my-postgres-db.co5wwiqius2w.us-east-1.rds.amazonaws.com \
  -p 5432 -U dbadmin -d postgres \
  -c "CREATE DATABASE capstone OWNER dbadmin;"

# Create the tables in database
PGPASSWORD='myStrongPassword123!' PGSSLMODE=require psql \
  -h my-postgres-db.co5wwiqius2w.us-east-1.rds.amazonaws.com \
  -p 5432 -U dbadmin -d capstone <<'SQL'
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
SQL

# Check schema to verify 
PGPASSWORD='myStrongPassword123!' PGSSLMODE=require psql \
  -h my-postgres-db.co5wwiqius2w.us-east-1.rds.amazonaws.com \
  -p 5432 -U dbadmin -d capstone -c "\dt"

# Db should look like this
           List of relations
 Schema |    Name    | Type  |  Owner  
--------+------------+-------+---------
 public | doc_chunks | table | dbadmin
 public | documents  | table | dbadmin
(2 rows)

```

### Wire the PDF Ingestion Lambda function to RDS
```bash
# Make public for dev (can later make secure)
REGION=us-east-1
DB_ID=my-postgres-db
SG_ID=$(aws rds describe-db-instances --db-instance-identifier "$DB_ID" --region "$REGION" \
  --query "DBInstances[0].VpcSecurityGroups[0].VpcSecurityGroupId" --output text)

aws ec2 authorize-security-group-ingress \
  --group-id "$SG_ID" --protocol tcp --port 5432 --cidr 0.0.0.0/0 --region "$REGION" || true

# Update env vars
REGION=us-east-1
FUNC=lambda_pdf_function
DB_HOST=my-postgres-db.co5wwiqius2w.us-east-1.rds.amazonaws.com
DB_NAME=capstone
DB_USER=dbadmin
DB_PASS='myStrongPassword123!'

aws lambda update-function-configuration \
  --function-name "$FUNC" --region "$REGION" \
  --environment "Variables={
    TEXTRACT_MAX_POLL_SECONDS=180,
    DB_HOST=$DB_HOST,
    DB_PORT=5432,
    DB_NAME=$DB_NAME,
    DB_USER=$DB_USER,
    DB_PASSWORD=$DB_PASS
  }"

# bundle pg8000 in Lambda zip
REGION=us-east-1
FUNC=lambda_pdf_function

# fresh build dir
rm -rf build && mkdir build

# put your handler in the build
cp lambda_pdf_function.py build/

# vendor the dependency into the zip
pip install --target build pg8000==1.29.8

# create deployment package
( cd build && zip -r ../function.zip . >/dev/null )

# deploy
aws lambda update-function-code \
  --function-name "$FUNC" \
  --zip-file fileb://function.zip \
  --region "$REGION"

aws lambda wait function-updated --function-name "$FUNC" --region "$REGION"

```

### OpenSearch Setup
```bash

REGION=us-east-1
OS_DOMAIN=capstone-search
ROLE_ARN=$(aws lambda get-function-configuration \
  --function-name lambda_pdf_function \
  --region "$REGION" \
  --query 'Role' --output text)
ACCOUNT_ID=$(aws sts get-caller-identity --query 'Account' --output text)


# build access policy
cat > os-access-policy.json <<EOF
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Sid": "AllowLambdaRole",
      "Effect": "Allow",
      "Principal": { "AWS": "$ROLE_ARN" },
      "Action": "es:ESHttp*",
      "Resource": "arn:aws:es:$REGION:$ACCOUNT_ID:domain/$OS_DOMAIN/*"
    }
  ]
}
EOF

# Create Domain
aws opensearch create-domain \
  --domain-name "$OS_DOMAIN" \
  --engine-version "OpenSearch_2.11" \
  --cluster-config InstanceType=t3.small.search,InstanceCount=1,ZoneAwarenessEnabled=false \
  --ebs-options EBSEnabled=true,VolumeType=gp3,VolumeSize=20 \
  --encryption-at-rest-options Enabled=true \
  --node-to-node-encryption-options Enabled=true \
  --domain-endpoint-options EnforceHTTPS=true,TLSSecurityPolicy=Policy-Min-TLS-1-2-2019-07 \
  --access-policies "$POLICY" \
  --region "$REGION"

# grab endpoint
aws opensearch wait domain-available --domain-name "$OS_DOMAIN" --region "$REGION"
OS_ENDPOINT=$(aws opensearch describe-domain --domain-name "$OS_DOMAIN" \
  --region "$REGION" --query 'DomainStatus.Endpoint' --output text)
echo "OS_ENDPOINT=https://$OS_ENDPOINT"


```

### Using OpenSearch for Semantic Search in CLI:

```bash
# In CLI
aws lambda invoke \
  --function-name capstone_search_api \
  --region "$REGION" \
  --cli-binary-format raw-in-base64-out \
  --payload '{"queryStringParameters":{"q":"ENTER YOUR SEARCH HERE","mode":"semantic"}}' \
  out.json >/dev/null

cat out.json | python -m json.tool
```
