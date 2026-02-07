# Soda–Collibra Lambda Setup


> Works for Python **3.11**. Replace placeholders like `<ACCOUNT_ID>`, `<REGION>`, `<BUCKET>`, etc.

---

## 0) Prerequisites
- AWS CLI configured (`aws configure`)
- Docker installed
- IAM permissions to create Roles, Secrets, S3 objects, Layers, and Lambda

---

## 1) Put non‑secret config in S3
**No need to put credentials in this file.**

```bash
aws s3 cp config.yaml s3://<BUCKET>/soda-collibra/config.yaml
```

---

## 2) Store credentials in Secrets Manager
Create two JSON secrets and note their **ARNs**.

**Soda**
```json
{"api_key_id":"<ID>","api_key_secret":"<SECRET>"}
```

**Collibra**
```json
{"username":"<USER>","password":"<PASS>"}
```

---

## 3) Create the Lambda execution role (once)
**Trust policy** (Lambda can assume the role):
```json
{
  "Version": "2012-10-17",
  "Statement": [{
    "Effect": "Allow",
    "Principal": {"Service": "lambda.amazonaws.com"},
    "Action": "sts:AssumeRole"
  }]
}
```

Attach **AWSLambdaBasicExecutionRole** (managed) and this **inline policy** (update ARNs; note the `-*` suffix on secrets):

```json
{
  "Version":"2012-10-17",
  "Statement":[
    {
      "Sid":"ReadConfigFromS3",
      "Effect":"Allow",
      "Action":["s3:GetObject"],
      "Resource":["arn:aws:s3:::<BUCKET>/soda-collibra/*"]
    },
    {
      "Sid":"ReadSodaSecret",
      "Effect":"Allow",
      "Action":["secretsmanager:GetSecretValue"],
      "Resource":["arn:aws:secretsmanager:<REGION>:<ACCOUNT_ID>:secret:soda/demo/api-*"]
    },
    {
      "Sid":"ReadCollibraSecret",
      "Effect":"Allow",
      "Action":["secretsmanager:GetSecretValue"],
      "Resource":["arn:aws:secretsmanager:<REGION>:<ACCOUNT_ID>:secret:collibra/prod/credentials-*"]
    }
  ]
}
```

> If your secrets use a CMK, also allow `kms:Decrypt` on that key.

---

## 4) Build the **Dependencies Layer** in Docker
This ensures native wheels like **pydantic‑core** match Lambda’s OS/ABI.



### Build `layer.zip`
```bash
PLATFORM=linux/amd64
IMG=public.ecr.aws/lambda/python:3.11

docker run --rm \
  --platform "$PLATFORM" \
  -v "$(pwd)":/work \
  -w /work \
  --entrypoint /bin/bash \
  "$IMG" \
  -c '
set -euo pipefail
rm -rf layer layer.zip
mkdir -p layer/python
python -m pip install --upgrade pip
pip install -r requirements.txt -t layer/python --no-cache-dir
python3 << "EOF"
import zipfile, os
with zipfile.ZipFile("layer.zip", "w", zipfile.ZIP_DEFLATED) as z:
    for root, _, files in os.walk("layer"):
        for f in files:
            z.write(os.path.join(root, f), os.path.relpath(os.path.join(root, f), "layer"))
print("✅ layer.zip created")
EOF
'
```

### Publish the layer
```bash
LAYER_ARN=$(aws lambda publish-layer-version   --layer-name soda-collibra-deps   --zip-file fileb://layer.zip   --compatible-runtimes python3.11   --query LayerVersionArn --output text)
echo "$LAYER_ARN"
```

---

## 5) Package the **function code** (ZIP)
Include your handler and all imported modules (not dependencies).

```bash
rm -f function.zip
zip -r function.zip   lambda_handler.py   main.py integration.py constants.py utils.py metrics.py legacy_tests.py   clients models
```


---

## 6) Create the Lambda function
Use the **role ARN** from step 3 and the **layer ARN** from step 4.

```bash
aws lambda create-function   --function-name soda-collibra-sync   --runtime python3.11   --architectures x86_64 \ # or arm64
  --role arn:aws:iam::<ACCOUNT_ID>:role/<LAMBDA_ROLE_NAME>   --handler lambda_handler.handler   --zip-file fileb://function.zip   --timeout 300   --memory-size 1024   --environment "Variables={CONFIG_S3_URI=s3://<BUCKET>/soda-collibra/config.yaml,SODA_SECRET_ARN=arn:aws:secretsmanager:<REGION>:<ACCOUNT_ID>:secret:soda/demo/api-XXXX,COLLIBRA_SECRET_ARN=arn:aws:secretsmanager:<REGION>:<ACCOUNT_ID>:secret:collibra/prod/credentials-YYYY,DEBUG=0}"   --layers "$LAYER_ARN"
```

---

## 7) Update loop
- **Code only**:
```bash
aws lambda update-function-code   --function-name soda-collibra-sync   --zip-file fileb://function.zip
```

- **Dependencies changed** (requirements.txt):
  1) Rebuild `layer.zip` in Docker (step 4)  
  2) Publish new layer → update function to use it:
```bash
aws lambda update-function-configuration   --function-name soda-collibra-sync   --layers "$NEW_LAYER_ARN"   --environment "Variables={CONFIG_S3_URI=s3://my-bucket/config.yaml,SODA_SECRET_ARN=arn:aws:secretsmanager:us-east-1:123456789:secret:soda/demo/api-XXXX,COLLIBRA_SECRET_ARN=arn:aws:secretsmanager:us-east-1:123456789:secret:collibra/prod/credentials-YYYY,DEBUG=0}"

```

- **Config change** → edit the file in S3 (no deploy)
- **Secret rotation** → update Secrets Manager (no deploy)

---

## 8) Test & Logs
```bash
# Invoke once
aws lambda invoke --function-name soda-collibra-sync --payload '{}' /dev/stdout

# Stream logs
aws logs tail /aws/lambda/soda-collibra-sync --follow
```

---

## 9) (Optional) Schedule with EventBridge
```bash
aws events put-rule   --name soda-collibra-hourly   --schedule-expression "rate(1 hour)"

aws events put-targets   --rule soda-collibra-hourly   --targets "Id"="1","Arn"="$(aws lambda get-function --function-name soda-collibra-sync --query 'Configuration.FunctionArn' --output text)"

aws lambda add-permission   --function-name soda-collibra-sync   --statement-id allow-events   --action lambda:InvokeFunction   --principal events.amazonaws.com   --source-arn "$(aws events describe-rule --name soda-collibra-hourly --query 'Arn' --output text)"
```

---

