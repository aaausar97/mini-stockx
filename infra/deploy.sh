#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")"

command -v terraform >/dev/null || { echo "terraform not found — install: https://developer.hashicorp.com/terraform/install"; exit 1; }
command -v aws >/dev/null || { echo "aws cli not found — install and run: aws configure"; exit 1; }

echo "→ Checking AWS credentials…"
aws sts get-caller-identity >/dev/null

if [[ ! -f terraform.tfvars ]]; then
  echo "→ First run: creating terraform.tfvars from example"
  cp terraform.tfvars.example terraform.tfvars
fi

echo "→ terraform init"
terraform init -input=false

if grep -Eq '^[[:space:]]*enable_msk[[:space:]]*=[[:space:]]*true' terraform.tfvars 2>/dev/null; then
  echo "→ MSK enabled — first apply can take ~20 minutes"
fi

echo "→ terraform apply"
terraform apply -auto-approve -input=false

echo "→ Writing ../.env from outputs"
./write-env.sh

echo
echo "Done. AWS resources are up to date."
echo "  SNS_TOPIC_ARN=$(terraform output -raw sns_topic_arn)"
echo "  SQS_PAYMENT_URL=$(terraform output -raw sqs_payment_url)"
echo "  SQS_NOTIFY_URL=$(terraform output -raw sqs_notify_url)"
echo
if grep -q 'your_key_here' ../.env 2>/dev/null; then
  echo "Next: set AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY in ../.env, then:"
else
  echo "Next:"
fi
if grep -Eq '^[[:space:]]*enable_msk[[:space:]]*=[[:space:]]*true' terraform.tfvars 2>/dev/null; then
  echo "  cd .. && docker-compose up --build api matcher payment-service notification-service"
else
  echo "  cd .. && docker-compose up --build"
fi
