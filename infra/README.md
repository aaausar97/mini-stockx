# Infrastructure — SNS + SQS fanout

Terraform for the AWS resources described in the project README:

- SNS topic `stockx-order-matched`
- SQS queues `stockx-payment` and `stockx-notify`
- SNS subscriptions + queue policies so SNS can publish to both queues

Kafka and the app containers stay local (`docker-compose`); only the match-event fanout runs in AWS.

## Prerequisites

- Terraform ≥ 1.5
- AWS CLI configured (`aws sts get-caller-identity`)

## Setup

```bash
cd infra

# optional overrides
cp terraform.tfvars.example terraform.tfvars

terraform init
terraform plan
terraform apply
```

## Wire into the app

```bash
./write-env.sh          # writes ../.env from terraform outputs
# edit ../.env if AWS_ACCESS_KEY_ID / AWS_SECRET_ACCESS_KEY are still placeholders

cd ..
docker-compose up --build
```

Or copy values manually:

```bash
terraform output sns_topic_arn
terraform output sqs_payment_url
terraform output sqs_notify_url
```

## Variables

| Variable | Default | Description |
| --- | --- | --- |
| `aws_region` | `us-east-1` | Region for SNS/SQS |
| `project_name` | `stockx` | Resource name prefix |

## Teardown

```bash
terraform destroy
```
