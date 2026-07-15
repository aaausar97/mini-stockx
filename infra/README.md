# Infrastructure — SNS + SQS fanout (+ optional MSK)

Terraform for the AWS resources the app needs:

- SNS topic `stockx-order-matched`
- SQS queues `stockx-payment` and `stockx-notify`
- SNS subscriptions + queue policies
- **Optional:** Amazon MSK with SASL/SCRAM, public access, and auto topic creation

Kafka defaults to local docker-compose. Set `enable_msk = true` to run Kafka in AWS instead.

## Architecture

### Default (local Kafka)

```
api/matcher (docker-compose) → local Kafka → matcher → SNS → SQS → services
```

### With MSK (`enable_msk = true`)

```
api/matcher (docker-compose) → AWS MSK (public SASL/SCRAM) → matcher → SNS → SQS → services
```

## Files

| File | Purpose |
| --- | --- |
| `deploy.sh` | One command: init, apply, refresh `.env` |
| `write-env.sh` | Writes `../.env` from terraform outputs |
| `main.tf` | SNS topic, SQS queues, subscriptions, policies |
| `msk.tf` | Optional MSK cluster, SCRAM secret, public access |
| `versions.tf` | Terraform + provider pins |
| `variables.tf` | Region, project name, MSK toggles |
| `outputs.tf` | ARNs/URLs and Kafka connection values |
| `terraform.tfvars.example` | Example overrides (copy to `terraform.tfvars`) |
| `.terraform.lock.hcl` | Provider lockfile — commit this |

**Not committed** (see root `.gitignore`): `.terraform/`, `*.tfstate`, `terraform.tfvars`.

## How the Terraform files fit together

All `.tf` files are one module — filenames are just organization.

```
terraform.tfvars → variables.tf → main.tf / msk.tf → outputs.tf → write-env.sh → ../.env
                      ↑
                 versions.tf (providers)
```

**`main.tf`** (always on): SNS topic → two SQS queues → queue policies (allow SNS to write) → subscriptions.

**`msk.tf`** (only if `enable_msk = true`): VPC/subnets lookup → KMS + SCRAM secret → security group + MSK config → cluster → secret association → bootstrap brokers output. Independent of `main.tf` — shares only `project_name` and `aws_region`.

**`outputs.tf`** exports resource ARNs/URLs (and Kafka creds when MSK is on). `write-env.sh` reads these into `.env`.

## Prerequisites

1. **Terraform** ≥ 1.5
2. **AWS CLI** configured (`aws sts get-caller-identity`)
3. **IAM permissions** for SNS, SQS, and (if MSK) Kafka, Secrets Manager, KMS, EC2/VPC

## Deploy (new or update)

```bash
./infra/deploy.sh
```

What it does:

1. Verifies `terraform` and `aws`
2. Seeds `terraform.tfvars` on first run
3. `terraform init` + `terraform apply -auto-approve`
4. Runs `write-env.sh` → `../.env`

Re-run after any Terraform change.

## Local Kafka (default)

```bash
./infra/deploy.sh
docker-compose up --build
```

Uses the `kafka` container in docker-compose. No MSK cost.

## AWS MSK (Terraform, no console)

**Cost:** ~$70/month for 2× `kafka.t3.small`. No free tier. Set `enable_msk = false` (or destroy) when done.

### 1. Enable MSK in tfvars

```bash
cd infra
cp terraform.tfvars.example terraform.tfvars
```

Edit `terraform.tfvars`:

```hcl
enable_msk = true
```

### 2. Deploy

```bash
./deploy.sh
```

First apply takes **~20 minutes**. Terraform creates:

| Resource | What |
| --- | --- |
| `aws_msk_cluster` | 2 brokers, `kafka.t3.small`, default VPC |
| `aws_msk_configuration` | `auto.create.topics.enable=true` (no manual topic step) |
| `aws_kms_key` + `aws_secretsmanager_secret` | SCRAM creds (`AmazonMSK_<project>`) |
| `aws_msk_scram_secret_association` | Wires secret to cluster |
| Public access | `SERVICE_PROVIDED_EIPS` — reachable from your laptop |
| Security group | Port 9196 open for SASL/SCRAM clients |

`write-env.sh` adds `KAFKA_BOOTSTRAP_SERVERS`, `KAFKA_USERNAME`, and `KAFKA_PASSWORD` to `.env`.

### 3. Run app without local Kafka

```bash
cd ..
docker-compose up --build api matcher payment-service notification-service
```

Omit the `kafka` service — app containers connect to MSK via `.env`.

The app auto-enables SASL/SCRAM when `KAFKA_USERNAME` and `KAFKA_PASSWORD` are set (`shared/kafka_config.py`).

### If bootstrap string is empty

Public bootstrap brokers can lag cluster creation. Wait a few minutes, then:

```bash
cd infra && terraform apply && ./write-env.sh
```

## Variables

| Variable | Default | Description |
| --- | --- | --- |
| `aws_region` | `us-east-1` | Region for all resources |
| `project_name` | `stockx` | Resource name prefix |
| `enable_msk` | `false` | Provision MSK instead of local Kafka |
| `kafka_version` | `3.6.0` | MSK Kafka version |
| `kafka_username` | `stockx` | SCRAM username |
| `msk_broker_count` | `2` | Broker nodes |
| `msk_instance_type` | `kafka.t3.small` | Broker instance type |

## Outputs

| Output | `.env` key |
| --- | --- |
| `sns_topic_arn` | `SNS_TOPIC_ARN` |
| `sqs_payment_url` | `SQS_PAYMENT_URL` |
| `sqs_notify_url` | `SQS_NOTIFY_URL` |
| `kafka_bootstrap_servers` | `KAFKA_BOOTSTRAP_SERVERS` (MSK only) |
| `kafka_username` | `KAFKA_USERNAME` (MSK only) |
| `kafka_password` | `KAFKA_PASSWORD` (MSK only, sensitive) |

## Teardown

```bash
cd infra
terraform destroy
```

Destroys SNS/SQS and MSK (if enabled), SCRAM secret, and KMS key.

## Not included

- Remote Terraform state (S3 backend + lock table)
- IAM user/role creation (use your existing AWS credentials)
- Running api/matcher in AWS (still local docker-compose)
