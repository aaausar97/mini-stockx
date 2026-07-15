# Infrastructure — SNS + SQS fanout

Terraform for the AWS resources the app needs after a match:

- SNS topic `stockx-order-matched`
- SQS queues `stockx-payment` and `stockx-notify`
- SNS subscriptions + queue policies so SNS can publish to both queues

Kafka and the app containers stay local (`docker-compose`). Only the match-event fanout runs in AWS.

## Architecture

```
matcher (local)
      |
  SNS topic: stockx-order-matched
      |          |
  SQS queue   SQS queue
  payment     notify
      |          |
  payment-   notification-
  service    service (local docker-compose)
```

## Files

| File | Purpose |
| --- | --- |
| `deploy.sh` | One command: init, apply, refresh `.env` |
| `write-env.sh` | Writes `../.env` from terraform outputs |
| `versions.tf` | Terraform + AWS provider pins |
| `variables.tf` | `aws_region`, `project_name` |
| `main.tf` | SNS topic, SQS queues, subscriptions, policies |
| `outputs.tf` | ARNs/URLs for `.env` |
| `terraform.tfvars.example` | Example overrides (copy to `terraform.tfvars`) |
| `.terraform.lock.hcl` | Provider lockfile — commit this |

**Not committed** (see root `.gitignore`): `.terraform/`, `*.tfstate`, `terraform.tfvars`.

## Prerequisites

1. **Terraform** ≥ 1.5
   ```bash
   brew tap hashicorp/tap && brew install hashicorp/tap/terraform
   ```

2. **AWS CLI** with credentials configured
   ```bash
   aws configure
   aws sts get-caller-identity
   ```

3. **IAM permissions** — needs SNS and SQS create/read/update/delete in your account.

## Deploy (new or update)

From the repo root:

```bash
./infra/deploy.sh
```

Or from this directory:

```bash
./deploy.sh
```

What it does:

1. Verifies `terraform` and `aws` are installed
2. Verifies AWS credentials (`aws sts get-caller-identity`)
3. Copies `terraform.tfvars.example` → `terraform.tfvars` on first run
4. `terraform init` + `terraform apply -auto-approve`
5. Runs `write-env.sh` to write/update `../.env`

Re-run `./deploy.sh` any time you change Terraform or need to refresh outputs into `.env`.

## First-time app setup

```bash
./infra/deploy.sh

# if deploy printed a creds reminder, edit .env:
#   AWS_ACCESS_KEY_ID
#   AWS_SECRET_ACCESS_KEY

docker-compose up --build
```

## Variables

| Variable | Default | Description |
| --- | --- | --- |
| `aws_region` | `us-east-1` | Region for SNS/SQS |
| `project_name` | `stockx` | Resource name prefix (`stockx-order-matched`, etc.) |

Override via `terraform.tfvars`:

```bash
cp terraform.tfvars.example terraform.tfvars
# edit terraform.tfvars
./deploy.sh
```

## Outputs

| Output | `.env` key |
| --- | --- |
| `sns_topic_arn` | `SNS_TOPIC_ARN` |
| `sqs_payment_url` | `SQS_PAYMENT_URL` |
| `sqs_notify_url` | `SQS_NOTIFY_URL` |
| `aws_region` | `AWS_DEFAULT_REGION` |

Manual copy:

```bash
terraform output sns_topic_arn
terraform output sqs_payment_url
terraform output sqs_notify_url
```

## Teardown

```bash
cd infra
terraform destroy
```

## Not included

- Remote Terraform state (S3 backend + lock table)
- IAM user/role creation (use your existing AWS credentials)
- Kafka or app services in AWS
