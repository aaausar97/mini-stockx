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

## Using AWS MSK instead of local Kafka

By default Kafka runs locally in docker-compose. You can swap it for Amazon MSK
(Managed Streaming for Kafka) so the broker lives in AWS too.

**Cost warning:** local Kafka is free; MSK is not. The smallest provisioned
cluster (2× `kafka.t3.small`) runs ~$70/month plus storage. There's no
free tier. Tear it down when you're not using it.

**Why provisioned and not MSK Serverless:** Serverless only allows IAM auth and
is only reachable from inside its VPC — your laptop's docker containers can't
connect. Provisioned MSK supports **public access with SASL/SCRAM**, which works
from anywhere with a username/password.

### 1. Create the cluster

AWS Console → MSK → Create cluster:

- Type: **Provisioned**, 2 brokers, `kafka.t3.small`, default VPC
- Access control: enable **SASL/SCRAM**, disable unauthenticated access
- Wait for it to reach Active (~20 min)

### 2. Create the SCRAM credentials

- Secrets Manager → Create secret → type "Other"
- Value: `{"username": "stockx", "password": "<strong password>"}`
- Name must start with `AmazonMSK_` (e.g. `AmazonMSK_stockx`)
- Must be encrypted with a **customer-managed KMS key** (default AWS key won't work)
- MSK → your cluster → Properties → Associate the secret

### 3. Enable public access

Only possible after the cluster is Active:

- MSK → cluster → Properties → Networking → Edit public access → Turn on

### 4. Get the bootstrap string

```bash
aws kafka get-bootstrap-brokers --cluster-arn <CLUSTER_ARN> \
  --query 'BootstrapBrokerStringPublicSaslScram' --output text
```

### 5. Create the topic

MSK doesn't auto-create topics by default. Create it once (any machine with
Kafka CLI tools and the SCRAM creds), or add a cluster configuration with
`auto.create.topics.enable=true`:

```bash
kafka-topics.sh --create --topic marketplace.events \
  --bootstrap-server <PUBLIC_BOOTSTRAP> \
  --command-config client.properties   # SASL_SSL + SCRAM creds
```

### 6. Point the app at MSK

The producer/consumer configs in `api/main.py` and `engine/matcher.py` currently
only set `bootstrap.servers`. SASL/SCRAM needs three more fields:

```python
{
    "bootstrap.servers": os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"),
    "security.protocol": "SASL_SSL",
    "sasl.mechanisms": "SCRAM-SHA-512",
    "sasl.username": os.getenv("KAFKA_USERNAME"),
    "sasl.password": os.getenv("KAFKA_PASSWORD"),
}
```

Then in `.env`:

```bash
KAFKA_BOOTSTRAP_SERVERS=<public bootstrap string from step 4>
KAFKA_USERNAME=stockx
KAFKA_PASSWORD=<password from step 2>
```

And in `docker-compose.yml`: delete the `kafka` service, every
`depends_on: kafka` block, and the `KAFKA_BOOTSTRAP_SERVERS: kafka:29092`
overrides so the value comes from `.env`.

### Teardown

MSK → Delete cluster (billing stops), then delete the `AmazonMSK_stockx` secret
and the KMS key.

## Teardown

```bash
cd infra
terraform destroy
```

## Not included

- Remote Terraform state (S3 backend + lock table)
- IAM user/role creation (use your existing AWS credentials)
- Terraform for MSK (documented above as manual steps — add if you settle on MSK long-term)
