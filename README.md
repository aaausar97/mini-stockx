# Mini StockX 🔥

A simplified order-matching marketplace built to understand the architecture behind StockX's core systems. Built with FastAPI, Kafka, and AWS SNS/SQS.

## Architecture

```
POST /bid or /ask
      |
  FastAPI (api)
      |
  Kafka Topic: marketplace.events   ← partitioned by product_id
      |
  Matching Engine (engine/matcher.py)
  - max-heap for bids
  - min-heap for asks
  - match fires when top_bid >= top_ask
      |
  AWS SNS Topic: order.matched
      |          |
  SQS Queue  SQS Queue
  (payment)  (notify)
      |          |
  payment-   notification-
  service    service
```

---

## Quick Start

```bash
git clone https://github.com/aaausar97/mini-stockx.git
cd mini-stockx

# 1. Provision AWS (SNS + SQS) and write .env
./infra/deploy.sh

# 2. Add AWS credentials to .env if deploy.sh flagged placeholders
#    AWS_ACCESS_KEY_ID / AWS_SECRET_ACCESS_KEY

# 3. Start everything
docker-compose up --build
```

Re-run `./infra/deploy.sh` after any Terraform changes — it applies updates and refreshes `.env`.

Terraform details: [`infra/README.md`](infra/README.md)

<details>
<summary>Manual AWS setup (console)</summary>

1. **SNS topic** — Standard, name `stockx-order-matched` → `SNS_TOPIC_ARN`
2. **SQS queue** — Standard, name `stockx-payment` → `SQS_PAYMENT_URL`
3. **SQS queue** — Standard, name `stockx-notify` → `SQS_NOTIFY_URL`
4. Subscribe both queues to the SNS topic (protocol: SQS)
5. On each queue, allow `sns.amazonaws.com` to `sqs:SendMessage` with `aws:SourceArn` = your topic ARN

Copy values into `.env` (see `.env.example`).

</details>

You should see 4 containers start:
- `kafka` — message broker (KRaft mode, no ZooKeeper needed)
- `api` — FastAPI on port 8000
- `matcher` — matching engine
- `payment-service` — SQS consumer
- `notification-service` — SQS consumer

---

## Test It

Open a second terminal and fire these curls:

```bash
# Place a bid at $210
curl -X POST http://localhost:8000/bid \
  -H "Content-Type: application/json" \
  -d '{"product_id": "jordan-1-chicago-10", "user_id": "buyer_001", "price": 210}'

# Place an ask at $215 — no match yet (210 < 215)
curl -X POST http://localhost:8000/ask \
  -H "Content-Type: application/json" \
  -d '{"product_id": "jordan-1-chicago-10", "user_id": "seller_001", "price": 215}'

# Place a bid at $215 — MATCH! (215 >= 215)
curl -X POST http://localhost:8000/bid \
  -H "Content-Type: application/json" \
  -d '{"product_id": "jordan-1-chicago-10", "user_id": "buyer_002", "price": 215}'

# Place ask at $220
curl -X POST http://localhost:8000/ask \
  -H "Content-Type: application/json" \
  -d '{"product_id": "jordan-1-chicago-10", "user_id": "seller_002", "price": 220}'

# Buy now
curl -X POST http://localhost:8000/buy-now \
  -H "Content-Type: application/json" \
  -d '{"product_id": "jordan-1-chicago-10", "user_id": "buyer_001"}' 
```

Watch your docker-compose logs — you should see:
1. `matcher` logs: `🎯 MATCH!`
2. `payment-service` logs: `💳 Payment processed`
3. `notification-service` logs: `🔔 Notifications sent`

All three triggered by one event. That's SNS fanout working.

---

## Key Concepts This Project Demonstrates

| Concept | Where |
|---|---|
| Kafka as durable ordered log | `api/main.py` → `engine/matcher.py` |
| Partition key for ordering | `_publish()` uses `product_id` as key |
| Max/min heap order book | `engine/matcher.py` `handle_event()` |
| SNS fanout to multiple SQS | `publish_match()` in matcher |
| SQS long polling | `receive_message(WaitTimeSeconds=20)` |
| Idempotent message deletion | `delete_message()` after success only |
| Consumer group isolation | `group.id: "matching-engine"` |

---

- I partitioned Kafka by product_id so all events for one product are ordered and processed sequentially — eliminating concurrent match conflicts without needing distributed locks
- SNS fans the match event to separate SQS queues so payment and notification services are fully decoupled — either can fail without affecting the other
- Messages not deleted from SQS on failure means automatic retry — the visibility timeout brings them back
- In production I'd move the order book heaps from in-memory to Redis so the matcher can scale horizontally
