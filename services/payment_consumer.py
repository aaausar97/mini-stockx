"""
Payment Service — mini-stockx

Polls its own SQS queue for matched orders.
In production this would charge the buyer via Stripe/payment processor.

Why SQS here instead of consuming Kafka directly?
- Each downstream service gets its own queue — fully decoupled
- SQS handles retries automatically if this service crashes
- SNS fans out one match event to ALL downstream SQS queues simultaneously
"""

import json
import os
import time

import boto3

sqs = boto3.client("sqs", region_name=os.getenv("AWS_DEFAULT_REGION", "us-east-1"))
QUEUE_URL = os.getenv("SQS_PAYMENT_URL")


def process_order(order: dict):
    """Simulate charging the buyer."""
    print(f"\n💳 PAYMENT SERVICE")
    print(f"   Charging buyer:  {order['buyer_id']}")
    print(f"   Amount:          ${order['price']:.2f}")
    print(f"   Product:         {order['product_id']}")
    print(f"   Order ID:        {order['order_id']}")
    print(f"   ✅ Payment processed successfully\n")


def run():
    if not QUEUE_URL:
        print("⚠️  SQS_PAYMENT_URL not set. Set it in your .env file.")
        print("   Waiting for config...")
        # Keep running so docker-compose doesn't restart loop
        while True:
            time.sleep(10)

    print(f"💳 Payment service polling: {QUEUE_URL}")

    while True:
        response = sqs.receive_message(
            QueueUrl=QUEUE_URL,
            MaxNumberOfMessages=10,
            WaitTimeSeconds=25,       # long polling — efficient, not hammering AWS
        )

        messages = response.get("Messages", [])

        for msg in messages:
            try:
                # SNS wraps the message body in its own envelope
                body = json.loads(msg["Body"])
                # SNS envelope has a "Message" key with our actual payload
                order = json.loads(body.get("Message", msg["Body"]))

                # shopify/stripe integration here
                process_order(order)

                # Delete from queue — tells SQS we processed it successfully
                # If we DON'T delete it, SQS will redeliver after visibility timeout
                sqs.delete_message(
                    QueueUrl=QUEUE_URL,
                    ReceiptHandle=msg["ReceiptHandle"]
                )

            except Exception as e:
                print(f"❌ Error processing message: {e}")
                # Don't delete — SQS will retry after visibility timeout
                # This is your automatic retry mechanism


if __name__ == "__main__":
    run()
