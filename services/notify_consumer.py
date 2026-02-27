"""
Notification Service

Polls its OWN SQS queue (separate from payment service).
Both payment and notification get the same match event via SNS fanout —
but each processes it independently, at their own pace.

"""

import json
import os
import time

import boto3

sqs = boto3.client("sqs", region_name=os.getenv("AWS_DEFAULT_REGION", "us-east-1"))
QUEUE_URL = os.getenv("SQS_NOTIFY_URL")


def process_notification(order: dict):
    """Simulate sending push notifications to buyer and seller."""
    print(f"\n🔔 NOTIFICATION SERVICE")
    print(f"   → Buyer  [{order['buyer_id']}]: You got em! {order['product_id']} @ ${order['price']:.2f}")
    print(f"   → Seller [{order['seller_id']}]: Your item sold! ${order['price']:.2f} incoming.")
    print(f"   ✅ Notifications sent\n")


def run():
    if not QUEUE_URL:
        print("⚠️  SQS_NOTIFY_URL not set. Set it in your .env file.")
        while True:
            time.sleep(10)

    print(f"🔔 Notification service polling: {QUEUE_URL}")

    while True:
        response = sqs.receive_message(
            QueueUrl=QUEUE_URL,
            MaxNumberOfMessages=10,
            WaitTimeSeconds=20,
        )

        messages = response.get("Messages", [])

        for msg in messages:
            try:
                body = json.loads(msg["Body"])
                order = json.loads(body.get("Message", msg["Body"]))

                # firebase/push notification integration here
                process_notification(order)

                sqs.delete_message(
                    QueueUrl=QUEUE_URL,
                    ReceiptHandle=msg["ReceiptHandle"]
                )

            except Exception as e:
                print(f"❌ Error processing notification: {e}")


if __name__ == "__main__":
    run()
