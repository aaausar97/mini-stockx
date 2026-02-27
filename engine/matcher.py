"""
Matching Engine — mini-stockx

This is the heart of the system.
Consumes every bid/ask from Kafka, maintains order books (heaps) per product,
and fires a match event to AWS SNS whenever a bid meets an ask.

Order book structure per product:
  bids  = max-heap  (we want highest bid on top)
  asks  = min-heap  (we want lowest ask on top)

A match fires when: top of bids >= top of asks
"""

import heapq
import json
import os
from collections import defaultdict

import boto3
from confluent_kafka import Consumer, KafkaError

from shared.models import MarketEvent, MatchedOrder


# ── AWS SNS setup ─────────────────────────────────────────────────────────────

sns = boto3.client("sns", region_name=os.getenv("AWS_DEFAULT_REGION", "us-east-1"))
SNS_TOPIC_ARN = os.getenv("SNS_TOPIC_ARN")


# ── Order Book (in-memory, per product) ──────────────────────────────────────
#
# In production this would live in Redis so multiple matcher instances
# can share state. For this project, one matcher instance owns it all.
#
# bids[product_id]  = max-heap stored as negated prices (Python only has min-heap)
# asks[product_id]  = min-heap
#
# Each heap entry: (price, event) — heapq compares by first element

bids: dict[str, list] = defaultdict(list)   # max-heap (negated)
asks: dict[str, list] = defaultdict(list)   # min-heap


def handle_event(event: MarketEvent):
    """Route incoming event to the right heap, then check for a match."""
    product = event.product_id

    if event.event_type == "bid":
        heapq.heappush(bids[product], (-event.price, event))
        print(f"📥 BID  ${event.price:>8.2f}  {event.product_id}  [{event.user_id}]")
        check_for_match(product)

    elif event.event_type == "ask":
        heapq.heappush(asks[product], (event.price, event))
        print(f"📤 ASK  ${event.price:>8.2f}  {event.product_id}  [{event.user_id}]")
        check_for_match(product)

    elif event.event_type == "buy_now":
        handle_buy_now(event)

    elif event.event_type == "sell_now":
        handle_sell_now(event)


def handle_buy_now(event: MarketEvent):
    """Buy Now: buyer accepts the current lowest ask price and executes immediately."""
    product = event.product_id

    if not asks[product]:
        print(f"⚠️  BUY NOW failed — no asks available for {product}  [{event.user_id}]")
        return

    ask_price, ask_event = heapq.heappop(asks[product])
    event.price = ask_price

    order = MatchedOrder.from_match(bid=event, ask=ask_event)
    print(f"\n⚡ BUY NOW! {order.product_id} @ ${order.price:.2f}")
    print(f"   buyer:  {order.buyer_id}")
    print(f"   seller: {order.seller_id}")
    print(f"   order:  {order.order_id}\n")

    publish_match(order)


def handle_sell_now(event: MarketEvent):
    """Sell Now: seller accepts the current highest bid price and executes immediately."""
    product = event.product_id

    if not bids[product]:
        print(f"⚠️  SELL NOW failed — no bids available for {product}  [{event.user_id}]")
        return

    neg_bid_price, bid_event = heapq.heappop(bids[product])
    event.price = -neg_bid_price

    order = MatchedOrder.from_match(bid=bid_event, ask=event)
    print(f"\n⚡ SELL NOW! {order.product_id} @ ${order.price:.2f}")
    print(f"   buyer:  {order.buyer_id}")
    print(f"   seller: {order.seller_id}")
    print(f"   order:  {order.order_id}\n")

    publish_match(order)


def check_for_match(product_id: str):
    """
    After every event, peek at the top of each heap.
    If highest bid >= lowest ask → match!
    """
    b = bids[product_id]
    a = asks[product_id]

    if not b or not a:
        return  # can't match with an empty side

    top_bid_price = -b[0][0]   # un-negate
    top_ask_price =  a[0][0]

    if top_bid_price >= top_ask_price:
        # Pop both off the heap — they're consumed
        _, bid_event = heapq.heappop(b)
        _, ask_event = heapq.heappop(a)

        order = MatchedOrder.from_match(bid=bid_event, ask=ask_event)
        print(f"\n🎯 MATCH! {order.product_id} @ ${order.price:.2f}")
        print(f"   buyer:  {order.buyer_id}")
        print(f"   seller: {order.seller_id}")
        print(f"   order:  {order.order_id}\n")

        publish_match(order)


def publish_match(order: MatchedOrder):
    """
    Publish matched order to AWS SNS.
    SNS fans this out to SQS queues for payment + notification services.
    Each downstream service gets its own SQS queue — they consume independently.
    """
    if not SNS_TOPIC_ARN:
        print("⚠️  SNS_TOPIC_ARN not set — printing match locally only")
        print(f"   Would publish: {json.dumps(order.to_dict(), indent=2)}")
        return

    response = sns.publish(
        TopicArn=SNS_TOPIC_ARN,
        Message=json.dumps(order.to_dict()),
        Subject="order.matched",
        MessageAttributes={
            "event_type": {
                "DataType": "String",
                "StringValue": "order.matched"
            }
        }
    )
    print(f"📨 Published to SNS: MessageId={response['MessageId']}")


# ── Kafka Consumer ────────────────────────────────────────────────────────────

def run():
    consumer = Consumer({
        "bootstrap.servers": os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"),
        "group.id": "matching-engine",        # consumer group
        "auto.offset.reset": "earliest",      # on first start, read from beginning
        "enable.auto.commit": True,
    })

    consumer.subscribe(["marketplace.events"])
    print("🔄 Matching engine running — waiting for events...")

    try:
        while True:
            msg = consumer.poll(timeout=1.0)

            if msg is None:
                continue

            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue  # end of partition, not an error
                print(f"❌ Kafka error: {msg.error()}")
                continue

            # Deserialize and handle the event
            data = json.loads(msg.value().decode("utf-8"))
            event = MarketEvent(**data)
            handle_event(event)

    except KeyboardInterrupt:
        print("\n🛑 Matcher shutting down")
    finally:
        consumer.close()


if __name__ == "__main__":
    run()
