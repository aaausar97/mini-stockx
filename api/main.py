"""
API Service — mini-stockx
Accepts bids and asks via REST, publishes them to Kafka.

Why Kafka here instead of calling the matcher directly?
- Decoupling: the API doesn't care if the matcher is slow or down
- Durability: if the matcher crashes, events wait in Kafka, nothing is lost
- Scale: we can run multiple API instances all writing to the same topic
"""

import json
import os
from contextlib import asynccontextmanager

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, Field
from confluent_kafka import Producer

from shared.models import MarketEvent


# ── Kafka Producer setup ──────────────────────────────────────────────────────

producer: Producer = None

def get_producer() -> Producer:
    return Producer({
        "bootstrap.servers": os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"),
        "client.id": "stockx-api",
    })

def delivery_report(err, msg):
    """Called by Kafka after each message is delivered (or fails)."""
    if err:
        print(f"❌ Delivery failed: {err}")
    else:
        print(f"✅ Delivered to partition [{msg.partition()}] offset {msg.offset()}")


@asynccontextmanager
async def lifespan(app: FastAPI):
    global producer
    producer = get_producer()
    print("🚀 Kafka producer connected")
    yield
    producer.flush()
    print("🛑 Kafka producer flushed and closed")


# ── FastAPI App ───────────────────────────────────────────────────────────────

app = FastAPI(
    title="Mini StockX API",
    description="Place bids and asks. Matching happens async via Kafka.",
    lifespan=lifespan
)


# ── Request Models ────────────────────────────────────────────────────────────

class BidRequest(BaseModel):
    product_id: str = Field(..., example="jordan-1-chicago-10")
    user_id: str    = Field(..., example="buyer_001")
    price: float    = Field(..., gt=0, example=215.00)

class AskRequest(BaseModel):
    product_id: str = Field(..., example="jordan-1-chicago-10")
    user_id: str    = Field(..., example="seller_001")
    price: float    = Field(..., gt=0, example=210.00)

class InstantOrderRequest(BaseModel):
    product_id: str = Field(..., example="jordan-1-chicago-10")
    user_id: str    = Field(..., example="buyer_001")


# ── Routes ────────────────────────────────────────────────────────────────────

@app.get("/")
def root():
    return {"status": "mini-stockx running 🔥"}


@app.get("/health")
def health():
    return {"status": "ok"}


@app.post("/bid", status_code=202)
def place_bid(req: BidRequest):
    """
    Place a bid (buyer sets max price they'll pay).
    202 Accepted = we got it, matching happens async.
    """
    event = MarketEvent.new_bid(
        product_id=req.product_id,
        user_id=req.user_id,
        price=req.price
    )
    _publish(event)
    return {
        "message": "Bid accepted",
        "event_id": event.event_id,
        "status": "pending_match"
    }


@app.post("/ask", status_code=202)
def place_ask(req: AskRequest):
    """
    Place an ask (seller sets minimum price they'll accept).
    202 Accepted = we got it, matching happens async.
    """
    event = MarketEvent.new_ask(
        product_id=req.product_id,
        user_id=req.user_id,
        price=req.price
    )
    _publish(event)
    return {
        "message": "Ask accepted",
        "event_id": event.event_id,
        "status": "pending_match"
    }


@app.post("/buy-now", status_code=202)
def buy_now(req: InstantOrderRequest):
    """
    Buy Now — instantly purchase at the current lowest ask price.
    The matcher will pop the best ask from the order book and execute.
    If no asks exist, the matcher drops the event (no match possible).
    """
    event = MarketEvent.new_buy_now(
        product_id=req.product_id,
        user_id=req.user_id
    )
    _publish(event)
    return {
        "message": "Buy Now accepted — matching at lowest ask",
        "event_id": event.event_id,
        "status": "pending_execution"
    }


@app.post("/sell-now", status_code=202)
def sell_now(req: InstantOrderRequest):
    """
    Sell Now — instantly sell at the current highest bid price.
    The matcher will pop the best bid from the order book and execute.
    If no bids exist, the matcher drops the event (no match possible).
    """
    event = MarketEvent.new_sell_now(
        product_id=req.product_id,
        user_id=req.user_id
    )
    _publish(event)
    return {
        "message": "Sell Now accepted — matching at highest bid",
        "event_id": event.event_id,
        "status": "pending_execution"
    }


@app.get("/orderbook/{product_id}")
def get_orderbook(product_id: str):
    """
    Placeholder — in a real system this would read from Redis
    where the matching engine maintains the live order book state.
    """
    return {
        "product_id": product_id,
        "note": "In production this reads live heaps from Redis"
    }


# ── Internal ──────────────────────────────────────────────────────────────────

def _publish(event: MarketEvent):
    """
    Publish a market event to Kafka.

    KEY DETAIL: we use product_id as the Kafka message key.
    Kafka guarantees all messages with the same key go to the same partition.
    This means all bids/asks for "jordan-1-chicago-10" are ordered and
    processed sequentially — no concurrency conflicts in the matcher.
    """
    try:
        producer.produce(
            topic="marketplace.events",
            key=event.product_id,           # <-- partition key, critical
            value=json.dumps(event.to_dict()),
            callback=delivery_report
        )
        producer.poll(0)  # trigger delivery callbacks without blocking
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to publish event: {e}")
