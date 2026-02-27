from dataclasses import dataclass, asdict
from typing import Literal
import time
import uuid


@dataclass
class MarketEvent:
    """
    A single bid or ask entering the marketplace.
    This is what gets published to Kafka topic: marketplace.events
    Partitioned by product_id so all events for one product
    go to the same partition — guaranteeing order.
    """
    event_id: str
    event_type: Literal["bid", "ask", "buy_now", "sell_now"]
    product_id: str       # e.g. "jordan-1-chicago-10"
    user_id: str
    price: float
    timestamp: float

    @classmethod
    def new_bid(cls, product_id: str, user_id: str, price: float):
        return cls(
            event_id=str(uuid.uuid4()),
            event_type="bid",
            product_id=product_id,
            user_id=user_id,
            price=price,
            timestamp=time.time()
        )

    @classmethod
    def new_ask(cls, product_id: str, user_id: str, price: float):
        return cls(
            event_id=str(uuid.uuid4()),
            event_type="ask",
            product_id=product_id,
            user_id=user_id,
            price=price,
            timestamp=time.time()
        )

    @classmethod
    def new_buy_now(cls, product_id: str, user_id: str):
        """Buyer accepts the current lowest ask — price is 0 as a sentinel,
        the matcher fills in the real price from the order book."""
        return cls(
            event_id=str(uuid.uuid4()),
            event_type="buy_now",
            product_id=product_id,
            user_id=user_id,
            price=0.0,
            timestamp=time.time()
        )

    @classmethod
    def new_sell_now(cls, product_id: str, user_id: str):
        """Seller accepts the current highest bid"""
        return cls(
            event_id=str(uuid.uuid4()),
            event_type="sell_now",
            product_id=product_id,
            user_id=user_id,
            price=0.0,
            timestamp=time.time()
        )

    def to_dict(self):
        return asdict(self)


@dataclass
class MatchedOrder:
    """
    Produced by the matching engine when a bid meets an ask.
    This gets published to SNS topic: order.matched
    SNS fans it out to SQS queues for payment + notification services.
    """
    order_id: str
    product_id: str
    buyer_id: str
    seller_id: str
    price: float           # the matched price (ask price wins)
    bid_event_id: str
    ask_event_id: str
    matched_at: float

    @classmethod
    def from_match(cls, bid: MarketEvent, ask: MarketEvent):
        return cls(
            order_id=str(uuid.uuid4()),
            product_id=bid.product_id,
            buyer_id=bid.user_id,
            seller_id=ask.user_id,
            price=ask.price,           # seller's ask price is the match price
            bid_event_id=bid.event_id,
            ask_event_id=ask.event_id,
            matched_at=time.time()
        )

    def to_dict(self):
        return asdict(self)
