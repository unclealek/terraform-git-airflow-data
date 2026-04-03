import argparse
import json
from datetime import datetime, timedelta, timezone
from pathlib import Path
from random import choice, randint, random, uniform
from uuid import uuid4

from faker import Faker


fake = Faker()


def _iso_timestamp(base_time: datetime | None = None, minutes_offset: int = 0) -> str:
    moment = (base_time or datetime.now(timezone.utc)) + timedelta(minutes=minutes_offset)
    return moment.replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _event_id(prefix: str) -> str:
    return f"{prefix}_{uuid4().hex[:10]}"


def _user_id() -> str:
    return f"user_{fake.uuid4()[:8]}"


def _session_id() -> str:
    return f"sess_{fake.uuid4()[:8]}"


def _product_id() -> str:
    return f"prod_{randint(10, 999)}"


def _base_event(event_type: str, source_file: str, event_time: datetime) -> dict:
    return {
        "event_id": _event_id(event_type),
        "event_type": event_type,
        "event_timestamp": _iso_timestamp(event_time),
        "user_id": _user_id(),
        "session_id": _session_id(),
        "product_id": None,
        "page_url": None,
        "quantity": None,
        "price": None,
        "currency": None,
        "source_file": source_file,
        "ingested_at": _iso_timestamp(event_time, 1),
    }


def _apply_common_dirty_rules(events: list[dict], dirty_rate: float) -> list[dict]:
    if dirty_rate <= 0:
        return events

    dirty_events = []
    for event in events:
        if random() < dirty_rate:
            corruption = choice(
                [
                    "null_event_id",
                    "bad_timestamp",
                    "blank_page_url",
                    "duplicate_row",
                ]
            )

            if corruption == "null_event_id":
                event["event_id"] = None
            elif corruption == "bad_timestamp":
                event["event_timestamp"] = choice(
                    ["invalid_timestamp", "03/99/2026 08:20:00", "not_a_timestamp"]
                )
            elif corruption == "blank_page_url" and "page_url" in event:
                event["page_url"] = ""
            elif corruption == "duplicate_row":
                dirty_events.append(dict(event))

        dirty_events.append(event)

    return dirty_events


def generate_page_views(count: int, source_file: str = "page_views.json", dirty_rate: float = 0.0) -> list[dict]:
    events = []
    for _ in range(count):
        event_time = fake.date_time_between(start_date="-30d", end_date="now", tzinfo=timezone.utc)
        event = _base_event("page_view", source_file, event_time)
        event["page_url"] = choice(
            [
                "/",
                "/home",
                "/products",
                f"/products/{_product_id()}",
                "/checkout",
            ]
        )
        event["event_properties"] = {
            "browser": fake.chrome(),
            "device": choice(["mobile", "desktop", "tablet"]),
        }
        events.append(event)
    return _apply_common_dirty_rules(events, dirty_rate)


def generate_product_clicks(count: int, source_file: str = "product_clicks.json", dirty_rate: float = 0.0) -> list[dict]:
    events = []
    for _ in range(count):
        event_time = fake.date_time_between(start_date="-30d", end_date="now", tzinfo=timezone.utc)
        event = _base_event("product_click", source_file, event_time)
        event["product_id"] = _product_id()
        event["event_properties"] = {
            "referrer": choice(["homepage", "search", "email_campaign", "category_page"]),
        }
        events.append(event)
    return _apply_common_dirty_rules(events, dirty_rate)


def generate_add_to_cart(count: int, source_file: str = "add_to_cart.json", dirty_rate: float = 0.0) -> list[dict]:
    events = []
    for _ in range(count):
        event_time = fake.date_time_between(start_date="-30d", end_date="now", tzinfo=timezone.utc)
        event = _base_event("add_to_cart", source_file, event_time)
        quantity = randint(1, 5)
        cart_value = round(uniform(10, 300), 2)
        event["product_id"] = _product_id()
        event["quantity"] = str(quantity)
        event["event_properties"] = {
            "cart_value": f"{cart_value:.2f}",
        }
        events.append(event)
    dirty_events = _apply_common_dirty_rules(events, dirty_rate)
    for event in dirty_events:
        if random() < dirty_rate:
            corruption = choice(["negative_quantity", "bad_quantity"])
            if corruption == "negative_quantity":
                event["quantity"] = str(-randint(1, 3))
            else:
                event["quantity"] = "abc"
    return dirty_events


def generate_purchases(count: int, source_file: str = "purchases.json", dirty_rate: float = 0.0) -> list[dict]:
    events = []
    for _ in range(count):
        event_time = fake.date_time_between(start_date="-30d", end_date="now", tzinfo=timezone.utc)
        quantity = randint(1, 4)
        price = round(uniform(9.99, 199.99), 2)
        event = _base_event("purchase", source_file, event_time)
        event["product_id"] = _product_id()
        event["quantity"] = str(quantity)
        event["price"] = f"{price:.2f}"
        event["currency"] = choice(["USD", "EUR", "GBP"])
        event["event_properties"] = {
            "payment_method": choice(["card", "paypal", "apple_pay"]),
        }
        events.append(event)
    dirty_events = _apply_common_dirty_rules(events, dirty_rate)
    for event in dirty_events:
        if random() < dirty_rate:
            corruption = choice(["bad_price", "bad_currency"])
            if corruption == "bad_price":
                event["price"] = choice(["not_a_price", "", None])
            else:
                event["currency"] = choice(["EURO", "", None])
    return dirty_events


def generate_user_sessions(count: int, source_file: str = "user_sessions.json", dirty_rate: float = 0.0) -> list[dict]:
    events = []
    for _ in range(count):
        session_start = fake.date_time_between(start_date="-30d", end_date="now", tzinfo=timezone.utc)
        duration_seconds = randint(60, 5400)
        session_end = session_start + timedelta(seconds=duration_seconds)
        event = _base_event("user_session", source_file, session_start)
        event["event_properties"] = {
            "session_start": _iso_timestamp(session_start),
            "session_end": _iso_timestamp(session_end),
            "duration_seconds": str(duration_seconds),
        }
        events.append(event)
    dirty_events = _apply_common_dirty_rules(events, dirty_rate)
    for event in dirty_events:
        if random() < dirty_rate:
            event["event_properties"]["session_end"] = None
    return dirty_events


def _write_events(output_dir: Path, folder: str, filename: str, events: list[dict]) -> None:
    path = output_dir / folder / filename
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(events, indent=2), encoding="utf-8")
    print(f"[INFO] Wrote {len(events)} records to {path}")


def main() -> None:
    parser = argparse.ArgumentParser(description="Generate fake e-commerce raw event data.")
    parser.add_argument("--count", type=int, default=25, help="Number of rows to generate per event type.")
    parser.add_argument(
        "--dirty-rate",
        type=float,
        default=0.15,
        help="Fraction of rows to corrupt with dirty data patterns. Range: 0.0 to 1.0.",
    )
    parser.add_argument(
        "--output-dir",
        default="data/raw",
        help="Base directory for generated raw event files.",
    )
    args = parser.parse_args()

    output_dir = Path(args.output_dir)

    _write_events(
        output_dir,
        "page_views",
        "page_views.json",
        generate_page_views(args.count, dirty_rate=args.dirty_rate),
    )
    _write_events(
        output_dir,
        "product_clicks",
        "product_clicks.json",
        generate_product_clicks(args.count, dirty_rate=args.dirty_rate),
    )
    _write_events(
        output_dir,
        "add_to_cart",
        "add_to_cart.json",
        generate_add_to_cart(args.count, dirty_rate=args.dirty_rate),
    )
    _write_events(
        output_dir,
        "purchases",
        "purchases.json",
        generate_purchases(args.count, dirty_rate=args.dirty_rate),
    )
    _write_events(
        output_dir,
        "user_sessions",
        "user_sessions.json",
        generate_user_sessions(args.count, dirty_rate=args.dirty_rate),
    )


if __name__ == "__main__":
    main()
