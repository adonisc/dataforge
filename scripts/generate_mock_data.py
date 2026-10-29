#!/usr/bin/env python3
"""Generate SYNTHETIC sample data for the local demo.

All data is synthetic, generated from a fixed seed. No real people or organisations.

It also injects defects on purpose, so the quality and dedupe logic has work to do:

  * exact redelivery (byte-identical duplicate)       -> dropped silently by hash dedupe
  * same key, two versions                            -> collapsed by last-write-wins
  * unknown enum value                                -> quarantined
  * negative amount                                   -> quarantined
  * null required field                               -> blocking on orders
  * malformed currency code                           -> quarantined
  * late-arriving record (dated 2 days earlier)       -> accepted, exercises the window
  * CDC delete                                        -> tombstone, closes SCD2 version

Usage:
    python scripts/generate_mock_data.py --date 2026-10-24 --out data/landing
"""

from __future__ import annotations

import argparse
import json
import random
from datetime import UTC, date, datetime, timedelta
from decimal import Decimal
from pathlib import Path

SEED = 20261024

# Customer id ranges. Only active customers place orders and only churned ones get
# deleted, so `assert_no_revenue_from_deleted_customers` doesn't fire on demo data.
ACTIVE_CUSTOMERS = (1, 350)
CHURNED_CUSTOMERS = (351, 400)
CURRENCIES = ["USD", "EUR", "GBP", "MXN", "GTQ", "BRL"]
CHANNELS = ["web", "ios", "android", "marketplace", "retail"]
STATUSES = ["placed", "paid", "shipped", "delivered", "cancelled"]
SEGMENTS = ["consumer", "smb", "enterprise"]
COUNTRIES = ["US", "GB", "DE", "MX", "GT", "BR"]
EVENT_NAMES = ["page_view", "add_to_cart", "checkout_start", "purchase", "search"]


def _write(path: Path, rows: list[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as handle:
        for row in rows:
            handle.write(json.dumps(row, default=str) + "\n")
    print(f"  {len(rows):>6} rows -> {path}")


def generate_orders(rng: random.Random, run_date: date, n: int) -> list[dict]:
    rows: list[dict] = []
    base = datetime.combine(run_date, datetime.min.time(), tzinfo=UTC)
    for i in range(n):
        order_ts = base + timedelta(minutes=rng.randint(0, 1439))
        customer = f"CUST-{rng.randint(*ACTIVE_CUSTOMERS):05d}"
        gross = Decimal(rng.randint(500, 95000)) / 100
        rows.append(
            {
                "order_id": f"ORD-{run_date:%Y%m%d}-{i:06d}",
                "customer_id": customer,
                "customer_email": f"user{rng.randint(1, 400)}@example.invalid",
                "order_status": rng.choice(STATUSES),
                "channel": rng.choice(CHANNELS),
                "currency_code": rng.choice(CURRENCIES),
                "gross_amount": str(gross),
                "discount_amount": str((gross * Decimal("0.1")).quantize(Decimal("0.01")))
                if rng.random() < 0.3
                else "0.00",
                "order_ts": order_ts.isoformat(),
                "updated_at": (order_ts + timedelta(minutes=rng.randint(0, 120))).isoformat(),
                "order_date": run_date.isoformat(),
                "promo_code": rng.choice([None, "SPRING10", "WELCOME"]),
            }
        )

    # --- injected defects -------------------------------------------------------------
    rows.append(dict(rows[0]))  # exact redelivery

    updated = dict(rows[1])  # same key, newer version
    updated["order_status"] = "refunded"
    updated["updated_at"] = (
        datetime.fromisoformat(updated["updated_at"]) + timedelta(hours=2)
    ).isoformat()
    rows.append(updated)

    bad_status = dict(rows[2])
    bad_status["order_id"] = f"ORD-{run_date:%Y%m%d}-BAD-STATUS"
    bad_status["order_status"] = "quantum_superposition"
    rows.append(bad_status)

    negative = dict(rows[3])
    negative["order_id"] = f"ORD-{run_date:%Y%m%d}-NEGATIVE"
    negative["gross_amount"] = "-42.00"
    rows.append(negative)

    bad_ccy = dict(rows[4])
    bad_ccy["order_id"] = f"ORD-{run_date:%Y%m%d}-BAD-CCY"
    bad_ccy["currency_code"] = "dollars"
    rows.append(bad_ccy)

    late = dict(rows[5])
    late["order_id"] = f"ORD-{run_date:%Y%m%d}-LATE-ARRIVAL"
    late["order_ts"] = (base - timedelta(days=2)).isoformat()
    late["updated_at"] = (base + timedelta(hours=1)).isoformat()
    rows.append(late)

    rng.shuffle(rows)
    return rows


def generate_customers_cdc(rng: random.Random, run_date: date, n: int) -> list[dict]:
    rows: list[dict] = []
    base = datetime.combine(run_date, datetime.min.time(), tzinfo=UTC)
    lsn = int(run_date.strftime("%Y%m%d")) * 100000
    for i in range(n):
        lsn += rng.randint(1, 50)
        # Deletes only hit churned customers, who never place orders.
        if rng.random() < 0.06:
            customer_id = f"CUST-{rng.randint(*CHURNED_CUSTOMERS):05d}"
            op = "d"
        else:
            customer_id = f"CUST-{rng.randint(*ACTIVE_CUSTOMERS):05d}"
            op = rng.choices(["c", "u"], weights=[0.2, 0.8])[0]
        rows.append(
            {
                "customer_id": customer_id,
                "email": f"user{customer_id[-3:]}@example.invalid",
                "email_for_segmentation": f"user{customer_id[-3:]}@example.invalid",
                "full_name": f"Synthetic Person {i:04d}",
                "phone": f"+1555{rng.randint(1000000, 9999999)}",
                "country_code": rng.choice(COUNTRIES),
                "customer_segment": rng.choice(SEGMENTS),
                "signup_ts": (base - timedelta(days=rng.randint(1, 900))).isoformat(),
                "source_ts": (base + timedelta(minutes=rng.randint(0, 1439))).isoformat(),
                "source_lsn": lsn,
                "op": op,
            }
        )
    return rows


def generate_app_events(rng: random.Random, run_date: date, n: int) -> list[dict]:
    rows: list[dict] = []
    base = datetime.combine(run_date, datetime.min.time(), tzinfo=UTC)
    for i in range(n):
        event_ts = base + timedelta(seconds=rng.randint(0, 86399))
        rows.append(
            {
                "event_id": f"EVT-{run_date:%Y%m%d}-{i:07d}",
                "session_id": f"SESS-{rng.randint(1, 2000):06d}",
                "customer_id": f"CUST-{rng.randint(*ACTIVE_CUSTOMERS):05d}"
                if rng.random() < 0.4
                else None,
                "event_name": rng.choice(EVENT_NAMES),
                "event_ts": event_ts.isoformat(),
                "event_date": run_date.isoformat(),
                "platform": rng.choice(["web", "ios", "android"]),
                "ip_address": f"198.51.100.{rng.randint(1, 254)}",  # TEST-NET-2, reserved
                "properties": json.dumps({"ab_variant": rng.choice(["a", "b"])}),
                "_partition": rng.randint(0, 5),
                "_offset": 1000 + i,
            }
        )
    rows.extend(rows[:5])  # at-least-once redelivery
    return rows


def generate_fx_rates(rng: random.Random, run_date: date) -> list[dict]:
    retrieved = datetime.combine(run_date, datetime.min.time(), tzinfo=UTC)
    anchors = {
        "USD": "1.0",
        "EUR": "1.08",
        "GBP": "1.27",
        "MXN": "0.058",
        "GTQ": "0.129",
        "BRL": "0.19",
    }
    return [
        {
            "rate_date": run_date.isoformat(),
            "currency_code": code,
            "rate_to_usd": str(Decimal(rate) * (1 + Decimal(rng.randint(-200, 200)) / 10000)),
            "retrieved_at": retrieved.isoformat(),
        }
        for code, rate in anchors.items()
    ]


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--date", default=date.today().isoformat())
    parser.add_argument("--out", default="data/landing")
    parser.add_argument("--orders", type=int, default=500)
    parser.add_argument("--customers", type=int, default=120)
    parser.add_argument("--events", type=int, default=3000)
    parser.add_argument("--days", type=int, default=1, help="generate N consecutive days")
    args = parser.parse_args()

    out = Path(args.out)
    start = date.fromisoformat(args.date)
    print("Generating SYNTHETIC data (no real persons or organisations)")

    for offset in range(args.days):
        run_date = start + timedelta(days=offset)
        rng = random.Random(SEED + offset)
        print(f"\n{run_date.isoformat()}")
        _write(
            out / f"orders/dt={run_date}/orders.jsonl", generate_orders(rng, run_date, args.orders)
        )
        _write(
            out / f"customers_cdc/dt={run_date}/customers.jsonl",
            generate_customers_cdc(rng, run_date, args.customers),
        )
        _write(
            out / f"app_events/dt={run_date}/events.jsonl",
            generate_app_events(rng, run_date, args.events),
        )
        _write(out / f"fx_rates/dt={run_date}/rates.jsonl", generate_fx_rates(rng, run_date))

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
