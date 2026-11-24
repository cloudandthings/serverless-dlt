"""Cost of the verbatim split vs the JSON parse RESTClient already does per page.

    uv run python scripts/bench_split.py

Run it on the target hardware (e.g. inside the Lambda image) before sizing.
Reference (Apple M2, ~1 KB nested records): split ~11 µs/record, about 2x the
stdlib parse; end to end the record envelope ran 2.7 s vs 7.1 s for dlt's
standard normaliser on 100k records, because it skips normalisation.
"""

from __future__ import annotations

import json
import statistics
import time

from api_bronze.split import split_records_parsed


def record(i: int) -> dict:
    return {
        "orderId": f"ORD-{i:07}",
        "updatedAt": f"2026-11-{1 + i % 28:02}T{i % 24:02}:15:00+02:00",
        "status": ["open", "paid", "shipped"][i % 3],
        "customer": {
            "customerId": f"C-{i % 5000}",
            "name": {"first": "Thandi", "last": "Nkosi"},
            "contact": {"email": None, "phones": [{"type": "mobile", "number": "+27 82 555 0101"}]},
        },
        "totals": {"subtotal": "1250.00", "tax": {"rate": 0.15, "amount": "187.50"}, "grand": 1437.5},
        "lineItems": [
            {
                "sku": f"SKU-{i % 97}-{n}",
                "qty": n + 1,
                "unitPrice": {"value": "625.00", "currency": "ZAR"},
                "attributes": {"colour": "teal", "size": "M"},
                "discounts": [],
            }
            for n in range(3)
        ],
        "tags": ["priority", "web"],
        "metadata": {"source": "web", "flags": {"fraudCheck": {"score": 0.02}}},
    }


def median_time(fn, reps: int) -> float:
    times = []
    for _ in range(reps):
        t = time.perf_counter()
        fn()
        times.append(time.perf_counter() - t)
    return statistics.median(times)


def main() -> None:
    print(f"{'records':>8} {'MB':>6} | {'gate parse':>10} {'split':>9} {'ratio':>6} {'per record':>11}")
    for n in (100, 1000, 10000):
        raw = json.dumps({"data": [record(i) for i in range(n)], "pagination": {"next": None}}, indent=1).encode()
        reps = max(5, 2000 // n)
        text = raw.decode("utf-8")
        t_parse = median_time(lambda raw=raw: json.loads(raw.decode("utf-8-sig", errors="replace")), reps)
        t_split = median_time(lambda text=text: split_records_parsed(text, "data"), reps)
        print(
            f"{n:>8} {len(raw) / 1e6:>6.2f} | {t_parse * 1e3:>7.2f} ms {t_split * 1e3:>6.2f} ms "
            f"{t_split / t_parse:>5.1f}x {t_split / n * 1e6:>8.1f} µs"
        )


if __name__ == "__main__":
    main()
