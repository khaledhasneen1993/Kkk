#!/usr/bin/env python3
"""Python 3.11+ | feeds.py | Public BTC five-minute paper-market data.
Termux: Python 3.11+ with aiohttp 3.x installed; run python feeds.py.
Keep Android time automatic. Optional: termux-wake-lock during collection.
Feed transitions go to stderr and a bounded rotating feeds.log.
"""
from __future__ import annotations

import argparse
import asyncio
import json
import logging
import random
import time
from collections import deque
from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import datetime
from decimal import Decimal
from logging.handlers import RotatingFileHandler
from typing import Any, Literal

import aiohttp

MARKET = "wss://ws-subscriptions-clob.polymarket.com/ws/market"
SPOT = "wss://data-stream.binance.vision/ws/btcusdt@aggTrade"
GAMMA = "https://gamma-api.polymarket.com/markets/slug/"
TWAP = "https://data.chain.link/streams/btc-usd-twap-60s-streams"
D = Decimal
log = logging.getLogger("feeds")


def utc_ms() -> int:
    return time.time_ns() // 1_000_000


def mono_ms() -> int:
    return time.monotonic_ns() // 1_000_000


def number(v: Any) -> Decimal:
    if isinstance(v, (float, bool)):
        raise ValueError("expected exact numeric input")
    n = D(v)
    if not n.is_finite():
        raise ValueError("nonfinite decimal")
    return n


def decode(s: str) -> Any:
    return json.loads(s, parse_float=D)


def epoch(s: str) -> int:
    dt = datetime.fromisoformat(s.replace("Z", "+00:00"))
    if dt.utcoffset() is None:
        raise ValueError("missing UTC offset")
    return int(dt.timestamp())


@dataclass(frozen=True, slots=True)
class Window:
    start: int
    condition: str
    up: str
    down: str
    rules: str
    oracle_candidate: str | None


async def discover(client: aiohttp.ClientSession, start: int) -> Window:
    slug = f"btc-updown-5m-{start}"
    async with client.get(GAMMA + slug) as response:
        response.raise_for_status()
        m = await response.json(loads=decode)
    def array(key: str) -> list:
        v = m[key]
        return decode(v) if isinstance(v, str) else v
    log.info("gamma start=%s end=%s closed=%s active=%s", m.get("eventStartTime"),
             m.get("endDate"), m.get("closed"), m.get("active"))
    labels, ids = array("outcomes"), array("clobTokenIds")
    if (m["slug"] != slug or epoch(m["eventStartTime"]) != start
            or epoch(m["endDate"]) != start + 300 or m["closed"] is not False
            or m["active"] is not True or len(labels) != 2 or len(ids) != 2
            or set(labels) != {"Up", "Down"} or len(set(ids)) != 2
            or not all(isinstance(i, str) and i.isdigit() for i in ids)
            or not m["conditionId"]):
        raise ValueError("VERIFY BTC window metadata or outcome mapping")
    tokens = dict(zip(labels, ids, strict=True))
    rules = m["description"]
    return Window(start, m["conditionId"], tokens["Up"], tokens["Down"],
                  rules, TWAP if TWAP in rules else None)


@dataclass(slots=True)
class Stamp:
    source: int
    received: int = field(default_factory=mono_ms)

    def fresh(self, limit: int) -> bool:
        return (0 <= utc_ms() - self.source <= limit
                and mono_ms() - self.received <= limit)


@dataclass(slots=True)
class Book:
    stamp: Stamp
    bids: dict[Decimal, Decimal] = field(default_factory=dict)
    asks: dict[Decimal, Decimal] = field(default_factory=dict)

    snapshot_ms: int = field(init=False)
    updates: dict[tuple[str, Decimal], int] = field(default_factory=dict)

    def __post_init__(self) -> None:
        self.snapshot_ms = self.stamp.source

    def level(self, side: str, row: dict, update: Stamp | None = None) -> None:
        levels = {"BUY": self.bids, "SELL": self.asks}[side]
        p, q = number(row["price"]), number(row["size"])
        if not (0 <= p <= 1 and q >= 0):
            raise ValueError("bad book level")
        if update is not None:
            key = (side, p)
            if update.source < self.updates.get(key, self.snapshot_ms):
                return
            self.updates[key] = update.source  # Retain deletions until the next snapshot.
            if update.source > self.stamp.source:
                self.stamp = update
        if q:
            levels[p] = q
        else:
            levels.pop(p, None)


@dataclass(frozen=True, slots=True)
class PaperEvent:
    kind: Literal["accepted", "rejected", "fill", "cancelled"]
    order_id: str
    token: str
    quantity: Decimal = D(0)
    price: Decimal | None = None


class PaperChannel:
    """Single-consumer FIFO with backpressure; delivery from the local simulator."""
    def __init__(self) -> None:
        self.queue: asyncio.Queue[PaperEvent] = asyncio.Queue(maxsize=256)

    async def publish(self, event: PaperEvent) -> None:
        if not event.order_id or not event.token:
            raise ValueError("missing paper order identity")
        if event.kind not in {"accepted", "rejected", "fill", "cancelled"}:
            raise ValueError("unknown paper event")
        if event.kind == "fill" and (
            not isinstance(event.price, Decimal) or not 0 <= number(event.price) <= 1
            or not isinstance(event.quantity, Decimal) or number(event.quantity) <= 0
        ):
            raise ValueError("invalid simulated fill")
        await self.queue.put(event)

    async def receive(self) -> PaperEvent:
        return await self.queue.get()


@dataclass(slots=True)
class Link:
    start: int = field(default_factory=mono_ms)
    since: int | None = None
    down: int = field(default_factory=mono_ms)
    total: int = 0
    count: int = 0
    failures: int = 0

    def online(self, name: str) -> None:
        t = mono_ms()
        self.since = t
        self.count += 1
        log.info("%s connected session=%d outage_ms=%d", name, self.count, t-self.down)

    def offline(self, name: str, reason: str, failed: bool = False) -> None:
        if failed:
            self.failures += 1
        if self.since is not None:
            self.down = mono_ms()
            self.total += self.down - self.since
            self.since = None
            log.info("%s disconnected reason=%s failed=%s", name, reason, failed)

    def uptime(self) -> Decimal:
        t = mono_ms()
        live = 0 if self.since is None else t - self.since
        return D(100) * (self.total + live) / max(1, t - self.start)


class Resnapshot(Exception):
    """Request a clean snapshot after a market configuration change."""


class Feeds:
    """Single event-loop ownership. Copy depth before awaiting in a consumer."""
    def __init__(self, stale_ms: int = 3000) -> None:
        if stale_ms <= 0:
            raise ValueError("stale_ms must be positive")
        self.samples = self.fresh_samples = 0
        self.skew_ms: int | None = None
        self.prefetched: dict[int, Window] = {}
        self.limit = stale_ms
        self.window: Window | None = None
        self.books: dict[str, Book] = {}
        self.spot: tuple[Stamp, Decimal] | None = None
        self.spot_ticks: deque[tuple[int, Stamp, Decimal]] = deque(maxlen=8192)
        self.links = {k: Link() for k in ("market", "spot")}
        self.users = PaperChannel()
        self.changed = asyncio.Event()

    def feeds_fresh(self) -> bool:
        w = self.window
        return bool(w and w.start <= utc_ms() // 1000 < w.start + 300
                    and self.spot and self.spot[0].fresh(self.limit)
                    and all(i in self.books and self.books[i].stamp.fresh(self.limit)
                            for i in (w.up, w.down)))

    def spot_frame(self, e: dict) -> None:
        if e["e"] != "aggTrade" or e["s"] != "BTCUSDT":
            raise ValueError("unexpected spot stream")
        stamp, p = Stamp(int(e["T"])), number(e["p"])
        self.skew_ms = utc_ms() - stamp.source
        if p <= 0 or (self.spot and stamp.source < self.spot[0].source):
            raise ValueError("invalid or reordered spot tick")
        trade_id = int(e["a"])
        if self.spot_ticks:
            previous = self.spot_ticks[-1][0]
            if trade_id == previous:
                return
            if trade_id != previous + 1:
                self.spot_ticks.clear()
                log.warning("spot aggregation ID gap; restart volatility warmup")
        self.spot_ticks.append((trade_id, stamp, p))
        self.spot = stamp, p
        self.changed.set()

    def market_frame(self, e: dict) -> None:
        w = self.window
        kind = e.get("event_type")
        if not w or kind not in {"book", "price_change", "tick_size_change"}:
            return
        if e["market"].lower() != w.condition.lower():
            raise ValueError("wrong condition")
        if kind == "tick_size_change":
            log.info("tick_size_change %s", e)
            return
        stamp = Stamp(int(e["timestamp"]))
        rows = [e] if kind == "book" else e["price_changes"]
        for row in rows:
            token = row["asset_id"]
            if token not in (w.up, w.down):
                raise ValueError("wrong asset")
            b = self.books.get(token)
            if kind == "book":
                if b and stamp.source < b.snapshot_ms:
                    continue
                prior = b
                b = Book(stamp)
                for side, key in (("BUY", "bids"), ("SELL", "asks")):
                    for level in row[key]:
                        b.level(side, level)
                if prior and prior.stamp.source > stamp.source:
                    for (side, price), ts in prior.updates.items():
                        if ts > stamp.source:
                            levels = prior.bids if side == "BUY" else prior.asks
                            b.level(side, {"price": price, "size": levels.get(price, 0)}, Stamp(ts))
                    b.stamp = prior.stamp
                self.books[token] = b
            elif b is None:
                raise ValueError("delta before snapshot")
            else:
                b.level(row["side"], row, stamp)
        self.changed.set()

    async def prefetch(self, client: aiohttp.ClientSession, start: int) -> None:
        try:
            self.prefetched[start] = await asyncio.wait_for(discover(client, start), 5)
        except Exception as exc:
            log.info("prefetch failed start=%s: %r", start, exc)

    async def session(self, client: aiohttp.ClientSession, name: str) -> None:
        market = name == "market"
        start = utc_ms() // 300000 * 300
        w = None
        task: asyncio.Task[None] | None = None
        if market:
            for expired in list(self.prefetched):
                if expired < start:
                    del self.prefetched[expired]
            w = self.prefetched.pop(start, None) or await discover(client, start)
        url = MARKET if market else SPOT
        async with client.ws_connect(url, autoping=True, max_msg_size=2**20) as ws:
            if w:
                self.window = w
                await ws.send_json({"type": "market", "assets_ids": [w.up, w.down]})
                log.info("window=%s condition=%s up=%s down=%s oracle_candidate=%s",
                         w.start, w.condition, w.up, w.down, w.oracle_candidate)
                log.info("RULES %s", w.rules)
            self.links[name].online(name)
            last_data = last_pong = mono_ms()
            ping_due = last_pong + 10000
            handler: Callable[[dict], None] = self.market_frame if w else self.spot_frame
            try:
                while True:
                    now = mono_ms()
                    if w and utc_ms() >= (w.start + 300) * 1000:
                        return
                    if (w and task is None and w.start + 300 not in self.prefetched
                            and utc_ms() >= (w.start + 280) * 1000):
                        task = asyncio.create_task(self.prefetch(client, w.start + 300))
                    if now - last_data > max(30000, 3 * self.limit):
                        raise TimeoutError("no book or spot data")
                    if w and now - last_pong > 25000:
                        raise TimeoutError("no PONG")
                    if w and now >= ping_due:
                        await ws.send_str("PING")
                        ping_due = now + 10000
                    try:
                        msg = await ws.receive(timeout=2)
                    except asyncio.TimeoutError:
                        continue
                    if msg.type != aiohttp.WSMsgType.TEXT:
                        raise ConnectionError(f"WS type={msg.type} close={ws.close_code}")
                    if w and msg.data == "PONG":
                        last_pong = mono_ms()
                        continue
                    payload = decode(msg.data)
                    for event in payload if isinstance(payload, list) else [payload]:
                        if w and not isinstance(event, dict):
                            continue
                        handler(event)
                        if not w or event.get("event_type") in {"book", "price_change"}:
                            last_data = mono_ms()
            finally:
                if w:
                    self.books.clear()
                else:
                    self.spot = None
                self.changed.set()
                if task is not None:
                    task.cancel()
                    await asyncio.gather(task, return_exceptions=True)

    async def supervise(self, client: aiohttp.ClientSession, name: str) -> None:
        delay = 1
        while True:
            reason = "window rollover"
            retry = False
            try:
                await self.session(client, name)
            except asyncio.CancelledError:
                reason = "shutdown"
                raise
            except Resnapshot as exc:
                reason = f"resnapshot: {exc}"
            except Exception as exc:
                retry = True
                reason = f"{type(exc).__name__}: {exc}"
                log.warning("%s session failure %s", name, reason)
            finally:
                link = self.links[name]
                if link.since is not None and mono_ms() - link.since >= 30000:
                    delay = 1
                link.offline(name, reason, retry)
                if name == "market":
                    self.window = None
                    self.books.clear()
                else:
                    self.spot = None
                    self.spot_ticks.clear()
                    self.skew_ms = None
                self.changed.set()
            if retry:
                await asyncio.sleep(random.uniform(delay / 2, delay))
                delay = min(60, delay * 2)

    async def sampler(self) -> None:
        while True:
            await asyncio.sleep(1)
            self.samples += 1
            self.fresh_samples += int(self.feeds_fresh())

    async def summary(self) -> None:
        while True:
            await asyncio.sleep(600)
            fresh_pct = D(100) * self.fresh_samples / max(1, self.samples)
            for name, link in self.links.items():
                log.info("10min summary feed=%s process_transport_uptime=%.2f%% "
                         "sessions=%d failures=%d fresh_pct=%.2f%% skew_ms=%s both_feeds_fresh=%s",
                         name, link.uptime(), link.count, link.failures, fresh_pct,
                         self.skew_ms, self.feeds_fresh())

    async def run(self) -> None:
        timeout = aiohttp.ClientTimeout(total=12, sock_connect=8)
        async with aiohttp.ClientSession(timeout=timeout) as client:
            async with asyncio.TaskGroup() as group:
                for name in self.links:
                    group.create_task(self.supervise(client, name))
                group.create_task(self.sampler())
                group.create_task(self.summary())


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--stale-ms", type=int, default=3000)
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(message)s",
                        handlers=[logging.StreamHandler(), RotatingFileHandler(
                            "feeds.log", maxBytes=2_000_000, backupCount=3)])
    try:
        asyncio.run(Feeds(args.stale_ms).run())
    except KeyboardInterrupt:
        pass
