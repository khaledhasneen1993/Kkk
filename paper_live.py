"""Python 3.11+ | Live BTC paper execution | Termux: python paper_live.py
Only the reference-price socket authenticates. All orders and balances are local.
"""
import argparse
import asyncio
import fcntl
import getpass
import json
import logging
import os
import random
import sqlite3
from collections import deque
from decimal import Decimal as D, ROUND_DOWN, ROUND_UP
from pathlib import Path
from logging.handlers import RotatingFileHandler

import aiohttp
from aiohttp import web
from dashboard import Activity, PAGE, START, snapshot
from feeds import Feeds, GAMMA, TWAP, decode, number, utc_ms

ZERO, ONE = D(0), D(1)
SQL = "SELECT body FROM ledger ORDER BY id"
LOG = logging.getLogger("paper")


class Ledger:
    def __init__(self, path):
        self.db = sqlite3.connect(path)
        self.db.execute("PRAGMA journal_mode=WAL")
        self.db.execute("CREATE TABLE IF NOT EXISTS ledger(id INTEGER PRIMARY KEY, body TEXT NOT NULL)")
        self.cash = START
        self.positions, self.pending = {}, {}
        self.attempted, self.windows = set(), set()
        self.history = deque(maxlen=100)
        self.trades = self.losses = self.cool = 0
        self.peak, self.realized = START, ZERO
        self.day, self.day_start = -1, START
        for body, in self.db.execute(SQL):
            self.apply(json.loads(body))
        for start in list(self.pending):
            self.record("reject", start, reason="restart before confirmed fill")

    def record(self, kind, start, **data):
        e = dict(kind=kind, start=start, ts=utc_ms(), **data)
        raw = json.dumps(e, default=str, separators=(",", ":"))
        with self.db:
            self.db.execute("INSERT INTO ledger(body) VALUES (?)", (raw,))
        self.apply(json.loads(raw))

    def apply(self, e):
        k, s = e["kind"], e["start"]
        if k == "window":
            self.windows.add(s)
        if k == "signal":
            self.pending[s] = e
            self.attempted.add(s)
        if k in ("fill", "reject"):
            self.pending.pop(s, None)
        if k == "fill":
            self.cash -= D(e["debit"])
            self.positions[s] = e
            self.trades += 1
        if k == "settle":
            p = self.positions.pop(s)
            gain = D(e["payout"]) - D(p["debit"])
            self.cash += D(e["payout"])
            day = e["ts"] // 86400000
            if day != self.day:
                self.day, self.day_start = day, START + self.realized
            self.realized += gain
            self.peak = max(self.peak, START + self.realized)
            self.losses = self.losses + 1 if gain < 0 else 0
            if self.losses >= 3 or START+self.realized <= self.day_start*D(".90"):
                self.cool = e["ts"] + 3600000
                self.losses = 0
                self.day_start = START + self.realized
        self.history.appendleft(e)


class Oracle:
    def __init__(self):
        self.points = {}
        self.status = "بانتظار اتصال مرجع الحسم"

    async def run(self, http):
        token = os.getenv("POLY_DATA_TOKEN")
        auth = {"accessToken": token} if token else {
            k: os.getenv(v) for k,v in (("apiKey","POLY_API_KEY"),("secret","POLY_API_SECRET"),
                                        ("passphrase","POLY_API_PASSPHRASE"))}
        if not all(auth.values()):
            self.status = "VERIFY · بيانات قراءة PolyBolt غير مضبوطة"
            return
        frame = {"op":"auth","rid":"auth","auth":auth}
        if os.getenv("CHAINLINK_API_KEY"):
            frame["chainlink_api_key"] = os.environ["CHAINLINK_API_KEY"]
        delay = 1
        while True:
            try:
                async with http.ws_connect("wss://ws-live-v2.polymarket.com/ws") as ws:
                    await ws.send_json(frame)
                    while True:
                        msg = await ws.receive_json(loads=decode, timeout=25)
                        if msg.get("op") == "error":
                            raise ValueError(str(msg.get("code","oracle access refused")))
                        if msg.get("op") == "authed":
                            await ws.send_json({"op":"subscribe","rid":"btc","subscriptions":[{
                                "channel":"price.crypto.twap","filter":{"symbol":"btcusd",
                                "window_seconds":60,"provider":"chainlink"}}]})
                        if msg.get("channel") != "price.crypto.twap" or "payload" not in msg:
                            continue
                        p = msg["payload"]
                        if (p.get("source") != "chainlink" or p.get("symbol") != "btcusd"
                                or p.get("window_seconds") != 60):
                            raise ValueError("VERIFY oracle identity")
                        if msg.get("dropped"):
                            raise ValueError("oracle dropped reports")
                        for row in p.get("data", [p]):
                            t = int(row["timestamp"])
                            price = number(row.get("full_accuracy_value",row["value"]))
                            if price <= 0 or t > utc_ms()+1000:
                                raise ValueError("invalid oracle sample")
                            self.points[t] = price
                        cutoff = utc_ms()-310000
                        self.points = {t:v for t,v in self.points.items() if t >= cutoff}
                        self.status, delay = "متصل", 1
            except (ValueError, TypeError, KeyError, OSError, aiohttp.ClientError, TimeoutError) as exc:
                self.status = f"VERIFY oracle · {type(exc).__name__}: {exc}"
                self.points.clear()
                await asyncio.sleep(random.uniform(delay/2,delay))
                delay = min(60,2*delay)


class RiskGuard:
    def __init__(self, ledger):
        self.l = ledger

    def budget(self, minimum, start):
        l = self.l
        if Path("PAPER_STOP").exists() or l.cool > utc_ms():
            return ZERO
        if START+l.realized <= l.peak*D(".8") or start in l.positions:
            return ZERO
        # Reserve enough for outstanding exposures to lose in full.
        daily = l.day_start if utc_ms()//86400000 == l.day else START+l.realized
        if l.cash < (START + l.realized) * D(".90") or START+l.realized < daily*D(".90"):
            return ZERO
        budget = l.cash*D(".05")
        return budget if budget >= max(minimum,l.cash*D(".02")) else ZERO


def sweep(book, limit, budget, rate, available=ONE):
    qty = cost = fees = ZERO
    for p, displayed in sorted(book.asks.items()):
        if p > limit:
            break
        if p <= 0:
            continue
        fee = rate*p*(1-p)
        room = max(ZERO,budget-cost-fees-D(".00001"))
        size = min(displayed*available,room/(p+fee)).quantize(D(".0001"), rounding=ROUND_DOWN)
        if size:
            qty += size
            cost += size*p
            fees += size*fee
    return qty, cost, fees.quantize(D(".00001"), rounding=ROUND_UP)


class Engine:
    def __init__(self, feeds, ledger, oracle, wire, ack, decay=D(".94")):
        self.f, self.l, self.o = feeds, ledger, oracle
        self.risk = RiskGuard(ledger)
        self.wire, self.ack, self.decay = wire, ack, decay
        self.meta = {}
        self.reason = "بدء الاتصال"
        self.prev = None
        self.var = ZERO
        self.samples = 0
        self.features = {}

    def observe(self):
        f = self.f
        if not f.spot or not f.spot[0].fresh(f.limit):
            self.prev, self.samples, self.var = None, 0, ZERO
            return
        t, p = f.spot[0].source//1000, f.spot[1]
        if self.prev and self.prev[0] == t:
            return
        if self.prev and t == self.prev[0]+1:
            r = (p/self.prev[1]).ln()
            self.var = self.decay*self.var+(ONE-self.decay)*r*r
            self.samples += 1
        else:
            self.samples, self.var = 0, ZERO
        self.prev = t,p

    def propose(self):
        f,w = self.f,self.f.window
        if not w or not f.feeds_fresh():
            return "انتظار دفاتر وأسعار حديثة"
        if w.start in self.l.attempted:
            return "تمت معالجة أمر لهذه النافذة"
        rules = " ".join(w.rules.lower().split())
        if (w.oracle_candidate != TWAP or not any(word in rules for word in ("start","beginning"))
                or "greater than or equal" not in rules):
            return "VERIFY · قواعد السوق لا تؤكد مرجع TWAP"
        anchor = self.o.points.get(w.start*1000)
        latest = max(self.o.points, default=0)
        if anchor is None or not 0 <= utc_ms()-latest <= f.limit:
            return "انتظار عينة الافتتاح الدقيقة ومرجع حديث · "+self.o.status
        if w.start not in self.meta:
            return "VERIFY · انتظار الرسوم والحد الأدنى"
        rate, minimum = self.meta[w.start]
        budget = self.risk.budget(minimum,w.start)
        if budget <= 0:
            return f"منع مخاطرة أو حد أدنى للسوق ({minimum} USD)"
        left = D((w.start+300)*1000-utc_ms())/1000
        if left <= 5 or self.samples < 30 or self.var <= D(".00001")**2:
            return "فترة إحماء أو تذبذب منخفض أو اقتراب الإغلاق"
        ref,spot = self.o.points[latest],f.spot[1]
        gap = (spot/ref).ln()
        if abs(gap) < D(".00005"):
            return "الفجوة أصغر من حد المرشح"
        # Conservative directional approximation; shrink when USD/USDT basis grows.
        z = (ref/anchor).ln()/(self.var*left).sqrt()
        z = max(D(-12),min(D(12),z))
        prob = ONE/(ONE+(-D("1.702")*z).exp())
        prob = D(".5")+(prob-D(".5"))/(ONE+abs(gap)*1000)
        self.features = dict(open=str(anchor),oracle=str(ref),spot=str(spot),
                             probability_up=str(prob),sigma=str(self.var.sqrt()),gap=str(gap))
        candidates = []
        for side,token,p in (("Up",w.up,prob),("Down",w.down,1-prob)):
            b = f.books[token]
            if not b.bids or not b.asks:
                continue
            bid,ask = max(b.bids),min(b.asks)
            if not ZERO < bid < ask < ONE or ask-bid>D(".04"):
                continue
            bid_depth=sum((q for x,q in b.bids.items() if x>=bid-D(".02")),ZERO)
            ask_depth=sum((q for x,q in b.asks.items() if x<=ask+D(".02")),ZERO)
            toxicity = ask_depth/(bid_depth+ask_depth)
            limit=min(ONE,ask*D("1.005"))
            qty,cost,fee=sweep(b,limit,budget,rate)
            if toxicity>D(".80") or cost<minimum or cost+fee<budget*D(".98") or not qty:
                continue
            edge=p-(cost+fee)/qty
            if edge>=D(".025"):
                candidates.append((edge,side,token,limit,budget,ask))
        return max(candidates) if candidates else "تخطي · الأفضلية أو السيولة أو السبريد أو ضغط البيع"

    async def execute(self, signal):
        edge,side,token,limit,budget,ask = signal
        w = self.f.window
        rate,minimum = self.meta[w.start]
        self.l.record("signal",w.start,condition=w.condition,side=side,token=token,
                      edge=edge,limit=limit,budget=budget,ask=ask,features=self.features)
        await asyncio.sleep(self.wire/1000)
        f=self.f
        if (not f.window or f.window.start!=w.start or not f.feeds_fresh()
                or self.risk.budget(minimum,w.start)<budget
                or not 0<=utc_ms()-max(self.o.points,default=0)<=f.limit):
            self.l.record("reject",w.start,reason="stale, risk or rollover on arrival")
            return
        book=f.books[token]
        # Scenario assumption: more displayed size remains when ask has fallen.
        adverse = bool(book.asks and min(book.asks)<ask)
        fill_pct = 85 if adverse else 55
        if random.randrange(100) >= fill_pct:
            self.l.record("reject",w.start,reason="simulated competing liquidity",
                          expected_fill_pct=fill_pct,adverse=adverse)
            return
        share=D(".85") if adverse else D(".55")
        qty,cost,fee=sweep(book,limit,budget,rate,share)
        if qty<=0 or cost<minimum:
            self.l.record("reject",w.start,reason="insufficient executable notional on arrival")
            return
        self.l.record("fill",w.start,condition=w.condition,side=side,token=token,qty=qty,
                      avg=cost/qty,fee=fee,debit=cost+fee,partial=cost+fee<budget*D(".98"),
                      available_fraction=share,expected_fill_pct=fill_pct,adverse=adverse,wire_ms=self.wire)
        await asyncio.sleep(self.ack/1000)
        self.l.record("ack",w.start,ack_ms=self.ack)

    async def markets(self,http):
        while True:
            w=self.f.window
            starts=list(self.l.positions)
            if w and w.start not in self.meta:
                starts.append(w.start)
            for s in set(starts):
                if s in self.l.positions and utc_ms()<(s+300)*1000:
                    continue
                try:
                    async with http.get(GAMMA+f"btc-updown-5m-{s}") as resp:
                        resp.raise_for_status()
                        m=await resp.json(loads=decode)
                    if s in self.l.positions:
                        pos=self.l.positions[s]
                        if m["conditionId"]!=pos["condition"] or m["closed"] is not True:
                            continue
                        async with http.get("https://clob.polymarket.com/markets/"+pos["condition"]) as result:
                            result.raise_for_status()
                            resolved=await result.json(loads=decode)
                        winners=[t for t in resolved["tokens"] if t.get("winner") is True]
                        if (resolved.get("condition_id")!=pos["condition"] or resolved.get("closed") is not True
                                or len(winners)!=1 or len(resolved["tokens"])!=2
                                or pos["token"] not in {str(t["token_id"]) for t in resolved["tokens"]}):
                            continue
                        won=str(winners[0]["token_id"])==pos["token"]
                        payout=D(pos["qty"]) if won else ZERO
                        self.l.record("settle",s,payout=payout,pnl=payout-D(pos["debit"]),
                                      winner=winners[0]["outcome"])
                    elif w and m["conditionId"]==w.condition and m["closed"] is False:
                        enabled=m["feesEnabled"]
                        if not isinstance(enabled,bool) or (enabled and m["feeSchedule"]["exponent"]!=1):
                            raise ValueError("VERIFY fee schedule")
                        rate=number(m["feeSchedule"]["rate"]) if enabled else ZERO
                        minimum=number(m["orderMinSize"])
                        if 0<=rate<=1 and minimum>0:
                            self.meta={s:(rate,minimum)}
                except (KeyError,ValueError,ArithmeticError,OSError,aiohttp.ClientError,TimeoutError) as exc:
                    LOG.warning("VERIFY market %s: %s",s,exc)
            await asyncio.sleep(8)

    async def loop(self):
        prev=None
        while True:
            self.observe()
            w=self.f.window
            if w and w.start not in self.l.windows:
                self.l.record("window",w.start)
                self.features={}
            signal=self.propose()
            if isinstance(signal,str):
                self.reason=signal
                state=(w.start if w else 0,signal)
                if state!=prev and w:
                    self.l.record("decision",w.start,reason=signal)
                prev=state
            else:
                self.reason="إرسال أمر ورقي"
                await self.execute(signal)
            await asyncio.sleep(.2)


async def main(args):
    f,l,o=Feeds(),Ledger(args.db),Oracle()
    e=Engine(f,l,o,args.wire_ms,args.ack_ms,args.ewma)
    activity=Activity()
    logging.basicConfig(level=logging.INFO,handlers=[activity,logging.StreamHandler(),
        RotatingFileHandler("feeds.log",maxBytes=2000000,backupCount=3)])
    async def state(request):
        s=snapshot(f,activity)
        mark=l.cash
        for p in l.positions.values():
            b=f.books.get(p["token"])
            if b and b.bids and b.stamp.fresh(f.limit):
                mark+=D(p["qty"])*max(b.bids)
            else:
                mark+=D(p["debit"])  # explicitly labelled cost fallback
        if request.query.get("depth") != "all":
            for book in s["books"]:
                for side in ("bids","asks"):
                    book[side]=book[side][:1]
        s.update(cash=str(l.cash),equity=str(mark),pnl=str(mark-START),realized=str(l.realized),
                 trades=list(l.history),fill_count=l.trades,orders=list(l.pending.values()),
                 positions=list(l.positions.values()),engine=e.reason,features=e.features,
                 oracle_status=o.status,skip_pct=str(round(100*D(len(l.windows)-len(l.attempted))
                 /max(1,len(l.windows)),2)))
        return web.json_response(s,headers={"Cache-Control":"no-store"})
    page=PAGE.replace("s.trades.length","s.fill_count").replace('<details open>',
        '<details open><summary>قرارات وأوامر وتعبئات وتسويات</summary><pre id="ledger"></pre></details><details open>',1)
    page=page.replace("put('events',", """put('ledger',JSON.stringify({oracle:s.oracle_status,
      model:s.features,realized:s.realized,skip_pct:s.skip_pct,positions:s.positions,
      recent:s.trades},null,2));put('events',""")
    page=page.replace("قيمة الحساب","القيمة التقديرية (آخر شراء أو تكلفة)")
    async def home(request):
        return web.Response(text=page,content_type="text/html")
    app=web.Application()
    app.add_routes([web.get("/",home),web.get("/api/state",state)])
    runner=web.AppRunner(app,access_log=None)
    await runner.setup()
    try:
        await web.TCPSite(runner,"127.0.0.1",args.port).start()
        LOG.info("PAPER ONLY http://127.0.0.1:%d cash=%s",args.port,l.cash)
        async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=10)) as http:
            async with asyncio.TaskGroup() as g:
                for coro in (f.run(),o.run(http),e.markets(http),e.loop()):
                    g.create_task(coro)
    finally:
        await runner.cleanup()
        l.db.close()


if __name__=="__main__":
    p=argparse.ArgumentParser(description=__doc__)
    p.add_argument("--ask-data",action="store_true",help="prompt locally for reference-feed access")
    p.add_argument("--db",default="paper35.sqlite")
    p.add_argument("--port",type=int,default=8765)
    p.add_argument("--ewma",type=number,default=D(".94"))
    p.add_argument("--wire-ms",type=int,default=650)
    p.add_argument("--ack-ms",type=int,default=350)
    args=p.parse_args()
    if not ZERO<args.ewma<ONE:
        p.error("EWMA decay must lie between 0 and 1")
    if args.ask_data:
        if not os.getenv("POLY_DATA_TOKEN"):
            for name in ("POLY_API_KEY","POLY_API_SECRET","POLY_API_PASSPHRASE"):
                if not os.getenv(name):
                    os.environ[name]=getpass.getpass(name+" (data socket): ")
        if not os.getenv("CHAINLINK_API_KEY"):
            os.environ["CHAINLINK_API_KEY"]=getpass.getpass("CHAINLINK_API_KEY: ")
    if min(args.wire_ms,args.ack_ms)<0:
        p.error("latencies must be nonnegative")
    with open(args.db+".lock","a") as lock:
        fcntl.flock(lock,fcntl.LOCK_EX|fcntl.LOCK_NB)
        try:
            asyncio.run(main(args))
        except KeyboardInterrupt:
            pass
