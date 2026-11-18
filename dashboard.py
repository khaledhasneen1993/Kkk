"""Python 3.11+ | dashboard.py | Local Termux monitor for the kkk paper session."""
import argparse
import asyncio
import logging
from collections import deque
from datetime import datetime, timezone
from decimal import Decimal
from logging.handlers import RotatingFileHandler

from aiohttp import web
from feeds import Feeds, utc_ms

START = Decimal("35.00")


class Activity(logging.Handler):
    def __init__(self):
        super().__init__()
        self.events = deque(maxlen=60)

    def emit(self, record):
        self.events.appendleft(self.format(record))


def snapshot(feeds, activity):
    now = utc_ms()
    w = feeds.window
    books = []
    if w:
        for side, token in (("Up", w.up), ("Down", w.down)):
            book = feeds.books.get(token)
            def levels(rows, reverse):
                return [[str(p), str(rows[p])] for p in sorted(rows, reverse=reverse)]
            books.append(dict(side=side, token=token,
                              age_ms=now-book.stamp.source if book else None,
                              bids=levels(book.bids, True) if book else [],
                              asks=levels(book.asks, False) if book else []))
    return dict(capital=str(START), cash=str(START), equity=str(START), pnl="0.00",
                positions=[], trades=[], orders=[], engine="بانتظار نموذج الاحتمال ومحرك التنفيذ",
                fresh=feeds.feeds_fresh(), skew_ms=feeds.skew_ms,
                fresh_pct=str(round(Decimal(100)*feeds.fresh_samples/feeds.samples, 2))
                if feeds.samples else None,
                price=str(feeds.spot[1]) if feeds.spot else None,
                spot_age_ms=now-feeds.spot[0].source if feeds.spot else None,
                links={k:dict(connected=v.since is not None, failures=v.failures,
                              sessions=v.count, uptime=str(round(v.uptime(), 2)))
                       for k,v in feeds.links.items()},
                remaining=max(0,w.start+300-now//1000) if w else None,
                window_start=w.start if w else None, condition=w.condition if w else None,
                rules=w.rules if w else None, oracle=w.oracle_candidate if w else None,
                books=books, events=list(activity.events),
                updated=datetime.now(timezone.utc).isoformat(timespec="seconds"))


PAGE = r"""<!doctype html><html lang="ar" dir="rtl"><meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1"><title>BTC · kkk</title>
<style>
*{box-sizing:border-box}body{max-width:900px;margin:auto;background:#101a24;color:#e7eff5;
font:16px system-ui;padding:18px}h1{font-size:24px}h2{font-size:18px;color:#8cdec8}
section,details{background:#1c2a39;border-radius:12px;padding:14px;margin:10px 0}
pre{white-space:pre-wrap;overflow-wrap:anywhere;font:14px/1.65 monospace;direction:ltr;text-align:left}
#account{font-size:19px}p{line-height:1.65;margin:5px 0} summary{cursor:pointer}
</style><h1>BTC · متابعة المحاكاة الورقية</h1><p id="status"></p>
<section><h2>الحساب بالدولار</h2><p id="account"></p><p id="engine"></p>
<p id="trades"></p></section>
<section><h2>النافذة والأسعار</h2><p id="spot"></p><p id="window"></p>
<p id="top"></p></section><section><h2>جودة البيانات</h2><p id="health"></p>
<pre id="links"></pre></section><details><summary>دفتر الأوامر الكامل · السعر والكمية</summary>
<pre id="depth"></pre></details><details><summary>السوق وقواعد الحسم</summary>
<pre id="rules"></pre></details><details open><summary>آخر أحداث التشغيل</summary>
<pre id="events"></pre></details>
<script>
const el=id=>document.getElementById(id), put=(id,v)=>el(id).textContent=v;
const val=x=>x??'—';
async function poll(){
 try{
 const paths=['depth','rules','events'];const expanded=paths.some(k=>el(k).parentNode.open&&k==='depth');
 const r=await fetch('/api/state'+(expanded?'?depth=all':''),{cache:'no-store',signal:AbortSignal.timeout(5000)});
 if(!r.ok)throw Error(r.status);
 const s=await r.json();
 put('status','تحديث UTC · '+s.updated);
 put('account','رأس المال '+s.capital+'  |  النقد '+s.cash+'  |  قيمة الحساب '+s.equity+'  |  الربح والخسارة '+s.pnl);
 put('engine',s.engine);
 put('trades','المراكز المفتوحة '+s.positions.length+' · الأوامر المعلّقة '+s.orders.length+' · الصفقات المنفذة '+s.trades.length);
 put('spot','BTC/USDT '+val(s.price)+' · عمر السعر '+val(s.spot_age_ms)+' ms');
 put('window','بداية النافذة UTC '+(s.window_start?new Date(s.window_start*1000).toISOString().slice(11,19):'—')+' · المتبقي '+val(s.remaining)+' ثانية');
 put('top',s.books.map(b=>b.side+' · أفضل شراء '+val(b.bids[0]?.[0])+' · أفضل بيع '+val(b.asks[0]?.[0])+' · عمر الدفتر '+val(b.age_ms)+' ms').join('\n'));
 put('health',(s.fresh?'الأسعار حديثة':'الأسعار ناقصة أو قديمة')+' · نسبة العينات الحديثة '+val(s.fresh_pct)+'% · فرق التوقيت '+val(s.skew_ms)+' ms');
 put('links',Object.entries(s.links).map(([n,l])=>n+' '+(l.connected?'online':'offline')+' | uptime '+l.uptime+'% | sessions '+l.sessions+' | failures '+l.failures).join('\n'));
 if(expanded)put('depth',s.books.map(b=>b.side+'\nBIDS\n'+b.bids.map(v=>v.join(' × ')).join('\n')+'\nASKS\n'+b.asks.map(v=>v.join(' × ')).join('\n')).join('\n\n'));
 put('rules',[s.condition,...s.books.map(b=>b.side+' '+b.token),s.oracle,s.rules].filter(Boolean).join('\n'));
 put('events',s.events.join('\n'));
 }catch(e){put('status','انقطع تحديث اللوحة · الأرقام الظاهرة من آخر تحديث');}
 setTimeout(poll,document.hidden?10000:2000);
}
poll();
</script></html>"""


async def run(port):
    activity = Activity()
    fmt = logging.Formatter("%(asctime)s %(message)s")
    activity.setFormatter(fmt)
    logging.basicConfig(level=logging.INFO, handlers=[
        logging.StreamHandler(), RotatingFileHandler("feeds.log", maxBytes=2000000, backupCount=3),
        activity], format="%(asctime)s %(message)s")
    feeds = Feeds()
    async def page(request):
        return web.Response(text=PAGE, content_type="text/html")
    async def state(request):
        data = snapshot(feeds, activity)
        if request.query.get("depth") != "all":
            for book in data["books"]:
                for side in ("bids", "asks"):
                    book[side] = book[side][:1]
        return web.json_response(data, headers={"Cache-Control":"no-store"})
    app = web.Application()
    app.add_routes([web.get("/", page), web.get("/api/state", state)])
    runner = web.AppRunner(app, access_log=None)
    await runner.setup()
    try:
        await web.TCPSite(runner, "127.0.0.1", port).start()
        logging.info("Dashboard http://127.0.0.1:%d | paper capital %s USD", port, START)
        await feeds.run()
    finally:
        await runner.cleanup()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--port", type=int, default=8765)
    try:
        asyncio.run(run(parser.parse_args().port))
    except KeyboardInterrupt:
        pass
