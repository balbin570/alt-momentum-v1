"""V90.2 standalone paper research scanner. No order endpoints or API keys."""
import asyncio
import json
import gc
import resource
from pathlib import Path
import os
import statistics
import time
from datetime import datetime, timezone
import httpx
from fastapi import FastAPI

app = FastAPI(title="ALT-MOMENTUM-V90")
BINANCE = "https://data-api.binance.vision"
MODEL = "ALT-MOMENTUM-V1"
MIN_QUOTE_VOLUME_USDT = 250_000
V26_NON_ALT_BASE_EXCLUSIONS = {'AVGO', 'SKHYB', 'MSTRB', 'WDC', 'SOXL', 'AVGOB', 'SOXLB', 'SKHY', 'GOOGL', 'AAPL', 'TSLA', 'GOOGLB', 'NVDA', 'QQQB', 'USTC', 'EURI', 'AAPLB', 'MSTR', 'QQQ', 'USDE', 'SOXSB', 'WDCB', 'INTC', 'NVDAB', 'XUSD', 'TSLAB', 'INTCB', 'SOXS', 'AMD', 'AMDB', 'MVLL', 'MVLLB'}

EXCLUDED_BASES = {
    "BTC",
    "ETH",
    "XAUT",
    "PAXG",
    "USDC",
    "FDUSD",
    "TUSD",
    "USDP",
    "DAI",
    "USD1",
    "RLUSD",
    "BFUSD",
    "EUR",
    "TRY",
    "GBP",
    "BRL",
    "AUD",
    "BIDR",
    "IDRT",
    "UAH",
    "RUB",
}

MODE_INFO = {
    "model": MODEL,
    "mode": "RESEARCH_PAPER_ONLY",
    "trading": False,
    "orders": False,
}


def utc_now():
    return datetime.now(timezone.utc).isoformat()


def pct_change(old, new):
    if old == 0:
        return 0.0
    return ((new / old) - 1.0) * 100.0


def valid_base(base):
    return bool(base) and base not in EXCLUDED_BASES and not base.endswith(("UP","DOWN","BULL","BEAR"))

async def get_json(client, path, params=None):
    response = await client.get(f"{BINANCE}{path}", params=params, timeout=30.0)
    response.raise_for_status()
    return response.json()

async def _build_universe_uncached(client):
    exchange_info, tickers = await asyncio.gather(
        get_json(client, "/api/v3/exchangeInfo"),
        get_json(client, "/api/v3/ticker/24hr"),
    )

    ticker_map = {
        x["symbol"]: x
        for x in tickers
        if "symbol" in x
    }

    results = []

    for s in exchange_info.get("symbols", []):
        symbol = s.get("symbol")
        base = s.get("baseAsset")
        quote = s.get("quoteAsset")

        if base and base.upper() in V26_NON_ALT_BASE_EXCLUSIONS:
            continue

        if quote != "USDT":
            continue

        if s.get("status") != "TRADING":
            continue

        if not s.get("isSpotTradingAllowed", False):
            continue

        if not valid_base(base):
            continue

        ticker = ticker_map.get(symbol)

        if not ticker:
            continue

        try:
            quote_volume = float(ticker.get("quoteVolume", 0))
        except Exception:
            continue

        if quote_volume < MIN_QUOTE_VOLUME_USDT:
            continue

        results.append(
            {
                "symbol": symbol,
                "base_asset": base,
                "quote_volume_24h": round(quote_volume, 2),
            }
        )

    results.sort(
        key=lambda x: x["quote_volume_24h"],
        reverse=True,
    )

    return results


_UNIVERSE_CACHE = {"ts": 0.0, "data": None}
_UNIVERSE_LOCK = None
UNIVERSE_CACHE_TTL_SECONDS = 60


async def build_universe(client):
    """60 sn onbellekli evren; V27/V32/V37 ayni anda cagirsa tek istek atilir."""
    import time as _t
    global _UNIVERSE_LOCK
    if _UNIVERSE_LOCK is None:
        _UNIVERSE_LOCK = asyncio.Lock()
    async with _UNIVERSE_LOCK:
        now = _t.time()
        if (
            _UNIVERSE_CACHE["data"] is not None
            and now - _UNIVERSE_CACHE["ts"] < UNIVERSE_CACHE_TTL_SECONDS
        ):
            return [dict(x) for x in _UNIVERSE_CACHE["data"]]
        data = await _build_universe_uncached(client)
        _UNIVERSE_CACHE["data"] = data
        _UNIVERSE_CACHE["ts"] = now
        return [dict(x) for x in data]


async def get_completed_5m_candles(client, symbol, limit=100):
    raw = await get_json(
        client,
        "/api/v3/klines",
        params={
            "symbol": symbol,
            "interval": "5m",
            "limit": limit,
        },
    )

    now_ms = int(datetime.now(timezone.utc).timestamp() * 1000)

    candles = []

    for k in raw:
        if int(k[6]) >= now_ms:
            continue

        candles.append(
            {
                "open_time": int(k[0]),
                "open": float(k[1]),
                "high": float(k[2]),
                "low": float(k[3]),
                "close": float(k[4]),
                "volume": float(k[5]),
                "close_time": int(k[6]),
            }
        )

    return candles


V22_TG_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "").strip()
V22_TG_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "").strip()
async def v22_telegram_send(message):
    if not V22_TG_TOKEN or not V22_TG_CHAT_ID:
        return {"sent": False, "reason": "telegram_not_configured"}
    async with httpx.AsyncClient(timeout=20.0) as client:
        response = await client.post(
            f"https://api.telegram.org/bot{V22_TG_TOKEN}/sendMessage",
            json={"chat_id": V22_TG_CHAT_ID, "text": message, "disable_web_page_preview": True})
        response.raise_for_status()
    return {"sent": True}

V90_STATE = {"started_utc": None, "last_scan": None, "last_error": None,
             "scans": 0, "signals": 0, "seen": {}, "trades": [],
             "telegram": {"configured": bool(V22_TG_TOKEN and V22_TG_CHAT_ID), "sent": 0, "failed": 0, "last": None},
             "frozen_legacy": ["V32", "V32.2", "V32.3-B", "V89.4", "V89-100", "BIG_MOVE_V5"]}
V90_TASK = None
V90_LOCK = None
V90_MAX_TRADES = 600
V90_SCAN_SECONDS = 300
V90_COST_PCT = 0.15
V90_DELAY_SECONDS = 300
V90_STATE_PATH = Path(os.getenv("V90_STATE_PATH", "/tmp/v90_state.json"))


def v903_memory_mb():
    # Linux ru_maxrss is the peak resident set size in KiB.
    return round(resource.getrusage(resource.RUSAGE_SELF).ru_maxrss / 1024, 1)


def v903_save_state():
    """Best-effort local checkpoint; Render ephemeral disk is NOT durable storage."""
    try:
        payload = {k: V90_STATE[k] for k in ("trades", "seen", "signals", "telegram")}
        temp = V90_STATE_PATH.with_suffix(".tmp")
        temp.write_text(json.dumps(payload, separators=(",", ":")), encoding="utf-8")
        temp.replace(V90_STATE_PATH)
    except Exception as exc:
        V90_STATE["last_error"] = "checkpoint: " + str(exc)[:150]


def v903_load_state():
    try:
        if V90_STATE_PATH.is_file():
            saved = json.loads(V90_STATE_PATH.read_text(encoding="utf-8"))
            for k in ("trades", "seen", "signals"):
                if k in saved:
                    V90_STATE[k] = saved[k]
            # Preserve current env-based Telegram configured state.
            tg = saved.get("telegram", {})
            for k in ("sent", "failed", "last"):
                if k in tg:
                    V90_STATE["telegram"][k] = tg[k]
    except Exception as exc:
        V90_STATE["last_error"] = "restore: " + str(exc)[:150]



async def v902_notify(message, event, symbol):
    # Notifications are best-effort; failures must not change paper trades.
    try:
        result = await v22_telegram_send(message)
        ok = bool(result.get("sent"))
        reason = result.get("reason") if not ok else None
    except Exception as exc:
        ok = False
        reason = f"{type(exc).__name__}: {str(exc)[:180]}"
    state = V90_STATE["telegram"]
    state["sent" if ok else "failed"] += 1
    state["last"] = {"utc": utc_now(), "event": event, "symbol": symbol,
                     "sent": ok, "reason": reason}
    print(f"V903_TELEGRAM event={event} symbol={symbol} sent={ok} reason={reason}", flush=True)
    return state["last"]




def v90_close(trade, price, reason, now_ms):
    trade["exit_price"] = float(price)
    trade["exit_utc"] = utc_now()
    trade["exit_reason"] = reason
    trade["status"] = "CLOSED_SHADOW"
    trade["net_pct"] = round(((float(price) / trade["entry_price"]) - 1) * 100 - V90_COST_PCT, 5)
    trade["hold_minutes"] = round((now_ms - trade["entry_ms"]) / 60000, 2)


def v90_stats(rows):
    closed = [t for t in rows if t["status"] == "CLOSED_SHADOW"]
    vals = [t["net_pct"] for t in closed]
    gains = sum(v for v in vals if v > 0)
    losses = -sum(v for v in vals if v < 0)
    return {"closed":len(closed), "open":sum(t["status"] == "OPEN_SHADOW" for t in rows),
            "mean_net_pct":round(sum(vals)/len(vals),4) if vals else None,
            "win_rate_pct":round(100*sum(v>0 for v in vals)/len(vals),2) if vals else None,
            "profit_factor":round(gains/losses,4) if losses else None,
            "sum_trade_returns_pct":round(sum(vals),4) if vals else None}


async def v90_monitor(client):
    now_ms = int(time.time()*1000)
    active = [t for t in V90_STATE["trades"] if t["status"] == "OPEN_SHADOW"]
    for sym in set(t["symbol"] for t in active):
        try:
            quote = await get_json(client, "/api/v3/ticker/bookTicker", {"symbol":sym})
            bid = float(quote["bidPrice"])
            if bid <= 0: continue
        except Exception as exc:
            V90_STATE["last_error"] = "quote " + sym + ": " + str(exc)[:120]
            continue
        for t in active:
            if t["symbol"] != sym: continue
            t["max_bid"] = max(t.get("max_bid",t["entry_price"]), bid)
            t["min_bid"] = min(t.get("min_bid",t["entry_price"]), bid)
            t["last_bid"] = bid
            if t["variant"] == "B" and t["max_bid"] >= t["entry_price"] * 1.02:
                t["stop_price"] = max(t["stop_price"], t["entry_price"])
            if t["variant"] == "C" and not t.get("tp1_done") and bid >= t["entry_price"]*1.03:
                t["tp1_done"] = True
                t["tp1_price"] = bid
                t["tp1_utc"] = utc_now()
            if bid <= t["stop_price"]:
                v90_close(t, bid, "STOP_MONITORED", now_ms)
            elif now_ms >= t["entry_ms"] + 120*60000:
                v90_close(t, bid, "TIME120", now_ms)
            if t["status"] == "CLOSED_SHADOW" and t["variant"] == "C" and t.get("tp1_done"):
                t["net_pct"] = round(((((t["tp1_price"]/t["entry_price"])-1)*100)*0.5 +
                                      (((t["exit_price"]/t["entry_price"])-1)*100)*0.5 - V90_COST_PCT),5)
    closed_now = [t for t in active if t["status"] == "CLOSED_SHADOW" and not t.get("telegram_exit_attempted")]
    for t in closed_now:
        t["telegram_exit_attempted"] = True
        msg = (f"ALT V90.3 PAPER CIKIS\n{t['symbol']} | Model {t['variant']}\n"
               f"Net: {t['net_pct']:+.3f}% | Neden: {t['exit_reason']}\n"
               f"Giris: {t['entry_price']:.10g} | Cikis: {t['exit_price']:.10g}\n"
               "PAPER ONLY | GERCEK EMIR YOK")
        await v902_notify(msg, "EXIT_" + t["variant"], t["symbol"])
    if closed_now:
        v903_save_state()
    return len(active)


async def v90_scan_once():
    global V90_LOCK
    if V90_LOCK is None: V90_LOCK = asyncio.Lock()
    if V90_LOCK.locked(): return {"status":"BUSY"}
    async with V90_LOCK:
        V90_STATE["scans"] += 1
        scan = {"utc":utc_now(),"checked":0,"candidates":0,"new_coins":0,"errors":0, "filter_counts":{}, "near_misses":[]}
        try:
            async with httpx.AsyncClient(timeout=30.0, limits=httpx.Limits(max_connections=4, max_keepalive_connections=2)) as client:
                await v90_monitor(client)
                universe = (await build_universe(client))[:100]
                # Only one network scan, three paper valuation models.
                semaphore = asyncio.Semaphore(3)
                from collections import Counter
                rejected = Counter()
                near_misses = []
                async def check(item):
                    async with semaphore:
                        try:
                            candles = await get_completed_5m_candles(client,item["symbol"],limit=25)
                            if len(candles)<20:
                                rejected["insufficient_candles"] += 1
                                return None
                            cs = [x["close"] for x in candles]
                            vs = [x["volume"] for x in candles]
                            ret60 = pct_change(cs[-13],cs[-1])
                            avg = sum(vs[-7:-1])/6
                            vol = vs[-1]/avg if avg else 0
                            past_returns = [pct_change(cs[i-12],cs[i]) for i in range(12,len(cs)-1)]
                            mu = statistics.mean(past_returns) if past_returns else 0
                            sd = statistics.pstdev(past_returns) if len(past_returns)>1 else 0
                            z = (ret60-mu)/sd if sd else 0
                            breakout = pct_change(max(x["high"] for x in candles[-7:-1]),cs[-1])
                            if ret60 < 2.5:
                                rejected["momentum_below_2_5"] += 1
                                return None
                            if not 1.0 <= vol < 3.5:
                                rejected["volume_outside_1_0_3_5"] += 1
                                return None
                            if z >= 3.5:
                                rejected["z_at_least_3_5"] += 1
                                return None
                            if breakout < 0.2:
                                rejected["breakout_below_0_2"] += 1
                                if len(near_misses) < 10:
                                    near_misses.append({"symbol":item["symbol"],"ret60_pct":round(ret60,3),
                                                        "volume_ratio":round(vol,3),"z":round(z,3),
                                                        "breakout_pct":round(breakout,3),"blocked_by":"breakout"})
                                return None
                            signal_ms = candles[-1]["close_time"]
                            delay = (int(time.time()*1000)-signal_ms)/1000
                            if not 0<=delay<V90_DELAY_SECONDS:
                                rejected["signal_delay_outside_0_300_seconds"] += 1
                                if len(near_misses) < 10:
                                    near_misses.append({"symbol":item["symbol"],"ret60_pct":round(ret60,3),
                                                        "delay_seconds":round(delay,1),"blocked_by":"delay"})
                                return None
                            return {"symbol":item["symbol"],"ret60_pct":ret60,"volume_ratio":vol,
                                    "z":z,"breakout_pct":breakout,"signal_ms":signal_ms,
                                    "breakout_price":cs[-1]}
                        except Exception:
                            scan["errors"]+=1
                            rejected["candle_or_calculation_error"] += 1
                            return None
                # Small batches avoid retaining 100 tasks/responses simultaneously.
                candidates = []
                for offset in range(0, len(universe), 5):
                    batch = await asyncio.gather(*(check(i) for i in universe[offset:offset+5]))
                    candidates.extend(r for r in batch if r)
                    del batch
                scan["checked"] = len(universe)
                scan["candidates"] = len(candidates)
                scan["filter_counts"] = dict(rejected)
                scan["near_misses"] = near_misses
                # One coin at a time, max five independent coins; all A/B/C share entries.
                active_syms = {t["symbol"] for t in V90_STATE["trades"] if t["status"]=="OPEN_SHADOW"}
                for c in sorted(candidates,key=lambda r:r["ret60_pct"],reverse=True):
                    if len(active_syms)>=5:break
                    sym=c["symbol"]
                    key=f'{sym}:{c["signal_ms"]}'
                    if sym in active_syms or key in V90_STATE["seen"]:
                        scan["filter_counts"]["already_open_or_seen"] = scan["filter_counts"].get("already_open_or_seen",0)+1
                        continue
                    try:
                        q=await get_json(client,"/api/v3/ticker/bookTicker",{"symbol":sym})
                        ask=float(q["askPrice"]);bid=float(q["bidPrice"])
                        if ask<=0 or bid<=0:
                            scan["filter_counts"]["invalid_quote"] = scan["filter_counts"].get("invalid_quote",0)+1
                            continue
                        spread=(ask-bid)/ask*100
                        chase=pct_change(c["breakout_price"],ask)
                        if spread>0.15 or chase>0.5:
                            reason = "spread_above_0_15" if spread>0.15 else "chase_above_0_5"
                            scan["filter_counts"][reason] = scan["filter_counts"].get(reason,0)+1
                            continue
                        entry_ms=int(time.time()*1000)
                        for variant in ("A","B","C"):
                            V90_STATE["trades"].append({**c,"variant":variant,"status":"OPEN_SHADOW",
                                "entry_utc":utc_now(),"entry_ms":entry_ms,"entry_price":ask,
                                "stop_price":ask*0.97,"spread_pct":round(spread,5),
                                "chase_pct":round(chase,5),"max_bid":ask,"min_bid":ask,
                                "tp1_done":False,"research_only":True})
                        V90_STATE["seen"][key]=entry_ms
                        active_syms.add(sym)
                        scan["new_coins"]+=1
                        V90_STATE["signals"]+=1
                        msg = (f"ALT V90.3 PAPER GIRIS\n{sym} | A/B/C (tek sinyal)\n"
                               f"Giris: {ask:.10g} | Stop: {ask*0.97:.10g}\n"
                               f"Momentum60: {c['ret60_pct']:+.2f}% | Hacim: {c['volume_ratio']:.2f}x\n"
                               f"Breakout: {c['breakout_pct']:+.3f}% | Spread: {spread:.3f}%\n"
                               "PAPER ONLY | GERCEK EMIR YOK")
                        await v902_notify(msg, "ENTRY", sym)
                        v903_save_state()
                    except Exception:
                        scan["errors"]+=1
                # Keep completed results for review, bounded RAM.
                if len(V90_STATE["trades"])>V90_MAX_TRADES:
                    closed=[t for t in V90_STATE["trades"] if t["status"]=="CLOSED_SHADOW"]
                    keep=set(id(t) for t in closed[:max(0,len(V90_STATE["trades"])-V90_MAX_TRADES)])
                    V90_STATE["trades"]=[t for t in V90_STATE["trades"] if id(t) not in keep]
                cutoff=int(time.time()*1000)-7*86400000
                V90_STATE["seen"]={k:v for k,v in V90_STATE["seen"].items() if v>cutoff}
            V90_STATE["last_scan"]=scan
            v903_save_state()
            gc.collect()
            return scan
        except Exception as exc:
            V90_STATE["last_error"]=str(exc)[:300]
            scan["error"]=str(exc)[:300]
            V90_STATE["last_scan"]=scan
            return scan


async def v90_loop():
    while True:
        try: await v90_scan_once()
        except asyncio.CancelledError:raise
        except Exception as exc:V90_STATE["last_error"]=str(exc)[:250]
        await asyncio.sleep(V90_SCAN_SECONDS)


@app.on_event("startup")
async def v90_startup():
    global V90_TASK
    V90_STATE["started_utc"]=utc_now()
    v903_load_state()
    if V90_TASK is None or V90_TASK.done():V90_TASK=asyncio.create_task(v90_loop())


@app.get("/v90-status")
async def v90_status():
    return {**MODE_INFO,"status":"OK","strategy":"V90_HYBRID_MOMENTUM", "diagnostic_version":"V90_3_MEMORY_CHECKPOINT",
            "paper_only":True,"orders":False,"auto_running":bool(V90_TASK and not V90_TASK.done()),
            "scan_interval_seconds":V90_SCAN_SECONDS,"last_scan":V90_STATE["last_scan"],
            "last_error":V90_STATE["last_error"],"total_signals":V90_STATE["signals"],
            "signal_delay_limit_seconds":V90_DELAY_SECONDS,"telegram":V90_STATE["telegram"],
            "memory_peak_mb":v903_memory_mb(), "checkpoint_path":str(V90_STATE_PATH),
            "frozen_legacy":V90_STATE["frozen_legacy"],
            "variants":{v:v90_stats([t for t in V90_STATE["trades"] if t["variant"]==v]) for v in ("A","B","C")},
            "recent_trades":V90_STATE["trades"][-24:],
            "limitations":["Local checkpoint on ephemeral filesystem; may disappear on Render restart/deploy", "5-minute scan can miss signals", 
                           "Stop/TP monitored by quotes every 5 minutes; gaps and slippage possible",
                           "No real fills, no real orders", "Experimental thresholds not validated"]}


@app.get("/v90-filter-analysis")
async def v90_filter_analysis():
    last = V90_STATE.get("last_scan") or {}
    return {**MODE_INFO, "status":"OK", "strategy":"V90_3_FILTER_ANALYSIS",
            "auto_running":bool(V90_TASK and not V90_TASK.done()),
            "scan_interval_seconds":V90_SCAN_SECONDS, "signal_delay_limit_seconds":V90_DELAY_SECONDS,
            "last_scan_utc":last.get("utc"), "checked":last.get("checked"),
            "candidates":last.get("candidates"), "new_coins":last.get("new_coins"),
            "errors":last.get("errors"), "filter_counts":last.get("filter_counts",{}),
            "near_misses":last.get("near_misses",[]),
            "note":"First failed filter per coin; no extra market API calls; no parameter changes; paper only."}


V902_TEST_LAST_MONO = 0.0

@app.get("/v90-telegram-test")
async def v902_telegram_test():
    global V902_TEST_LAST_MONO
    now = time.monotonic()
    if V902_TEST_LAST_MONO and now - V902_TEST_LAST_MONO < 60:
        return {**MODE_INFO, "status": "RATE_LIMITED", "reason": "one_test_per_60_seconds"}
    V902_TEST_LAST_MONO = now
    delivery = await v902_notify("ALT V90.3 TELEGRAM TEST\nBildirim testi. Islem acilmadi.\nPAPER ONLY | GERCEK EMIR YOK", "TEST", "-")
    return {**MODE_INFO, "status": "OK" if delivery["sent"] else "NOT_SENT", "telegram": delivery}
