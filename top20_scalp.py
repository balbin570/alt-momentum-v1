"""Independent 5m/10m short-momentum spot PAPER scanner. No exchange orders."""
import asyncio
import json
import time
from datetime import datetime, timezone

import httpx

MODEL = "TOP20_SCALP_V1"
SCAN_SECONDS = 60
MAX_OPEN = 5
MAX_UNIVERSE = 400
MIN_DAILY_USDT = 1_000_000
TP_GROSS_PCT = 0.60
SL_GROSS_PCT = -0.40
TIME_EXIT_MINUTES = 15
COST_PCT = 0.15
MAX_SPREAD_PCT = 0.06
MAX_CHASE_PCT = 0.12
COOLDOWN_SECONDS = 180


def change_pct(first, last):
    return 100 * (last / first - 1) if first > 0 else 0.0


def select_leaders(five, ten, daily_volume):
    """Choose the top movers of each short window, not 24h gainers."""
    def metrics(rows):
        result = {}
        for x in rows:
            symbol = x.get("symbol")
            if symbol not in daily_volume:
                continue
            try:
                start, finish = float(x["openPrice"]), float(x["lastPrice"])
                if start <= 0 or finish <= 0:
                    continue
                result[symbol] = change_pct(start, finish)
            except (ValueError, TypeError, KeyError):
                continue
        return result

    m5, m10 = metrics(five), metrics(ten)
    leaders5 = [s for s in sorted(m5, key=m5.get, reverse=True) if m5[s] > 0][:20]
    leaders10 = [s for s in sorted(m10, key=m10.get, reverse=True) if m10[s] > 0][:20]
    union = set(leaders5 + leaders10)
    ranked = []
    for symbol in union:
        if symbol not in m5 or symbol not in m10:
            continue
        p5, p10 = m5[symbol], m10[symbol]
        if not (0.35 <= p5 <= 4.0 and 0.5 <= p10 <= 7.0):
            continue
        ranked.append({"symbol": symbol, "ret5_pct": round(p5, 5),
                       "ret10_pct": round(p10, 5),
                       "quote_volume_24h": daily_volume[symbol],
                       "score": round(0.65 * p5 + 0.175 * p10, 6)})
    ranked.sort(key=lambda x: (-x["score"], x["symbol"]))
    return ranked[:20]


def confirm_pullback(raw, now_ms):
    """Use completed one-minute candles only; require pullback then rebound."""
    bars = [k for k in raw if int(k[6]) < now_ms]
    if len(bars) < 12:
        return None
    last = bars[-1]
    if now_ms - int(last[6]) > 130_000:
        return None
    prev = bars[-2]
    recent_high = max(float(k[2]) for k in bars[-6:-1])
    prev_close, last_close = float(prev[4]), float(last[4])
    if min(recent_high, prev_close, last_close) <= 0:
        return None
    dip = change_pct(recent_high, prev_close)
    bounce = change_pct(prev_close, last_close)
    if not (-0.85 <= dip <= -0.12 and bounce >= 0.06):
        return None
    if last_close > recent_high * 1.0015:
        return None
    return {"signal_ms": int(last[6]), "signal_price": last_close,
            "pullback_pct": round(dip, 5), "rebound_pct": round(bounce, 5)}


class Top20Scalper:
    def __init__(self):
        self.report = {
            "last_scan_utc": None, "last_monitor_utc": None, "scans": 0,
            "monitor_checks": 0, "universe": 0, "leaders": [],
            "new_positions": 0, "last_scan_rejections": {},
            "last_error": None, "last_scan_duration_seconds": None,
        }

    async def rolling(self, core, client, symbols, window):
        """Binance /ticker accepts <=100 symbols per request, MINI uses open/last price."""
        chunks = [symbols[i:i + 80] for i in range(0, len(symbols), 80)]
        semaphore = asyncio.Semaphore(3)
        async def fetch(chunk):
            async with semaphore:
                answer = await core.get_json(client, "/api/v3/ticker",
                    {"symbols": json.dumps(chunk, separators=(",", ":")),
                     "windowSize": window, "type": "MINI"})
                return answer if isinstance(answer, list) else [answer]
        results = await asyncio.gather(*(fetch(c) for c in chunks))
        return [x for batch in results for x in batch]

    async def monitor(self, core, client):
        """Paper TP/SL/time at observed bid; not a live exchange resting stop."""
        if core.V90_LOCK is None:
            core.V90_LOCK = asyncio.Lock()
        async with core.V90_LOCK:
            open_symbols = list({t["symbol"] for t in core.V90_STATE["trades"]
                if t.get("variant") == MODEL and t.get("status") == "OPEN_SHADOW"})
        self.report["monitor_checks"] += 1
        self.report["last_monitor_utc"] = core.utc_now()
        if not open_symbols:
            return
        quotes = {}
        for symbol in open_symbols:
            q = await core.get_json(client, "/api/v3/ticker/bookTicker", {"symbol": symbol})
            price = float(q["bidPrice"])
            if price > 0:
                quotes[symbol] = price
        now_ms = int(time.time() * 1000)
        closed = []
        async with core.V90_LOCK:
            for t in core.V90_STATE["trades"]:
                if t.get("variant") != MODEL or t.get("status") != "OPEN_SHADOW":
                    continue
                bid = quotes.get(t["symbol"])
                if bid is None:
                    continue
                t["last_bid"] = bid
                t["max_bid"] = max(t.get("max_bid", t["entry_price"]), bid)
                t["min_bid"] = min(t.get("min_bid", t["entry_price"]), bid)
                if bid <= t["stop_price"]:
                    reason = "STOP_MONITORED"
                elif bid >= t["take_profit_price"]:
                    reason = "TAKE_PROFIT_MONITORED"
                elif now_ms >= t["entry_ms"] + TIME_EXIT_MINUTES * 60_000:
                    reason = "TIME15"
                else:
                    continue
                core.v90_close(t, bid, reason, now_ms)
                t["telegram_exit_attempted"] = True
                closed.append(t)
            if closed:
                core.v903_save_state()
                if not core.V915_STORAGE["last_save_ok"]:
                    self.report["last_error"] = "CHECKPOINT_SAVE_FAILED_ON_EXIT"
                    return
        for trade in closed:
            await core.v902_notify(
                f"TOP20 SCALP PAPER CIKIS\n{trade['symbol']} | "
                f"Net: {trade['net_pct']:+.3f}% | {trade['exit_reason']}\n"
                "GERCEK EMIR YOK", "EXIT_" + MODEL, trade["symbol"])
        if closed:
            async with core.V90_LOCK:
                core.v903_save_state()

    async def scan(self, core, client):
        universe = [x for x in await core.build_universe(client)
                    if float(x.get("quote_volume_24h", 0)) >= MIN_DAILY_USDT
                    and isinstance(x.get("symbol"), str)
                    and x["symbol"].isascii() and x["symbol"].isalnum()
                    and x["symbol"].endswith("USDT")][:MAX_UNIVERSE]
        daily = {x["symbol"]: float(x["quote_volume_24h"]) for x in universe}
        symbols = list(daily)
        self.report["universe"] = len(symbols)
        if not symbols:
            raise ValueError("NO_ELIGIBLE_SPOT_PAIRS")
        five, ten = await asyncio.gather(
            self.rolling(core, client, symbols, "5m"),
            self.rolling(core, client, symbols, "10m"))
        leaders = select_leaders(five, ten, daily)
        self.report["leaders"] = leaders
        rejects = {"no_rebound": 0, "price_or_spread": 0, "already_open_or_cooldown": 0,
                   "max_open": 0, "data_error": 0, "storage_unavailable": 0}
        self.report["new_positions"] = 0
        for candidate in leaders:
            symbol = candidate["symbol"]
            if core.V90_LOCK is None:
                core.V90_LOCK = asyncio.Lock()
            async with core.V90_LOCK:
                open_rows = [t for t in core.V90_STATE["trades"]
                             if t.get("variant") == MODEL and t.get("status") == "OPEN_SHADOW"]
                if len(open_rows) >= MAX_OPEN:
                    rejects["max_open"] += 1
                    break
                previous = [t for t in reversed(core.V90_STATE["trades"])
                            if t.get("variant") == MODEL and t.get("symbol") == symbol]
                previous_event_ms = previous[0]["entry_ms"] if previous else 0
                if previous and previous[0].get("exit_utc"):
                    try:
                        previous_event_ms = int(datetime.fromisoformat(
                            previous[0]["exit_utc"].replace("Z", "+00:00")).timestamp() * 1000)
                    except (ValueError, TypeError):
                        pass
                if (any(t["symbol"] == symbol for t in open_rows) or
                    (previous and int(time.time() * 1000) - previous_event_ms
                     < COOLDOWN_SECONDS * 1000)):
                    rejects["already_open_or_cooldown"] += 1
                    continue
            try:
                raw = await core.get_json(client, "/api/v3/klines",
                                          {"symbol": symbol, "interval": "1m", "limit": 15})
                signal = confirm_pullback(raw, int(time.time() * 1000))
                if signal is None:
                    rejects["no_rebound"] += 1
                    continue
                quote = await core.get_json(client, "/api/v3/ticker/bookTicker",
                                            {"symbol": symbol})
                bid, ask = float(quote["bidPrice"]), float(quote["askPrice"])
                if bid <= 0 or ask <= 0 or ask < bid:
                    rejects["price_or_spread"] += 1
                    continue
                spread = (ask - bid) / ask * 100
                chase = change_pct(signal["signal_price"], ask)
                if spread > MAX_SPREAD_PCT or chase > MAX_CHASE_PCT or chase < -0.75:
                    rejects["price_or_spread"] += 1
                    continue
            except Exception:
                rejects["data_error"] += 1
                continue
            new_trade = None
            async with core.V90_LOCK:
                open_rows = [t for t in core.V90_STATE["trades"]
                             if t.get("variant") == MODEL and t.get("status") == "OPEN_SHADOW"]
                seen_key = f"{MODEL}:{symbol}:{signal['signal_ms']}"
                if (len(open_rows) >= MAX_OPEN or
                    any(t["symbol"] == symbol for t in open_rows) or
                    seen_key in core.V90_STATE["seen"]):
                    rejects["already_open_or_cooldown"] += 1
                    continue
                if core.V915_STORAGE.get("last_save_ok") is False or \
                   core.V915_STORAGE.get("last_restore_status") != "RESTORED":
                    rejects["storage_unavailable"] += 1
                    continue
                entry_ms = int(time.time() * 1000)
                new_trade = {**candidate, **signal, "variant": MODEL,
                    "symbol": symbol, "status": "OPEN_SHADOW",
                    "entry_ms": entry_ms, "entry_utc": core.utc_now(), "entry_price": ask,
                    "stop_price": ask * (1 + SL_GROSS_PCT / 100),
                    "take_profit_price": ask * (1 + TP_GROSS_PCT / 100),
                    "spread_pct": round(spread, 5), "chase_pct": round(chase, 5),
                    "max_bid": ask, "min_bid": ask,
                    "research_only": True, "orders": False,
                    "paper_notional_usdt": 100,
                    "assumed_cost_pct": COST_PCT,
                    "exit_monitor_seconds": SCAN_SECONDS,
                    "telegram_exit_attempted": False}
                core.V90_STATE["trades"].append(new_trade)
                core.V90_STATE["seen"][seen_key] = entry_ms
                core.v903_save_state()
                if not core.V915_STORAGE["last_save_ok"]:
                    core.V90_STATE["trades"].pop()
                    core.V90_STATE["seen"].pop(seen_key, None)
                    rejects["storage_unavailable"] += 1
                    new_trade = None
            if new_trade is not None:
                self.report["new_positions"] += 1
                await core.v902_notify(
                    f"TOP20 SCALP PAPER GIRIS\n{symbol}\n"
                    f"5dk: {candidate['ret5_pct']:+.2f}% | 10dk: {candidate['ret10_pct']:+.2f}%\n"
                    f"Giris: {ask:.10g} | TP: +{TP_GROSS_PCT:.2f}% | "
                    f"SL: {SL_GROSS_PCT:.2f}%\nPAPER ONLY | GERCEK EMIR YOK",
                    "ENTRY_" + MODEL, symbol)
                async with core.V90_LOCK:
                    core.v903_save_state()
        self.report["last_scan_rejections"] = rejects

    async def cycle(self, core):
        start = time.monotonic()
        self.report["scans"] += 1
        if (core.V915_STORAGE.get("last_restore_status") != "RESTORED"
            or core.V915_STORAGE.get("last_save_ok") is False):
            self.report["last_error"] = "CHECKPOINT_NOT_READY"
            return
        try:
            async with httpx.AsyncClient(timeout=30.0,
                    limits=httpx.Limits(max_connections=8, max_keepalive_connections=5)) as client:
                await self.monitor(core, client)
                if core.V915_STORAGE.get("last_save_ok") is False:
                    self.report["last_error"] = "CHECKPOINT_SAVE_FAILED_ON_EXIT"
                    return
                await self.scan(core, client)
            self.report["last_error"] = (None if core.V915_STORAGE.get("last_save_ok")
                                         else "CHECKPOINT_SAVE_FAILED_ON_ENTRY")
        except asyncio.CancelledError:
            raise
        except httpx.HTTPStatusError as exc:
            try:
                body = exc.response.json()
                code = body.get("code") if isinstance(body, dict) else None
            except (ValueError, TypeError):
                code = None
            self.report["last_error"] = (
                f"BINANCE_HTTP_{exc.response.status_code}_CODE_{code}"
                f"_PATH_{exc.request.url.path}")
        except Exception as exc:
            self.report["last_error"] = type(exc).__name__
        finally:
            self.report["last_scan_utc"] = core.utc_now()
            self.report["last_scan_duration_seconds"] = round(time.monotonic() - start, 2)

    def status(self, core, running):
        rows = [t for t in core.V90_STATE["trades"] if t.get("variant") == MODEL]
        return {**core.MODE_INFO, "status": "OK" if self.report["last_error"] is None else "DEGRADED",
            "model_name": MODEL, "paper_only": True, "auto_running": running,
            "scan_interval_seconds": SCAN_SECONDS, "max_open": MAX_OPEN,
            "universe_limit": MAX_UNIVERSE, "min_24h_quote_volume_usdt": MIN_DAILY_USDT,
            "volume_spike_entry_required": False,
            "top_movers_windows_minutes": [5, 10],
            "tp_gross_pct": TP_GROSS_PCT, "sl_gross_pct": SL_GROSS_PCT,
            "time_exit_minutes": TIME_EXIT_MINUTES,
            "assumed_round_trip_cost_pct": COST_PCT,
            "max_spread_pct": MAX_SPREAD_PCT, "exit_monitor_seconds": SCAN_SECONDS,
            "stats": core.v90_stats(rows),
            "open_positions": [{"symbol": t["symbol"], "entry_utc": t["entry_utc"],
                                "entry_price": t["entry_price"], "take_profit_price": t["take_profit_price"],
                                "stop_price": t["stop_price"]} for t in rows
                               if t.get("status") == "OPEN_SHADOW"],
            "last_report": dict(self.report),
            "note": "Sampled paper exits; no guaranteed fills. Ranking covers at most 400 liquid spot USDT coins."}
