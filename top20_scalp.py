"""Early momentum spot PAPER scanner. No exchange orders."""
import asyncio
import json
import time
from datetime import datetime, timezone

import httpx

MODEL = "TOP20_SCALP_V5"
ACTIVE_MODELS = (MODEL, "TOP20_SCALP_V3", "TOP20_SCALP_V4", "TOP20_SCALP_V2", "TOP20_SCALP_V1")
MIN_RET10_PCT = 0.0
MAX_RET10_PCT = 1.50
MONITOR_SECONDS = 10
SCAN_SECONDS = 60
MAX_OPEN = 5
MAX_UNIVERSE = 100
MIN_DAILY_USDT = 1_000_000
TP_GROSS_PCT = 0.40
SL_GROSS_PCT = -0.25
TIME_EXIT_MINUTES = 5
CONFIRM_SECONDS = 25
CONFIRM_MIN_BID_RISE_PCT = 0.03
EARLY_EXIT_SECONDS = 60
EARLY_EXIT_LOSS_PCT = -0.10
EARLY_EXIT_MAX_RISE_PCT = 0.15
COST_PCT = 0.15
MAX_SPREAD_PCT = 0.06
MAX_CHASE_PCT = 0.12
COOLDOWN_SECONDS = 180


def change_pct(first, last):
    return 100 * (last / first - 1) if first > 0 else 0.0


def confirm_early_momentum(raw, now_ms):
    """Exact completed 1m candles; reject extended moves and require volume."""
    bars = [k for k in raw if int(k[6]) < now_ms]
    if len(bars) < 11 or now_ms - int(bars[-1][6]) > 130_000:
        return None
    if any(int(bars[i][0]) - int(bars[i-1][0]) != 60000 for i in range(len(bars)-10, len(bars))):
        return None
    last = bars[-1]
    price = float(last[4])
    if min(float(k[4]) for k in bars[-11:]) <= 0:
        return None
    p1, p3, p5, p10 = [change_pct(float(bars[-1-n][4]), price) for n in (1,3,5,10)]
    baseline = sum(float(k[7]) for k in bars[-11:-1]) / 10
    ratio = float(last[7]) / baseline if baseline > 0 else 0
    if not (p1 >= 0.10 and p3 >= 0.20 and 0.15 <= p5 <= 0.80
            and MIN_RET10_PCT < p10 <= MAX_RET10_PCT and ratio >= 1.5 and price > float(last[1])):
        return None
    return {"signal_ms": int(last[6]), "signal_price": price,
            "ret1_pct": round(p1,5), "ret3_pct": round(p3,5),
            "ret5_pct": round(p5,5), "ret10_pct": round(p10,5),
            "volume_ratio": round(ratio,5),
            "score": round(p1 + 0.5*p3 + 0.1*min(ratio,4),6)}


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
                if t.get("variant") in ACTIVE_MODELS and t.get("status") == "OPEN_SHADOW"})
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
                if t.get("variant") not in ACTIVE_MODELS or t.get("status") != "OPEN_SHADOW":
                    continue
                bid = quotes.get(t["symbol"])
                if bid is None:
                    continue
                t["last_bid"] = bid
                t["max_bid"] = max(t.get("max_bid", t["entry_price"]), bid)
                t["min_bid"] = min(t.get("min_bid", t["entry_price"]), bid)
                age_ms = now_ms - t["entry_ms"]
                is_v5 = t.get("variant") == MODEL
                if is_v5:
                    samples = t.setdefault("early_bid_samples", [])
                    if age_ms <= 120_000:
                        samples.append({"age_seconds": round(age_ms / 1000, 2), "bid": bid})
                if bid <= t["stop_price"]:
                    reason = "STOP_MONITORED"
                elif bid >= t["take_profit_price"]:
                    reason = "TAKE_PROFIT_MONITORED"
                elif is_v5 and age_ms >= EARLY_EXIT_SECONDS * 1000 and change_pct(t["entry_price"], bid) <= EARLY_EXIT_LOSS_PCT and change_pct(t["entry_price"], t["max_bid"]) < EARLY_EXIT_MAX_RISE_PCT:
                    reason = "MOMENTUM_LOST"
                elif age_ms >= (TIME_EXIT_MINUTES if is_v5 else 10) * 60_000:
                    reason = "TIME5" if is_v5 else "TIME10"
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
                f"{trade['variant']} PAPER CIKIS\n{trade['symbol']} | "
                f"Net: {trade['net_pct']:+.3f}% | {trade['exit_reason']}\n"
                "GERCEK EMIR YOK", "EXIT_" + trade["variant"], trade["symbol"])
        if closed:
            async with core.V90_LOCK:
                core.v903_save_state()

    async def scan(self, core, client):
        universe = [x for x in await core.build_universe(client)
                    if float(x.get("quote_volume_24h", 0)) >= MIN_DAILY_USDT
                    and isinstance(x.get("symbol"), str)
                    and x["symbol"].isascii() and x["symbol"].isalnum()
                    and x["symbol"].endswith("USDT")]
        universe = sorted(universe, key=lambda x: float(x["quote_volume_24h"]), reverse=True)[:MAX_UNIVERSE]
        daily = {x["symbol"]: float(x["quote_volume_24h"]) for x in universe}
        symbols = list(daily)
        self.report["universe"] = len(symbols)
        if not symbols:
            raise ValueError("NO_ELIGIBLE_SPOT_PAIRS")
        rejects = {"no_early_momentum": 0, "price_or_spread": 0, "already_open_or_cooldown": 0,
                   "max_open": 0, "data_error": 0, "storage_unavailable": 0}
        semaphore = asyncio.Semaphore(5)
        async def evaluate(symbol):
            async with semaphore:
                try:
                    raw = await core.get_json(client, "/api/v3/klines",
                        {"symbol": symbol, "interval": "1m", "limit": 15})
                    signal = confirm_early_momentum(raw, int(time.time()*1000))
                    if signal is None:
                        rejects["no_early_momentum"] += 1
                        return None
                    return {"symbol": symbol, "quote_volume_24h": daily[symbol], **signal}
                except Exception:
                    rejects["data_error"] += 1
                    return None
        results = await asyncio.gather(*(evaluate(symbol) for symbol in symbols))
        leaders = sorted((x for x in results if x is not None),
                         key=lambda x: (-x["score"], x["symbol"]))[:20]
        self.report["leaders"] = leaders
        self.report["new_positions"] = 0
        # Keep the hybrid independent of scalp confirmation and exit rules.
        hybrid_leaders = sorted((x for x in results if x is not None and x["ret10_pct"] >= 0.40),
                                key=lambda x: (-x["score"], x["symbol"]))[:20]
        await self.scan_hybrid(core, client, hybrid_leaders)
        rejects["confirmation_failed"] = 0
        async def confirmation_quote(candidate):
            async with semaphore:
                try:
                    q = await core.get_json(client, "/api/v3/ticker/bookTicker", {"symbol": candidate["symbol"]})
                    bid, ask = float(q["bidPrice"]), float(q["askPrice"])
                    if not (0 < bid <= ask) or (ask-bid)/ask*100 > MAX_SPREAD_PCT:
                        return None
                    return (bid, ask, time.monotonic())
                except Exception:
                    return None
        first_quotes = await asyncio.gather(*(confirmation_quote(c) for c in leaders))
        if any(q is not None for q in first_quotes):
            await asyncio.sleep(CONFIRM_SECONDS)
        second_quotes = await asyncio.gather(*(confirmation_quote(c) for c in leaders))
        confirmed = []
        for candidate, first, second in zip(leaders, first_quotes, second_quotes):
            if first is None or second is None or second[2]-first[2] < CONFIRM_SECONDS or change_pct(first[0], second[0]) < CONFIRM_MIN_BID_RISE_PCT:
                rejects["confirmation_failed"] += 1
                continue
            confirmed.append({**candidate, "confirmation_bid_start": first[0],
                              "confirmation_bid_end": second[0],
                              "confirmation_seconds": round(second[2]-first[2], 2),
                              "confirmation_rise_pct": round(change_pct(first[0], second[0]), 5)})
        self.report["confirmed_leaders"] = len(confirmed)
        for candidate in confirmed:
            symbol = candidate["symbol"]
            if core.V90_LOCK is None:
                core.V90_LOCK = asyncio.Lock()
            async with core.V90_LOCK:
                open_rows = [t for t in core.V90_STATE["trades"]
                             if t.get("variant") in ACTIVE_MODELS and t.get("status") == "OPEN_SHADOW"]
                if len(open_rows) >= MAX_OPEN:
                    rejects["max_open"] += 1
                    break
                previous = [t for t in reversed(core.V90_STATE["trades"])
                            if t.get("variant") in ACTIVE_MODELS and t.get("symbol") == symbol]
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
                signal = candidate
                quote = await core.get_json(client, "/api/v3/ticker/bookTicker",
                                            {"symbol": symbol})
                bid, ask = float(quote["bidPrice"]), float(quote["askPrice"])
                if bid <= 0 or ask <= 0 or ask < bid:
                    rejects["price_or_spread"] += 1
                    continue
                if bid < candidate["confirmation_bid_end"]:
                    rejects["confirmation_failed"] += 1
                    continue
                spread = (ask - bid) / ask * 100
                chase = change_pct(signal["signal_price"], ask)
                if spread > MAX_SPREAD_PCT or chase > MAX_CHASE_PCT or chase < -0.10:
                    rejects["price_or_spread"] += 1
                    continue
            except Exception:
                rejects["data_error"] += 1
                continue
            new_trade = None
            async with core.V90_LOCK:
                open_rows = [t for t in core.V90_STATE["trades"]
                             if t.get("variant") in ACTIVE_MODELS and t.get("status") == "OPEN_SHADOW"]
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
                    "max_bid": ask, "min_bid": ask, "early_bid_samples": [],
                    "research_only": True, "orders": False,
                    "paper_notional_usdt": 100,
                    "assumed_cost_pct": COST_PCT,
                    "exit_monitor_seconds": MONITOR_SECONDS,
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
                    f"TOP20 SCALP V5 CONFIRMED MOMENTUM PAPER GIRIS\n{symbol}\n"
                    f"5dk: {candidate['ret5_pct']:+.2f}% | 10dk: {candidate['ret10_pct']:+.2f}%\n"
                    f"Giris: {ask:.10g} | TP: +{TP_GROSS_PCT:.2f}% | "
                    f"SL: {SL_GROSS_PCT:.2f}%\nPAPER ONLY | GERCEK EMIR YOK",
                    "ENTRY_" + MODEL, symbol)
                async with core.V90_LOCK:
                    core.v903_save_state()
        self.report["last_scan_rejections"] = rejects
 
    async def scan_hybrid(self, core, client, leaders):
        hybrid_model = "C_SHADOW_V4_C_EXIT"
        hybrid_models = (hybrid_model, "C_SHADOW_MOM1")
        rejects = {"price_or_spread": 0, "already_open_or_cooldown": 0,
                   "max_open": 0, "data_error": 0, "storage_unavailable": 0}
        self.report["hybrid_new_positions"] = 0
        for candidate in leaders:
            symbol = candidate["symbol"]
            if core.V90_LOCK is None:
                core.V90_LOCK = asyncio.Lock()
            async with core.V90_LOCK:
                open_rows = [t for t in core.V90_STATE["trades"]
                             if t.get("variant") in hybrid_models and t.get("status") == "OPEN_SHADOW"]
                if len(open_rows) >= MAX_OPEN:
                    rejects["max_open"] += 1
                    break
                previous = [t for t in reversed(core.V90_STATE["trades"])
                            if t.get("variant") in hybrid_models and t.get("symbol") == symbol]
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
                signal = candidate
                quote = await core.get_json(client, "/api/v3/ticker/bookTicker",
                                            {"symbol": symbol})
                bid, ask = float(quote["bidPrice"]), float(quote["askPrice"])
                if bid <= 0 or ask <= 0 or ask < bid:
                    rejects["price_or_spread"] += 1
                    continue
                spread = (ask - bid) / ask * 100
                chase = change_pct(signal["signal_price"], ask)
                if spread > MAX_SPREAD_PCT or chase > MAX_CHASE_PCT or chase < -0.10:
                    rejects["price_or_spread"] += 1
                    continue
            except Exception:
                rejects["data_error"] += 1
                continue
            new_trade = None
            async with core.V90_LOCK:
                open_rows = [t for t in core.V90_STATE["trades"]
                             if t.get("variant") in hybrid_models and t.get("status") == "OPEN_SHADOW"]
                seen_key = f"{hybrid_model}:{symbol}:{signal['signal_ms']}"
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
                new_trade = {**candidate, **signal, "variant": hybrid_model,
                    "symbol": symbol, "status": "OPEN_SHADOW",
                    "entry_ms": entry_ms, "entry_utc": core.utc_now(), "entry_price": ask,
                    "stop_price": ask * 0.97,
                    "tp1_done": False, "trail_active": False, "be_armed": False,
                    "c_forward_version": core.C_FORWARD_VERSION,
                    "shadow_entry_version": hybrid_model,
                    "spread_pct": round(spread, 5), "chase_pct": round(chase, 5),
                    "max_bid": ask, "min_bid": ask,
                    "research_only": True, "orders": False,
                    "paper_notional_usdt": 100,
                    "assumed_cost_pct": COST_PCT,
                    "exit_monitor_seconds": core.C_MONITOR_SECONDS,
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
                self.report["hybrid_new_positions"] += 1
                await core.v902_notify(
                    f"C SHADOW V4 C EXIT PAPER GIRIS\n{symbol}\n"
                    f"5dk: {candidate['ret5_pct']:+.2f}% | 10dk: {candidate['ret10_pct']:+.2f}%\n"
                    f"Giris: {ask:.10g} | Yari TP: +3% | SL: -3% | 120dk\n"
                    "PAPER ONLY | GERCEK EMIR YOK",
                    "ENTRY_" + hybrid_model, symbol)
                async with core.V90_LOCK:
                    core.v903_save_state()
        self.report["hybrid_rejections"] = rejects

    async def monitor_loop(self, core):
        async with httpx.AsyncClient(timeout=8.0) as client:
            while True:
                started = time.monotonic()
                try:
                    if core.V915_STORAGE.get("last_restore_status") == "RESTORED" and core.V915_STORAGE.get("last_save_ok") is not False:
                        await self.monitor(core, client)
                except asyncio.CancelledError:
                    raise
                except Exception as exc:
                    self.report["last_monitor_error"] = type(exc).__name__
                else:
                    self.report["last_monitor_error"] = None
                await asyncio.sleep(max(0.1, MONITOR_SECONDS - (time.monotonic() - started)))

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
        open_rows = [t for t in core.V90_STATE["trades"] if t.get("variant") in ACTIVE_MODELS and t.get("status") == "OPEN_SHADOW"]
        return {**core.MODE_INFO, "status": "OK" if self.report["last_error"] is None else "DEGRADED",
            "model_name": MODEL, "paper_only": True, "auto_running": running,
            "scan_interval_seconds": SCAN_SECONDS, "max_open": MAX_OPEN,
            "universe_limit": MAX_UNIVERSE, "min_24h_quote_volume_usdt": MIN_DAILY_USDT,
            "volume_spike_entry_required": True, "min_volume_ratio": 1.5,
            "momentum_windows_minutes": [1, 3, 5, 10],
            "min_ret10_pct": MIN_RET10_PCT, "max_ret10_pct": MAX_RET10_PCT,
            "tp_gross_pct": TP_GROSS_PCT, "sl_gross_pct": SL_GROSS_PCT,
            "time_exit_minutes": TIME_EXIT_MINUTES,
            "confirmation_seconds": CONFIRM_SECONDS, "confirmation_min_bid_rise_pct": CONFIRM_MIN_BID_RISE_PCT,
            "early_exit_seconds": EARLY_EXIT_SECONDS, "early_exit_loss_pct": EARLY_EXIT_LOSS_PCT,
            "early_exit_max_rise_pct": EARLY_EXIT_MAX_RISE_PCT,
            "assumed_round_trip_cost_pct": COST_PCT,
            "max_spread_pct": MAX_SPREAD_PCT, "exit_monitor_seconds": MONITOR_SECONDS,
            "stats": core.v90_stats(rows),
            "ret10_high_band_stats": core.v90_stats([t for t in rows if 1.0 <= t.get("ret10_pct", 0) <= MAX_RET10_PCT]),
            "open_positions": [{"symbol": t["symbol"], "entry_utc": t["entry_utc"],
                                "entry_price": t["entry_price"], "take_profit_price": t["take_profit_price"],
                                "stop_price": t["stop_price"]} for t in open_rows],
            "last_report": dict(self.report),
            "note": "Sampled paper exits; no guaranteed fills. Early momentum scans at most 100 liquid spot USDT coins."}
