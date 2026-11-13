"""V90.2 standalone paper research scanner. No order endpoints or API keys."""
import asyncio
import hashlib
import json
import gc
import resource
from pathlib import Path
import os
import statistics
import tempfile
import time
from datetime import datetime, timezone
import httpx
from fastapi import FastAPI
from paper_store import SupabasePaperStore
from top20_scalp import Top20Scalper, MODEL as TOP20_MODEL, SCAN_SECONDS as TOP20_SCAN_SECONDS, ACTIVE_MODELS as TOP20_ACTIVE_MODELS

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
             "filter_observations": {}, "recovery_metadata": {},
             "telegram": {"configured": bool(V22_TG_TOKEN and V22_TG_CHAT_ID), "sent": 0, "failed": 0, "last": None},
             "frozen_legacy": ["V32", "V32.2", "V32.3-B", "V89.4", "V89-100", "BIG_MOVE_V5"]}
V90_TASK = None
C_MONITOR_TASK = None
TOP20_TASK = None
TOP20_MONITOR_TASK = None
TOP20_SCALPER = Top20Scalper()
C_MONITOR_SECONDS = 60
C_FORWARD_VERSION = "C_FORWARD_SAFETY_V1"
C_MONITOR_STATE = {"last_check_utc": None, "checks": 0, "last_error": None}
V90_LOCK = None
# Historical paper trades are never automatically pruned.
V91_MODELS = ("C", "C_TREND", "C_TRAIL", "C_TIME60", "C_MARKET")
V93_TP_TARGETS = {"C_TP1": 1.0, "C_TP2": 2.0, "C_TP3": 3.0, "C_TP4": 4.0}
# V93 risk research arms: C itself and all previous models remain unchanged.
V93_RISK_MODELS = ("C_SL2", "C_SL25", "C_BE", "C_TP2_SL25")
V93_RISK_STOPS = {"C_SL2": 0.98, "C_SL25": 0.975, "C_TP2_SL25": 0.975}
V94_MODELS = ("V94_A", "V94_B", "V94_C")
V95_MODELS = ("V95_EARLY5", "V95_EARLY10")
V96_RETIRED = V94_MODELS  # no NEW V94 entries; historical open trades still monitored
V97_TRAIL_MODELS = ("C_SL5_TRAIL5", "C_SL10_TRAIL5")
V97_TRAIL_STOPS = {"C_SL5_TRAIL5": 0.95, "C_SL10_TRAIL5": 0.90}
# Only baseline C may open NEW paper trades (user-selected research reference).
# Previously opened trades from all retired models are still monitored to closure.
V93_ALL_ACTIVE = ("C",)
# New independent forward-only paper research model. Baseline C stays untouched.
C_SHADOW_MODELS = ("C_SHADOW_MOM1", "C_SHADOW_V4_C_EXIT")
C_ENTRY_MODELS = V93_ALL_ACTIVE  # hybrid entries are generated by shared V4 scanner
C_NEW_VARIANTS = ("C_LADDER3", "C_LADDER4", "C_PROTECT", "C_TRAIL_PRO", "C_TIME_ADAPT")
C_FROZEN_VARIANTS = ("C_TREND", "C_BE", "C_OBS120", "C_CONFIRM1", *C_NEW_VARIANTS)
# Passive mirror of C_TREND entries; not an active signal model.
C_OBS120 = "C_OBS120"
C_CONFIRM1 = "C_CONFIRM1"  # Independent 15-minute +1% confirmation research arm
V92_RETIRED = ("V91_PULLBACK", "V91_MOMENTUM", "V91_REGIME", "V913_PULLBACK_MARKET", "V913_PULLBACK_TRAIL15")
V92_ALL_MODELS = V91_MODELS + V92_RETIRED
V91_FROZEN = ("A", "B")
V90_SCAN_SECONDS = 60
V90_COST_PCT = 0.15
V90_DELAY_SECONDS = 300
V90_STATE_PATH = Path(os.getenv("V90_STATE_PATH", "/tmp/v90_state.json"))
C_REMOTE_STORE = SupabasePaperStore.from_environment()


def v903_memory_mb():
    # Linux ru_maxrss is the peak resident set size in KiB.
    return round(resource.getrusage(resource.RUSAGE_SELF).ru_maxrss / 1024, 1)


V915_STORAGE = {"last_save_utc": None, "last_save_ok": None, "last_save_error": None,
                "last_restore_utc": None, "last_restore_status": "NOT_ATTEMPTED",
                "restored_trades": 0, "save_count_since_start": 0}


def v903_save_state():
    """Atomic checkpoint to configured disk; errors exposed in storage status."""
    if V915_STORAGE["last_restore_status"].startswith(("ERROR:", "MISSING_CHECKPOINT")):
        V915_STORAGE["last_save_ok"] = False
        V915_STORAGE["last_save_error"] = "SAVE_BLOCKED_AFTER_RESTORE_ERROR"
        return
    tmp = None
    try:
        if C_REMOTE_STORE is not None:
            payload = {k: V90_STATE[k] for k in ("trades", "seen", "signals", "telegram", "filter_observations", "recovery_metadata")}
            result = C_REMOTE_STORE.save(payload)
            V915_STORAGE.update(last_save_utc=utc_now(), last_save_ok=True,
                last_save_error=None, remote_revision=result["revision"],
                compressed_checkpoint_bytes=result["compressed_bytes"])
            V915_STORAGE["save_count_since_start"] += 1
            return
        V90_STATE_PATH.parent.mkdir(parents=True, exist_ok=True)
        payload = {k: V90_STATE[k] for k in ("trades", "seen", "signals", "telegram", "filter_observations", "recovery_metadata")}
        # Same-directory temp file ensures atomic rename on the mounted disk.
        with tempfile.NamedTemporaryFile(mode="w", encoding="utf-8", dir=str(V90_STATE_PATH.parent),
                                         prefix=".v90-checkpoint-", suffix=".tmp", delete=False) as f:
            tmp = Path(f.name)
            json.dump(payload, f, separators=(",", ":"))
            f.flush()
            os.fsync(f.fileno())
        os.replace(tmp, V90_STATE_PATH)
        tmp = None
        V915_STORAGE["last_save_utc"] = utc_now()
        V915_STORAGE["last_save_ok"] = True
        V915_STORAGE["last_save_error"] = None
        V915_STORAGE["save_count_since_start"] += 1
    except Exception as exc:
        message = f"{type(exc).__name__}: {str(exc)[:180]}"
        V915_STORAGE["last_save_ok"] = False
        V915_STORAGE["last_save_error"] = message
        V90_STATE["last_error"] = "checkpoint: " + message
    finally:
        if tmp is not None:
            try:
                tmp.unlink(missing_ok=True)
            except OSError:
                pass


def v903_load_state():
    V915_STORAGE["last_restore_utc"] = utc_now()
    try:
        if C_REMOTE_STORE is not None:
            saved = C_REMOTE_STORE.load()
            V915_STORAGE["remote_revision"] = C_REMOTE_STORE.revision
            V915_STORAGE["backup_path"] = "supabase:alt_momentum_v1_recovery_backup"
        else:
            if not V90_STATE_PATH.is_file():
                V915_STORAGE["last_restore_status"] = "MISSING_CHECKPOINT_STOPPED"
                return
            raw = V90_STATE_PATH.read_bytes()
            saved = json.loads(raw)
            if not isinstance(saved, dict) or not isinstance(saved.get("trades"), list) or not isinstance(saved.get("seen"), dict):
                raise ValueError("checkpoint structure invalid")
            backup = c_backup_checkpoint(raw)
            V915_STORAGE["backup_path"] = str(backup)
            V915_STORAGE["loaded_checkpoint_sha256"] = hashlib.sha256(raw).hexdigest()
        V90_STATE["recovery_metadata"] = saved.get("recovery_metadata", {})
        for k in ("trades", "seen", "signals"):
            if k in saved:
                V90_STATE[k] = saved[k]
        observations = saved.get("filter_observations", {})
        if isinstance(observations, dict):
            V90_STATE["filter_observations"] = observations
        tg = saved.get("telegram", {})
        if isinstance(tg, dict):
            for k in ("sent", "failed", "last"):
                if k in tg:
                    V90_STATE["telegram"][k] = tg[k]
        V915_STORAGE["restored_trades"] = len(V90_STATE["trades"])
        V915_STORAGE["last_restore_status"] = "RESTORED"
    except Exception as exc:
        message = f"{type(exc).__name__}: {str(exc)[:180]}"
        V915_STORAGE["last_restore_status"] = "ERROR: " + message
        V90_STATE["last_error"] = "restore: " + message


@app.get("/v915-storage-status")
async def v915_storage_status():
    """Read-only disk diagnostics: never alters the checkpoint."""
    if C_REMOTE_STORE is not None:
        return {**MODE_INFO, "status": "OK" if V915_STORAGE["last_save_ok"] else "STORAGE_ERROR",
            "version": "C_SUPABASE_PERSISTENCE_V1", "backend": "SUPABASE",
            "checkpoint_path": "supabase:alt_momentum_v1_checkpoint",
            "checkpoint_exists": V915_STORAGE["last_restore_status"] == "RESTORED",
            "storage": dict(V915_STORAGE), "current_trades_in_memory": len(V90_STATE["trades"]),
            "recovery_metadata": V90_STATE["recovery_metadata"],
            "note": "Read-only diagnostics; no credentials or checkpoint contents returned."}
    path = V90_STATE_PATH
    try:
        exists = path.is_file()
        size = path.stat().st_size if exists else None
        modified = datetime.fromtimestamp(path.stat().st_mtime, timezone.utc).isoformat() if exists else None
        stat_error = None
    except OSError as exc:
        exists, size, modified = False, None, None
        stat_error = f"{type(exc).__name__}: {str(exc)[:180]}"
    return {**MODE_INFO, "status": "OK", "version": "V91.5_STORAGE_DIAGNOSTICS",
            "checkpoint_path": str(path), "checkpoint_exists": exists,
            "checkpoint_bytes": size, "checkpoint_modified_utc": modified,
            "checkpoint_stat_error": stat_error, "storage": dict(V915_STORAGE),
            "current_trades_in_memory": len(V90_STATE["trades"]),
            "note": "Read-only endpoint; checkpoint is not overwritten or restarted."}



async def v902_notify(message, event, symbol):
    # Notifications are best-effort; failures must not change paper trades.
    try:
        result = await v22_telegram_send(message)
        ok = bool(result.get("sent"))
        reason = result.get("reason") if not ok else None
    except Exception as exc:
        ok = False
        # HTTP exception text can contain the Telegram token in its request URL.
        reason = type(exc).__name__
        if isinstance(exc, httpx.HTTPStatusError):
            reason += f" HTTP_{exc.response.status_code}"
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


# Independent shadow exits. Original C_TREND and its +3% half-profit are untouched.
C_LADDER_RULES = {
    "C_LADDER3": ((1.5, .30), (3.0, .30), (5.0, .40)),
    "C_LADDER4": ((1.0, .20), (2.0, .25), (3.5, .25), (5.0, .30)),
    "C_PROTECT": ((1.5, .30), (3.0, .30), (5.0, .40)),
    "C_TRAIL_PRO": ((1.5, .25), (3.0, .25)),
    "C_TIME_ADAPT": ((1.5, .30), (3.0, .30), (5.0, .40)),
}

def c_new_close(t, bid, reason, now_ms):
    remaining = max(0., float(t.get("remaining_fraction", 1.)))
    realized = float(t.get("realized_gross_pct", 0.))
    gross = realized + remaining * pct_change(t["entry_price"], bid)
    v90_close(t, bid, reason, now_ms)
    t["net_pct"] = round(gross - V90_COST_PCT, 5)
    t["remaining_fraction"] = 0.
    t["realized_gross_pct"] = round(gross, 7)

def c_new_monitor(t, bid, now_ms):
    variant = t["variant"]
    entry = t["entry_price"]
    elapsed = (now_ms - t["entry_ms"]) / 60000
    # Price checks are snapshots, not exchange resting orders. Stop takes precedence.
    if bid <= t["stop_price"]:
        c_new_close(t, bid, "STOP_MONITORED", now_ms)
        return
    if variant == "C_TIME_ADAPT":
        if elapsed >= 60 and t.get("time_60_checked") is not True:
            t["time_60_checked"] = True
            if t.get("max_bid", entry) < entry * 1.01:
                c_new_close(t, bid, "TIME60_NO_PLUS1", now_ms)
                return
        if elapsed >= 90 and t.get("time_90_checked") is not True:
            t["time_90_checked"] = True
            # Momentum proxy: last observed bid vs previous sampled bid.
            prev = t.get("previous_observed_bid")
            if prev is None or bid <= prev:
                c_new_close(t, bid, "TIME90_NO_MOMENTUM", now_ms)
                return
    # TP targets checked at observed bid. Multiple levels can fill at the same quote.
    for i, (target, fraction) in enumerate(C_LADDER_RULES[variant]):
        if i in t.get("filled_steps", []):
            continue
        if bid >= entry * (1 + target / 100):
            actual = min(fraction, t.get("remaining_fraction", 1.))
            t["realized_gross_pct"] = t.get("realized_gross_pct", 0.) + actual * pct_change(entry, bid)
            t["remaining_fraction"] = max(0., t.get("remaining_fraction", 1.) - actual)
            t.setdefault("filled_steps", []).append(i)
            t.setdefault("partial_exits", []).append({"target_pct": target, "fraction": actual,
                "observed_price": bid, "utc": utc_now()})
            if variant == "C_PROTECT" and i == 0:
                t["stop_price"] = max(t["stop_price"], entry)
    if variant == "C_TRAIL_PRO" and t.get("filled_steps"):
        t["trail_active"] = True
        t["stop_price"] = max(t["stop_price"], t["max_bid"] * .985)
    if t.get("remaining_fraction", 1.) < 1e-8:
        c_new_close(t, bid, "LADDER_COMPLETE", now_ms)
    elif now_ms >= t["entry_ms"] + 120 * 60000:
        c_new_close(t, bid, "TIME120", now_ms)
    t["previous_observed_bid"] = bid

async def v90_monitor(client, fast_only=False):
    now_ms = int(time.time()*1000)
    active = [t for t in V90_STATE["trades"] if t["status"] == "OPEN_SHADOW"
              and t.get("variant") not in TOP20_ACTIVE_MODELS
              and (not fast_only or (t.get("variant") in ("C", *C_SHADOW_MODELS) and
                   t.get("c_forward_version") == C_FORWARD_VERSION))]
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
            if t["variant"] in C_NEW_VARIANTS:
                c_new_monitor(t, bid, now_ms)
                continue
            # V95: snapshot earliest available monitored bid at/after 5m and 10m.
            # Capture from baseline C only, without modifying its exits.
            if t["variant"] == "C":
                for minute in (5, 10):
                    field = f"early_{minute}m"
                    if field not in t and now_ms >= t["entry_ms"] + minute * 60000:
                        t[field] = {"observed_ms": now_ms,
                                    "gross_pct": round((bid / t["entry_price"] - 1) * 100, 5)}
            if t["variant"] == "B" and t["max_bid"] >= t["entry_price"] * 1.02:
                t["stop_price"] = max(t["stop_price"], t["entry_price"])
            if (t["variant"] in V92_ALL_MODELS or t["variant"] in ("C_SL2", "C_SL25", "C_BE", "C_SHADOW_V4_C_EXIT", *V94_MODELS, *V95_MODELS)) and not t.get("tp1_done") and bid >= t["entry_price"]*1.03:
                t["tp1_done"] = True
                t["tp1_price"] = bid
                t["tp1_utc"] = utc_now()
            # V91.3 trail arm: activate only after +1.5% gross gain; trail 1.5% below highest observed bid.
            # Preserve original -3% hard stop until activation. This is sampled every 5 minutes.
            if t["variant"] in ("V913_PULLBACK_TRAIL15", "C_TRAIL"):
                if t.get("trail_active") or t["max_bid"] >= t["entry_price"] * 1.015:
                    t["trail_active"] = True
                    t["stop_price"] = max(t["stop_price"], t["max_bid"] * 0.985)
            # V97: +5% activation, then 5% below the highest observed bid.
            # Before activation the initial -5% / -10% stop remains in force.
            if t["variant"] in V97_TRAIL_MODELS:
                if t.get("trail_active") or t["max_bid"] >= t["entry_price"] * 1.05:
                    t["trail_active"] = True
                    t["stop_price"] = max(t["stop_price"], t["max_bid"] * 0.95)
            # New independent C_BE research arm; no change to baseline C.
            if t["variant"] == "C_BE" and t["max_bid"] >= t["entry_price"] * 1.015:
                t["be_armed"] = True
                t["stop_price"] = max(t["stop_price"], t["entry_price"])
            if t["variant"] == "C_TP2_SL25" and bid >= t["entry_price"] * 1.02:
                v90_close(t, bid, "FIXED_TP_MONITORED", now_ms)
            elif t["variant"] in V93_TP_TARGETS and bid >= t["entry_price"] * (1 + V93_TP_TARGETS[t["variant"]] / 100):
                v90_close(t, bid, "FIXED_TP_MONITORED", now_ms)
            elif t["variant"] != C_OBS120 and bid <= t["stop_price"]:
                v90_close(t, bid, ("TRAIL5_MONITORED" if t["variant"] in V97_TRAIL_MODELS and t.get("trail_active")
                                  else "TRAIL15_MONITORED" if t["variant"] in ("V913_PULLBACK_TRAIL15", "C_TRAIL") and t.get("trail_active")
                                  else "STOP_MONITORED"), now_ms)
            elif now_ms >= (t.get("confirm1_original_deadline_ms", t["entry_ms"] + (60 if t["variant"] == "C_TIME60" else 120)*60000)):
                v90_close(t, bid, "TIME60" if t["variant"] == "C_TIME60" else "TIME120", now_ms)
            if t["status"] == "CLOSED_SHADOW" and (t["variant"] in V92_ALL_MODELS or t["variant"] in ("C_SL2", "C_SL25", "C_BE", "C_SHADOW_V4_C_EXIT", *V94_MODELS, *V95_MODELS)) and t.get("tp1_done"):
                t["net_pct"] = round(((((t["tp1_price"]/t["entry_price"])-1)*100)*0.5 +
                                      (((t["exit_price"]/t["entry_price"])-1)*100)*0.5 - V90_COST_PCT),5)
    closed_now = [t for t in active if t["status"] == "CLOSED_SHADOW" and not t.get("telegram_exit_attempted")]
    if closed_now:
        v903_save_state()  # Persist exits before best-effort Telegram delivery.
    for t in closed_now:
        t["telegram_exit_attempted"] = True
        if t["variant"] == C_OBS120:
            high_pct = (t.get("max_bid", t["entry_price"]) / t["entry_price"] - 1) * 100
            low_pct = (t.get("min_bid", t["entry_price"]) / t["entry_price"] - 1) * 100
            msg = (f"C_OBS120 PAPER GOZLEM SONU\n{t['symbol']} | C_TREND ayni giris\n"
                   f"120dk net: {t['net_pct']:+.3f}% | Sure: {t.get('hold_minutes', 0):.1f}dk\n"
                   f"En yuksek: {high_pct:+.3f}% | En dusuk: {low_pct:+.3f}%\n"
                   f"Giris: {t['entry_price']:.10g} | Cikis: {t['exit_price']:.10g}\n"
                   "STOPS UYGULANMADI | PAPER ONLY | GERCEK EMIR YOK")
            await v902_notify(msg, "EXIT_" + C_OBS120, t["symbol"])
            continue
        msg = (f"ALT V90.3 PAPER CIKIS\n{t['symbol']} | Model {t['variant']}\n"
               f"Net: {t['net_pct']:+.3f}% | Neden: {t['exit_reason']}\n"
               f"Giris: {t['entry_price']:.10g} | Cikis: {t['exit_price']:.10g}\n"
               "PAPER ONLY | GERCEK EMIR YOK")
        await v902_notify(msg, "EXIT_" + t["variant"], t["symbol"])
    if closed_now:
        v903_save_state()
    return len(active)


def v94_regime(now_ms):
    """Use only already closed C trades. Decisions are frozen for one scan."""
    closed = []
    for t in V90_STATE["trades"]:
        if t.get("variant") != "C" or t.get("status") != "CLOSED_SHADOW":
            continue
        try:
            exit_ms = int(datetime.fromisoformat(t["exit_utc"].replace("Z", "+00:00")).timestamp() * 1000)
        except (KeyError, ValueError, TypeError):
            continue
        if exit_ms <= now_ms:
            closed.append((exit_ms, t))
    closed.sort(key=lambda item: (item[0], item[1].get("entry_ms", 0)))
    last = closed[-1] if closed else None
    last3 = closed[-3:]
    outcome = {}
    for model in V94_MODELS:
        mode, until_ms = "NORMAL", None
        if model == "V94_A" and last:
            ms, t = last
            if now_ms - ms < 600000:
                mode = "PAUSE" if t["net_pct"] < 0 else ("BOOST" if t["net_pct"] > 0 else "NORMAL")
                until_ms = ms + 600000
        elif model == "V94_B" and len(last3) == 3:
            losses = sum(t["net_pct"] < 0 for _, t in last3)
            wins = sum(t["net_pct"] > 0 for _, t in last3)
            ms = last3[-1][0]
            if losses >= 2 and now_ms - ms < 900000:
                mode, until_ms = "PAUSE", ms + 900000
            elif wins >= 2 and now_ms - ms < 600000:
                mode, until_ms = "BOOST", ms + 600000
        elif model == "V94_C" and len(last3) == 3:
            total = sum(t["net_pct"] for _, t in last3)
            ms = last3[-1][0]
            if now_ms - ms < 600000:
                mode = "BOOST" if total > 0 else ("PAUSE" if total < 0 else "NORMAL")
                until_ms = ms + 600000
        outcome[model] = {"mode": mode, "max_open": 10 if mode == "BOOST" else 5,
                          "until_utc": datetime.fromtimestamp(until_ms / 1000, timezone.utc).isoformat() if until_ms else None}
    return outcome


def v95_regime(now_ms):
    """Early unrealized C PnL, timestamped at observation; never use final exit PnL.

    Latest eligible observation wins. After negative: PAUSE 10m; after positive:
    BOOST 10m (capacity 10). Neutral observations produce NORMAL.
    """
    outcomes = {}
    for model, minute in (("V95_EARLY5", 5), ("V95_EARLY10", 10)):
        observations = []
        for trade in V90_STATE["trades"]:
            if trade.get("variant") != "C":
                continue
            snapshot = trade.get(f"early_{minute}m")
            if not isinstance(snapshot, dict):
                continue
            obs_ms = snapshot.get("observed_ms")
            if not isinstance(obs_ms, (int, float)) or obs_ms > now_ms:
                continue
            observations.append((obs_ms, trade.get("entry_ms", 0), trade, snapshot))
        observations.sort(key=lambda x: (x[0], x[1]))
        mode, until_ms, signal = "NORMAL", None, None
        if observations:
            obs_ms, _, trade, snapshot = observations[-1]
            pnl = snapshot["gross_pct"]
            signal = {"symbol": trade["symbol"], "entry_ms": trade["entry_ms"],
                      "observed_utc": datetime.fromtimestamp(obs_ms / 1000, timezone.utc).isoformat(),
                      "gross_pct": pnl}
            if now_ms < obs_ms + 600000:
                mode = "PAUSE" if pnl < 0 else ("BOOST" if pnl > 0 else "NORMAL")
                until_ms = obs_ms + 600000 if mode != "NORMAL" else None
        outcomes[model] = {"mode": mode, "max_open": 10 if mode == "BOOST" else 5,
                           "until_utc": datetime.fromtimestamp(until_ms / 1000, timezone.utc).isoformat() if until_ms else None,
                           "source": signal}
    return outcomes


async def v90_scan_once():
    global V90_LOCK
    if V90_LOCK is None: V90_LOCK = asyncio.Lock()
    if V90_LOCK.locked(): return {"status":"BUSY"}
    async with V90_LOCK:
        V90_STATE["scans"] += 1
        scan_started_ms = int(time.time()*1000)
        scan = {"utc":utc_now(),"checked":0,"candidates":0,"new_coins":0,"errors":0, "filter_counts":{}, "near_misses":[], "model_filters":{}}
        try:
            async with httpx.AsyncClient(timeout=30.0, limits=httpx.Limits(max_connections=4, max_keepalive_connections=2)) as client:
                await v90_monitor(client)
                await cconfirm1_monitor(client)  # finish existing pending confirmations; no new ones
                universe = (await build_universe(client))[:100]
                # Only one network scan, three paper valuation models.
                semaphore = asyncio.Semaphore(3)
                from collections import Counter
                rejected = Counter()
                near_misses = []
                market_returns = []
                model_rejected = {m: Counter() for m in C_ENTRY_MODELS}
                model_passed = {m: 0 for m in C_ENTRY_MODELS}
                # BTC is fetched once per scan for the regime research arm.
                btc_candles = await get_completed_5m_candles(client, "BTCUSDT", limit=25)
                btc_closes = [x["close"] for x in btc_candles]
                btc_ret60 = pct_change(btc_closes[-13], btc_closes[-1]) if len(btc_closes) >= 13 else None
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
                            market_returns.append(ret60)
                            avg = sum(vs[-7:-1])/6
                            vol = vs[-1]/avg if avg else 0
                            past_returns = [pct_change(cs[i-12],cs[i]) for i in range(12,len(cs)-1)]
                            mu = statistics.mean(past_returns) if past_returns else 0
                            sd = statistics.pstdev(past_returns) if len(past_returns)>1 else 0
                            z = (ret60-mu)/sd if sd else 0
                            breakout = pct_change(max(x["high"] for x in candles[-7:-1]),cs[-1])
                            # Shared checks are data quality/freshness only; each model
                            # applies its own entry filters below.
                            signal_ms = candles[-1]["close_time"]
                            delay = (int(time.time()*1000)-signal_ms)/1000
                            if not 0 <= delay < V90_DELAY_SECONDS:
                                rejected["signal_delay_outside_0_300_seconds"] += 1
                                return None
                            return {"symbol":item["symbol"],"ret60_pct":ret60,"volume_ratio":vol,
                                    "z":z,"breakout_pct":breakout,"signal_ms":signal_ms,
                                    "breakout_price":cs[-1],
                                    "pullback_reclaim": (cs[-3] < cs[-4] and cs[-2] > cs[-3] and cs[-1] > cs[-2] and cs[-1] > cs[-7]),
                                    "ret15_pct": pct_change(cs[-4], cs[-1])}
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
                scan["candidates"] = len(candidates)  # valid, fresh candles; NOT entry signals
                scan["filter_counts"] = dict(rejected)
                scan["near_misses"] = near_misses
                # A/B are frozen: existing positions are monitored until closed, no new entries.
                # New research arms share C's candidate signals but maintain independent shadow positions.
                breadth = (sum(x > 0 for x in market_returns) / len(market_returns)
                           if market_returns else None)
                scan["market_context"] = {"btc_ret60_pct":btc_ret60,
                                           "alt_positive_breadth":round(breadth,4) if breadth is not None else None,
                                           "breadth_sample":len(market_returns)}
                # Counts only: no quotes, orders, positions, or strategy decisions.
                c_observe_filter_ranges(candidates, scan)
                regimes = v94_regime(scan_started_ms)
                scan["v94_regime"] = regimes
                early_regimes = v95_regime(scan_started_ms)
                scan["v95_regime"] = early_regimes
                active_by_model = {v:{t["symbol"] for t in V90_STATE["trades"]
                                      if t["status"]=="OPEN_SHADOW" and t["variant"]==v}
                                   for v in C_ENTRY_MODELS}
                for c in sorted(candidates,key=lambda r:r["ret60_pct"],reverse=True):
                    sym=c["symbol"]
                    key=f'{sym}:{c["signal_ms"]}'
                    selected = []
                    for model in C_ENTRY_MODELS:
                        reason = None
                        ret, vol, z, breakout = c["ret60_pct"], c["volume_ratio"], c["z"], c["breakout_pct"]
                        if model in C_SHADOW_MODELS:
                            # Shadow follows the same C volume/z/breakout gates, only momentum starts at +1%.
                            if not 1.0 <= ret <= 8.0: reason = "momentum_range"
                            elif not 1.2 <= vol < 3.5: reason = "volume_range"
                            elif z >= 3.5: reason = "z_extreme"
                            elif not 0.2 <= breakout <= 1.5: reason = "breakout_range"
                        elif model in ("C", "C_TRAIL", "C_TIME60", "C_MARKET", "C_TREND", *V93_TP_TARGETS, *V93_RISK_MODELS, *V94_MODELS, *V95_MODELS, *V97_TRAIL_MODELS):
                             if model == "C_MARKET" and (btc_ret60 is None or btc_ret60 < 0 or breadth is None or breadth < 0.50):
                                 reason = "market_regime"
                             if model == "C_TREND" and not 3.0 <= ret <= 7.0:
                                 reason = "momentum_range"
                             if model == "C_TREND" and not 1.5 <= vol < 3.5:
                                 reason = "volume_range"
                             if reason is None:
                                 if not 2.5 <= ret <= 8.0: reason = "momentum_range"
                                 elif not 1.2 <= vol < 3.5: reason = "volume_range"
                                 elif z >= 3.5: reason = "z_extreme"
                                 elif not 0.2 <= breakout <= 1.5: reason = "breakout_range"
                        elif model in ("V91_PULLBACK", "V913_PULLBACK_MARKET", "V913_PULLBACK_TRAIL15"):
                            if model == "V913_PULLBACK_MARKET" and (btc_ret60 is None or btc_ret60 < 0 or breadth is None or breadth < 0.50):
                                reason = "market_regime"
                            # Pullback entry rules are otherwise identical for fair comparison.
                            if reason is None:
                            # Unlike C, can test a modest recovery after a pullback.
                                if not 0.2 <= ret <= 6.0: reason = "trend_range"
                                elif not 0.6 <= vol < 3.0: reason = "volume_range"
                                elif not c["pullback_reclaim"]: reason = "no_reclaim"
                                elif z >= 3.5: reason = "z_extreme"
                        elif model == "V91_MOMENTUM":
                            if not 0.8 <= ret <= 5.0: reason = "momentum_range"
                            elif not 0.9 <= vol < 3.0: reason = "volume_range"
                            elif z > 2.8: reason = "z_extreme"
                            elif not 0.1 <= breakout <= 1.0: reason = "breakout_range"
                        elif model == "V91_REGIME":
                            if btc_ret60 is None or btc_ret60 < -0.10 or breadth is None or breadth < 0.25:
                                reason = "market_regime"
                            elif not 0.5 <= ret <= 5.0: reason = "momentum_range"
                            elif not 1.0 <= vol < 3.5: reason = "volume_range"
                            elif z >= 3.5: reason = "z_extreme"
                            elif not 0.1 <= breakout <= 1.5: reason = "breakout_range"
                        if reason:
                            model_rejected[model][reason] += 1
                        else:
                            model_passed[model] += 1
                            # Dedup per model so one model cannot block another.
                            if model in V94_MODELS and regimes[model]["mode"] == "PAUSE":
                                model_rejected[model]["regime_pause"] += 1
                            elif model in V95_MODELS and early_regimes[model]["mode"] == "PAUSE":
                                model_rejected[model]["regime_pause"] += 1
                            elif f"{model}:{key}" in V90_STATE["seen"] or sym in active_by_model[model]:
                                model_rejected[model]["already_seen_or_open"] += 1
                            elif len(active_by_model[model]) >= (regimes[model]["max_open"] if model in V94_MODELS else early_regimes[model]["max_open"] if model in V95_MODELS else 5):
                                model_rejected[model]["max_open_positions"] += 1
                            else:
                                selected.append(model)
                    if not selected:continue
                    if V915_STORAGE["last_save_ok"] is False:
                        for model in selected:
                            model_rejected[model]["storage_unavailable"] += 1
                        continue
                    try:
                        q=await get_json(client,"/api/v3/ticker/bookTicker",{"symbol":sym})
                        ask=float(q["askPrice"]);bid=float(q["bidPrice"])
                        if ask<=0 or bid<=0:
                            for model in selected: model_rejected[model]["invalid_quote"] += 1
                            continue
                        spread=(ask-bid)/ask*100
                        chase=pct_change(c["breakout_price"],ask)
                        if spread>0.15 or chase>0.5:
                            for model in selected:
                                model_rejected[model]["spread_too_wide" if spread>0.15 else "price_chase"] += 1
                            continue
                        if spread > 0.12:
                            for cm in ("C", *C_SHADOW_MODELS, *V93_TP_TARGETS, *V93_RISK_MODELS, *V94_MODELS, *V95_MODELS, *V97_TRAIL_MODELS):
                                if cm in selected:
                                    selected.remove(cm)
                                    model_rejected[cm]["c_spread"] += 1
                        if not selected:continue
                        entry_ms=int(time.time()*1000)
                        # Entry-time market regime snapshot, identical for matched shadow arms.
                        c = {**c, "entry_btc_ret60_pct": btc_ret60,
                             "entry_alt_positive_breadth": round(breadth, 4) if breadth is not None else None,
                             "entry_breadth_sample": len(market_returns),
                             "entry_market_context_utc": scan["utc"]}
                        for variant in selected:
                            entry_record = {**c,"variant":variant,"status":"OPEN_SHADOW",
                                "entry_utc":utc_now(),"entry_ms":entry_ms,"entry_price":ask,
                                "stop_price":ask*V97_TRAIL_STOPS.get(variant, V93_RISK_STOPS.get(variant, 0.97)),"spread_pct":round(spread,5),
                                "chase_pct":round(chase,5),"max_bid":ask,"min_bid":ask,
                                "tp1_done":False,"trail_active":False,"research_only":True,"be_armed":False}
                            if variant in C_SHADOW_MODELS:
                                entry_record.update(c_forward_version=C_FORWARD_VERSION,
                                    exit_monitor_seconds=C_MONITOR_SECONDS,
                                    shadow_entry_version="C_SHADOW_MOM1_FORWARD_V1")
                            V90_STATE["trades"].append(entry_record)
                            active_by_model[variant].add(sym)
                            if variant == "C_TREND":
                                # Five independent shadow exits; entry and stop match C_TREND.
                                for arm in C_NEW_VARIANTS:
                                    V90_STATE["trades"].append({**c, "variant": arm,
                                        "status": "OPEN_SHADOW", "entry_utc": utc_now(),
                                        "entry_ms": entry_ms, "entry_price": ask,
                                        "stop_price": ask * .97, "spread_pct": round(spread, 5),
                                        "chase_pct": round(chase, 5), "max_bid": ask,
                                        "min_bid": ask, "tp1_done": False, "trail_active": False,
                                        "research_only": True, "be_armed": False,
                                        "source_model": "C_TREND", "remaining_fraction": 1.,
                                        "realized_gross_pct": 0., "filled_steps": [], "partial_exits": []})
                        # V96: baseline C trade records which V95 pauses would have skipped it.
                        if "C" in selected:
                            for trade in reversed(V90_STATE["trades"]):
                                if trade.get("variant") == "C" and trade.get("entry_ms") == entry_ms and trade.get("symbol") == sym:
                                    trade["c_forward_version"] = C_FORWARD_VERSION
                                    trade["exit_monitor_seconds"] = C_MONITOR_SECONDS
                                    trade["c_loss_observation_v1"] = c_loss_entry_flags(trade)
                                    trade["v96_early_decisions"] = {m: {"mode": early_regimes[m]["mode"], "would_block": early_regimes[m]["mode"] == "PAUSE"} for m in V95_MODELS}
                                    break
                        for variant in selected:
                            V90_STATE["seen"][f"{variant}:{key}"]=entry_ms
                        scan["new_coins"]+=1
                        V90_STATE["signals"]+=1
                        msg = (f"ALT V91 PAPER GIRIS\n{sym} | {', '.join(selected)}\n"
                               f"Giris: {ask:.10g} | Stop: {ask*0.97:.10g}\n"
                               f"Momentum60: {c['ret60_pct']:+.2f}% | Hacim: {c['volume_ratio']:.2f}x\n"
                               "PAPER ONLY | GERCEK EMIR YOK")
                        v903_save_state()
                        await v902_notify(msg, "ENTRY", sym)
                        v903_save_state()
                    except Exception as exc:
                        scan["errors"]+=1
                        for model in selected: model_rejected[model]["entry_exception"] += 1
                        V90_STATE["last_error"] = "entry: " + str(exc)[:180]
                scan["model_filters"] = {m:{"evaluated":len(candidates), "passed_entry_filters":model_passed[m],
                    "rejected":dict(model_rejected[m]),
                    "new_positions":sum(t["variant"]==m and t["entry_ms"] >= scan_started_ms for t in V90_STATE["trades"]),
                    "post_filter_rejections":sum(n for reason,n in model_rejected[m].items() if reason in ("spread_too_wide", "price_chase", "invalid_quote", "c_spread", "entry_exception", "already_seen_or_open", "max_open_positions"))}
                    for m in C_ENTRY_MODELS}
                # Preserve all historical paper records; monitor disk/RAM before deployment.
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


async def top20_scalp_loop():
    while True:
        started = time.monotonic()
        await TOP20_SCALPER.cycle(__import__(__name__))
        report = TOP20_SCALPER.report
        print(f"TOP20_SCALP_CYCLE universe={report['universe']} leaders={len(report['leaders'])} "
              f"new_positions={report['new_positions']} errors={report['last_error']} "
              f"duration_s={report['last_scan_duration_seconds']}", flush=True)
        await asyncio.sleep(max(1, TOP20_SCAN_SECONDS - (time.monotonic() - started)))


@app.on_event("startup")
async def v90_startup():
    global V90_TASK, C_MONITOR_TASK, TOP20_TASK, TOP20_MONITOR_TASK
    V90_STATE["started_utc"]=utc_now()
    v903_load_state()
    # Never run a scanner from a missing/corrupt/unwritable historical checkpoint.
    if V915_STORAGE["last_restore_status"] != "RESTORED":
        return
    v903_save_state()
    if not V915_STORAGE["last_save_ok"]:
        return
    if V90_TASK is None or V90_TASK.done():
        V90_TASK = asyncio.create_task(v90_loop())
    if C_MONITOR_TASK is None or C_MONITOR_TASK.done():
        C_MONITOR_TASK = asyncio.create_task(c_fast_monitor_loop())
    if TOP20_TASK is None or TOP20_TASK.done():
        TOP20_TASK = asyncio.create_task(top20_scalp_loop())
    if TOP20_MONITOR_TASK is None or TOP20_MONITOR_TASK.done():
        TOP20_MONITOR_TASK = asyncio.create_task(TOP20_SCALPER.monitor_loop(__import__(__name__)))


@app.get("/top20-scalp-status")
async def top20_scalp_status():
    return TOP20_SCALPER.status(__import__(__name__), bool(TOP20_TASK and not TOP20_TASK.done() and TOP20_MONITOR_TASK and not TOP20_MONITOR_TASK.done()))


@app.get("/c-shadow-mom1-status")
async def c_shadow_mom1_status():
    """Read-only independent forward paper cohort; never modifies the reference C."""
    rows = [t for t in V90_STATE["trades"] if t.get("variant") == "C_SHADOW_V4_C_EXIT"]
    legacy = [t for t in V90_STATE["trades"] if t.get("variant") == "C_SHADOW_MOM1"]
    return {**MODE_INFO, "status": "OK", "version": "C_SHADOW_V4_C_EXIT",
            "paper_only": True, "active_shadow_model": "C_SHADOW_V4_C_EXIT",
            "reference_model_unchanged": "C",
            "entry_momentum_1m_min_pct": 0.10, "entry_momentum_3m_min_pct": 0.20,
            "entry_momentum_5m_pct": [0.15, 0.80], "entry_momentum_10m_pct": [0.40, 1.50],
            "min_volume_ratio": 1.5, "max_spread_pct": 0.06, "price_chase_pct": [-0.10, 0.12],
            "stop_loss_pct": -3.0, "partial_tp_pct": 3.0, "partial_tp_fraction": 0.5,
            "time_exit_minutes": 120, "assumed_round_trip_cost_pct": V90_COST_PCT,
            "scan_interval_seconds": TOP20_SCAN_SECONDS, "exit_monitor_seconds": C_MONITOR_SECONDS,
            "stats": v90_stats(rows), "legacy_stats": v90_stats(legacy),
            "open_positions": [{"symbol": t["symbol"], "variant": t["variant"], "entry_utc": t.get("entry_utc"),
                                "entry_price": t.get("entry_price")} for t in rows + legacy
                               if t.get("status") == "OPEN_SHADOW"],
            "last_scan_utc": TOP20_SCALPER.report.get("last_scan_utc"),
            "last_scan_filters": TOP20_SCALPER.report.get("hybrid_rejections"),
            "note": "Independent V4 entry / C exit paper cohort; legacy entries retired."}


@app.get("/v90-status")
async def v90_status():
    return {**MODE_INFO,"status":"OK","strategy":"V90_HYBRID_MOMENTUM", "diagnostic_version":"V93_C_RISK_FAMILY_PAPER",
            "paper_only":True,"orders":False,"auto_running":bool(V90_TASK and not V90_TASK.done()),
            "scan_interval_seconds":V90_SCAN_SECONDS,"last_scan":V90_STATE["last_scan"],
            "forward_version": C_FORWARD_VERSION, "new_C_exit_monitor_seconds": C_MONITOR_SECONDS,
            "fast_monitor_running": bool(C_MONITOR_TASK and not C_MONITOR_TASK.done()),
            "fast_monitor": dict(C_MONITOR_STATE), "recovery_metadata": V90_STATE["recovery_metadata"],
            "last_error":V90_STATE["last_error"],"total_signals":V90_STATE["signals"],
            "signal_delay_limit_seconds":V90_DELAY_SECONDS,"telegram":V90_STATE["telegram"],
            "memory_peak_mb":v903_memory_mb(), "checkpoint_path":("supabase:alt_momentum_v1_checkpoint" if C_REMOTE_STORE else str(V90_STATE_PATH)),
            "frozen_legacy":V90_STATE["frozen_legacy"],
             "frozen_models":list(dict.fromkeys(V91_FROZEN + V92_RETIRED + V96_RETIRED +
                 tuple(m for m in (V91_MODELS + tuple(V93_TP_TARGETS) + V93_RISK_MODELS) if m not in V93_ALL_ACTIVE) + C_FROZEN_VARIANTS)),
             "active_models":list(C_ENTRY_MODELS),
            "observation_models": [],
            "independent_shadow_models":list(C_SHADOW_MODELS),
            "retained_exit_observation_models": list(C_NEW_VARIANTS),
             "frozen_no_new_entries": list(C_FROZEN_VARIANTS),
            "variants":{v:v90_stats([t for t in V90_STATE["trades"] if t["variant"]==v])
                        for v in dict.fromkeys((*V91_FROZEN, *V92_ALL_MODELS, *V93_TP_TARGETS, *V93_RISK_MODELS, *V94_MODELS, *V95_MODELS, *V97_TRAIL_MODELS, C_OBS120, C_CONFIRM1, *C_NEW_VARIANTS))},
            "recent_trades":V90_STATE["trades"][-24:],
            "limitations":["Checkpoint persistence requires a writable persistent mount and successful saves", "5-minute scan can miss signals", 
                           "Stop/TP monitored by quotes every 5 minutes; gaps and slippage possible",
                           "V91/V91.3 models use exploratory entry filters, not validated strategies", "Trailing stop is sampled every 5 minutes and may exceed the threshold on gaps",
                           "No real fills, no real orders", "Experimental thresholds not validated"]}


@app.get("/c-obs120")
async def c_obs120_status():
    rows = [t for t in V90_STATE["trades"] if t.get("variant") == C_OBS120]
    closed = [t for t in rows if t.get("status") == "CLOSED_SHADOW"]
    return {"model": C_OBS120, "paper_only": True,
            "source": "C_TREND", "no_stop": True, "no_take_profit": True,
            "max_hold_minutes": 120, "scan_seconds": V90_SCAN_SECONDS,
            "stats": v90_stats(rows),
            "closed_observations": [{"symbol": t["symbol"], "entry_utc": t["entry_utc"],
                "entry_price": t["entry_price"], "exit_price": t.get("exit_price"),
                "net_pct": t.get("net_pct"), "max_bid": t.get("max_bid"),
                "min_bid": t.get("min_bid"), "hold_minutes": t.get("hold_minutes")}
                for t in closed[-100:]], "open_observations": [t for t in rows if t.get("status") == "OPEN_SHADOW"]}


@app.get("/v90-filter-analysis")
async def v90_filter_analysis():
    last = V90_STATE.get("last_scan") or {}
    return {**MODE_INFO, "status":"OK", "strategy":"V90_3_FILTER_ANALYSIS",
            "auto_running":bool(V90_TASK and not V90_TASK.done()),
            "scan_interval_seconds":V90_SCAN_SECONDS, "signal_delay_limit_seconds":V90_DELAY_SECONDS,
            "last_scan_utc":last.get("utc"), "checked":last.get("checked"),
            "candidates":last.get("candidates"), "new_coins":last.get("new_coins"),
            "errors":last.get("errors"), "filter_counts":last.get("filter_counts",{}),
            "near_misses":last.get("near_misses",[]), "model_filters":last.get("model_filters",{}),
            "note":"See /c-entry-diagnostics for independent baseline C failures. Historical C_TREND labels gave volume precedence over momentum."}


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


# V91.4 preparation: read-only research diagnostics; no changes to trading logic.
def v914_group_stats(rows):
    return v90_stats(rows)


def v914_bucket(value, bounds):
    if value is None:
        return "missing"
    try:
        value = float(value)
    except (TypeError, ValueError):
        return "missing"
    for bound in bounds:
        if value < bound:
            return f"<{bound:g}"
    return f">={bounds[-1]:g}"


@app.get("/v914-research-analysis")
async def v914_research_analysis():
    closed = [t for t in V90_STATE["trades"] if t.get("status") == "CLOSED_SHADOW"
              and t.get("variant") in V92_ALL_MODELS]
    dimensions = {
        "ret60_pct": [0, 0.5, 1, 2, 3, 5],
        "ret15_pct": [0, 0.3, 0.7, 1.5, 3],
        "volume_ratio": [0.6, 1, 1.5, 2, 3],
        "z": [-1, 0, 1, 2, 3],
        "breakout_pct": [0, 0.2, 0.5, 1],
        "spread_pct": [0.02, 0.05, 0.1, 0.15],
        "chase_pct": [-0.3, 0, 0.2, 0.5],
        "hold_minutes": [15, 30, 60, 90, 120],
    }
    by_model = {}
    for model in V93_ALL_ACTIVE:
        rows = [t for t in closed if t.get("variant") == model]
        breakdowns = {}
        for field, bounds in dimensions.items():
            groups = {}
            for t in rows:
                bucket = v914_bucket(t.get(field), bounds)
                groups.setdefault(bucket, []).append(t)
            breakdowns[field] = {k: v914_group_stats(v) for k, v in sorted(groups.items())}
        reasons = {}
        for t in rows:
            reasons.setdefault(t.get("exit_reason", "UNKNOWN"), []).append(t)
        by_model[model] = {
            "summary": v914_group_stats([t for t in V90_STATE["trades"] if t.get("variant") == model]),
            "exit_reasons": {k: v914_group_stats(v) for k, v in sorted(reasons.items())},
            "breakdowns": breakdowns,
        }
    # Match the same entry by symbol and entry timestamp, allowing sub-second variation.
    pullback_models = ("V91_PULLBACK", "V913_PULLBACK_MARKET", "V913_PULLBACK_TRAIL15")
    paired = {}
    for t in closed:
        if t.get("variant") not in pullback_models:
            continue
        key = (t.get("symbol"), t.get("entry_ms"))
        paired.setdefault(key, {})[t["variant"]] = t
    pairwise = {}
    for other in pullback_models[1:]:
        matches = []
        for (symbol, entry_ms), records in paired.items():
            if "V91_PULLBACK" in records and other in records:
                base = records["V91_PULLBACK"]
                candidate = records[other]
                matches.append({"symbol": symbol, "entry_ms": entry_ms,
                                "normal_net_pct": base["net_pct"],
                                "variant_net_pct": candidate["net_pct"],
                                "difference_pct_points": round(candidate["net_pct"] - base["net_pct"], 5),
                                "normal_exit": base.get("exit_reason"),
                                "variant_exit": candidate.get("exit_reason")})
        diffs = [r["difference_pct_points"] for r in matches]
        pairwise[other] = {"matched_closed_pairs": len(matches),
                           "mean_improvement_pct_points": round(sum(diffs)/len(diffs), 5) if diffs else None,
                           "improved": sum(d > 0 for d in diffs),
                           "worsened": sum(d < 0 for d in diffs),
                           "equal": sum(d == 0 for d in diffs),
                           "recent_pairs": matches[-30:]}
    return {**MODE_INFO, "status": "OK", "report_version": "V914_DIAGNOSTICS_ONLY",
            "total_closed_in_memory": len(closed), "by_model": by_model,
            "pullback_pairwise": pairwise,
            "limitations": ["Only trades retained in memory/checkpoint are analyzed",
                            "Checkpoint persistence depends on V90_STATE_PATH and successful disk writes",
                            "5-minute sampled exits may differ from executable prices",
                            "Overlapping model trades are not independent observations",
                            "Report does not change entry/exit rules or place orders"]}


@app.get("/v914-closed-trades")
async def v914_closed_trades(model: str = "ALL", limit: int = 500, offset: int = 0):
    # Bounded pagination for export and reproducible external analysis.
    limit = max(1, min(limit, 500))
    offset = max(0, offset)
    rows = [t for t in V90_STATE["trades"] if t.get("status") == "CLOSED_SHADOW"
            and (model == "ALL" or t.get("variant") == model)]
    return {**MODE_INFO, "status": "OK", "model_filter": model,
            "total": len(rows), "offset": offset, "limit": limit,
            "trades": rows[offset:offset + limit],
            "note": "Read-only retained paper trade records; pagination via offset and limit."}


# V91.6 read-only diagnostics retained; V92 adds independent C variants.
@app.get("/v916-performance")
async def v916_performance():
    models = {}
    for model in V93_ALL_ACTIVE:
        rows = [t for t in V90_STATE["trades"] if t.get("variant") == model]
        opened = [t for t in rows if t.get("status") == "OPEN_SHADOW"]
        closed = [t for t in rows if t.get("status") == "CLOSED_SHADOW"]
        positions = []
        for t in opened:
            entry = t.get("entry_price")
            bid = t.get("last_bid")
            gross = ((bid / entry) - 1) * 100 if isinstance(entry, (int, float)) and entry > 0 and isinstance(bid, (int, float)) and bid > 0 else None
            positions.append({"symbol": t.get("symbol"), "entry_utc": t.get("entry_utc"),
                              "entry_price": entry, "last_observed_bid": bid,
                              "gross_unrealized_pct": round(gross, 5) if gross is not None else None,
                              "estimated_net_unrealized_pct": round(gross - V90_COST_PCT, 5) if gross is not None else None,
                              "quote_is_live": False,
                              "max_bid": t.get("max_bid"), "min_bid": t.get("min_bid"),
                              "stop_price": t.get("stop_price"), "trail_active": t.get("trail_active")})
        reasons = {}
        for t in closed:
            reason = t.get("exit_reason", "UNKNOWN")
            reasons.setdefault(reason, []).append(t)
        models[model] = {"summary": v90_stats(rows),
                         "open_positions": positions,
                         "closed_by_exit_reason": {k: v90_stats(v) for k, v in sorted(reasons.items())},
                         "closed_gross_mean_pct": round(sum(t.get("net_pct", 0) + V90_COST_PCT for t in closed) / len(closed), 5) if closed else None,
                         "assumed_round_trip_cost_pct": V90_COST_PCT}
    return {**MODE_INFO, "status": "OK", "report_version": "V91.6_PERFORMANCE_READ_ONLY",
            "reference_model": "C", "c_entry_exit_rules_unchanged": True,
            "total_open_model_positions": sum(v["summary"]["open"] for v in models.values()),
            "total_closed_model_positions": sum(v["summary"]["closed"] for v in models.values()),
            "by_model": models,
            "limitations": ["Open PnL uses last observed bid, not a real-time quote",
                            "Net open PnL subtracts assumed 0.15 percentage-point round-trip cost",
                            "Models may share underlying symbols; positions are not independent",
                            "Five-minute exit sampling can miss intrabar stops", "Paper only; no orders"]}


@app.get("/v92-c-family")
async def v92_c_family():
    """Read-only C-family comparison. No paper positions or checkpoints modified."""
    models = {}
    for model in V93_ALL_ACTIVE:
        rows = [t for t in V90_STATE["trades"] if t.get("variant") == model]
        reasons = {}
        for t in rows:
            if t.get("status") == "CLOSED_SHADOW":
                reasons.setdefault(t.get("exit_reason", "UNKNOWN"), []).append(t)
        models[model] = {"summary": v90_stats(rows),
                         "exit_reasons": {k: v90_stats(v) for k, v in reasons.items()}}
    return {**MODE_INFO, "status": "OK", "version": "V92_C_FAMILY_PAPER",
            "reference_model": "C", "c_rules_unchanged": True,
            "active_models": list(V93_ALL_ACTIVE), "retired_no_new_entries": list(dict.fromkeys(V92_RETIRED + tuple(m for m in (V91_MODELS + V93_RISK_MODELS + V94_MODELS + V95_MODELS + V97_TRAIL_MODELS) if m not in V93_ALL_ACTIVE))),
            "by_model": models, "note": "Paper only; same symbols can be shared across models; no real orders."}


@app.get("/v93-c-fixed-tp")
async def v93_c_fixed_tp():
    """Read-only side-by-side comparison; original C is untouched."""
    models = ("C", *V93_TP_TARGETS)
    result = {}
    for model in models:
        rows = [t for t in V90_STATE["trades"] if t.get("variant") == model]
        closed = [t for t in rows if t.get("status") == "CLOSED_SHADOW"]
        reasons = {}
        for t in closed:
            reason = t.get("exit_reason", "UNKNOWN")
            reasons[reason] = reasons.get(reason, 0) + 1
        result[model] = {
            "take_profit_pct": V93_TP_TARGETS.get(model),
            "summary": v90_stats(rows),
            "exit_counts": reasons,
            "recent_closed": [
                {"symbol": t["symbol"], "net_pct": t.get("net_pct"),
                 "exit_reason": t.get("exit_reason"),
                 "hold_minutes": t.get("hold_minutes")}
                for t in closed[-12:]
            ],
        }
    return {**MODE_INFO, "version": "V93_C_FIXED_TP_PAPER",
            "status": "RESEARCH_ONLY", "reference_model": "C",
            "c_rules_unchanged": True,
            "fixed_tp_full_position": True,
            "stop_loss_pct": -3.0, "time_exit_minutes": 120,
            "assumed_round_trip_cost_pct": V90_COST_PCT,
            "exit_monitoring_interval_seconds": V90_SCAN_SECONDS,
            "by_model": result,
            "limitations": [
                "Fixed TP is checked at the observed bid every scan, not continuously",
                "An intrabar TP touch may be missed; observed exit may differ from target",
                "TP and stop are sampled; gaps and slippage may affect realized returns",
                "All variants are shadow trades only; no actual orders",
                "Existing C keeps its original +3% half-profit and remaining position logic",
                "Overlapping symbols are correlated, not independent samples",
            ]}


@app.get("/v93-c-risk-family")
async def v93_c_risk_family():
    """Read-only paper research. Never changes C, trades, or checkpoint."""
    models = ("C", *V93_RISK_MODELS)
    by_model = {}
    for model in models:
        rows = [t for t in V90_STATE["trades"] if t.get("variant") == model]
        reasons = {}
        for t in rows:
            if t.get("status") == "CLOSED_SHADOW":
                reasons.setdefault(t.get("exit_reason", "UNKNOWN"), []).append(t)
        by_model[model] = {"summary": v90_stats(rows),
                           "exit_reasons": {k: v90_stats(v) for k, v in reasons.items()}}
    closed = [t for t in V90_STATE["trades"] if t.get("status") == "CLOSED_SHADOW" and t.get("variant") in models]
    paired = {}
    for t in closed:
        paired.setdefault((t.get("symbol"), t.get("entry_ms")), {})[t["variant"]] = t
    comparisons = {}
    for model in V93_RISK_MODELS:
        matches = []
        for (symbol, entry_ms), records in paired.items():
            if "C" in records and model in records:
                c, variant = records["C"], records[model]
                matches.append({"symbol": symbol, "entry_ms": entry_ms,
                                "c_net_pct": c["net_pct"], "variant_net_pct": variant["net_pct"],
                                "difference_pct_points": round(variant["net_pct"] - c["net_pct"], 5),
                                "c_exit": c.get("exit_reason"), "variant_exit": variant.get("exit_reason")})
        differences = [x["difference_pct_points"] for x in matches]
        comparisons[model] = {"matched_closed_pairs": len(matches),
                              "mean_difference_pct_points": round(sum(differences) / len(differences), 5) if differences else None,
                              "better": sum(x > 0 for x in differences),
                              "worse": sum(x < 0 for x in differences),
                              "equal": sum(x == 0 for x in differences),
                              "recent_pairs": matches[-30:]}
    return {**MODE_INFO, "status": "RESEARCH_ONLY", "version": "V93_C_RISK_FAMILY_PAPER",
            "reference_model": "C", "c_rules_unchanged": True,
            "risk_variants": {"C_SL2": "C entry, -2% initial stop, original C half TP at +3%",
                              "C_SL25": "C entry, -2.5% initial stop, original C half TP at +3%",
                              "C_BE": "C entry, original -3% stop; after observed +1.5% bid, stop raised to entry; original C half TP at +3%",
                              "C_TP2_SL25": "C entry, -2.5% stop, full exit at observed +2% bid, no half TP"},
            "by_model": by_model, "paired_vs_C": comparisons,
            "assumed_round_trip_cost_pct": V90_COST_PCT,
            "monitor_interval_seconds": V90_SCAN_SECONDS,
            "limitations": ["Only positions opened after deployment participate in new variants",
                            "Pairs require both trades closed and matching symbol and entry_ms",
                            "Stops and take profits are checked at observed bids every ~5 minutes, not continuously",
                            "Breakeven at entry price does not imply net breakeven after costs",
                            "Same symbols and entry times across models are correlated",
                            "No actual orders, fills or real-money execution"]}


@app.get("/v94-regime-report")
async def v94_regime_report():
    """Read-only regime research. C and C_SL2 are never modified."""
    now_ms = int(time.time() * 1000)
    regimes = v94_regime(now_ms)
    rows = V90_STATE["trades"]
    c_rows = [t for t in rows if t.get("variant") == "C"]
    last_scan = V90_STATE.get("last_scan") or {}
    filters = last_scan.get("model_filters") or {}
    by_model = {m: {"summary": v90_stats([t for t in rows if t.get("variant") == m]),
                    "regime": regimes[m],
                    "last_scan_filters": filters.get(m, {}),
                    "recent_closed": [{"symbol": t["symbol"], "net_pct": t.get("net_pct"),
                                       "exit_reason": t.get("exit_reason"), "exit_utc": t.get("exit_utc")}
                                      for t in rows if t.get("variant") == m and t.get("status") == "CLOSED_SHADOW"][-15:]}
                for m in V94_MODELS}
    return {**MODE_INFO, "status": "RESEARCH_ONLY", "version": "V94_REGIME_PAPER",
            "c_rules_unchanged": True, "c_sl2_rules_unchanged": True,
            "paper_only": True, "orders": False, "trading": False,
            "baseline_C": v90_stats(c_rows),
            "baseline_C_SL2": v90_stats([t for t in rows if t.get("variant") == "C_SL2"]),
            "models": by_model, "reference_signal": "closed C trade net_pct only",
            "rules": {"V94_A": "last C close: loss -> pause 10m; win -> capacity 10 for 10m",
                      "V94_B": "last 3 C closes: >=2 losses -> pause 15m; >=2 wins -> capacity 10 for 10m",
                      "V94_C": "last 3 C closes: sum net negative -> pause 10m; positive -> capacity 10 for 10m"},
            "default_capacity": 5, "boost_capacity": 10,
            "last_scan_utc": last_scan.get("utc"),
            "limitations": ["Models use identical C entry filters and exits; only entry gating/capacity differs",
                            "If C filters yield fewer than 5 concurrent signals, boost cannot add trades",
                            "New arms have no retroactive positions; histories and exposure differ",
                            "Sampled stops can exceed nominal stop; no actual exchange orders"]}


@app.get("/v95-early-regime-report")
async def v95_early_regime_report():
    """Read-only early-PnL regime arms. C and C_SL2 unchanged."""
    rows = V90_STATE["trades"]
    regimes = v95_regime(int(time.time() * 1000))
    filters = (V90_STATE.get("last_scan") or {}).get("model_filters") or {}
    return {**MODE_INFO, "version": "V95_EARLY_REGIME_PAPER", "status": "RESEARCH_ONLY",
            "paper_only": True, "trading": False, "orders": False,
            "c_rules_unchanged": True, "c_sl2_rules_unchanged": True,
            "baseline_C": v90_stats([t for t in rows if t.get("variant") == "C"]),
            "baseline_C_SL2": v90_stats([t for t in rows if t.get("variant") == "C_SL2"]),
            "models": {m: {"summary": v90_stats([t for t in rows if t.get("variant") == m]),
                            "regime": regimes[m], "last_scan_filters": filters.get(m, {}),
                            "recent_closed": [{"symbol": t["symbol"], "net_pct": t.get("net_pct"),
                                               "exit_utc": t.get("exit_utc")}
                                              for t in rows if t.get("variant") == m and t.get("status") == "CLOSED_SHADOW"][-15:]}
                       for m in V95_MODELS},
            "rules": {"V95_EARLY5": "C observed gross PnL at first monitor >=5m: loss PAUSE 10m; profit BOOST capacity 10 for 10m",
                      "V95_EARLY10": "C observed gross PnL at first monitor >=10m: loss PAUSE 10m; profit BOOST capacity 10 for 10m"},
            "limitations": ["Only C observations recorded after deployment count; old trades cannot be reconstructed",
                            "Monitored every ~5 minutes: observations may occur later than exact 5m or 10m",
                            "Signals come from baseline C, not other arms; avoids duplicate correlated votes",
                            "Existing open positions are never force-closed by PAUSE",
                            "BOOST raises capacity only; does not relax C entry filters",
                            "No real orders; results are paper estimates"]}


@app.get("/v96-opportunity-report")
async def v96_opportunity_report():
    """Read-only paper comparison of actual C entries against V95 pause decisions."""
    c = [t for t in V90_STATE["trades"] if t.get("variant") == "C"]
    result = {}
    for model in V95_MODELS:
        tagged = [t for t in c if model in t.get("v96_early_decisions", {})]
        blocked = [t for t in tagged if t["v96_early_decisions"][model]["would_block"]]
        closed = [t for t in blocked if t.get("status") == "CLOSED_SHADOW" and isinstance(t.get("net_pct"), (int, float))]
        result[model] = {"observed_C_entries": len(tagged), "would_block": len(blocked),
                         "blocked_closed": len(closed), "blocked_still_open": len(blocked)-len(closed),
                         "losers_avoided": sum(t["net_pct"] < 0 for t in closed),
                         "winners_missed": sum(t["net_pct"] > 0 for t in closed),
                         "sum_blocked_C_net_pct": round(sum(t["net_pct"] for t in closed), 5),
                         "estimated_avoided_pct_points": round(-sum(t["net_pct"] for t in closed), 5),
                         "recent": [{"symbol":t["symbol"], "net_pct":t["net_pct"], "exit_utc":t.get("exit_utc")} for t in closed[-20:]]}
    return {**MODE_INFO, "version":"V96_OPPORTUNITY_PAPER", "paper_only":True,
            "c_rules_unchanged":True, "c_sl2_rules_unchanged":True,
             "retired_no_new_entries":list(dict.fromkeys(V96_RETIRED +
                 tuple(m for m in (V91_MODELS + tuple(V93_TP_TARGETS) + V93_RISK_MODELS + V94_MODELS + V95_MODELS + V97_TRAIL_MODELS) if m not in V93_ALL_ACTIVE))),
            "active_models":list(V93_ALL_ACTIVE),
            "baseline_C":v90_stats(c), "comparison":result,
            "limitations":["Only actual baseline C entries after V96 deployment are evaluated; not all possible candidate signals.",
                           "Avoided results assume skipping the C trade does not affect other entries.",
                           "BOOST extra entries not independently evaluated.",
                           "No checkpoint data deleted; existing retired positions finish normally."]}


@app.get("/v97-trailing-report")
async def v97_trailing_report():
    """Read-only V97 comparison; no new orders or mutations."""
    return {**MODE_INFO, "status": "OK", "version": "V97_SEVEN_MODELS_PAPER",
            "paper_only": True, "orders": False, "trading": False,
            "active_models": list(V93_ALL_ACTIVE),
            "trailing_activation_gross_pct": 5.0,
            "trailing_distance_pct": 5.0, "max_hold_minutes": 120,
            "monitor_interval_seconds": V90_SCAN_SECONDS,
            "by_model": {m: {"initial_stop_pct": -5.0 if m == "C_SL5_TRAIL5" else -10.0,
                             "summary": v90_stats([t for t in V90_STATE["trades"] if t.get("variant") == m]),
                             "recent_trades": [t for t in V90_STATE["trades"] if t.get("variant") == m][-10:]}
                         for m in V97_TRAIL_MODELS},
            "limitations": ["Trailing activation and stop are sampled every 5 minutes, not continuously",
                            "A price jump can cause fills beyond the nominal stop",
                            "Same C entry rules; no retroactive historical positions",
                            "Paper only; no real exchange orders"]}


async def cconfirm1_monitor(client):
    """Independent forward paper study; observe book bid on existing 5m scans.

    A confirmation requires an observed executable-side bid >= original ask * 1.01
    within 15m of the C_TREND entry. Paper entry uses the observed ask, not the
    original ask or the threshold, so a jump is not assumed to fill at +1%.
    """
    now_ms = int(time.time() * 1000)
    pending = [t for t in V90_STATE["trades"] if t.get("variant") == C_CONFIRM1
               and t.get("status") == "PENDING_CONFIRM"]
    if not pending:
        return
    changed = False
    for sym in set(t["symbol"] for t in pending):
        candidates = [t for t in pending if t["symbol"] == sym]
        # Expired candidates never receive an after-deadline confirmation.
        valid = [t for t in candidates if now_ms <= t["confirmation_deadline_ms"]]
        for t in candidates:
            if t not in valid:
                t["status"] = "EXPIRED_NO_CONFIRM"
                t["expired_utc"] = utc_now()
                changed = True
        if not valid:
            continue
        try:
            quote = await get_json(client, "/api/v3/ticker/bookTicker", {"symbol": sym})
            bid, ask = float(quote["bidPrice"]), float(quote["askPrice"])
            if bid <= 0 or ask <= 0 or ask < bid:
                continue
        except Exception as exc:
            V90_STATE["last_error"] = "confirm quote " + sym + ": " + str(exc)[:120]
            continue
        for t in valid:
            t["confirmation_checks"] += 1
            t["last_bid"] = bid
            changed = True
            if bid < t["confirmation_target_bid"]:
                continue
            t["status"] = "OPEN_SHADOW"
            t["confirmation_utc"] = utc_now()
            t["confirmation_ms"] = now_ms
            t["confirmation_delay_minutes"] = round((now_ms - t["signal_entry_ms"]) / 60000, 3)
            t["entry_utc"] = utc_now()
            t["entry_ms"] = now_ms
            t["entry_price"] = ask
            t["stop_price"] = ask * 0.97
            t["spread_pct"] = round((ask - bid) / ask * 100, 5)
            t["max_bid"] = bid
            t["min_bid"] = bid
            t["tp1_done"] = False
            t["trail_active"] = False
            t["be_armed"] = False
            # Existing v90_monitor handles the -3% stop, but its generic TIME120
            # is measured from paper entry. Override the time exit independently.
            t["confirm1_original_deadline_ms"] = t["time_exit_ms"]
            print(f"C_CONFIRM1 PAPER CONFIRMED symbol={sym} delay_min={t['confirmation_delay_minutes']}", flush=True)
    if changed:
        v903_save_state()


@app.get("/c-confirm1")
async def cconfirm1_report():
    rows = [t for t in V90_STATE["trades"] if t.get("variant") == C_CONFIRM1]
    closed = [t for t in rows if t.get("status") == "CLOSED_SHADOW"]
    return {**MODE_INFO, "status": "OK", "model": C_CONFIRM1,
            "source_model": "C_TREND", "paper_only": True,
            "c_trend_unchanged": True, "c_be_unchanged": True,
            "confirmation": "+1% observed bid versus original ask within 15 minutes",
            "confirmation_sampling_seconds": V90_SCAN_SECONDS,
            "stop_loss_pct": -3, "time_exit_minutes_from_original_entry": 120,
            "assumed_round_trip_cost_pct": V90_COST_PCT,
            "pending": sum(t.get("status") == "PENDING_CONFIRM" for t in rows),
            "expired_no_confirm": sum(t.get("status") == "EXPIRED_NO_CONFIRM" for t in rows),
            "stats": v90_stats(rows), "recent_closed": closed[-30:],
            "recent_pending": [t for t in rows if t.get("status") == "PENDING_CONFIRM"][-15:],
            "limitations": ["5-minute sampled bid may miss intrabar +1% moves",
                            "Paper entry uses observed ask; no actual execution",
                            "Time exit is from original C_TREND entry, not confirmation",
                            "No historical backfill; only new C_TREND entries are tracked"]}


@app.get("/c-new-variants")
async def c_new_variants_report():
    rows = V90_STATE["trades"]
    return {**MODE_INFO, "status": "OK", "version": "C_TREND_EXIT_RESEARCH_V1",
            "reference": "C_TREND", "active_signal_models": list(V93_ALL_ACTIVE),
            "frozen_no_new_entries": list(C_FROZEN_VARIANTS),
            "shadow_models": [], "frozen_exit_models": list(C_NEW_VARIANTS),
            "rules": {k: [{"target_gross_pct": p, "fraction_of_original": f}
                           for p, f in v] for k, v in C_LADDER_RULES.items()},
            "by_model": {m: {"summary": v90_stats([t for t in rows if t.get("variant") == m]),
                             "recent_closed": [t for t in rows if t.get("variant") == m and
                                               t.get("status") == "CLOSED_SHADOW"][-10:]}
                         for m in ("C_TREND", *C_NEW_VARIANTS)},
            "limitations": ["New variants start only after deployment; no backfill",
                            "Observed 5-minute bids can miss target/stop touches",
                            "Multiple TP targets reached between scans fill at one observed bid",
                            "One 0.15% round-trip cost applied to position-weighted gross return",
                            "Paper only; no orders or real execution"]}


# Read-only, forward-only market regime study. No entry/exit rules are changed.
def c_market_regime_bucket_btc(value):
    if value is None:
        return "UNKNOWN_HISTORICAL"
    if value < -1.0:
        return "BTC_BELOW_MINUS_1"
    if value < 0:
        return "BTC_MINUS_1_TO_0"
    return "BTC_NONNEGATIVE"


def c_market_regime_bucket_breadth(value):
    if value is None:
        return "UNKNOWN_HISTORICAL"
    if value < 0.20:
        return "BREADTH_BELOW_20PCT"
    if value <= 0.50:
        return "BREADTH_20_TO_50PCT"
    return "BREADTH_ABOVE_50PCT"


def c_market_regime_summary(rows):
    stats = v90_stats(rows)
    vals = [float(t["net_pct"]) for t in rows]
    stats["positive_count"] = sum(v > 0 for v in vals)
    stats["negative_count"] = sum(v < 0 for v in vals)
    stats["zero_count"] = sum(v == 0 for v in vals)
    return stats


@app.get("/c-market-regime-analysis")
async def c_market_regime_analysis():
    all_closed = [t for t in V90_STATE["trades"]
                  if t.get("variant") == "C_TREND" and t.get("status") == "CLOSED_SHADOW"]
    with_context = [t for t in all_closed if
                    isinstance(t.get("entry_btc_ret60_pct"), (int, float)) and
                    isinstance(t.get("entry_alt_positive_breadth"), (int, float))]
    by_btc, by_breadth, by_combination = {}, {}, {}
    for t in with_context:
        b = c_market_regime_bucket_btc(t["entry_btc_ret60_pct"])
        a = c_market_regime_bucket_breadth(t["entry_alt_positive_breadth"])
        by_btc.setdefault(b, []).append(t)
        by_breadth.setdefault(a, []).append(t)
        by_combination.setdefault(b + "__" + a, []).append(t)
    return {**MODE_INFO, "status": "OK", "version": "C_MARKET_REGIME_FORWARD_V1",
            "reference_model": "C_TREND", "paper_only": True,
            "entry_exit_rules_unchanged": True,
            "closed_total_retained": len(all_closed),
            "closed_with_entry_market_context": len(with_context),
            "closed_missing_entry_market_context": len(all_closed) - len(with_context),
            "overall_retained": c_market_regime_summary(all_closed),
            "context_cohort": c_market_regime_summary(with_context),
            "by_btc_60m": {k: c_market_regime_summary(v) for k, v in sorted(by_btc.items())},
            "by_alt_breadth": {k: c_market_regime_summary(v) for k, v in sorted(by_breadth.items())},
            "by_combination": {k: c_market_regime_summary(v) for k, v in sorted(by_combination.items())},
            "recent_context_trades": [{"symbol": t.get("symbol"), "entry_utc": t.get("entry_utc"),
                "net_pct": t.get("net_pct"), "btc_ret60_pct": t.get("entry_btc_ret60_pct"),
                "alt_positive_breadth": t.get("entry_alt_positive_breadth"),
                "exit_reason": t.get("exit_reason")}
                for t in with_context[-30:]],
            "limitations": ["Forward-only: old trades without an entry market snapshot are not assigned a regime",
                            "Regime metrics use last completed 5-minute candles, not tick-perfect entry-time values",
                            "Breadth uses the sampled top-100 universe, not every altcoin",
                            "Groups are descriptive; small samples and correlated trades limit statistical inference",
                            "Paper-only quotes and sampled exits are not executable fills",
                            "Only retained checkpoint history is available; previously pruned trades cannot be reconstructed"]}


# Observation ranges are research counters, never active entry models.
C_OBSERVATION_MOMENTUM_RANGES = ((1., 8.), (2., 8.), (2.5, 8.))
C_OBSERVATION_VOLUME_RANGES = ((1., 3.5), (1.2, 3.5), (1.5, 3.5))


def c_observe_filter_ranges(candidates, scan):
    """Observe existing candle snapshots only, without additional network requests."""
    failures = {k: 0 for k in ("momentum", "volume", "z", "breakout")}
    for c in candidates:
        failures["momentum"] += not 2.5 <= c["ret60_pct"] <= 8.
        failures["volume"] += not 1.2 <= c["volume_ratio"] < 3.5
        failures["z"] += c["z"] >= 3.5
        failures["breakout"] += not .2 <= c["breakout_pct"] <= 1.5
    counts = {}
    for lo, hi in C_OBSERVATION_MOMENTUM_RANGES:
        for vlo, vhi in C_OBSERVATION_VOLUME_RANGES:
            key = f"momentum_{lo:g}_{hi:g}__volume_{vlo:g}_{vhi:g}"
            matching = [c for c in candidates if lo <= c["ret60_pct"] <= hi
                        and vlo <= c["volume_ratio"] < vhi]
            counts[key] = {"momentum_volume_matches": len(matching),
                "with_other_candle_filters": sum(c["z"] < 3.5 and
                    .2 <= c["breakout_pct"] <= 1.5 for c in matching)}
    state = V90_STATE["filter_observations"]
    state.setdefault("started_utc", scan["utc"])
    state["scans"] = state.get("scans", 0) + 1
    state["candidate_observations"] = state.get("candidate_observations", 0) + len(candidates)
    totals = state.setdefault("range_totals", {})
    for key, counts_for_range in counts.items():
        total = totals.setdefault(key, {name: 0 for name in counts_for_range})
        for name, value in counts_for_range.items():
            total[name] += value
    state["last_scan"] = {"utc": scan["utc"], "candidate_count": len(candidates),
        "independent_failures": failures, "ranges": counts,
        "market_context": dict(scan.get("market_context", {}))}


@app.get("/c-entry-diagnostics")
async def c_entry_diagnostics():
    """Read-only diagnostics; does not scan, notify, or write a checkpoint."""
    rows = [t for t in V90_STATE["trades"] if t.get("variant") == "C"]
    latest = max(rows, key=lambda t: t.get("entry_ms", 0), default=None)
    fields = ("entry_btc_ret60_pct", "entry_alt_positive_breadth", "ret60_pct",
              "volume_ratio", "spread_pct", "entry_utc")
    return {**MODE_INFO, "paper_only": True, "version": "C_ENTRY_OBSERVATION_V1",
        "active_models": list(V93_ALL_ACTIVE), "frozen_no_new_entries": list(C_FROZEN_VARIANTS),
        "last_retained_entry": ({k: latest.get(k) for k in
            ("symbol", "entry_ms", *fields)} if latest else None),
        "entry_field_coverage": {field: sum(t.get(field) is not None for t in rows)
                                 for field in fields},
        "reference_model": "C", "c_trades_retained": len(rows),
        "observations": V90_STATE["filter_observations"],
        "limitations": ["Counts are candidate-scan observations, not unique signals or paper trades",
            "Spread, price chase, deduplication and capacity are not checked for alternatives",
            "Independent failure counts overlap; counts use baseline C candle filters",
            "Alternatives have no measured returns; counts do not establish profitability",
            "No historical backfill; counters start with deployment",
            "Entry market context reflects completed candles from the scan, not tick-time data"]}


# Explicit, entry-time hypotheses. These flags NEVER gate baseline C entries.
C_LOSS_HYPOTHESES = {
    "btc_negative": ("entry_btc_ret60_pct", "below", 0.),
    "breadth_below_half": ("entry_alt_positive_breadth", "below", .5),
    "chase_above_quarter_pct": ("chase_pct", "above", .25),
    "spread_above_tenth_pct": ("spread_pct", "above", .1),
    "z_at_most_2_5": ("z", "at_most", 2.5),
}


def c_loss_entry_flags(trade):
    flags = {}
    for name, (field, direction, threshold) in C_LOSS_HYPOTHESES.items():
        value = trade.get(field)
        known = isinstance(value, (int, float)) and not isinstance(value, bool)
        flags[name] = ((value < threshold if direction == "below" else
                        value <= threshold if direction == "at_most" else
                        value > threshold) if known else None)
    return {"version": "C_LOSS_OBSERVATION_V1", "recorded_utc": utc_now(),
            "would_skip": flags}


def c_loss_comparison(rows, flag):
    eligible = [t for t in rows if isinstance(
        t.get("c_loss_observation_v1", {}).get("would_skip", {}).get(flag), bool)]
    skipped = [t for t in eligible if t["c_loss_observation_v1"]["would_skip"][flag]]
    kept = [t for t in eligible if not t["c_loss_observation_v1"]["would_skip"][flag]]
    delta = -sum(t["net_pct"] for t in skipped)
    return {"eligible_closed": len(eligible), "unknown_closed": len(rows) - len(eligible),
        "would_skip": v90_stats(skipped), "would_keep": v90_stats(kept),
        "losers_avoided": sum(t["net_pct"] < 0 for t in skipped),
        "winners_missed": sum(t["net_pct"] > 0 for t in skipped),
        "hypothetical_change_sum_return_pct_points": round(delta, 5) if eligible else None,
        "evidence_status": "OBSERVATION_ONLY_NOT_VALIDATED"}


@app.get("/c-loss-analysis")
async def c_loss_analysis():
    """Baseline C only. No inferred historical entry context or new orders."""
    rows = [t for t in V90_STATE["trades"] if t.get("variant") == "C"]
    closed = sorted([t for t in rows if t.get("status") == "CLOSED_SHADOW"],
                    key=lambda t: t.get("exit_utc", ""))
    reasons = {}
    for t in closed:
        reasons.setdefault(t.get("exit_reason", "UNKNOWN"), []).append(t)
    breakdowns = {}
    dimensions = {"ret60_pct": [2.5, 3, 4, 5, 6, 8],
        "volume_ratio": [1.2, 1.5, 2, 2.5, 3, 3.5], "z": [0, 1, 2, 2.5, 3, 3.5],
        "breakout_pct": [.2, .5, 1, 1.5], "spread_pct": [.05, .1, .12],
        "chase_pct": [0, .1, .25, .5]}
    for field, bounds in dimensions.items():
        groups = {}
        for t in closed:
            groups.setdefault(v914_bucket(t.get(field), bounds), []).append(t)
        breakdowns[field] = {k: v90_stats(v) for k, v in groups.items()}
    return {**MODE_INFO, "reference_model": "C", "paper_only": True,
        "version": "C_LOSS_OBSERVATION_V1", "entry_thresholds_and_exit_levels_unchanged": True,
        "new_C_exit_monitor_seconds": C_MONITOR_SECONDS,
        "automatic_entry_blocking": False, "summary": v90_stats(rows),
        "historical_C": v90_stats([t for t in rows if t.get("c_forward_version") != C_FORWARD_VERSION]),
        "forward_C": v90_stats([t for t in rows if t.get("c_forward_version") == C_FORWARD_VERSION]),
        "loss_count": sum(t["net_pct"] < 0 for t in closed),
        "exit_reasons": {k: v90_stats(v) for k, v in reasons.items()},
        "historical_descriptive_breakdowns": breakdowns,
        "recent_20_closed": v90_stats(closed[-20:]),
        "hypotheses": {name: {"field": field, "comparison": direction, "threshold": threshold,
            **c_loss_comparison(closed, name)}
            for name, (field, direction, threshold) in C_LOSS_HYPOTHESES.items()},
        "limitations": ["Only baseline C records are included; C_TREND, V97 and V13 are not pooled",
            "Historical breakdowns describe association, not causes or validated filters",
            "Forward comparisons use only flags recorded at entry after deployment; no backfill",
            "Skipped returns omit capacity effects and alternative entries; not portfolio returns",
            "Hypotheses overlap; avoided returns must not be added across hypotheses",
            "Overlapping symbols and entry times are correlated; sufficient forward data required",
            "No hypothesis gates C entries; new forward C exits are checked approximately every 60 seconds"]}




def c_fsync_directory(folder):
    fd = os.open(str(folder), os.O_RDONLY | os.O_DIRECTORY)
    try:
        os.fsync(fd)
    finally:
        os.close(fd)




def c_backup_checkpoint(raw):
    digest = hashlib.sha256(raw).hexdigest()
    backup = V90_STATE_PATH.with_name(V90_STATE_PATH.name + ".before-c-forward-" + digest[:16] + ".bak")
    if backup.exists():
        if hashlib.sha256(backup.read_bytes()).hexdigest() != digest:
            raise ValueError("checkpoint backup checksum mismatch")
        return backup
    with backup.open("xb") as f:
        f.write(raw)
        f.flush()
        os.fsync(f.fileno())
    c_fsync_directory(backup.parent)
    return backup


async def c_fast_monitor_once():
    global V90_LOCK
    if V90_LOCK is None:
        V90_LOCK = asyncio.Lock()
    async with V90_LOCK:
        if not any(t.get("status") == "OPEN_SHADOW" and t.get("variant") in ("C", *C_SHADOW_MODELS)
                   and t.get("c_forward_version") == C_FORWARD_VERSION for t in V90_STATE["trades"]):
            return
        async with httpx.AsyncClient(timeout=20.) as client:
            await v90_monitor(client, fast_only=True)
        v903_save_state()  # Also persist partial-profit and extrema updates.
        C_MONITOR_STATE["last_check_utc"] = utc_now()
        C_MONITOR_STATE["checks"] += 1


async def c_fast_monitor_loop():
    while True:
        try:
            await c_fast_monitor_once()
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            C_MONITOR_STATE["last_error"] = type(exc).__name__
        await asyncio.sleep(C_MONITOR_SECONDS)


@app.on_event("shutdown")
async def c_paper_shutdown():
    tasks = [task for task in (V90_TASK, C_MONITOR_TASK, TOP20_TASK, TOP20_MONITOR_TASK) if task is not None]
    for task in tasks:
        task.cancel()
    if tasks:
        await asyncio.gather(*tasks, return_exceptions=True)
    if V915_STORAGE["last_restore_status"] == "RESTORED":
        v903_save_state()


@app.get("/paper-records")
async def c_paper_records(limit: int = 250, offset: int = 0):
    """Read-only export of every retained status, never Telegram credentials."""
    limit, offset = max(1, min(limit, 500)), max(0, offset)
    return {**MODE_INFO, "total": len(V90_STATE["trades"]), "limit": limit,
        "offset": offset, "trades": V90_STATE["trades"][offset:offset + limit],
        "recovery_metadata": V90_STATE["recovery_metadata"]}
