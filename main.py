from datetime import timedelta
import bisect
import statistics
import inspect
import psycopg
# ============================================================
# V61.2 SAFE EVENT-STUDY PATCH
# ------------------------------------------------------------
# V61.1 strategy / entry / risk / FAST_3S behavior is unchanged.
# This patch adds a persistent, low-load event-study job registry
# so research progress/result survives Render restarts.
# PAPER / RESEARCH ONLY. No real orders.
# ============================================================

# ============================================================
# ALT-MOMENTUM-V1 / V61 (paper-only)
# V60 uzerine eklenenler (giris/cikis KURALLARI DEGISMEDI):
#  1) Hizli stop izleyici (3 sn): sert stop/trailing artik tarama dongusunu (~60 sn)
#     beklemiyor. Ayni V55 kurallari, ayni kayit yapisi; stop_monitor=FAST_3S ile etiketli.
#  2) Giris baglami kaydi (ctx_*): 60dk zirveye uzaklik, aralik konumu, hacim orani,
#     5m ATR, BTC 30dk/60dk/4s getirisi. Sonraki analizler icin.
#  3) Cikis tanisi: stop_overshoot_pct, hold_minutes, stop_monitor.
#  4) /v61-stop-review: gercek mumlarla 120dk karsi-olgusal, MAE/MFE, BTC baglami,
#     kume/ust uste islem analizi, tekrar giris listesi (state'e dokunmaz).
#  5) /v61-event-study-start + /v61-event-study: gecmis veride (varsayilan 60 sembol,
#     45 gun) kuralin replikasi, erken giris ve rastgele-giris kiyasi, cikis izgarasi,
#     gun-bazli bootstrap guven araligi.
#  6) Telegram cikis mesajina neden; giris mesajina stop seviyesi.
# ============================================================
# ============================================================
# ALT-MOMENTUM-V1  --  tek dosya, V42 operasyonel guncelleme
# Orijinal V1-V41 arastirma/forward kodu korunmustur. Eklenenler:
#  1) Canli giris fiyati (bookTicker ask) + spread filtresi + gecikme kaydi
#  2) Cikis suresi canli girisen itibaren 120 dk (HOLD_FROM_LIVE_ENTRY)
#  3) Golge stop-loss olcumu (%2 / %3) + MAE/MFE (gercek cikisi DEGISTIRMEZ)
#  4) Telegram: hata-guvenli, zengin mesajlar, gunluk ozet
#  5) State kaydi ONCE, bildirim SONRA (V22 hata-bozulma duzeltmesi)
#  6) seen_signal_keys 7 gunden eskileri temizler
#  7) build_universe 60 sn onbellek (V27/V32/V37 tekrar yukunu azaltir)
#  8) cohort_size kaydi, /alt-live-overview ve /alt-daily-summary endpoint'leri
#  9) Turkce karakter/emoji (cift encoding) bozulmasi duzeltildi
# Hepsi paper-only; gercek emir gonderilmez.
# ============================================================
import json
import os
import asyncio
import math
from datetime import datetime, timezone
from statistics import mean, median

import httpx
from fastapi import FastAPI, Query
import time

app = FastAPI(title="ALT-MOMENTUM-V1")

BINANCE = "https://data-api.binance.vision"
MODEL = "ALT-MOMENTUM-V1"

MIN_QUOTE_VOLUME_USDT = 250_000
ROUND_TRIP_COST_PCT = 0.15

# Altcoin araÅŸtÄ±rmasÄ± iÃ§in istemediÄŸimiz baz varlÄ±klar
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
    if base in EXCLUDED_BASES:
        return False

    if base.endswith(("UP", "DOWN", "BULL", "BEAR")):
        return False

    return True


async def get_json(client, path, params=None):
    response = await client.get(
        f"{BINANCE}{path}",
        params=params,
        timeout=30.0,
    )
    response.raise_for_status()
    return response.json()


V26_NON_ALT_BASE_EXCLUSIONS = {'AVGO', 'SKHYB', 'MSTRB', 'WDC', 'SOXL', 'AVGOB', 'SOXLB', 'SKHY', 'GOOGL', 'AAPL', 'TSLA', 'GOOGLB', 'NVDA', 'QQQB', 'USTC', 'EURI', 'AAPLB', 'MSTR', 'QQQ', 'USDE', 'SOXSB', 'WDCB', 'INTC', 'NVDAB', 'XUSD', 'TSLAB', 'INTCB', 'SOXS', 'AMD', 'AMDB', 'MVLL', 'MVLLB'}

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


def analyze_latest(symbol, candles, quote_volume):
    if len(candles) < 8:
        return None

    closes = [x["close"] for x in candles]
    volumes = [x["volume"] for x in candles]

    current = closes[-1]

    mom5 = pct_change(closes[-2], current)
    mom15 = pct_change(closes[-4], current)
    mom30 = pct_change(closes[-7], current)

    previous_volumes = volumes[-7:-1]

    avg_volume = (
        mean(previous_volumes)
        if previous_volumes
        else 0
    )

    volume_ratio = (
        volumes[-1] / avg_volume
        if avg_volume > 0
        else 0
    )

    research_score = (
        (mom5 * 3)
        + (mom15 * 2)
        + mom30
        + min(volume_ratio, 5)
    )

    return {
        "symbol": symbol,
        "price": current,
        "momentum_5m_pct": round(mom5, 4),
        "momentum_15m_pct": round(mom15, 4),
        "momentum_30m_pct": round(mom30, 4),
        "volume_ratio": round(volume_ratio, 3),
        "quote_volume_24h": quote_volume,
        "research_score": round(research_score, 4),
    }


def historical_events(candles):
    """
    Burada AL sinyali Ã¼retmiyoruz.

    AmaÃ§:
    GeÃ§miÅŸte belirli momentum + hacim koÅŸullarÄ± oluÅŸtuÄŸunda
    fiyatÄ±n 15/30/60 dakika sonra ne yaptÄ±ÄŸÄ±nÄ± Ã¶lÃ§mek.
    """

    events = []

    # Ä°leri Ã¶lÃ§Ã¼m iÃ§in 12 mum = 60 dakika gerekir.
    for i in range(7, len(candles) - 12):

        current = candles[i]["close"]

        mom5 = pct_change(
            candles[i - 1]["close"],
            current,
        )

        mom15 = pct_change(
            candles[i - 3]["close"],
            current,
        )

        mom30 = pct_change(
            candles[i - 6]["close"],
            current,
        )

        previous_volumes = [
            candles[j]["volume"]
            for j in range(i - 6, i)
        ]

        avg_volume = mean(previous_volumes)

        volume_ratio = (
            candles[i]["volume"] / avg_volume
            if avg_volume > 0
            else 0
        )

        # Ä°lk araÅŸtÄ±rma koÅŸulu.
        # Bunlar nihai parametre deÄŸildir.
        if mom5 < 0.30:
            continue

        if mom15 < 0.50:
            continue

        if mom30 < 0.50:
            continue

        if volume_ratio < 1.20:
            continue

        # Ã‡oktan aÅŸÄ±rÄ± koÅŸmuÅŸ hareketleri ilk aÅŸamada ayÄ±r.
        if mom5 > 3.00:
            continue

        if mom30 > 6.00:
            continue

        price_15 = candles[i + 3]["close"]
        price_30 = candles[i + 6]["close"]
        price_60 = candles[i + 12]["close"]

        ret15 = pct_change(current, price_15)
        ret30 = pct_change(current, price_30)
        ret60 = pct_change(current, price_60)

        events.append(
            {
                "time_utc": datetime.fromtimestamp(
                    candles[i]["close_time"] / 1000,
                    tz=timezone.utc,
                ).isoformat(),
                "entry_reference": current,
                "momentum_5m_pct": round(mom5, 4),
                "momentum_15m_pct": round(mom15, 4),
                "momentum_30m_pct": round(mom30, 4),
                "volume_ratio": round(volume_ratio, 3),
                "return_15m_pct": round(ret15, 4),
                "return_30m_pct": round(ret30, 4),
                "return_60m_pct": round(ret60, 4),
            }
        )

    return events


def summarize_events(events):
    if not events:
        return {
            "event_count": 0,
            "return_15m": None,
            "return_30m": None,
            "return_60m": None,
        }

    def stats(field):
        values = [x[field] for x in events]

        return {
            "mean_pct": round(mean(values), 4),
            "win_rate_pct": round(
                sum(1 for x in values if x > 0)
                / len(values)
                * 100,
                2,
            ),
            "best_pct": round(max(values), 4),
            "worst_pct": round(min(values), 4),
        }

    return {
        "event_count": len(events),
        "return_15m": stats("return_15m_pct"),
        "return_30m": stats("return_30m_pct"),
        "return_60m": stats("return_60m_pct"),
    }


@app.get("/")
async def root():
    return {
        **MODE_INFO,
        "status": "OK",
        "endpoints": [
            "/health",
            "/universe",
            "/scan?count=20",
            "/backtest/TIAUSDT?limit=1000",
            "/backtest-all?count=30&limit=1000",
            "/backtest30?count=30&days=30",
            "/backtest90?count=30&days=90",
            "/continuation90?count=30&days=90",
            "/continuation-quality90?count=30&days=90",
            "/continuation-combo90?count=30&days=90",
            "/h6-validate?count=30&days=365",
            "/h6-regime?count=30&days=365",
            "/h6-trailing?count=30&days=365",
            "/pattern-discovery?count=30&days=365",
            "/pullback-reentry?count=30&days=365",
            "/relative-momentum?count=30&days=365",
            "/relative-momentum-horizons?count=10&days=90",
            "/relative-momentum-entry-delay?count=10&days=90",
            "/post-rise-behavior?count=10&days=90",
            "/v16-validate?count=30&days=365",
        ],
    }


@app.get("/health")
async def health():
    try:
        async with httpx.AsyncClient() as client:
            data = await get_json(
                client,
                "/api/v3/time",
            )

        return {
            **MODE_INFO,
            "status": "OK",
            "binance": "CONNECTED",
            "server_time": data.get("serverTime"),
            "checked_utc": utc_now(),
        }

    except Exception as e:
        return {
            **MODE_INFO,
            "status": "ERROR",
            "error": str(e),
            "checked_utc": utc_now(),
        }


@app.get("/universe")
async def universe():
    try:
        async with httpx.AsyncClient() as client:
            symbols = await build_universe(client)

        return {
            **MODE_INFO,
            "status": "OK",
            "min_quote_volume_usdt": MIN_QUOTE_VOLUME_USDT,
            "symbol_count": len(symbols),
            "symbols": symbols,
        }

    except Exception as e:
        return {
            **MODE_INFO,
            "status": "ERROR",
            "error": str(e),
        }


@app.get("/scan")
async def scan(
    count: int = Query(
        default=20,
        ge=5,
        le=50,
    )
):
    try:
        async with httpx.AsyncClient() as client:

            universe_data = await build_universe(client)

            semaphore = asyncio.Semaphore(12)

            async def worker(item):
                async with semaphore:
                    try:
                        candles = await get_completed_5m_candles(
                            client,
                            item["symbol"],
                            20,
                        )

                        return analyze_latest(
                            item["symbol"],
                            candles,
                            item["quote_volume_24h"],
                        )

                    except Exception:
                        return None

            tasks = [
                worker(item)
                for item in universe_data
            ]

            results = await asyncio.gather(*tasks)

        results = [
            x for x in results
            if x is not None
        ]

        results.sort(
            key=lambda x: x["research_score"],
            reverse=True,
        )

        return {
            **MODE_INFO,
            "status": "OK",
            "signal": False,
            "note": "RESEARCH RANKING ONLY - NOT A BUY SIGNAL",
            "universe_size": len(universe_data),
            "analyzed": len(results),
            "returned": min(count, len(results)),
            "generated_utc": utc_now(),
            "leaders": results[:count],
        }

    except Exception as e:
        return {
            **MODE_INFO,
            "status": "ERROR",
            "error": str(e),
        }


@app.get("/backtest/{symbol}")
async def backtest(
    symbol: str,
    limit: int = Query(
        default=1000,
        ge=100,
        le=1000,
    ),
):
    """
    Research endpoint.
    Emir Ã¼retmez.
    Paper trade aÃ§maz.
    """

    symbol = symbol.upper().strip()

    try:
        async with httpx.AsyncClient() as client:
            candles = await get_completed_5m_candles(
                client,
                symbol,
                limit,
            )

        events = historical_events(candles)
        summary = summarize_events(events)

        return {
            **MODE_INFO,
            "status": "OK",
            "signal": False,
            "symbol": symbol,
            "candle_count": len(candles),
            "research_conditions": {
                "momentum_5m_min_pct": 0.30,
                "momentum_15m_min_pct": 0.50,
                "momentum_30m_min_pct": 0.50,
                "volume_ratio_min": 1.20,
                "momentum_5m_max_pct": 3.00,
                "momentum_30m_max_pct": 6.00,
            },
            "summary": summary,
            "last_events": events[-10:],
            "generated_utc": utc_now(),
            "note": (
                "RESEARCH EVENT STUDY ONLY - "
                "NOT A VALIDATED TRADING STRATEGY"
            ),
        }

    except Exception as e:
        return {
            **MODE_INFO,
            "status": "ERROR",
            "symbol": symbol,
            "error": str(e),
            "generated_utc": utc_now(),
        }

def apply_cooldown(events, cooldown_minutes):
    """
    AynÄ± coin iÃ§in birbirine Ã§ok yakÄ±n eventleri baÄŸÄ±msÄ±z iÅŸlem gibi
    saymamak amacÄ±yla cooldown uygular.
    """
    if not events:
        return []

    kept = []
    last_kept_time = None
    cooldown_seconds = cooldown_minutes * 60

    for event in events:
        event_time = datetime.fromisoformat(event["time_utc"])

        if last_kept_time is None:
            kept.append(event)
            last_kept_time = event_time
            continue

        if (event_time - last_kept_time).total_seconds() >= cooldown_seconds:
            kept.append(event)
            last_kept_time = event_time

    return kept


def summarize_events_with_cost(events, cost_pct=ROUND_TRIP_COST_PCT):
    """
    15/30/60 dk brÃ¼t sonuÃ§larÄ± ve sabit round-trip maliyet sonrasÄ±
    net sonuÃ§larÄ± birlikte Ã¶zetler.
    """
    if not events:
        return {
            "event_count": 0,
            "cost_pct": cost_pct,
            "return_15m": None,
            "return_30m": None,
            "return_60m": None,
        }

    def stats(field):
        gross_values = [float(x[field]) for x in events]
        net_values = [x - cost_pct for x in gross_values]

        return {
            "gross_mean_pct": round(mean(gross_values), 4),
            "gross_win_rate_pct": round(
                sum(1 for x in gross_values if x > 0)
                / len(gross_values)
                * 100,
                2,
            ),
            "net_mean_pct": round(mean(net_values), 4),
            "net_win_rate_pct": round(
                sum(1 for x in net_values if x > 0)
                / len(net_values)
                * 100,
                2,
            ),
            "best_gross_pct": round(max(gross_values), 4),
            "worst_gross_pct": round(min(gross_values), 4),
            "best_net_pct": round(max(net_values), 4),
            "worst_net_pct": round(min(net_values), 4),
        }

    return {
        "event_count": len(events),
        "cost_pct": cost_pct,
        "return_15m": stats("return_15m_pct"),
        "return_30m": stats("return_30m_pct"),
        "return_60m": stats("return_60m_pct"),
    }


def compact_coin_result(symbol, quote_volume, candle_count, raw_events):
    events_30 = apply_cooldown(raw_events, 30)
    events_60 = apply_cooldown(raw_events, 60)

    return {
        "symbol": symbol,
        "quote_volume_24h": quote_volume,
        "candle_count": candle_count,
        "raw": summarize_events_with_cost(raw_events),
        "cooldown_30m": summarize_events_with_cost(events_30),
        "cooldown_60m": summarize_events_with_cost(events_60),
    }


@app.get("/backtest-all")
async def backtest_all(
    count: int = Query(
        default=30,
        ge=5,
        le=50,
    ),
    limit: int = Query(
        default=1000,
        ge=100,
        le=1000,
    ),
):
    """
    Ã‡oklu altcoin research/event-study endpointi.

    - Emir Ã¼retmez.
    - Paper trade aÃ§maz.
    - Mevcut 24h likidite evreninden en yÃ¼ksek hacimli coinleri seÃ§er.
    - Ham eventleri, 30 dk cooldown ve 60 dk cooldown sonuÃ§larÄ±nÄ± karÅŸÄ±laÅŸtÄ±rÄ±r.
    - %0.15 round-trip araÅŸtÄ±rma maliyetini net sonuÃ§lardan dÃ¼ÅŸer.

    Not:
    Bu ilk geniÅŸ test current-universe yaklaÅŸÄ±mÄ± kullanÄ±r; dolayÄ±sÄ±yla
    survivorship / current-liquidity bias iÃ§erebilir.
    """
    try:
        async with httpx.AsyncClient() as client:
            universe_data = await build_universe(client)
            selected = universe_data[:count]

            semaphore = asyncio.Semaphore(12)

            async def worker(item):
                async with semaphore:
                    try:
                        candles = await get_completed_5m_candles(
                            client,
                            item["symbol"],
                            limit,
                        )

                        raw_events = historical_events(candles)

                        return {
                            "ok": True,
                            "symbol": item["symbol"],
                            "quote_volume_24h": item["quote_volume_24h"],
                            "candle_count": len(candles),
                            "raw_events": raw_events,
                        }

                    except Exception as e:
                        return {
                            "ok": False,
                            "symbol": item["symbol"],
                            "error": str(e),
                        }

            results = await asyncio.gather(
                *[worker(item) for item in selected]
            )

        successful = [x for x in results if x.get("ok")]
        failed = [x for x in results if not x.get("ok")]

        all_raw = []
        all_cd30 = []
        all_cd60 = []
        per_coin = []

        for item in successful:
            raw_events = item["raw_events"]
            cd30 = apply_cooldown(raw_events, 30)
            cd60 = apply_cooldown(raw_events, 60)

            all_raw.extend(raw_events)
            all_cd30.extend(cd30)
            all_cd60.extend(cd60)

            per_coin.append(
                compact_coin_result(
                    item["symbol"],
                    item["quote_volume_24h"],
                    item["candle_count"],
                    raw_events,
                )
            )

        # Coin listesini hacme gÃ¶re okunabilir sÄ±rada tut.
        per_coin.sort(
            key=lambda x: x["quote_volume_24h"],
            reverse=True,
        )

        return {
            **MODE_INFO,
            "status": "OK",
            "signal": False,
            "note": (
                "MULTI-COIN RESEARCH EVENT STUDY ONLY - "
                "NOT A VALIDATED TRADING STRATEGY"
            ),
            "universe_size": len(universe_data),
            "requested_coin_count": count,
            "selected_coin_count": len(selected),
            "successful_coin_count": len(successful),
            "failed_coin_count": len(failed),
            "candle_limit_per_coin": limit,
            "round_trip_cost_pct": ROUND_TRIP_COST_PCT,
            "research_conditions": {
                "momentum_5m_min_pct": 0.30,
                "momentum_15m_min_pct": 0.50,
                "momentum_30m_min_pct": 0.50,
                "volume_ratio_min": 1.20,
                "momentum_5m_max_pct": 3.00,
                "momentum_30m_max_pct": 6.00,
            },
            "combined": {
                "raw": summarize_events_with_cost(all_raw),
                "cooldown_30m": summarize_events_with_cost(all_cd30),
                "cooldown_60m": summarize_events_with_cost(all_cd60),
            },
            "per_coin": per_coin,
            "failed": failed,
            "generated_utc": utc_now(),
            "limitations": [
                "Current liquid universe is used; historical survivorship bias is possible.",
                "Entry reference is the event candle close, not a simulated next-candle fill.",
                "Cost is a fixed research assumption and does not model variable slippage.",
                "1000 x 5m candles is only about 3.5 days of data.",
            ],
        }

    except Exception as e:
        return {
            **MODE_INFO,
            "status": "ERROR",
            "signal": False,
            "error": str(e),
            "generated_utc": utc_now(),
        }

async def get_5m_candles_days(client, symbol, days=30):
    """
    Binance 1000-kline limitini geriye doÄŸru sayfalayarak tamamlanmÄ±ÅŸ
    5m mumlarÄ± toplar. VarsayÄ±lan 30 gÃ¼n ~= 8640 mum.
    """
    now_ms = int(datetime.now(timezone.utc).timestamp() * 1000)
    start_ms = now_ms - (days * 24 * 60 * 60 * 1000)
    end_time = now_ms
    by_open_time = {}

    # 30 gÃ¼n iÃ§in yaklaÅŸÄ±k 9 istek gerekir. GÃ¼venli Ã¼st sÄ±nÄ±r bÄ±rakÄ±yoruz.
    max_pages = max(2, int((days * 288) / 1000) + 3)

    for _ in range(max_pages):
        raw = await get_json(
            client,
            "/api/v3/klines",
            params={
                "symbol": symbol,
                "interval": "5m",
                "limit": 1000,
                "endTime": end_time,
            },
        )

        if not raw:
            break

        oldest_open = int(raw[0][0])

        for k in raw:
            open_time = int(k[0])
            close_time = int(k[6])

            if close_time >= now_ms:
                continue

            if open_time < start_ms:
                continue

            by_open_time[open_time] = {
                "open_time": open_time,
                "open": float(k[1]),
                "high": float(k[2]),
                "low": float(k[3]),
                "close": float(k[4]),
                "volume": float(k[5]),
                "close_time": close_time,
            }

        if oldest_open <= start_ms:
            break

        # Bir Ã¶nceki sayfanÄ±n sonundan daha eski veriye git.
        end_time = oldest_open - 1

        # Binance'a gereksiz burst yapmamak iÃ§in kÃ¼Ã§Ã¼k bir ara.
        await asyncio.sleep(0.03)

    candles = sorted(
        by_open_time.values(),
        key=lambda x: x["open_time"],
    )

    return candles


def historical_trades_v2(candles, symbol):
    """
    V2 research trade modeli:
    - Sinyal tamamlanmÄ±ÅŸ 5m mum kapanÄ±ÅŸÄ±nda hesaplanÄ±r.
    - GiriÅŸ bir sonraki 5m mumun OPEN fiyatÄ±dÄ±r.
    - Ã‡Ä±kÄ±ÅŸ giriÅŸten tam 60 dakika sonraki mumun OPEN fiyatÄ±dÄ±r.
    - AynÄ± coin iÃ§in giriÅŸler arasÄ±nda en az 60 dakika cooldown.
    - Sabit %0.15 round-trip araÅŸtÄ±rma maliyeti.
    """
    trades = []
    last_entry_open_time = None
    cooldown_ms = 60 * 60 * 1000

    # i sinyal mumu; i+1 giriÅŸ; i+13 = giriÅŸten 60 dk sonraki open.
    for i in range(7, len(candles) - 13):
        signal_close = candles[i]["close"]

        mom5 = pct_change(
            candles[i - 1]["close"],
            signal_close,
        )
        mom15 = pct_change(
            candles[i - 3]["close"],
            signal_close,
        )
        mom30 = pct_change(
            candles[i - 6]["close"],
            signal_close,
        )

        previous_volumes = [
            candles[j]["volume"]
            for j in range(i - 6, i)
        ]
        avg_volume = mean(previous_volumes)

        volume_ratio = (
            candles[i]["volume"] / avg_volume
            if avg_volume > 0
            else 0
        )

        if mom5 < 0.30:
            continue
        if mom15 < 0.50:
            continue
        if mom30 < 0.50:
            continue
        if volume_ratio < 1.20:
            continue
        if mom5 > 3.00:
            continue
        if mom30 > 6.00:
            continue

        entry_candle = candles[i + 1]
        exit_candle = candles[i + 13]

        entry_open_time = entry_candle["open_time"]

        if (
            last_entry_open_time is not None
            and entry_open_time - last_entry_open_time < cooldown_ms
        ):
            continue

        entry_price = entry_candle["open"]
        exit_price = exit_candle["open"]

        gross_pct = pct_change(entry_price, exit_price)
        net_pct = gross_pct - ROUND_TRIP_COST_PCT

        trades.append(
            {
                "symbol": symbol,
                "signal_time_utc": datetime.fromtimestamp(
                    candles[i]["close_time"] / 1000,
                    tz=timezone.utc,
                ).isoformat(),
                "entry_time_utc": datetime.fromtimestamp(
                    entry_candle["open_time"] / 1000,
                    tz=timezone.utc,
                ).isoformat(),
                "exit_time_utc": datetime.fromtimestamp(
                    exit_candle["open_time"] / 1000,
                    tz=timezone.utc,
                ).isoformat(),
                "entry_open_time": entry_open_time,
                "entry_price": entry_price,
                "exit_price": exit_price,
                "momentum_5m_pct": round(mom5, 4),
                "momentum_15m_pct": round(mom15, 4),
                "momentum_30m_pct": round(mom30, 4),
                "volume_ratio": round(volume_ratio, 3),
                "gross_pct": round(gross_pct, 6),
                "net_pct": round(net_pct, 6),
            }
        )

        last_entry_open_time = entry_open_time

    return trades


def summarize_trades_v2(trades):
    if not trades:
        return {
            "trade_count": 0,
            "mean_net_pct": None,
            "median_net_pct": None,
            "win_rate_net_pct": None,
            "profit_factor": None,
            "max_drawdown_pct": None,
            "compounded_return_pct": None,
            "best_net_pct": None,
            "worst_net_pct": None,
        }

    ordered = sorted(
        trades,
        key=lambda x: x["entry_open_time"],
    )

    values = [float(x["net_pct"]) for x in ordered]

    wins = [x for x in values if x > 0]
    losses = [x for x in values if x < 0]

    gross_profit = sum(wins)
    gross_loss = abs(sum(losses))

    if gross_loss > 0:
        profit_factor = gross_profit / gross_loss
    elif gross_profit > 0:
        profit_factor = None
    else:
        profit_factor = 0.0

    equity = 1.0
    peak = 1.0
    max_dd = 0.0

    for value in values:
        equity *= 1.0 + (value / 100.0)
        peak = max(peak, equity)

        if peak > 0:
            dd = ((equity / peak) - 1.0) * 100.0
            max_dd = min(max_dd, dd)

    return {
        "trade_count": len(values),
        "mean_net_pct": round(mean(values), 4),
        "median_net_pct": round(median(values), 4),
        "win_rate_net_pct": round(
            len(wins) / len(values) * 100.0,
            2,
        ),
        "profit_factor": (
            round(profit_factor, 4)
            if profit_factor is not None
            else None
        ),
        "max_drawdown_pct": round(max_dd, 4),
        "compounded_return_pct": round(
            (equity - 1.0) * 100.0,
            4,
        ),
        "best_net_pct": round(max(values), 4),
        "worst_net_pct": round(min(values), 4),
    }


def split_dev_oos(trades):
    if not trades:
        return [], []

    ordered = sorted(
        trades,
        key=lambda x: x["entry_open_time"],
    )

    split_index = int(len(ordered) * 2 / 3)

    # Ã‡ok kÃ¼Ã§Ã¼k Ã¶rneklerde yine kronolojik bir ayrÄ±m yap.
    if len(ordered) >= 2:
        split_index = min(
            max(split_index, 1),
            len(ordered) - 1,
        )

    return ordered[:split_index], ordered[split_index:]


@app.get("/backtest30")
async def backtest30(
    count: int = Query(
        default=30,
        ge=5,
        le=30,
    ),
    days: int = Query(
        default=30,
        ge=7,
        le=30,
    ),
):
    """
    30 gÃ¼nlÃ¼k Ã§oklu-altcoin V2 araÅŸtÄ±rma backtesti.

    Bu endpoint:
    - gerÃ§ek emir Ã¼retmez,
    - paper trade aÃ§maz,
    - next-candle-open giriÅŸ kullanÄ±r,
    - 60 dk hold + 60 dk same-symbol cooldown kullanÄ±r,
    - %0.15 maliyet dÃ¼ÅŸer,
    - kronolojik DEV/OOS raporu Ã¼retir.
    """
    try:
        async with httpx.AsyncClient(
            timeout=httpx.Timeout(90.0)
        ) as client:
            universe_data = await build_universe(client)
            selected = universe_data[:count]

            # 30 coin x ~9 Binance sayfasÄ±. Render/Binance iÃ§in kontrollÃ¼ eÅŸzamanlÄ±lÄ±k.
            semaphore = asyncio.Semaphore(3)

            async def worker(item):
                async with semaphore:
                    try:
                        candles = await get_5m_candles_days(
                            client,
                            item["symbol"],
                            days,
                        )

                        trades = historical_trades_v2(
                            candles,
                            item["symbol"],
                        )

                        dev, oos = split_dev_oos(trades)

                        return {
                            "ok": True,
                            "symbol": item["symbol"],
                            "quote_volume_24h": item["quote_volume_24h"],
                            "candle_count": len(candles),
                            "all": summarize_trades_v2(trades),
                            "dev": summarize_trades_v2(dev),
                            "oos": summarize_trades_v2(oos),
                            "trades": trades,
                        }

                    except Exception as e:
                        return {
                            "ok": False,
                            "symbol": item["symbol"],
                            "error": str(e),
                        }

            results = await asyncio.gather(
                *[worker(item) for item in selected]
            )

        successful = [x for x in results if x.get("ok")]
        failed = [x for x in results if not x.get("ok")]

        all_trades = []
        per_coin = []

        for item in successful:
            all_trades.extend(item["trades"])
            per_coin.append(
                {
                    "symbol": item["symbol"],
                    "quote_volume_24h": item["quote_volume_24h"],
                    "candle_count": item["candle_count"],
                    "all": item["all"],
                    "dev": item["dev"],
                    "oos": item["oos"],
                }
            )

        # BirleÅŸik DEV/OOS coin baÅŸÄ±na deÄŸil, tÃ¼m iÅŸlemlerin kronolojik
        # ilk 2/3 ve son 1/3'Ã¼ olarak da raporlanÄ±r.
        combined_dev, combined_oos = split_dev_oos(all_trades)

        return {
            **MODE_INFO,
            "status": "OK",
            "signal": False,
            "note": (
                "30-DAY MULTI-COIN V2 RESEARCH BACKTEST ONLY - "
                "NOT A VALIDATED TRADING STRATEGY"
            ),
            "days": days,
            "universe_size": len(universe_data),
            "requested_coin_count": count,
            "selected_coin_count": len(selected),
            "successful_coin_count": len(successful),
            "failed_coin_count": len(failed),
            "round_trip_cost_pct": ROUND_TRIP_COST_PCT,
            "entry_model": "NEXT_5M_CANDLE_OPEN",
            "exit_model": "OPEN_60_MIN_AFTER_ENTRY",
            "same_symbol_cooldown_minutes": 60,
            "research_conditions": {
                "momentum_5m_min_pct": 0.30,
                "momentum_15m_min_pct": 0.50,
                "momentum_30m_min_pct": 0.50,
                "volume_ratio_min": 1.20,
                "momentum_5m_max_pct": 3.00,
                "momentum_30m_max_pct": 6.00,
            },
            "combined": {
                "all": summarize_trades_v2(all_trades),
                "dev_first_2_3": summarize_trades_v2(combined_dev),
                "oos_last_1_3": summarize_trades_v2(combined_oos),
            },
            "per_coin": per_coin,
            "failed": failed,
            "generated_utc": utc_now(),
            "limitations": [
                "Universe is selected from current 24h liquidity; survivorship/current-universe bias remains.",
                "Fixed 0.15% round-trip cost is an assumption; variable slippage is not modeled.",
                "DEV/OOS is chronological but this version does not optimize parameters on DEV.",
                "Per-coin DEV/OOS samples may be small even with 30 days of data.",
            ],
        }

    except Exception as e:
        return {
            **MODE_INFO,
            "status": "ERROR",
            "signal": False,
            "error": str(e),
            "generated_utc": utc_now(),
        }

def ema_series(values, period):
    if not values:
        return []

    alpha = 2.0 / (period + 1.0)
    result = [float(values[0])]

    for value in values[1:]:
        result.append(
            (float(value) * alpha)
            + (result[-1] * (1.0 - alpha))
        )

    return result


def btc_regime_map_from_5m(candles):
    """
    BTC 5m verisini 1 saatlik kapanÄ±ÅŸlara indirger.
    Rejim:
      BULL    = close > EMA50 > EMA200
      BEAR    = close < EMA50 < EMA200
      NEUTRAL = diÄŸer durumlar
    Her 5m zaman damgasÄ± iÃ§in yalnÄ±zca o ana kadar tamamlanmÄ±ÅŸ 1H bilgi kullanÄ±lÄ±r.
    """
    if not candles:
        return {}

    hourly = {}
    hour_ms = 60 * 60 * 1000

    for c in candles:
        bucket = (c["open_time"] // hour_ms) * hour_ms
        hourly[bucket] = c["close"]

    hours = sorted(hourly)
    closes = [hourly[h] for h in hours]

    ema50 = ema_series(closes, 50)
    ema200 = ema_series(closes, 200)

    regime_by_hour = {}

    for idx, h in enumerate(hours):
        if idx < 199:
            regime_by_hour[h] = "UNKNOWN"
            continue

        close = closes[idx]
        e50 = ema50[idx]
        e200 = ema200[idx]

        if close > e50 > e200:
            regime = "BULL"
        elif close < e50 < e200:
            regime = "BEAR"
        else:
            regime = "NEUTRAL"

        regime_by_hour[h] = regime

    return regime_by_hour


def attach_regime(trades, regime_by_hour):
    hour_ms = 60 * 60 * 1000

    for t in trades:
        bucket = (
            int(t["entry_open_time"]) // hour_ms
        ) * hour_ms

        t["market_regime"] = regime_by_hour.get(
            bucket,
            "UNKNOWN",
        )

    return trades


def summarize_by_regime(trades):
    result = {}

    for regime in ["BULL", "NEUTRAL", "BEAR", "UNKNOWN"]:
        subset = [
            x for x in trades
            if x.get("market_regime") == regime
        ]
        result[regime] = summarize_trades_v2(subset)

    return result


def simulate_portfolio_v3(
    trades,
    max_positions=5,
    allocation_per_trade_pct=20.0,
):
    """
    Daha gerÃ§ekÃ§i ortak-portfÃ¶y araÅŸtÄ±rma simÃ¼lasyonu.

    - BaÅŸlangÄ±Ã§ equity = 100.
    - AynÄ± anda en fazla max_positions.
    - Her yeni pozisyon baÅŸlangÄ±Ã§taki deÄŸil, o andaki equity'nin sabit yÃ¼zdesi
      kadar nominal sermaye kullanÄ±r.
    - Pozisyonlar 60 dk sonra kapanÄ±r.
    - AynÄ± timestamp'teki sinyaller deterministik olarak symbol sÄ±rasÄ±na gÃ¶re iÅŸlenir.
    - KaldÄ±raÃ§ yok; toplam hedef tahsis <= %100.
    """
    if not trades:
        return {
            "accepted_trade_count": 0,
            "skipped_capacity_count": 0,
            "ending_equity": 100.0,
            "return_pct": 0.0,
            "max_drawdown_pct": 0.0,
            "max_positions": max_positions,
            "allocation_per_trade_pct": allocation_per_trade_pct,
        }

    ordered = sorted(
        trades,
        key=lambda x: (
            x["entry_open_time"],
            x["symbol"],
        ),
    )

    equity = 100.0
    peak = 100.0
    max_dd = 0.0
    active = []
    accepted = 0
    skipped = 0

    def close_due(now_ms):
        nonlocal equity, peak, max_dd, active

        still_open = []

        for pos in active:
            if pos["exit_ms"] <= now_ms:
                pnl = (
                    pos["notional"]
                    * pos["net_pct"]
                    / 100.0
                )
                equity += pnl
                peak = max(peak, equity)

                if peak > 0:
                    dd = (
                        (equity / peak) - 1.0
                    ) * 100.0
                    max_dd = min(max_dd, dd)
            else:
                still_open.append(pos)

        active = still_open

    for t in ordered:
        entry_ms = int(t["entry_open_time"])
        close_due(entry_ms)

        if len(active) >= max_positions:
            skipped += 1
            continue

        notional = equity * (
            allocation_per_trade_pct / 100.0
        )

        exit_ms = int(
            datetime.fromisoformat(
                t["exit_time_utc"]
            ).timestamp() * 1000
        )

        active.append(
            {
                "exit_ms": exit_ms,
                "notional": notional,
                "net_pct": float(t["net_pct"]),
            }
        )
        accepted += 1

    # Kalan pozisyonlarÄ± son exit zamanÄ±na kadar kapat.
    if active:
        final_ms = max(x["exit_ms"] for x in active)
        close_due(final_ms)

    return {
        "accepted_trade_count": accepted,
        "skipped_capacity_count": skipped,
        "ending_equity": round(equity, 4),
        "return_pct": round(equity - 100.0, 4),
        "max_drawdown_pct": round(max_dd, 4),
        "max_positions": max_positions,
        "allocation_per_trade_pct": allocation_per_trade_pct,
    }


@app.get("/backtest90")
async def backtest90(
    count: int = Query(
        default=30,
        ge=5,
        le=30,
    ),
    days: int = Query(
        default=90,
        ge=30,
        le=90,
    ),
):
    """
    ALT-MOMENTUM V3:
    - 30-90 gÃ¼n sayfalÄ± 5m veri
    - next-candle-open giriÅŸ
    - 60 dk hold / 60 dk same-symbol cooldown
    - %0.15 round-trip cost
    - BTC 1H EMA50/EMA200 piyasa rejimi
    - kronolojik DEV/OOS
    - max 5 eÅŸzamanlÄ± pozisyonlu ortak portfÃ¶y simÃ¼lasyonu
    """
    try:
        async with httpx.AsyncClient(
            timeout=httpx.Timeout(120.0)
        ) as client:
            universe_data = await build_universe(client)
            selected = universe_data[:count]

            # Ã–nce BTC rejim verisi.
            btc_candles = await get_5m_candles_days(
                client,
                "BTCUSDT",
                days,
            )
            regime_map = btc_regime_map_from_5m(
                btc_candles
            )

            # 90 gÃ¼n x 30 coin aÄŸÄ±r bir iÅŸ; kontrollÃ¼ concurrency.
            semaphore = asyncio.Semaphore(3)

            async def worker(item):
                async with semaphore:
                    try:
                        candles = await get_5m_candles_days(
                            client,
                            item["symbol"],
                            days,
                        )

                        trades = historical_trades_v2(
                            candles,
                            item["symbol"],
                        )
                        attach_regime(
                            trades,
                            regime_map,
                        )

                        return {
                            "ok": True,
                            "symbol": item["symbol"],
                            "quote_volume_24h": item["quote_volume_24h"],
                            "candle_count": len(candles),
                            "trades": trades,
                        }

                    except Exception as e:
                        return {
                            "ok": False,
                            "symbol": item["symbol"],
                            "error": str(e),
                        }

            results = await asyncio.gather(
                *[worker(item) for item in selected]
            )

        successful = [
            x for x in results if x.get("ok")
        ]
        failed = [
            x for x in results if not x.get("ok")
        ]

        all_trades = []
        per_coin = []

        for item in successful:
            trades = item["trades"]
            dev, oos = split_dev_oos(trades)

            all_trades.extend(trades)

            per_coin.append(
                {
                    "symbol": item["symbol"],
                    "quote_volume_24h": item["quote_volume_24h"],
                    "candle_count": item["candle_count"],
                    "all": summarize_trades_v2(trades),
                    "dev": summarize_trades_v2(dev),
                    "oos": summarize_trades_v2(oos),
                    "regimes": summarize_by_regime(trades),
                }
            )

        combined_dev, combined_oos = split_dev_oos(
            all_trades
        )

        # PortfÃ¶y simÃ¼lasyonu aynÄ± birleÅŸik kronolojik trade akÄ±ÅŸÄ±nda.
        portfolio_all = simulate_portfolio_v3(
            all_trades
        )
        portfolio_dev = simulate_portfolio_v3(
            combined_dev
        )
        portfolio_oos = simulate_portfolio_v3(
            combined_oos
        )

        return {
            **MODE_INFO,
            "status": "OK",
            "signal": False,
            "note": (
                "ALT-MOMENTUM V3 90-DAY RESEARCH BACKTEST ONLY - "
                "NOT A VALIDATED TRADING STRATEGY"
            ),
            "days": days,
            "universe_size": len(universe_data),
            "requested_coin_count": count,
            "selected_coin_count": len(selected),
            "successful_coin_count": len(successful),
            "failed_coin_count": len(failed),
            "excluded_from_alt_universe": [
                "BTC",
                "ETH",
                "XAUT",
                "PAXG",
                "stable/fiat-like assets",
            ],
            "round_trip_cost_pct": ROUND_TRIP_COST_PCT,
            "entry_model": "NEXT_5M_CANDLE_OPEN",
            "exit_model": "OPEN_60_MIN_AFTER_ENTRY",
            "same_symbol_cooldown_minutes": 60,
            "market_regime_model": (
                "BTC 1H: BULL=close>EMA50>EMA200; "
                "BEAR=close<EMA50<EMA200; else NEUTRAL"
            ),
            "research_conditions": {
                "momentum_5m_min_pct": 0.30,
                "momentum_15m_min_pct": 0.50,
                "momentum_30m_min_pct": 0.50,
                "volume_ratio_min": 1.20,
                "momentum_5m_max_pct": 3.00,
                "momentum_30m_max_pct": 6.00,
            },
            "combined": {
                "all": summarize_trades_v2(
                    all_trades
                ),
                "dev_first_2_3": summarize_trades_v2(
                    combined_dev
                ),
                "oos_last_1_3": summarize_trades_v2(
                    combined_oos
                ),
                "regimes_all": summarize_by_regime(
                    all_trades
                ),
                "regimes_oos": summarize_by_regime(
                    combined_oos
                ),
            },
            "portfolio": {
                "assumption": (
                    "max 5 simultaneous positions; "
                    "20% current-equity notional per accepted trade; "
                    "no leverage"
                ),
                "all": portfolio_all,
                "dev_first_2_3": portfolio_dev,
                "oos_last_1_3": portfolio_oos,
            },
            "per_coin": per_coin,
            "failed": failed,
            "generated_utc": utc_now(),
            "limitations": [
                "Universe is selected from current 24h liquidity; survivorship/current-universe bias remains.",
                "Fixed 0.15% round-trip cost is an assumption; variable slippage is not modeled.",
                "BTC regime uses completed historical 1H information derived from 5m candles.",
                "DEV/OOS is chronological and no V3 parameter optimization is performed.",
                "Portfolio capacity selection among simultaneous signals is deterministic by symbol, not a ranking model.",
            ],
        }

    except Exception as e:
        return {
            **MODE_INFO,
            "status": "ERROR",
            "signal": False,
            "error": str(e),
            "generated_utc": utc_now(),
        }

def continuation_events_v4(candles, symbol):
    """
    AmaÃ§ tahmin etmek deÄŸil:
    Coin ZATEN yÃ¼kselmiÅŸken ve hacim artmÄ±ÅŸken, sonraki 60 dakikada
    hareket devam ediyor mu sorusunu Ã¶lÃ§mek.

    Sinyal tamamlanmÄ±ÅŸ 5m mum kapanÄ±ÅŸÄ±nda gÃ¶rÃ¼lÃ¼r.
    GiriÅŸ bir sonraki 5m mum OPEN.
    Ã‡Ä±kÄ±ÅŸ 60 dakika sonraki OPEN.
    """
    events = []

    for i in range(7, len(candles) - 13):
        signal_close = candles[i]["close"]

        mom5 = pct_change(candles[i - 1]["close"], signal_close)
        mom15 = pct_change(candles[i - 3]["close"], signal_close)
        mom30 = pct_change(candles[i - 6]["close"], signal_close)

        previous_volumes = [
            candles[j]["volume"]
            for j in range(i - 6, i)
        ]
        avg_volume = mean(previous_volumes)
        volume_ratio = (
            candles[i]["volume"] / avg_volume
            if avg_volume > 0 else 0
        )

        # Sadece halihazÄ±rda yÃ¼kselmiÅŸ hareketleri inceliyoruz.
        if mom5 <= 0 or mom15 <= 0 or mom30 < 0.50:
            continue

        entry = candles[i + 1]
        exit_60 = candles[i + 13]

        gross = pct_change(entry["open"], exit_60["open"])
        net = gross - ROUND_TRIP_COST_PCT

        events.append({
            "symbol": symbol,
            "signal_time_utc": datetime.fromtimestamp(
                candles[i]["close_time"] / 1000,
                tz=timezone.utc,
            ).isoformat(),
            "entry_time_utc": datetime.fromtimestamp(
                entry["open_time"] / 1000,
                tz=timezone.utc,
            ).isoformat(),
            "entry_open_time": entry["open_time"],
            "momentum_5m_pct": mom5,
            "momentum_15m_pct": mom15,
            "momentum_30m_pct": mom30,
            "volume_ratio": volume_ratio,
            "gross_pct": gross,
            "net_pct": net,
        })

    return events


def bucket_label(value, cuts, labels):
    for idx, cut in enumerate(cuts):
        if value < cut:
            return labels[idx]
    return labels[-1]


def continuation_bucket_report(events):
    """
    Ã–nceden belirlenmiÅŸ geniÅŸ kovalar.
    Bunlar optimize edilmiÅŸ giriÅŸ eÅŸikleri deÄŸildir; davranÄ±ÅŸÄ± gÃ¶rmek iÃ§indir.
    """
    momentum_labels = [
        "0.50-0.99%",
        "1.00-1.49%",
        "1.50-1.99%",
        "2.00-2.99%",
        "3.00%+",
    ]
    volume_labels = [
        "<1.20x",
        "1.20-1.49x",
        "1.50-1.99x",
        "2.00-2.99x",
        "3.00x+",
    ]

    groups = {}

    for e in events:
        m = bucket_label(
            e["momentum_30m_pct"],
            [1.00, 1.50, 2.00, 3.00],
            momentum_labels,
        )
        v = bucket_label(
            e["volume_ratio"],
            [1.20, 1.50, 2.00, 3.00],
            volume_labels,
        )

        groups.setdefault((m, v), []).append(e)

    rows = []

    for m in momentum_labels:
        for v in volume_labels:
            subset = groups.get((m, v), [])
            if not subset:
                continue

            s = summarize_trades_v2(subset)

            rows.append({
                "momentum_30m_bucket": m,
                "volume_ratio_bucket": v,
                **s,
            })

    return rows


def apply_symbol_cooldown_v4(events, minutes=60):
    by_symbol = {}

    for e in sorted(
        events,
        key=lambda x: (x["symbol"], x["entry_open_time"]),
    ):
        by_symbol.setdefault(e["symbol"], []).append(e)

    kept = []
    cooldown_ms = minutes * 60 * 1000

    for symbol, rows in by_symbol.items():
        last_entry = None

        for e in rows:
            if (
                last_entry is None
                or e["entry_open_time"] - last_entry >= cooldown_ms
            ):
                kept.append(e)
                last_entry = e["entry_open_time"]

    return sorted(
        kept,
        key=lambda x: (x["entry_open_time"], x["symbol"]),
    )


@app.get("/continuation90")
async def continuation90(
    count: int = Query(default=30, ge=5, le=30),
    days: int = Query(default=90, ge=30, le=90),
):
    """
    V4 continuation study:
    'Hangisi yÃ¼kselecek?' deÄŸil,
    'Zaten yÃ¼kselmiÅŸ coin ne zaman yÃ¼kselmeye devam ediyor?' testi.
    """
    try:
        async with httpx.AsyncClient(
            timeout=httpx.Timeout(120.0)
        ) as client:
            universe_data = await build_universe(client)
            selected = universe_data[:count]

            semaphore = asyncio.Semaphore(3)

            async def worker(item):
                async with semaphore:
                    try:
                        candles = await get_5m_candles_days(
                            client,
                            item["symbol"],
                            days,
                        )
                        events = continuation_events_v4(
                            candles,
                            item["symbol"],
                        )
                        return {
                            "ok": True,
                            "symbol": item["symbol"],
                            "candle_count": len(candles),
                            "events": events,
                        }
                    except Exception as e:
                        return {
                            "ok": False,
                            "symbol": item["symbol"],
                            "error": str(e),
                        }

            results = await asyncio.gather(
                *[worker(x) for x in selected]
            )

        successful = [x for x in results if x.get("ok")]
        failed = [x for x in results if not x.get("ok")]

        raw_events = []
        per_coin = []

        for item in successful:
            raw_events.extend(item["events"])
            cooled = apply_symbol_cooldown_v4(
                item["events"],
                60,
            )
            per_coin.append({
                "symbol": item["symbol"],
                "candle_count": item["candle_count"],
                "raw_event_count": len(item["events"]),
                "cooldown_event_count": len(cooled),
                "cooldown_summary": summarize_trades_v2(cooled),
            })

        cooled_all = apply_symbol_cooldown_v4(
            raw_events,
            60,
        )
        dev, oos = split_dev_oos(cooled_all)

        return {
            **MODE_INFO,
            "status": "OK",
            "signal": False,
            "study": "MOMENTUM_CONTINUATION_AFTER_PRICE_ALREADY_RISEN",
            "question": (
                "After an altcoin has already risen, which combinations "
                "of 30m momentum and volume expansion are followed by "
                "positive 60m continuation?"
            ),
            "days": days,
            "selected_coin_count": len(selected),
            "successful_coin_count": len(successful),
            "failed_coin_count": len(failed),
            "entry_model": "NEXT_5M_CANDLE_OPEN_AFTER_OBSERVED_RISE",
            "exit_model": "OPEN_60_MIN_AFTER_ENTRY",
            "round_trip_cost_pct": ROUND_TRIP_COST_PCT,
            "same_symbol_cooldown_minutes": 60,
            "raw_event_count": len(raw_events),
            "cooldown_event_count": len(cooled_all),
            "combined": {
                "all": summarize_trades_v2(cooled_all),
                "dev_first_2_3": summarize_trades_v2(dev),
                "oos_last_1_3": summarize_trades_v2(oos),
            },
            "bucket_matrix_all": continuation_bucket_report(cooled_all),
            "bucket_matrix_dev": continuation_bucket_report(dev),
            "bucket_matrix_oos": continuation_bucket_report(oos),
            "per_coin": per_coin,
            "failed": failed,
            "generated_utc": utc_now(),
            "interpretation_note": (
                "Buckets are descriptive research groups, not optimized "
                "buy thresholds. We will look for continuation patterns "
                "that remain positive in OOS rather than choosing the "
                "best in-sample bucket."
            ),
        }

    except Exception as e:
        return {
            **MODE_INFO,
            "status": "ERROR",
            "signal": False,
            "error": str(e),
            "generated_utc": utc_now(),
        }

def continuation_quality_events_v5(candles, symbol):
    """
    V5: Coin zaten yÃ¼kselmiÅŸken hareketin KALÄ°TESÄ°NÄ° Ã¶lÃ§er.
    GeleceÄŸi tahmin eden Ã¶zellik kullanÄ±lmaz.

    Sinyal anÄ±nda bilinenler:
    - son 30m yÃ¼kseliÅŸ
    - son altÄ± 5m getirinin yapÄ±sÄ±
    - pozitif 5m mum sayÄ±sÄ±
    - son 15m / ilk 15m momentum karÅŸÄ±laÅŸtÄ±rmasÄ±
    - 30m iÃ§i peak'ten mevcut close'a geri Ã§ekilme
    - son 3 mum hacminin Ã¶nceki 3 muma gÃ¶re devamlÄ±lÄ±ÄŸÄ±

    GiriÅŸ: sonraki 5m OPEN
    Ã‡Ä±kÄ±ÅŸ: giriÅŸten 60m sonraki OPEN
    """
    events = []

    for i in range(7, len(candles) - 13):
        signal_close = candles[i]["close"]

        mom5 = pct_change(candles[i - 1]["close"], signal_close)
        mom15 = pct_change(candles[i - 3]["close"], signal_close)
        mom30 = pct_change(candles[i - 6]["close"], signal_close)

        # Biz sadece zaten yÃ¼kselmiÅŸ hareketleri inceliyoruz.
        if mom5 <= 0 or mom15 <= 0 or mom30 < 0.50:
            continue

        five_min_returns = []
        for j in range(i - 5, i + 1):
            five_min_returns.append(
                pct_change(candles[j]["open"], candles[j]["close"])
            )

        positive_candles = sum(1 for r in five_min_returns if r > 0)

        first15 = pct_change(
            candles[i - 6]["close"],
            candles[i - 3]["close"],
        )
        last15 = pct_change(
            candles[i - 3]["close"],
            signal_close,
        )

        acceleration = last15 - first15

        window_high = max(
            candles[j]["high"] for j in range(i - 5, i + 1)
        )
        pullback_from_peak = pct_change(
            window_high,
            signal_close,
        )

        old_vol = mean(
            candles[j]["volume"] for j in range(i - 5, i - 2)
        )
        recent_vol = mean(
            candles[j]["volume"] for j in range(i - 2, i + 1)
        )
        volume_persistence = (
            recent_vol / old_vol if old_vol > 0 else 0
        )

        prior6_vol = mean(
            candles[j]["volume"] for j in range(i - 6, i)
        )
        signal_volume_ratio = (
            candles[i]["volume"] / prior6_vol
            if prior6_vol > 0 else 0
        )

        entry = candles[i + 1]
        exit_60 = candles[i + 13]

        gross = pct_change(entry["open"], exit_60["open"])
        net = gross - ROUND_TRIP_COST_PCT

        events.append({
            "symbol": symbol,
            "signal_time_utc": datetime.fromtimestamp(
                candles[i]["close_time"] / 1000,
                tz=timezone.utc,
            ).isoformat(),
            "entry_time_utc": datetime.fromtimestamp(
                entry["open_time"] / 1000,
                tz=timezone.utc,
            ).isoformat(),
            "entry_open_time": entry["open_time"],
            "momentum_5m_pct": mom5,
            "momentum_15m_pct": mom15,
            "momentum_30m_pct": mom30,
            "positive_5m_candles_last_30m": positive_candles,
            "first_15m_pct": first15,
            "last_15m_pct": last15,
            "acceleration_pct_points": acceleration,
            "pullback_from_30m_peak_pct": pullback_from_peak,
            "volume_persistence_ratio": volume_persistence,
            "signal_volume_ratio": signal_volume_ratio,
            "gross_pct": gross,
            "net_pct": net,
        })

    return events


def quality_bucket_report_v5(events):
    """
    Tek deÄŸiÅŸkenli davranÄ±ÅŸ raporu.
    AmaÃ§ en iyi hÃ¼creyi seÃ§mek deÄŸil, hangi hareket Ã¶zelliklerinin
    OOS'ta continuation ile iliÅŸkili olduÄŸunu gÃ¶rmek.
    """
    dimensions = {
        "positive_candle_count": [
            ("2_or_less", lambda e: e["positive_5m_candles_last_30m"] <= 2),
            ("3", lambda e: e["positive_5m_candles_last_30m"] == 3),
            ("4", lambda e: e["positive_5m_candles_last_30m"] == 4),
            ("5", lambda e: e["positive_5m_candles_last_30m"] == 5),
            ("6", lambda e: e["positive_5m_candles_last_30m"] == 6),
        ],
        "acceleration": [
            ("decelerating", lambda e: e["acceleration_pct_points"] < -0.25),
            ("roughly_steady", lambda e: -0.25 <= e["acceleration_pct_points"] <= 0.25),
            ("accelerating", lambda e: e["acceleration_pct_points"] > 0.25),
        ],
        "pullback_from_peak": [
            ("0_to_-0.25%", lambda e: e["pullback_from_30m_peak_pct"] >= -0.25),
            ("-0.25_to_-0.75%", lambda e: -0.75 <= e["pullback_from_30m_peak_pct"] < -0.25),
            ("below_-0.75%", lambda e: e["pullback_from_30m_peak_pct"] < -0.75),
        ],
        "volume_persistence": [
            ("<0.8x", lambda e: e["volume_persistence_ratio"] < 0.8),
            ("0.8-1.19x", lambda e: 0.8 <= e["volume_persistence_ratio"] < 1.2),
            ("1.2-1.99x", lambda e: 1.2 <= e["volume_persistence_ratio"] < 2.0),
            ("2.0x+", lambda e: e["volume_persistence_ratio"] >= 2.0),
        ],
    }

    report = {}

    for dimension, buckets in dimensions.items():
        rows = []
        for label, predicate in buckets:
            subset = [e for e in events if predicate(e)]
            rows.append({
                "bucket": label,
                **summarize_trades_v2(subset),
            })
        report[dimension] = rows

    return report


def focused_continuation_zone_v5(events):
    """
    V4'te keÅŸfedilen genel bÃ¶lgeyi ayrÄ± raporlar:
    30m momentum 1.0%-1.99%.
    Burada yeni kalite Ã¶zelliklerini inceliyoruz; yeni eÅŸik optimize etmiyoruz.
    """
    return [
        e for e in events
        if 1.0 <= e["momentum_30m_pct"] < 2.0
    ]


@app.get("/continuation-quality90")
async def continuation_quality90(
    count: int = Query(default=30, ge=5, le=30),
    days: int = Query(default=90, ge=30, le=90),
):
    try:
        async with httpx.AsyncClient(
            timeout=httpx.Timeout(120.0)
        ) as client:
            universe_data = await build_universe(client)
            selected = universe_data[:count]
            semaphore = asyncio.Semaphore(3)

            async def worker(item):
                async with semaphore:
                    try:
                        candles = await get_5m_candles_days(
                            client, item["symbol"], days
                        )
                        events = continuation_quality_events_v5(
                            candles, item["symbol"]
                        )
                        return {
                            "ok": True,
                            "symbol": item["symbol"],
                            "candle_count": len(candles),
                            "events": events,
                        }
                    except Exception as e:
                        return {
                            "ok": False,
                            "symbol": item["symbol"],
                            "error": str(e),
                        }

            results = await asyncio.gather(
                *[worker(x) for x in selected]
            )

        successful = [x for x in results if x.get("ok")]
        failed = [x for x in results if not x.get("ok")]

        raw = []
        for item in successful:
            raw.extend(item["events"])

        cooled = apply_symbol_cooldown_v4(raw, 60)
        dev, oos = split_dev_oos(cooled)

        focus_all = focused_continuation_zone_v5(cooled)
        focus_dev = focused_continuation_zone_v5(dev)
        focus_oos = focused_continuation_zone_v5(oos)

        return {
            **MODE_INFO,
            "status": "OK",
            "signal": False,
            "study": "CONTINUATION_QUALITY_AFTER_RISE_ALREADY_STARTED",
            "days": days,
            "selected_coin_count": len(selected),
            "successful_coin_count": len(successful),
            "failed_coin_count": len(failed),
            "entry_model": "NEXT_5M_CANDLE_OPEN_AFTER_OBSERVED_RISE",
            "exit_model": "OPEN_60_MIN_AFTER_ENTRY",
            "round_trip_cost_pct": ROUND_TRIP_COST_PCT,
            "same_symbol_cooldown_minutes": 60,
            "raw_event_count": len(raw),
            "cooldown_event_count": len(cooled),
            "combined": {
                "all": summarize_trades_v2(cooled),
                "dev_first_2_3": summarize_trades_v2(dev),
                "oos_last_1_3": summarize_trades_v2(oos),
            },
            "quality_all": quality_bucket_report_v5(cooled),
            "quality_dev": quality_bucket_report_v5(dev),
            "quality_oos": quality_bucket_report_v5(oos),
            "focus_zone": {
                "definition": "30m momentum >=1.0% and <2.0%",
                "all": summarize_trades_v2(focus_all),
                "dev": summarize_trades_v2(focus_dev),
                "oos": summarize_trades_v2(focus_oos),
                "quality_all": quality_bucket_report_v5(focus_all),
                "quality_dev": quality_bucket_report_v5(focus_dev),
                "quality_oos": quality_bucket_report_v5(focus_oos),
            },
            "failed": failed,
            "generated_utc": utc_now(),
            "interpretation_note": (
                "This endpoint studies the SHAPE of an already-started rise. "
                "It does not predict which coin will rise. Quality buckets are "
                "descriptive and are not yet live-entry rules."
            ),
        }

    except Exception as e:
        return {
            **MODE_INFO,
            "status": "ERROR",
            "signal": False,
            "error": str(e),
            "generated_utc": utc_now(),
        }

def combo_hypotheses_v6():
    """
    Ã–nceden tanÄ±mlÄ± continuation hipotezleri.
    AmaÃ§ OOS sonucuna gÃ¶re eÅŸik uydurmak deÄŸil; V4/V5'te gÃ¶zlenen
    yapÄ±larÄ± ayrÄ±, anlaÅŸÄ±lÄ±r hipotezler olarak test etmektir.
    """
    return [
        (
            "H1_VOLUME_CONTINUATION",
            lambda e:
                1.0 <= e["momentum_30m_pct"] < 2.0
                and e["volume_persistence_ratio"] >= 1.2
        ),
        (
            "H2_VOLUME_PLUS_ACCELERATION",
            lambda e:
                1.0 <= e["momentum_30m_pct"] < 2.0
                and e["volume_persistence_ratio"] >= 1.2
                and e["acceleration_pct_points"] > 0.25
        ),
        (
            "H3_ORDERLY_RISE",
            lambda e:
                1.0 <= e["momentum_30m_pct"] < 2.0
                and e["volume_persistence_ratio"] >= 1.2
                and e["positive_5m_candles_last_30m"] >= 5
        ),
        (
            "H4_ACCELERATING_ORDERLY_RISE",
            lambda e:
                1.0 <= e["momentum_30m_pct"] < 2.0
                and e["volume_persistence_ratio"] >= 1.2
                and e["positive_5m_candles_last_30m"] >= 5
                and e["acceleration_pct_points"] > 0.25
        ),
        (
            "H5_PULLBACK_CONTINUATION",
            lambda e:
                1.0 <= e["momentum_30m_pct"] < 2.0
                and e["volume_persistence_ratio"] >= 1.2
                and e["pullback_from_30m_peak_pct"] < -0.75
        ),
        (
            "H6_PULLBACK_REACCELERATION",
            lambda e:
                1.0 <= e["momentum_30m_pct"] < 2.0
                and e["volume_persistence_ratio"] >= 1.2
                and e["pullback_from_30m_peak_pct"] < -0.75
                and e["momentum_5m_pct"] > 0
                and e["acceleration_pct_points"] > 0.25
        ),
    ]


def combo_report_v6(events):
    report = []

    for name, predicate in combo_hypotheses_v6():
        subset = [e for e in events if predicate(e)]
        report.append({
            "hypothesis": name,
            **summarize_trades_v2(subset),
        })

    return report


@app.get("/continuation-combo90")
async def continuation_combo90(
    count: int = Query(default=30, ge=5, le=30),
    days: int = Query(default=90, ge=30, le=90),
):
    """
    V6: already-rising momentum continuation combination study.
    Research only. No orders.
    """
    try:
        async with httpx.AsyncClient(
            timeout=httpx.Timeout(120.0)
        ) as client:
            universe_data = await build_universe(client)
            selected = universe_data[:count]
            semaphore = asyncio.Semaphore(3)

            async def worker(item):
                async with semaphore:
                    try:
                        candles = await get_5m_candles_days(
                            client,
                            item["symbol"],
                            days,
                        )
                        events = continuation_quality_events_v5(
                            candles,
                            item["symbol"],
                        )
                        return {
                            "ok": True,
                            "symbol": item["symbol"],
                            "events": events,
                        }
                    except Exception as e:
                        return {
                            "ok": False,
                            "symbol": item["symbol"],
                            "error": str(e),
                        }

            results = await asyncio.gather(
                *[worker(x) for x in selected]
            )

        successful = [x for x in results if x.get("ok")]
        failed = [x for x in results if not x.get("ok")]

        raw = []
        for item in successful:
            raw.extend(item["events"])

        cooled = apply_symbol_cooldown_v4(raw, 60)
        dev, oos = split_dev_oos(cooled)

        # AyrÄ± coin geniÅŸliÄŸi: sonuÃ§ tek/az sayÄ±da coin tarafÄ±ndan mÄ± taÅŸÄ±nÄ±yor?
        breadth = {}
        for name, predicate in combo_hypotheses_v6():
            coin_rows = []
            for item in successful:
                coin_events = apply_symbol_cooldown_v4(
                    item["events"], 60
                )
                subset = [e for e in coin_events if predicate(e)]
                s = summarize_trades_v2(subset)
                if s["trade_count"] > 0:
                    coin_rows.append({
                        "symbol": item["symbol"],
                        "trade_count": s["trade_count"],
                        "mean_net_pct": s["mean_net_pct"],
                        "profit_factor": s["profit_factor"],
                    })

            positive = sum(
                1 for x in coin_rows
                if x["mean_net_pct"] > 0
            )
            breadth[name] = {
                "coins_with_trades": len(coin_rows),
                "coins_positive_mean": positive,
                "positive_coin_pct": round(
                    positive / len(coin_rows) * 100, 2
                ) if coin_rows else 0,
                "per_coin": coin_rows,
            }

        return {
            **MODE_INFO,
            "status": "OK",
            "signal": False,
            "study": "PREDEFINED_CONTINUATION_COMBINATION_HYPOTHESES",
            "days": days,
            "selected_coin_count": len(selected),
            "successful_coin_count": len(successful),
            "failed_coin_count": len(failed),
            "entry_model": "NEXT_5M_CANDLE_OPEN_AFTER_OBSERVED_RISE",
            "exit_model": "OPEN_60_MIN_AFTER_ENTRY",
            "round_trip_cost_pct": ROUND_TRIP_COST_PCT,
            "same_symbol_cooldown_minutes": 60,
            "base_zone": "30m momentum >=1.0% and <2.0%",
            "hypothesis_definitions": {
                "H1_VOLUME_CONTINUATION": "base zone + volume persistence >=1.2x",
                "H2_VOLUME_PLUS_ACCELERATION": "H1 + last15 minus first15 >0.25 percentage points",
                "H3_ORDERLY_RISE": "H1 + at least 5 of last 6 five-minute candles positive",
                "H4_ACCELERATING_ORDERLY_RISE": "H3 + acceleration >0.25 percentage points",
                "H5_PULLBACK_CONTINUATION": "H1 + current close >0.75% below 30m peak",
                "H6_PULLBACK_REACCELERATION": "H5 + positive last 5m + acceleration >0.25 percentage points",
            },
            "all": combo_report_v6(cooled),
            "dev_first_2_3": combo_report_v6(dev),
            "oos_last_1_3": combo_report_v6(oos),
            "breadth_all_period": breadth,
            "failed": failed,
            "generated_utc": utc_now(),
            "interpretation_note": (
                "These are predefined research hypotheses based on earlier "
                "continuation studies. OOS is reported separately. No result "
                "is automatically promoted to a live trading rule."
            ),
        }

    except Exception as e:
        return {
            **MODE_INFO,
            "status": "ERROR",
            "signal": False,
            "error": str(e),
            "generated_utc": utc_now(),
        }

def h6_validation_events_v7(candles, symbol):
    """
    V7 VALIDATION ONLY.
    H6 is frozen from V6:
      - observed 30m rise >=1.0% and <2.0%
      - volume persistence >=1.2x
      - close is >0.75% below the last-30m peak
      - latest 5m momentum >0
      - last15 - first15 >0.25 percentage points

    Entry = next 5m OPEN.
    Reports 15/30/60/120 minute exits.
    No optimization and no orders.
    """
    events = []

    # Need 25 future 5m candles for the 120m OPEN exit.
    for i in range(7, len(candles) - 25):
        signal_close = candles[i]["close"]

        mom5 = pct_change(candles[i - 1]["close"], signal_close)
        mom30 = pct_change(candles[i - 6]["close"], signal_close)

        if not (1.0 <= mom30 < 2.0):
            continue

        first15 = pct_change(
            candles[i - 6]["close"],
            candles[i - 3]["close"],
        )
        last15 = pct_change(
            candles[i - 3]["close"],
            signal_close,
        )
        acceleration = last15 - first15

        window_high = max(
            candles[j]["high"] for j in range(i - 5, i + 1)
        )
        pullback = pct_change(window_high, signal_close)

        old_vol = mean(
            candles[j]["volume"] for j in range(i - 5, i - 2)
        )
        recent_vol = mean(
            candles[j]["volume"] for j in range(i - 2, i + 1)
        )
        vol_persistence = recent_vol / old_vol if old_vol > 0 else 0

        # Frozen H6 rule. Do not tune here.
        if vol_persistence < 1.2:
            continue
        if pullback >= -0.75:
            continue
        if mom5 <= 0:
            continue
        if acceleration <= 0.25:
            continue

        entry = candles[i + 1]
        entry_price = entry["open"]

        exits = {
            "15m": candles[i + 4]["open"],
            "30m": candles[i + 7]["open"],
            "60m": candles[i + 13]["open"],
            "120m": candles[i + 25]["open"],
        }

        row = {
            "symbol": symbol,
            "signal_time_utc": datetime.fromtimestamp(
                candles[i]["close_time"] / 1000,
                tz=timezone.utc,
            ).isoformat(),
            "entry_time_utc": datetime.fromtimestamp(
                entry["open_time"] / 1000,
                tz=timezone.utc,
            ).isoformat(),
            "entry_open_time": entry["open_time"],
            "momentum_5m_pct": mom5,
            "momentum_30m_pct": mom30,
            "volume_persistence_ratio": vol_persistence,
            "pullback_from_30m_peak_pct": pullback,
            "acceleration_pct_points": acceleration,
        }

        for label, exit_price in exits.items():
            gross = pct_change(entry_price, exit_price)
            row[f"gross_{label}_pct"] = gross
            row[f"net_{label}_pct"] = gross - ROUND_TRIP_COST_PCT

        # Compatibility with existing cooldown/split helpers.
        row["gross_pct"] = row["gross_60m_pct"]
        row["net_pct"] = row["net_60m_pct"]
        events.append(row)

    return events


def summarize_horizon_v7(events, horizon):
    values = [e[f"net_{horizon}_pct"] for e in events]
    if not values:
        return {
            "trade_count": 0,
            "mean_net_pct": None,
            "median_net_pct": None,
            "win_rate_net_pct": None,
            "profit_factor": None,
            "best_net_pct": None,
            "worst_net_pct": None,
        }

    vals = sorted(values)
    n = len(vals)
    if n % 2:
        med = vals[n // 2]
    else:
        med = (vals[n // 2 - 1] + vals[n // 2]) / 2

    wins = [x for x in values if x > 0]
    losses = [x for x in values if x < 0]
    gross_profit = sum(wins)
    gross_loss = abs(sum(losses))

    pf = None
    if gross_loss > 0:
        pf = gross_profit / gross_loss
    elif gross_profit > 0:
        pf = None

    return {
        "trade_count": n,
        "mean_net_pct": round(mean(values), 4),
        "median_net_pct": round(med, 4),
        "win_rate_net_pct": round(len(wins) / n * 100, 2),
        "profit_factor": round(pf, 4) if pf is not None else None,
        "best_net_pct": round(max(values), 4),
        "worst_net_pct": round(min(values), 4),
    }


def horizon_report_v7(events):
    return {
        h: summarize_horizon_v7(events, h)
        for h in ("15m", "30m", "60m", "120m")
    }


@app.get("/h6-validate")
async def h6_validate_v7(
    count: int = Query(default=30, ge=5, le=30),
    days: int = Query(default=365, ge=90, le=365),
):
    """
    Frozen H6 long-window validation.
    Research/paper only; no live trading or order execution.
    """
    try:
        async with httpx.AsyncClient(
            timeout=httpx.Timeout(240.0)
        ) as client:
            universe_data = await build_universe(client)
            selected = universe_data[:count]
            semaphore = asyncio.Semaphore(8)

            async def worker(item):
                async with semaphore:
                    try:
                        candles = await get_5m_candles_days(
                            client, item["symbol"], days
                        )
                        events = h6_validation_events_v7(
                            candles, item["symbol"]
                        )
                        cooled = apply_symbol_cooldown_v4(events, 60)
                        return {
                            "ok": True,
                            "symbol": item["symbol"],
                            "candle_count": len(candles),
                            "events": cooled,
                        }
                    except Exception as e:
                        return {
                            "ok": False,
                            "symbol": item["symbol"],
                            "error": str(e),
                        }

            results = await asyncio.gather(
                *[worker(x) for x in selected]
            )

        successful = [x for x in results if x.get("ok")]
        failed = [x for x in results if not x.get("ok")]

        all_events = []
        for item in successful:
            all_events.extend(item["events"])

        # Existing chronological helper retained for consistency.
        dev, oos = split_dev_oos(all_events)

        per_coin = []
        positive_60 = 0
        coins_with_trades = 0

        for item in successful:
            ev = item["events"]
            if ev:
                coins_with_trades += 1
                s60 = summarize_horizon_v7(ev, "60m")
                if s60["mean_net_pct"] is not None and s60["mean_net_pct"] > 0:
                    positive_60 += 1
                per_coin.append({
                    "symbol": item["symbol"],
                    "trade_count": len(ev),
                    "horizons": horizon_report_v7(ev),
                })

        return {
            **MODE_INFO,
            "status": "OK",
            "signal": False,
            "study": "FROZEN_H6_LONG_WINDOW_VALIDATION",
            "days": days,
            "selected_coin_count": len(selected),
            "successful_coin_count": len(successful),
            "failed_coin_count": len(failed),
            "frozen_rule": {
                "momentum_30m_pct": ">=1.0 and <2.0",
                "volume_persistence_ratio": ">=1.2",
                "pullback_from_30m_peak_pct": "<-0.75",
                "momentum_5m_pct": ">0",
                "acceleration_pct_points": ">0.25",
            },
            "entry_model": "NEXT_5M_CANDLE_OPEN",
            "exit_horizons": ["15m", "30m", "60m", "120m"],
            "round_trip_cost_pct": ROUND_TRIP_COST_PCT,
            "same_symbol_cooldown_minutes": 60,
            "combined": {
                "all": horizon_report_v7(all_events),
                "dev_first_2_3": horizon_report_v7(dev),
                "oos_last_1_3": horizon_report_v7(oos),
            },
            "breadth_60m_all_period": {
                "coins_with_trades": coins_with_trades,
                "coins_positive_mean": positive_60,
                "positive_coin_pct": round(
                    positive_60 / coins_with_trades * 100, 2
                ) if coins_with_trades else 0,
            },
            "per_coin": per_coin,
            "failed": failed,
            "generated_utc": utc_now(),
            "interpretation_note": (
                "H6 thresholds are frozen from the prior study. "
                "This endpoint validates the same rule over a longer window "
                "and reports multiple fixed exit horizons. It does not "
                "optimize thresholds or place orders."
            ),
        }

    except Exception as e:
        return {
            **MODE_INFO,
            "status": "ERROR",
            "signal": False,
            "error": str(e),
            "generated_utc": utc_now(),
        }

def enrich_h6_with_regime_v8(events, alt_candles, btc_candles):
    """
    V8: H6 eÅŸiklerine DOKUNMADAN, sinyal anÄ±ndaki rejimi ekler.
    Gelecek veri kullanÄ±lmaz.

    Rejim deÄŸiÅŸkenleri:
      BTC 1h / 4h / 24h return
      ALT 1h / 4h / 24h return
    5m tamamlanmÄ±ÅŸ mumlardan hesaplanÄ±r.
    """
    alt_by_close = {c["close_time"]: idx for idx, c in enumerate(alt_candles)}
    btc_by_close = {c["close_time"]: idx for idx, c in enumerate(btc_candles)}

    enriched = []

    for e in events:
        # Signal close time from ISO.
        signal_ms = int(
            datetime.fromisoformat(
                e["signal_time_utc"].replace("Z", "+00:00")
            ).timestamp() * 1000
        )

        # Binance close_time may differ by 1 ms; use latest completed candle <= signal.
        alt_idx = None
        btc_idx = None

        # Fast exact/near-exact lookup first.
        for delta in (0, -1, 1):
            if signal_ms + delta in alt_by_close:
                alt_idx = alt_by_close[signal_ms + delta]
                break
        for delta in (0, -1, 1):
            if signal_ms + delta in btc_by_close:
                btc_idx = btc_by_close[signal_ms + delta]
                break

        if alt_idx is None:
            # fallback: aligned 5m open time derived from signal
            candidates = [
                i for i, c in enumerate(alt_candles)
                if c["close_time"] <= signal_ms
            ]
            if candidates:
                alt_idx = candidates[-1]

        if btc_idx is None:
            candidates = [
                i for i, c in enumerate(btc_candles)
                if c["close_time"] <= signal_ms
            ]
            if candidates:
                btc_idx = candidates[-1]

        if alt_idx is None or btc_idx is None:
            continue

        # Need 24h history = 288 x 5m.
        if alt_idx < 288 or btc_idx < 288:
            continue

        row = dict(e)

        for label, bars in (("1h", 12), ("4h", 48), ("24h", 288)):
            row[f"alt_{label}_return_pct"] = pct_change(
                alt_candles[alt_idx - bars]["close"],
                alt_candles[alt_idx]["close"],
            )
            row[f"btc_{label}_return_pct"] = pct_change(
                btc_candles[btc_idx - bars]["close"],
                btc_candles[btc_idx]["close"],
            )

        enriched.append(row)

    return enriched


def regime_report_v8(events):
    """
    Ã–nceden tanÄ±mlÄ±, kaba rejim ayrÄ±mlarÄ±.
    AmaÃ§ 'en iyi eÅŸik' aramak deÄŸil; H6'nÄ±n hangi piyasa yÃ¶nÃ¼nde
    bozulduÄŸunu veya iyileÅŸtiÄŸini gÃ¶rmek.
    """
    regimes = [
        ("ALL", lambda e: True),

        ("BTC_1H_POS", lambda e: e["btc_1h_return_pct"] > 0),
        ("BTC_1H_NEG_OR_ZERO", lambda e: e["btc_1h_return_pct"] <= 0),

        ("BTC_4H_POS", lambda e: e["btc_4h_return_pct"] > 0),
        ("BTC_4H_NEG_OR_ZERO", lambda e: e["btc_4h_return_pct"] <= 0),

        ("BTC_24H_POS", lambda e: e["btc_24h_return_pct"] > 0),
        ("BTC_24H_NEG_OR_ZERO", lambda e: e["btc_24h_return_pct"] <= 0),

        ("ALT_4H_POS", lambda e: e["alt_4h_return_pct"] > 0),
        ("ALT_4H_NEG_OR_ZERO", lambda e: e["alt_4h_return_pct"] <= 0),

        ("ALT_24H_POS", lambda e: e["alt_24h_return_pct"] > 0),
        ("ALT_24H_NEG_OR_ZERO", lambda e: e["alt_24h_return_pct"] <= 0),

        (
            "BTC4H_POS_AND_ALT4H_POS",
            lambda e:
                e["btc_4h_return_pct"] > 0
                and e["alt_4h_return_pct"] > 0
        ),
        (
            "BTC4H_NEG_AND_ALT4H_POS",
            lambda e:
                e["btc_4h_return_pct"] <= 0
                and e["alt_4h_return_pct"] > 0
        ),
        (
            "BTC4H_POS_AND_ALT4H_NEG",
            lambda e:
                e["btc_4h_return_pct"] > 0
                and e["alt_4h_return_pct"] <= 0
        ),
        (
            "BTC4H_NEG_AND_ALT4H_NEG",
            lambda e:
                e["btc_4h_return_pct"] <= 0
                and e["alt_4h_return_pct"] <= 0
        ),
    ]

    rows = []
    for name, predicate in regimes:
        subset = [e for e in events if predicate(e)]
        rows.append({
            "regime": name,
            "horizons": horizon_report_v7(subset),
        })
    return rows


@app.get("/h6-regime")
async def h6_regime_v8(
    count: int = Query(default=30, ge=5, le=30),
    days: int = Query(default=365, ge=90, le=365),
):
    """
    V8 regime study.
    Frozen H6 signal + BTC/ALT broader market direction.
    Research/paper only. No orders.
    """
    try:
        async with httpx.AsyncClient(
            timeout=httpx.Timeout(240.0)
        ) as client:
            universe_data = await build_universe(client)
            selected = universe_data[:count]

            # Fetch BTC once as the common market-regime reference.
            btc_candles = await get_5m_candles_days(
                client, "BTCUSDT", days
            )

            semaphore = asyncio.Semaphore(2)

            async def worker(item):
                async with semaphore:
                    try:
                        candles = await get_5m_candles_days(
                            client, item["symbol"], days
                        )
                        h6 = h6_validation_events_v7(
                            candles, item["symbol"]
                        )
                        h6 = apply_symbol_cooldown_v4(h6, 60)
                        enriched = enrich_h6_with_regime_v8(
                            h6, candles, btc_candles
                        )
                        return {
                            "ok": True,
                            "symbol": item["symbol"],
                            "events": enriched,
                        }
                    except Exception as e:
                        return {
                            "ok": False,
                            "symbol": item["symbol"],
                            "error": str(e),
                        }

            results = await asyncio.gather(
                *[worker(x) for x in selected]
            )

        successful = [x for x in results if x.get("ok")]
        failed = [x for x in results if not x.get("ok")]

        all_events = []
        for item in successful:
            all_events.extend(item["events"])

        # Same existing split method retained so V7 and V8 are directly comparable.
        dev, oos = split_dev_oos(all_events)

        return {
            **MODE_INFO,
            "status": "OK",
            "signal": False,
            "study": "FROZEN_H6_MARKET_REGIME_STUDY",
            "days": days,
            "selected_coin_count": len(selected),
            "successful_coin_count": len(successful),
            "failed_coin_count": len(failed),
            "h6_rule_changed": False,
            "frozen_rule": {
                "momentum_30m_pct": ">=1.0 and <2.0",
                "volume_persistence_ratio": ">=1.2",
                "pullback_from_30m_peak_pct": "<-0.75",
                "momentum_5m_pct": ">0",
                "acceleration_pct_points": ">0.25",
            },
            "regime_features": [
                "BTC 1h return",
                "BTC 4h return",
                "BTC 24h return",
                "ALT 1h return",
                "ALT 4h return",
                "ALT 24h return",
            ],
            "entry_model": "NEXT_5M_CANDLE_OPEN",
            "exit_horizons": ["15m", "30m", "60m", "120m"],
            "round_trip_cost_pct": ROUND_TRIP_COST_PCT,
            "same_symbol_cooldown_minutes": 60,
            "event_count": len(all_events),
            "regimes_all": regime_report_v8(all_events),
            "regimes_dev": regime_report_v8(dev),
            "regimes_oos": regime_report_v8(oos),
            "failed": failed,
            "generated_utc": utc_now(),
            "interpretation_note": (
                "H6 thresholds remain frozen. V8 asks whether the same H6 "
                "event behaves differently depending on BTC and altcoin "
                "broader trend direction. Regime buckets use only information "
                "available at the signal time and are descriptive, not yet "
                "entry filters."
            ),
        }

    except Exception as e:
        return {
            **MODE_INFO,
            "status": "ERROR",
            "signal": False,
            "error": str(e),
            "generated_utc": utc_now(),
        }

TRAILING_LEVELS_V9 = (0.5, 1.0, 1.5)
MAX_HOLD_MINUTES_V9 = 120


def simulate_trailing_v9(candles, signal_index, trail_pct):
    """
    Conservative 5m OHLC trailing-stop simulation.

    Entry:
      next 5m candle OPEN after the completed H6 signal.

    Intrabar rule:
      At each candle, first test LOW against the stop that was known
      BEFORE that candle's HIGH is allowed to raise the stop.
      This avoids assuming a favorable HIGH-before-LOW path.

    Exit:
      trailing stop if hit, otherwise OPEN exactly 120m after entry.
    """
    entry_idx = signal_index + 1
    max_exit_idx = signal_index + 25  # 120m after next-candle-open entry

    if max_exit_idx >= len(candles):
        return None

    entry_price = candles[entry_idx]["open"]
    highest = entry_price
    stop = highest * (1.0 - trail_pct / 100.0)

    exit_price = None
    exit_time = None
    exit_reason = None
    exit_idx = None

    # Process entry candle through the candle immediately before max-time OPEN.
    for j in range(entry_idx, max_exit_idx):
        c = candles[j]

        # Conservative ordering: known stop is tested before current high
        # can tighten it.
        if c["low"] <= stop:
            exit_price = stop
            exit_time = c["close_time"]
            exit_reason = "TRAILING_STOP"
            exit_idx = j
            break

        if c["high"] > highest:
            highest = c["high"]
            stop = highest * (1.0 - trail_pct / 100.0)

    if exit_price is None:
        exit_price = candles[max_exit_idx]["open"]
        exit_time = candles[max_exit_idx]["open_time"]
        exit_reason = "MAX_120M_OPEN"
        exit_idx = max_exit_idx

    gross = pct_change(entry_price, exit_price)
    net = gross - ROUND_TRIP_COST_PCT

    # Approximate hold from 5m bars; max-time exit is exactly 120m.
    hold_minutes = (exit_idx - entry_idx) * 5
    if exit_reason == "TRAILING_STOP":
        hold_minutes += 5

    return {
        "trail_pct": trail_pct,
        "entry_price": entry_price,
        "exit_price": exit_price,
        "gross_pct": gross,
        "net_pct": net,
        "exit_reason": exit_reason,
        "hold_minutes": hold_minutes,
        "highest_price_seen": highest,
    }


def h6_trailing_events_v9(candles, symbol):
    """
    Frozen H6 entry. Only exit mechanics vary.
    """
    events = []

    for i in range(7, len(candles) - 25):
        signal_close = candles[i]["close"]

        mom5 = pct_change(candles[i - 1]["close"], signal_close)
        mom30 = pct_change(candles[i - 6]["close"], signal_close)

        if not (1.0 <= mom30 < 2.0):
            continue

        first15 = pct_change(
            candles[i - 6]["close"],
            candles[i - 3]["close"],
        )
        last15 = pct_change(
            candles[i - 3]["close"],
            signal_close,
        )
        acceleration = last15 - first15

        window_high = max(
            candles[j]["high"] for j in range(i - 5, i + 1)
        )
        pullback = pct_change(window_high, signal_close)

        old_vol = mean(
            candles[j]["volume"] for j in range(i - 5, i - 2)
        )
        recent_vol = mean(
            candles[j]["volume"] for j in range(i - 2, i + 1)
        )
        vol_persistence = recent_vol / old_vol if old_vol > 0 else 0

        # H6 remains frozen.
        if vol_persistence < 1.2:
            continue
        if pullback >= -0.75:
            continue
        if mom5 <= 0:
            continue
        if acceleration <= 0.25:
            continue

        entry = candles[i + 1]

        row = {
            "symbol": symbol,
            "signal_time_utc": datetime.fromtimestamp(
                candles[i]["close_time"] / 1000,
                tz=timezone.utc,
            ).isoformat(),
            "entry_time_utc": datetime.fromtimestamp(
                entry["open_time"] / 1000,
                tz=timezone.utc,
            ).isoformat(),
            "entry_open_time": entry["open_time"],
            "momentum_5m_pct": mom5,
            "momentum_30m_pct": mom30,
            "volume_persistence_ratio": vol_persistence,
            "pullback_from_30m_peak_pct": pullback,
            "acceleration_pct_points": acceleration,
            "trailing": {},
        }

        for trail in TRAILING_LEVELS_V9:
            sim = simulate_trailing_v9(candles, i, trail)
            if sim is not None:
                row["trailing"][str(trail)] = sim

        if len(row["trailing"]) == len(TRAILING_LEVELS_V9):
            # Compatibility: cooldown helper only needs timing; these fields
            # make the row compatible with existing utilities if required.
            row["gross_pct"] = row["trailing"]["1.0"]["gross_pct"]
            row["net_pct"] = row["trailing"]["1.0"]["net_pct"]
            events.append(row)

    return events


def summarize_trailing_v9(events, trail_pct):
    key = str(trail_pct)
    vals = [
        e["trailing"][key]["net_pct"]
        for e in events
        if key in e["trailing"]
    ]

    if not vals:
        return {
            "trade_count": 0,
            "mean_net_pct": None,
            "median_net_pct": None,
            "win_rate_net_pct": None,
            "profit_factor": None,
            "best_net_pct": None,
            "worst_net_pct": None,
            "trailing_stop_exit_pct": None,
            "max_120m_exit_pct": None,
            "mean_hold_minutes": None,
        }

    svals = sorted(vals)
    n = len(svals)
    med = (
        svals[n // 2]
        if n % 2
        else (svals[n // 2 - 1] + svals[n // 2]) / 2
    )

    wins = [x for x in vals if x > 0]
    losses = [x for x in vals if x < 0]
    gp = sum(wins)
    gl = abs(sum(losses))
    pf = gp / gl if gl > 0 else None

    reasons = [
        e["trailing"][key]["exit_reason"]
        for e in events
        if key in e["trailing"]
    ]
    holds = [
        e["trailing"][key]["hold_minutes"]
        for e in events
        if key in e["trailing"]
    ]

    stop_n = sum(1 for x in reasons if x == "TRAILING_STOP")
    max_n = sum(1 for x in reasons if x == "MAX_120M_OPEN")

    return {
        "trade_count": n,
        "mean_net_pct": round(mean(vals), 4),
        "median_net_pct": round(med, 4),
        "win_rate_net_pct": round(len(wins) / n * 100, 2),
        "profit_factor": round(pf, 4) if pf is not None else None,
        "best_net_pct": round(max(vals), 4),
        "worst_net_pct": round(min(vals), 4),
        "trailing_stop_exit_pct": round(stop_n / n * 100, 2),
        "max_120m_exit_pct": round(max_n / n * 100, 2),
        "mean_hold_minutes": round(mean(holds), 2),
    }


def trailing_report_v9(events):
    return {
        f"trail_{trail_pct:.1f}pct": summarize_trailing_v9(
            events, trail_pct
        )
        for trail_pct in TRAILING_LEVELS_V9
    }


@app.get("/h6-trailing")
async def h6_trailing_v9(
    count: int = Query(default=30, ge=5, le=30),
    days: int = Query(default=365, ge=90, le=365),
):
    """
    V9: Frozen H6 entry + trailing-stop exit study.
    Research/paper only. No orders.
    """
    try:
        async with httpx.AsyncClient(
            timeout=httpx.Timeout(240.0)
        ) as client:
            universe_data = await build_universe(client)
            selected = universe_data[:count]
            semaphore = asyncio.Semaphore(2)

            async def worker(item):
                async with semaphore:
                    try:
                        candles = await get_5m_candles_days(
                            client, item["symbol"], days
                        )
                        events = h6_trailing_events_v9(
                            candles, item["symbol"]
                        )
                        events = apply_symbol_cooldown_v4(events, 60)
                        return {
                            "ok": True,
                            "symbol": item["symbol"],
                            "events": events,
                        }
                    except Exception as e:
                        return {
                            "ok": False,
                            "symbol": item["symbol"],
                            "error": str(e),
                        }

            results = await asyncio.gather(
                *[worker(x) for x in selected]
            )

        successful = [x for x in results if x.get("ok")]
        failed = [x for x in results if not x.get("ok")]

        all_events = []
        for item in successful:
            all_events.extend(item["events"])

        # Retain the same split convention for comparability with V7/V8.
        dev, oos = split_dev_oos(all_events)

        per_coin = []
        for item in successful:
            if item["events"]:
                per_coin.append({
                    "symbol": item["symbol"],
                    "trade_count": len(item["events"]),
                    "trailing": trailing_report_v9(item["events"]),
                })

        return {
            **MODE_INFO,
            "status": "OK",
            "signal": False,
            "study": "FROZEN_H6_TRAILING_EXIT_STUDY",
            "days": days,
            "selected_coin_count": len(selected),
            "successful_coin_count": len(successful),
            "failed_coin_count": len(failed),
            "h6_entry_rule_changed": False,
            "frozen_h6_rule": {
                "momentum_30m_pct": ">=1.0 and <2.0",
                "volume_persistence_ratio": ">=1.2",
                "pullback_from_30m_peak_pct": "<-0.75",
                "momentum_5m_pct": ">0",
                "acceleration_pct_points": ">0.25",
            },
            "entry_model": "NEXT_5M_CANDLE_OPEN",
            "exit_models": [
                "0.5% trailing stop; max 120m",
                "1.0% trailing stop; max 120m",
                "1.5% trailing stop; max 120m",
            ],
            "intrabar_assumption": (
                "CONSERVATIVE: candle LOW tests the previously-known stop "
                "before that candle HIGH may raise the trailing stop"
            ),
            "round_trip_cost_pct": ROUND_TRIP_COST_PCT,
            "same_symbol_signal_cooldown_minutes": 60,
            "event_count": len(all_events),
            "combined": {
                "all": trailing_report_v9(all_events),
                "dev_first_2_3": trailing_report_v9(dev),
                "oos_last_1_3": trailing_report_v9(oos),
            },
            "per_coin": per_coin,
            "failed": failed,
            "generated_utc": utc_now(),
            "interpretation_note": (
                "H6 entry thresholds are frozen. V9 changes only exit "
                "management. Trailing levels are predefined at 0.5%, 1.0%, "
                "and 1.5%, each with a 120-minute maximum holding time. "
                "Results are descriptive research and do not place orders."
            ),
        }

    except Exception as e:
        return {
            **MODE_INFO,
            "status": "ERROR",
            "signal": False,
            "error": str(e),
            "generated_utc": utc_now(),
        }

def safe_median_v10(values):
    if not values:
        return None
    vals = sorted(values)
    n = len(vals)
    return vals[n // 2] if n % 2 else (vals[n // 2 - 1] + vals[n // 2]) / 2


def pattern_events_v10(candles, symbol):
    """
    V10 descriptive pattern discovery.

    Universe of observations:
      An already-started rise: 30m return >= +1.0%.
    No H6 pullback/volume/acceleration filters are used.

    Outcome:
      next 5m OPEN entry reference -> OPEN 60m later, net of 0.15% cost.

    Features use only the six completed 5m candles available at signal time.
    """
    rows = []

    for i in range(7, len(candles) - 13):
        start_close = candles[i - 6]["close"]
        signal_close = candles[i]["close"]
        mom30 = pct_change(start_close, signal_close)

        if mom30 < 1.0:
            continue

        six = candles[i - 5:i + 1]

        # Close-to-close steps over the six 5m intervals.
        step_returns = []
        prev_close = candles[i - 6]["close"]
        for c in six:
            step_returns.append(pct_change(prev_close, c["close"]))
            prev_close = c["close"]

        positive_steps = sum(1 for x in step_returns if x > 0)
        max_step = max(step_returns)
        max_step_share = max_step / mom30 if mom30 > 0 else None

        # Path efficiency: net move / total absolute close-to-close path.
        total_path = sum(abs(x) for x in step_returns)
        efficiency = mom30 / total_path if total_path > 0 else None

        high30 = max(c["high"] for c in six)
        low30 = min(c["low"] for c in six)
        range30 = high30 - low30
        close_location = (
            (signal_close - low30) / range30
            if range30 > 0 else None
        )

        body_sum = sum(abs(c["close"] - c["open"]) for c in six)
        full_range_sum = sum(max(c["high"] - c["low"], 0) for c in six)
        body_to_range = (
            body_sum / full_range_sum
            if full_range_sum > 0 else None
        )

        upper_wick_sum = sum(
            max(c["high"] - max(c["open"], c["close"]), 0)
            for c in six
        )
        upper_wick_ratio = (
            upper_wick_sum / full_range_sum
            if full_range_sum > 0 else None
        )

        up_volume = sum(
            c["volume"] for c in six if c["close"] > c["open"]
        )
        down_volume = sum(
            c["volume"] for c in six if c["close"] <= c["open"]
        )
        up_volume_share = (
            up_volume / (up_volume + down_volume)
            if (up_volume + down_volume) > 0 else None
        )

        first15 = pct_change(
            candles[i - 6]["close"],
            candles[i - 3]["close"],
        )
        last15 = pct_change(
            candles[i - 3]["close"],
            signal_close,
        )
        acceleration = last15 - first15

        entry_price = candles[i + 1]["open"]
        exit_price = candles[i + 13]["open"]
        gross60 = pct_change(entry_price, exit_price)
        net60 = gross60 - ROUND_TRIP_COST_PCT

        rows.append({
            "symbol": symbol,
            "signal_time_utc": datetime.fromtimestamp(
                candles[i]["close_time"] / 1000,
                tz=timezone.utc,
            ).isoformat(),
            "entry_open_time": candles[i + 1]["open_time"],
            "momentum_30m_pct": mom30,
            "positive_5m_steps": positive_steps,
            "path_efficiency": efficiency,
            "max_single_5m_return_pct": max_step,
            "max_single_candle_share_of_30m_move": max_step_share,
            "close_location_in_30m_range": close_location,
            "body_to_total_range_ratio": body_to_range,
            "upper_wick_to_total_range_ratio": upper_wick_ratio,
            "up_candle_volume_share": up_volume_share,
            "acceleration_pct_points": acceleration,
            "gross_60m_pct": gross60,
            "net_60m_pct": net60,
            # compatibility for existing cooldown/split helpers
            "gross_pct": gross60,
            "net_pct": net60,
        })

    return rows


def summarize_outcome_v10(events):
    vals = [e["net_60m_pct"] for e in events]
    if not vals:
        return {
            "n": 0,
            "mean_net_60m_pct": None,
            "median_net_60m_pct": None,
            "win_rate_pct": None,
            "profit_factor": None,
        }

    wins = [x for x in vals if x > 0]
    losses = [x for x in vals if x < 0]
    gp = sum(wins)
    gl = abs(sum(losses))
    pf = gp / gl if gl > 0 else None

    return {
        "n": len(vals),
        "mean_net_60m_pct": round(mean(vals), 4),
        "median_net_60m_pct": round(safe_median_v10(vals), 4),
        "win_rate_pct": round(len(wins) / len(vals) * 100, 2),
        "profit_factor": round(pf, 4) if pf is not None else None,
    }


def feature_quantiles_v10(events, feature):
    vals = sorted(
        e[feature] for e in events
        if e.get(feature) is not None
    )
    if len(vals) < 10:
        return None

    def q(p):
        idx = int((len(vals) - 1) * p)
        return vals[idx]

    return {
        "q20": q(0.20),
        "q40": q(0.40),
        "q60": q(0.60),
        "q80": q(0.80),
    }


def feature_report_v10(reference_events, target_events, feature):
    """
    Bin edges are learned ONLY from the reference set.
    The same fixed edges are then applied to target events.
    """
    qs = feature_quantiles_v10(reference_events, feature)
    if qs is None:
        return {"feature": feature, "bins": []}

    edges = [
        float("-inf"),
        qs["q20"], qs["q40"], qs["q60"], qs["q80"],
        float("inf"),
    ]
    labels = ["Q1_LOW", "Q2", "Q3", "Q4", "Q5_HIGH"]

    bins = []
    for k in range(5):
        lo, hi = edges[k], edges[k + 1]
        if k < 4:
            subset = [
                e for e in target_events
                if e.get(feature) is not None
                and lo <= e[feature] < hi
            ]
        else:
            subset = [
                e for e in target_events
                if e.get(feature) is not None
                and lo <= e[feature] <= hi
            ]

        bins.append({
            "bin": labels[k],
            "lower": None if lo == float("-inf") else round(lo, 6),
            "upper": None if hi == float("inf") else round(hi, 6),
            "outcome": summarize_outcome_v10(subset),
        })

    return {
        "feature": feature,
        "reference_quantiles": {
            k: round(v, 6) for k, v in qs.items()
        },
        "bins": bins,
    }


def positive_step_report_v10(events):
    return [
        {
            "positive_5m_steps": n,
            "outcome": summarize_outcome_v10(
                [e for e in events if e["positive_5m_steps"] == n]
            ),
        }
        for n in range(0, 7)
    ]


@app.get("/pattern-discovery")
async def pattern_discovery_v10(
    count: int = Query(default=30, ge=5, le=30),
    days: int = Query(default=365, ge=90, le=365),
):
    """
    V10: descriptive pattern discovery after a rise has already started.
    No live trading and no order placement.
    """
    try:
        async with httpx.AsyncClient(
            timeout=httpx.Timeout(240.0)
        ) as client:
            universe_data = await build_universe(client)
            selected = universe_data[:count]
            semaphore = asyncio.Semaphore(2)

            async def worker(item):
                async with semaphore:
                    try:
                        candles = await get_5m_candles_days(
                            client, item["symbol"], days
                        )
                        events = pattern_events_v10(
                            candles, item["symbol"]
                        )
                        # Reduce overlapping observations from the same coin.
                        events = apply_symbol_cooldown_v4(events, 60)
                        return {
                            "ok": True,
                            "symbol": item["symbol"],
                            "events": events,
                        }
                    except Exception as e:
                        return {
                            "ok": False,
                            "symbol": item["symbol"],
                            "error": str(e),
                        }

            results = await asyncio.gather(
                *[worker(x) for x in selected]
            )

        successful = [x for x in results if x.get("ok")]
        failed = [x for x in results if not x.get("ok")]

        all_events = []
        for item in successful:
            all_events.extend(item["events"])

        # Chronological ordering before split.
        all_events.sort(key=lambda e: e["entry_open_time"])
        cut = int(len(all_events) * 2 / 3)
        discovery = all_events[:cut]
        reference = all_events[cut:]

        features = [
            "path_efficiency",
            "max_single_candle_share_of_30m_move",
            "close_location_in_30m_range",
            "body_to_total_range_ratio",
            "upper_wick_to_total_range_ratio",
            "up_candle_volume_share",
            "acceleration_pct_points",
        ]

        return {
            **MODE_INFO,
            "status": "OK",
            "signal": False,
            "study": "POST_RISE_PATTERN_DISCOVERY",
            "days": days,
            "selected_coin_count": len(selected),
            "successful_coin_count": len(successful),
            "failed_coin_count": len(failed),
            "observation_definition": (
                "30m price rise >=1.0%; no H6 pullback, volume-persistence "
                "or acceleration entry filters"
            ),
            "outcome_definition": (
                "next 5m OPEN to OPEN 60m later, minus 0.15% round-trip cost"
            ),
            "same_symbol_observation_cooldown_minutes": 60,
            "event_count": len(all_events),
            "all_outcome": summarize_outcome_v10(all_events),
            "discovery_first_2_3_outcome": summarize_outcome_v10(discovery),
            "reference_last_1_3_outcome": summarize_outcome_v10(reference),
            "positive_step_counts": {
                "discovery": positive_step_report_v10(discovery),
                "reference": positive_step_report_v10(reference),
            },
            "feature_reports": {
                feature: {
                    "discovery": feature_report_v10(
                        discovery, discovery, feature
                    ),
                    "reference_using_discovery_edges": feature_report_v10(
                        discovery, reference, feature
                    ),
                }
                for feature in features
            },
            "failed": failed,
            "generated_utc": utc_now(),
            "interpretation_note": (
                "V10 is descriptive pattern discovery, not a trading rule. "
                "Feature quintile edges are defined from the first 2/3 only "
                "and then reused unchanged on the last 1/3. Do not select "
                "a final entry rule from this endpoint alone."
            ),
        }

    except Exception as e:
        return {
            **MODE_INFO,
            "status": "ERROR",
            "signal": False,
            "error": str(e),
            "generated_utc": utc_now(),
        }

PULLBACK_LEVELS_V11 = (0.5, 1.0, 1.5)
PULLBACK_WATCH_MINUTES_V11 = 60
REENTRY_HOLD_MINUTES_V11 = 60


def pullback_reentry_events_v11(candles, symbol, pullback_pct):
    """
    V11 hypothesis:
      1) Observe an already-completed 30m rise >= +1.0%.
      2) Do NOT buy immediately.
      3) During the next 60m, wait until price has pulled back at least
         pullback_pct from the post-signal running high.
      4) After that pullback has occurred, wait for the FIRST subsequently
         completed bullish 5m candle (close > previous close).
      5) Enter at the NEXT 5m candle OPEN.
      6) Exit at OPEN exactly 60m after entry.
      7) Subtract 0.15% round-trip cost.

    No future data is used to decide the entry.
    """
    rows = []

    # Need: 30m lookback + 60m watch + 60m post-entry horizon.
    for i in range(7, len(candles) - 26):
        signal_close = candles[i]["close"]
        mom30 = pct_change(candles[i - 6]["close"], signal_close)

        if mom30 < 1.0:
            continue

        signal_time = candles[i]["close_time"]

        # Start with the signal close as the known peak reference.
        running_high = signal_close
        pullback_seen = False
        pullback_first_seen_idx = None
        reentry_signal_idx = None
        max_pullback_seen = 0.0

        # Next 12 completed 5m candles = 60m observation window.
        for j in range(i + 1, min(i + 13, len(candles) - 13)):
            c = candles[j]

            if c["high"] > running_high:
                running_high = c["high"]

            # Conservative: pullback is considered observed from completed
            # candle data. We do not enter inside the same candle.
            dd_from_high = pct_change(running_high, c["low"])
            if dd_from_high < max_pullback_seen:
                max_pullback_seen = dd_from_high

            if (not pullback_seen) and dd_from_high <= -pullback_pct:
                pullback_seen = True
                pullback_first_seen_idx = j
                # No same-candle re-entry confirmation. Confirmation must
                # occur on a later completed candle.
                continue

            if pullback_seen and j > pullback_first_seen_idx:
                # First close-to-close positive 5m candle after pullback.
                if c["close"] > candles[j - 1]["close"]:
                    reentry_signal_idx = j
                    break

        if reentry_signal_idx is None:
            continue

        entry_idx = reentry_signal_idx + 1
        exit_idx = entry_idx + 12  # OPEN 60m after entry OPEN

        if exit_idx >= len(candles):
            continue

        entry_price = candles[entry_idx]["open"]
        exit_price = candles[exit_idx]["open"]
        gross = pct_change(entry_price, exit_price)
        net = gross - ROUND_TRIP_COST_PCT

        rows.append({
            "symbol": symbol,
            "pullback_level_pct": pullback_pct,
            "initial_signal_time_utc": datetime.fromtimestamp(
                signal_time / 1000, tz=timezone.utc
            ).isoformat(),
            "pullback_first_seen_time_utc": datetime.fromtimestamp(
                candles[pullback_first_seen_idx]["close_time"] / 1000,
                tz=timezone.utc
            ).isoformat(),
            "reentry_confirmation_time_utc": datetime.fromtimestamp(
                candles[reentry_signal_idx]["close_time"] / 1000,
                tz=timezone.utc
            ).isoformat(),
            "entry_time_utc": datetime.fromtimestamp(
                candles[entry_idx]["open_time"] / 1000,
                tz=timezone.utc
            ).isoformat(),
            "entry_open_time": candles[entry_idx]["open_time"],
            "initial_momentum_30m_pct": mom30,
            "max_pullback_seen_pct": max_pullback_seen,
            "minutes_signal_to_entry": round(
                (candles[entry_idx]["open_time"] - candles[i]["close_time"])
                / 60000, 2
            ),
            "entry_price": entry_price,
            "exit_price_60m": exit_price,
            "gross_60m_pct": gross,
            "net_60m_pct": net,
            # compatibility with existing helper
            "gross_pct": gross,
            "net_pct": net,
        })

    return rows


def summarize_v11(events):
    vals = [e["net_60m_pct"] for e in events]
    if not vals:
        return {
            "n": 0,
            "mean_net_60m_pct": None,
            "median_net_60m_pct": None,
            "win_rate_pct": None,
            "profit_factor": None,
            "best_net_pct": None,
            "worst_net_pct": None,
            "mean_minutes_signal_to_entry": None,
        }

    wins = [x for x in vals if x > 0]
    losses = [x for x in vals if x < 0]
    gp = sum(wins)
    gl = abs(sum(losses))
    pf = gp / gl if gl > 0 else None

    return {
        "n": len(vals),
        "mean_net_60m_pct": round(mean(vals), 4),
        "median_net_60m_pct": round(safe_median_v10(vals), 4),
        "win_rate_pct": round(len(wins) / len(vals) * 100, 2),
        "profit_factor": round(pf, 4) if pf is not None else None,
        "best_net_pct": round(max(vals), 4),
        "worst_net_pct": round(min(vals), 4),
        "mean_minutes_signal_to_entry": round(
            mean([e["minutes_signal_to_entry"] for e in events]), 2
        ),
    }


def v11_level_report(events_by_level):
    return {
        f"pullback_{level:.1f}pct": summarize_v11(
            events_by_level.get(level, [])
        )
        for level in PULLBACK_LEVELS_V11
    }


@app.get("/pullback-reentry")
async def pullback_reentry_v11(
    count: int = Query(default=30, ge=5, le=30),
    days: int = Query(default=365, ge=90, le=365),
):
    """
    V11: post-rise pullback + re-entry hypothesis study.
    Research/paper only; no orders.
    """
    try:
        async with httpx.AsyncClient(
            timeout=httpx.Timeout(240.0)
        ) as client:
            universe_data = await build_universe(client)
            selected = universe_data[:count]
            semaphore = asyncio.Semaphore(2)

            async def worker(item):
                async with semaphore:
                    try:
                        candles = await get_5m_candles_days(
                            client, item["symbol"], days
                        )

                        by_level = {}
                        for level in PULLBACK_LEVELS_V11:
                            ev = pullback_reentry_events_v11(
                                candles, item["symbol"], level
                            )
                            # Same-symbol re-entry signals at least 60m apart.
                            ev = apply_symbol_cooldown_v4(ev, 60)
                            by_level[level] = ev

                        return {
                            "ok": True,
                            "symbol": item["symbol"],
                            "events_by_level": by_level,
                        }
                    except Exception as e:
                        return {
                            "ok": False,
                            "symbol": item["symbol"],
                            "error": str(e),
                        }

            results = await asyncio.gather(
                *[worker(x) for x in selected]
            )

        successful = [x for x in results if x.get("ok")]
        failed = [x for x in results if not x.get("ok")]

        combined = {level: [] for level in PULLBACK_LEVELS_V11}
        per_coin = []

        for item in successful:
            coin_report = {"symbol": item["symbol"], "levels": {}}
            for level in PULLBACK_LEVELS_V11:
                events = item["events_by_level"][level]
                combined[level].extend(events)
                coin_report["levels"][f"{level:.1f}"] = summarize_v11(events)
            per_coin.append(coin_report)

        # Chronological split independently for each predefined level.
        split_reports = {}
        for level in PULLBACK_LEVELS_V11:
            ev = sorted(
                combined[level],
                key=lambda e: e["entry_open_time"]
            )
            cut = int(len(ev) * 2 / 3)
            dev = ev[:cut]
            ref = ev[cut:]

            split_reports[f"pullback_{level:.1f}pct"] = {
                "all": summarize_v11(ev),
                "discovery_first_2_3": summarize_v11(dev),
                "reference_last_1_3": summarize_v11(ref),
            }

        return {
            **MODE_INFO,
            "status": "OK",
            "signal": False,
            "study": "POST_RISE_PULLBACK_REENTRY",
            "days": days,
            "selected_coin_count": len(selected),
            "successful_coin_count": len(successful),
            "failed_coin_count": len(failed),
            "initial_event": "completed 30m rise >= +1.0%",
            "pullback_levels_pct": list(PULLBACK_LEVELS_V11),
            "pullback_watch_window_minutes": PULLBACK_WATCH_MINUTES_V11,
            "reentry_confirmation": (
                "first subsequently completed 5m candle whose close is "
                "above the previous 5m close; confirmation cannot be the "
                "same candle that first satisfies the pullback"
            ),
            "entry_model": "NEXT_5M_CANDLE_OPEN_AFTER_CONFIRMATION",
            "primary_exit": "OPEN_60_MIN_AFTER_ENTRY",
            "round_trip_cost_pct": ROUND_TRIP_COST_PCT,
            "same_symbol_reentry_cooldown_minutes": 60,
            "results": split_reports,
            "per_coin": per_coin,
            "failed": failed,
            "generated_utc": utc_now(),
            "interpretation_note": (
                "V11 tests three predefined pullback depths after an "
                "already-observed >=1% 30m rise. It does not predict which "
                "coin will rise. The three levels are hypothesis variants, "
                "not optimized thresholds. Reference results must be read "
                "together with discovery results; a single favorable level "
                "should not be promoted automatically."
            ),
        }

    except Exception as e:
        return {
            **MODE_INFO,
            "status": "ERROR",
            "signal": False,
            "error": str(e),
            "generated_utc": utc_now(),
        }

RELATIVE_Z_THRESHOLDS_V12 = (1.0, 1.5, 2.0)
RELATIVE_RANK_BUCKETS_V12 = (
    ("TOP_10_PCT", 0.90),
    ("TOP_20_PCT", 0.80),
    ("TOP_30_PCT", 0.70),
)


def precompute_rolling_volatility_v12(candles, lookback_bars=288):
    """
    O(n) rolling mean/std for 30m returns.
    Replaces the original O(n*288) implementation to avoid Render gateway
    timeouts on 365d histories.
    """
    n = len(candles)
    ret30 = [None] * n
    mus = [None] * n
    sigmas = [None] * n

    for i in range(6, n):
        ret30[i] = pct_change(candles[i - 6]["close"], candles[i]["close"])

    window = []
    running_sum = 0.0
    running_sq = 0.0
    left = 6

    for i in range(6, n):
        x = ret30[i]
        window.append(x)
        running_sum += x
        running_sq += x * x

        while i - left + 1 > lookback_bars:
            old = ret30[left]
            running_sum -= old
            running_sq -= old * old
            left += 1

        count = len(window) - (left - 6)
        if count >= 30:
            mu = running_sum / count
            var = max(0.0, running_sq / count - mu * mu)
            mus[i] = mu
            sigmas[i] = var ** 0.5

    return ret30, mus, sigmas


def relative_candidates_v12(candles, symbol):
    """
    Build time-aligned observations.

    Feature 1: 30m momentum z-score versus the coin's own trailing 24h
               distribution of 30m returns.
    Feature 2: cross-sectional percentile rank among the selected liquid
               coins at the same completed 5m signal time.

    Outcome: next 5m OPEN -> OPEN 60m later, minus 0.15% cost.
    """
    rows = []
    ret30, mus, sigmas = precompute_rolling_volatility_v12(candles, 288)

    for i in range(294, len(candles) - 13):
        mom30 = ret30[i]

        # We are still studying coins that have ALREADY risen.
        if mom30 is None or mom30 <= 0:
            continue

        mu, sigma = mus[i], sigmas[i]
        if mu is None or sigma is None or sigma <= 0:
            continue

        z = (mom30 - mu) / sigma

        entry_price = candles[i + 1]["open"]
        exit_price = candles[i + 13]["open"]
        gross = pct_change(entry_price, exit_price)
        net = gross - ROUND_TRIP_COST_PCT

        rows.append({
            "symbol": symbol,
            "signal_time_ms": candles[i]["close_time"],
            "signal_time_utc": datetime.fromtimestamp(
                candles[i]["close_time"] / 1000, tz=timezone.utc
            ).isoformat(),
            "entry_open_time": candles[i + 1]["open_time"],
            "momentum_30m_pct": mom30,
            "own_24h_mean_30m_return_pct": mu,
            "own_24h_sigma_30m_return_pct": sigma,
            "relative_momentum_z": z,
            "gross_60m_pct": gross,
            "net_60m_pct": net,
            "gross_pct": gross,
            "net_pct": net,
        })

    return rows


def add_cross_section_rank_v12(events):
    by_time = {}
    for e in events:
        by_time.setdefault(e["signal_time_ms"], []).append(e)

    ranked = []
    for _, group in by_time.items():
        if len(group) < 5:
            continue

        ordered = sorted(group, key=lambda x: x["relative_momentum_z"])
        n = len(ordered)

        for idx, e in enumerate(ordered):
            row = dict(e)
            # 0 = weakest, 1 = strongest.
            row["cross_section_percentile"] = (
                idx / (n - 1) if n > 1 else 1.0
            )
            row["cross_section_coin_count"] = n
            ranked.append(row)

    return ranked


def summarize_relative_v12(events):
    vals = [e["net_60m_pct"] for e in events]
    if not vals:
        return {
            "n": 0,
            "mean_net_60m_pct": None,
            "median_net_60m_pct": None,
            "win_rate_pct": None,
            "profit_factor": None,
        }

    wins = [x for x in vals if x > 0]
    losses = [x for x in vals if x < 0]
    gp = sum(wins)
    gl = abs(sum(losses))
    pf = gp / gl if gl > 0 else None

    return {
        "n": len(vals),
        "mean_net_60m_pct": round(mean(vals), 4),
        "median_net_60m_pct": round(safe_median_v10(vals), 4),
        "win_rate_pct": round(len(wins) / len(vals) * 100, 2),
        "profit_factor": round(pf, 4) if pf is not None else None,
        "best_net_pct": round(max(vals), 4),
        "worst_net_pct": round(min(vals), 4),
    }


def split_chrono_v12(events):
    ev = sorted(events, key=lambda e: e["entry_open_time"])
    cut = int(len(ev) * 2 / 3)
    return ev[:cut], ev[cut:]


def relative_hypothesis_report_v12(events):
    reports = {}

    # Own-volatility normalized momentum only.
    for zt in RELATIVE_Z_THRESHOLDS_V12:
        subset = [e for e in events if e["relative_momentum_z"] >= zt]
        dev, ref = split_chrono_v12(subset)
        reports[f"Z_GE_{zt:.1f}"] = {
            "definition": f"relative_momentum_z >= {zt:.1f}",
            "all": summarize_relative_v12(subset),
            "discovery_first_2_3": summarize_relative_v12(dev),
            "reference_last_1_3": summarize_relative_v12(ref),
        }

    # Cross-sectional strength plus a fixed z floor.
    for label, pct in RELATIVE_RANK_BUCKETS_V12:
        subset = [
            e for e in events
            if e["relative_momentum_z"] >= 1.0
            and e["cross_section_percentile"] >= pct
        ]
        dev, ref = split_chrono_v12(subset)
        reports[f"Z_GE_1.0_AND_{label}"] = {
            "definition": (
                f"relative_momentum_z >= 1.0 and "
                f"cross_section_percentile >= {pct:.2f}"
            ),
            "all": summarize_relative_v12(subset),
            "discovery_first_2_3": summarize_relative_v12(dev),
            "reference_last_1_3": summarize_relative_v12(ref),
        }

    return reports


@app.get("/relative-momentum")
async def relative_momentum_v12(
    count: int = Query(default=30, ge=10, le=30),
    days: int = Query(default=365, ge=90, le=365),
):
    """
    V12: Relative momentum study.
    Research/paper only. No orders.

    It asks whether a coin that has ALREADY risen is more likely to continue
    when that rise is unusually strong relative to:
      (a) its own recent volatility, and
      (b) other liquid altcoins at the same time.
    """
    try:
        async with httpx.AsyncClient(
            timeout=httpx.Timeout(240.0)
        ) as client:
            universe_data = await build_universe(client)
            selected = universe_data[:count]
            semaphore = asyncio.Semaphore(2)

            async def worker(item):
                async with semaphore:
                    try:
                        candles = await get_5m_candles_days(
                            client, item["symbol"], days
                        )
                        events = relative_candidates_v12(
                            candles, item["symbol"]
                        )
                        return {
                            "ok": True,
                            "symbol": item["symbol"],
                            "events": events,
                        }
                    except Exception as e:
                        return {
                            "ok": False,
                            "symbol": item["symbol"],
                            "error": str(e),
                        }

            results = await asyncio.gather(
                *[worker(x) for x in selected]
            )

        successful = [x for x in results if x.get("ok")]
        failed = [x for x in results if not x.get("ok")]

        raw = []
        for item in successful:
            raw.extend(item["events"])

        ranked = add_cross_section_rank_v12(raw)

        # Prevent repeated overlapping observations from the same coin.
        ranked.sort(key=lambda e: (e["symbol"], e["entry_open_time"]))
        cooled = []
        last_by_symbol = {}
        for e in ranked:
            t = e["entry_open_time"]
            last = last_by_symbol.get(e["symbol"])
            if last is None or t - last >= 60 * 60 * 1000:
                cooled.append(e)
                last_by_symbol[e["symbol"]] = t

        cooled.sort(key=lambda e: e["entry_open_time"])

        return {
            **MODE_INFO,
            "status": "OK",
            "signal": False,
            "study": "RELATIVE_MOMENTUM_AFTER_RISE_ALREADY_STARTED",
            "days": days,
            "selected_coin_count": len(selected),
            "successful_coin_count": len(successful),
            "failed_coin_count": len(failed),
            "concept": (
                "No fixed +1% trigger. Candidate must already have a positive "
                "30m return. Strength is normalized by its own trailing 24h "
                "30m-return volatility and ranked against other selected "
                "liquid coins at the same completed 5m timestamp."
            ),
            "own_history_lookback": "24h",
            "outcome": "NEXT_5M_OPEN_TO_OPEN_60M_LATER_MINUS_0.15PCT_COST",
            "same_symbol_observation_cooldown_minutes": 60,
            "raw_positive_momentum_observations": len(raw),
            "ranked_and_cooled_observations": len(cooled),
            "predefined_hypotheses": {
                "z_thresholds": list(RELATIVE_Z_THRESHOLDS_V12),
                "cross_section_rank_buckets": [
                    {"label": label, "minimum_percentile": pct}
                    for label, pct in RELATIVE_RANK_BUCKETS_V12
                ],
            },
            "results": relative_hypothesis_report_v12(cooled),
            "failed": failed,
            "generated_utc": utc_now(),
            "interpretation_note": (
                "V12 is a new hypothesis study, not a trading rule. "
                "Thresholds are predefined before viewing V12 results. "
                "A candidate should not be promoted unless discovery and "
                "reference results point in the same direction with adequate "
                "sample size and breadth. Current-universe survivorship bias "
                "remains."
            ),
        }

    except Exception as e:
        return {
            **MODE_INFO,
            "status": "ERROR",
            "signal": False,
            "error": str(e),
            "generated_utc": utc_now(),
        }

V13_HORIZONS_MINUTES = (5, 10, 15, 30, 60, 120)


def relative_candidates_v13(candles, symbol):
    """
    Same V12 signal features; only the outcome horizon changes.
    Entry = next 5m OPEN after the completed signal candle.
    Outcomes = OPEN-to-OPEN at 5/10/15/30/60/120m, each minus 0.15% cost.
    """
    rows = []
    ret30, mus, sigmas = precompute_rolling_volatility_v12(candles, 288)

    max_bars = max(V13_HORIZONS_MINUTES) // 5

    for i in range(294, len(candles) - max_bars - 1):
        mom30 = ret30[i]
        if mom30 is None or mom30 <= 0:
            continue

        mu, sigma = mus[i], sigmas[i]
        if mu is None or sigma is None or sigma <= 0:
            continue

        z = (mom30 - mu) / sigma
        entry_idx = i + 1
        entry_price = candles[entry_idx]["open"]

        outcomes = {}
        for minutes in V13_HORIZONS_MINUTES:
            exit_idx = entry_idx + minutes // 5
            exit_price = candles[exit_idx]["open"]
            gross = pct_change(entry_price, exit_price)
            outcomes[minutes] = gross - ROUND_TRIP_COST_PCT

        rows.append({
            "symbol": symbol,
            "signal_time_ms": candles[i]["close_time"],
            "entry_open_time": candles[entry_idx]["open_time"],
            "actionable_time_ms": candles[entry_idx]["close_time"] + 1,
            "momentum_30m_pct": mom30,
            "relative_momentum_z": z,
            "outcomes": outcomes,
        })

    return rows


def summarize_horizon_v13(events, minutes):
    vals = [e["outcomes"][minutes] for e in events]
    if not vals:
        return {
            "n": 0,
            "mean_net_pct": None,
            "median_net_pct": None,
            "win_rate_pct": None,
            "profit_factor": None,
        }

    wins = [x for x in vals if x > 0]
    losses = [x for x in vals if x < 0]
    gp = sum(wins)
    gl = abs(sum(losses))
    pf = gp / gl if gl > 0 else None

    return {
        "n": len(vals),
        "mean_net_pct": round(mean(vals), 4),
        "median_net_pct": round(safe_median_v10(vals), 4),
        "win_rate_pct": round(len(wins) / len(vals) * 100, 2),
        "profit_factor": round(pf, 4) if pf is not None else None,
    }


def horizon_report_v13(events):
    ev = sorted(events, key=lambda e: e["entry_open_time"])
    cut = int(len(ev) * 2 / 3)
    dev, ref = ev[:cut], ev[cut:]

    report = {}
    for minutes in V13_HORIZONS_MINUTES:
        report[f"{minutes}m"] = {
            "all": summarize_horizon_v13(ev, minutes),
            "discovery_first_2_3": summarize_horizon_v13(dev, minutes),
            "reference_last_1_3": summarize_horizon_v13(ref, minutes),
        }
    return report


@app.get("/relative-momentum-horizons")
async def relative_momentum_horizons_v13(
    count: int = Query(default=10, ge=10, le=30),
    days: int = Query(default=90, ge=90, le=365),
):
    """
    V13: Exit-horizon diagnostic for V12 relative momentum.
    No entry optimization. Research/paper only.
    """
    try:
        async with httpx.AsyncClient(
            timeout=httpx.Timeout(240.0)
        ) as client:
            universe_data = await build_universe(client)
            selected = universe_data[:count]
            semaphore = asyncio.Semaphore(2)

            async def worker(item):
                async with semaphore:
                    try:
                        candles = await get_5m_candles_days(
                            client, item["symbol"], days
                        )
                        return {
                            "ok": True,
                            "symbol": item["symbol"],
                            "events": relative_candidates_v13(
                                candles, item["symbol"]
                            ),
                        }
                    except Exception as e:
                        return {
                            "ok": False,
                            "symbol": item["symbol"],
                            "error": str(e),
                        }

            results = await asyncio.gather(
                *[worker(x) for x in selected]
            )

        successful = [x for x in results if x.get("ok")]
        failed = [x for x in results if not x.get("ok")]

        raw = []
        for item in successful:
            raw.extend(item["events"])

        # Cross-sectional rank by V12 z-score at same completed timestamp.
        by_time = {}
        for e in raw:
            by_time.setdefault(e["signal_time_ms"], []).append(e)

        ranked = []
        for group in by_time.values():
            if len(group) < 5:
                continue
            ordered = sorted(group, key=lambda x: x["relative_momentum_z"])
            n = len(ordered)
            for idx, e in enumerate(ordered):
                row = dict(e)
                row["cross_section_percentile"] = (
                    idx / (n - 1) if n > 1 else 1.0
                )
                ranked.append(row)

        # Same-symbol 60m cooldown, unchanged from V12.
        ranked.sort(key=lambda e: (e["symbol"], e["entry_open_time"]))
        cooled = []
        last_by_symbol = {}
        for e in ranked:
            t = e["entry_open_time"]
            last = last_by_symbol.get(e["symbol"])
            if last is None or t - last >= 60 * 60 * 1000:
                cooled.append(e)
                last_by_symbol[e["symbol"]] = t

        hypotheses = {
            "Z_GE_1.0": [
                e for e in cooled
                if e["relative_momentum_z"] >= 1.0
            ],
            "Z_GE_1.0_AND_TOP_10_PCT": [
                e for e in cooled
                if e["relative_momentum_z"] >= 1.0
                and e["cross_section_percentile"] >= 0.90
            ],
            "Z_GE_1.0_AND_TOP_20_PCT": [
                e for e in cooled
                if e["relative_momentum_z"] >= 1.0
                and e["cross_section_percentile"] >= 0.80
            ],
        }

        return {
            **MODE_INFO,
            "status": "OK",
            "signal": False,
            "study": "RELATIVE_MOMENTUM_EXIT_HORIZON_DIAGNOSTIC",
            "days": days,
            "selected_coin_count": len(selected),
            "successful_coin_count": len(successful),
            "failed_coin_count": len(failed),
            "entry_hypotheses_frozen_from_v12": [
                "Z_GE_1.0",
                "Z_GE_1.0_AND_TOP_10_PCT",
                "Z_GE_1.0_AND_TOP_20_PCT",
            ],
            "entry_model": "NEXT_5M_OPEN_AFTER_COMPLETED_SIGNAL",
            "exit_horizons_minutes": list(V13_HORIZONS_MINUTES),
            "round_trip_cost_pct_applied_to_each_horizon": ROUND_TRIP_COST_PCT,
            "same_symbol_observation_cooldown_minutes": 60,
            "results": {
                name: horizon_report_v13(events)
                for name, events in hypotheses.items()
            },
            "failed": failed,
            "generated_utc": utc_now(),
            "interpretation_note": (
                "V13 changes only the exit horizon. Entry hypotheses are "
                "frozen from V12. The purpose is to determine whether any "
                "continuation exists briefly after entry and then decays or "
                "reverses. Do not choose a horizon from one favorable cell; "
                "look for a coherent time pattern in discovery and reference."
            ),
        }

    except Exception as e:
        return {
            **MODE_INFO,
            "status": "ERROR",
            "signal": False,
            "error": str(e),
            "generated_utc": utc_now(),
        }

V14_ENTRY_DELAYS_MINUTES = (0, 15, 30, 60, 120)
V14_HOLD_MINUTES = 120


def relative_candidates_v14(candles, symbol):
    """
    V14 keeps the V12 relative-momentum signal frozen and changes only
    entry timing.

    Signal: completed 5m candle; coin has already risen over 30m.
    Entry delays tested: 0/15/30/60/120 minutes after the original
    next-5m-open entry point.
    Hold: fixed 120 minutes from each delayed entry.
    Cost: 0.15% round trip for every delay.
    """
    rows = []
    ret30, mus, sigmas = precompute_rolling_volatility_v12(candles, 288)

    max_delay_bars = max(V14_ENTRY_DELAYS_MINUTES) // 5
    hold_bars = V14_HOLD_MINUTES // 5

    for i in range(294, len(candles) - max_delay_bars - hold_bars - 2):
        mom30 = ret30[i]
        if mom30 is None or mom30 <= 0:
            continue

        mu, sigma = mus[i], sigmas[i]
        if mu is None or sigma is None or sigma <= 0:
            continue

        z = (mom30 - mu) / sigma
        original_entry_idx = i + 1

        outcomes = {}
        for delay in V14_ENTRY_DELAYS_MINUTES:
            entry_idx = original_entry_idx + delay // 5
            exit_idx = entry_idx + hold_bars
            entry_price = candles[entry_idx]["open"]
            exit_price = candles[exit_idx]["open"]
            gross = pct_change(entry_price, exit_price)
            outcomes[delay] = gross - ROUND_TRIP_COST_PCT

        rows.append({
            "symbol": symbol,
            "signal_time_ms": candles[i]["close_time"],
            "entry_open_time": candles[original_entry_idx]["open_time"],
            "momentum_30m_pct": mom30,
            "relative_momentum_z": z,
            "outcomes": outcomes,
        })

    return rows


def summarize_delay_v14(events, delay):
    vals = [e["outcomes"][delay] for e in events]
    if not vals:
        return {
            "n": 0,
            "mean_net_120m_pct": None,
            "median_net_120m_pct": None,
            "win_rate_pct": None,
            "profit_factor": None,
        }

    wins = [x for x in vals if x > 0]
    losses = [x for x in vals if x < 0]
    gp = sum(wins)
    gl = abs(sum(losses))
    pf = gp / gl if gl > 0 else None

    return {
        "n": len(vals),
        "mean_net_120m_pct": round(mean(vals), 4),
        "median_net_120m_pct": round(safe_median_v10(vals), 4),
        "win_rate_pct": round(len(wins) / len(vals) * 100, 2),
        "profit_factor": round(pf, 4) if pf is not None else None,
    }


def delay_report_v14(events):
    ev = sorted(events, key=lambda e: e["entry_open_time"])
    cut = int(len(ev) * 2 / 3)
    dev, ref = ev[:cut], ev[cut:]

    report = {}
    for delay in V14_ENTRY_DELAYS_MINUTES:
        report[f"delay_{delay}m"] = {
            "all": summarize_delay_v14(ev, delay),
            "discovery_first_2_3": summarize_delay_v14(dev, delay),
            "reference_last_1_3": summarize_delay_v14(ref, delay),
        }
    return report


@app.get("/relative-momentum-entry-delay")
async def relative_momentum_entry_delay_v14(
    count: int = Query(default=10, ge=10, le=30),
    days: int = Query(default=90, ge=90, le=365),
):
    """
    V14 entry-delay diagnostic.
    Entry hypotheses remain frozen from V12/V13.
    Only entry timing changes; hold is fixed at 120m.
    Research/paper only.
    """
    try:
        async with httpx.AsyncClient(
            timeout=httpx.Timeout(240.0)
        ) as client:
            universe_data = await build_universe(client)
            selected = universe_data[:count]
            semaphore = asyncio.Semaphore(2)

            async def worker(item):
                async with semaphore:
                    try:
                        candles = await get_5m_candles_days(
                            client, item["symbol"], days
                        )
                        return {
                            "ok": True,
                            "symbol": item["symbol"],
                            "events": relative_candidates_v14(
                                candles, item["symbol"]
                            ),
                        }
                    except Exception as e:
                        return {
                            "ok": False,
                            "symbol": item["symbol"],
                            "error": str(e),
                        }

            results = await asyncio.gather(
                *[worker(x) for x in selected]
            )

        successful = [x for x in results if x.get("ok")]
        failed = [x for x in results if not x.get("ok")]

        raw = []
        for item in successful:
            raw.extend(item["events"])

        # Same V12/V13 cross-sectional ranking.
        by_time = {}
        for e in raw:
            by_time.setdefault(e["signal_time_ms"], []).append(e)

        ranked = []
        for group in by_time.values():
            if len(group) < 5:
                continue
            ordered = sorted(group, key=lambda x: x["relative_momentum_z"])
            n = len(ordered)
            for idx, e in enumerate(ordered):
                row = dict(e)
                row["cross_section_percentile"] = (
                    idx / (n - 1) if n > 1 else 1.0
                )
                ranked.append(row)

        # Keep the original signal-observation cooldown frozen at 60m.
        ranked.sort(key=lambda e: (e["symbol"], e["entry_open_time"]))
        cooled = []
        last_by_symbol = {}
        for e in ranked:
            t = e["entry_open_time"]
            last = last_by_symbol.get(e["symbol"])
            if last is None or t - last >= 60 * 60 * 1000:
                cooled.append(e)
                last_by_symbol[e["symbol"]] = t

        hypotheses = {
            "Z_GE_1.0": [
                e for e in cooled
                if e["relative_momentum_z"] >= 1.0
            ],
            "Z_GE_1.0_AND_TOP_10_PCT": [
                e for e in cooled
                if e["relative_momentum_z"] >= 1.0
                and e["cross_section_percentile"] >= 0.90
            ],
            "Z_GE_1.0_AND_TOP_20_PCT": [
                e for e in cooled
                if e["relative_momentum_z"] >= 1.0
                and e["cross_section_percentile"] >= 0.80
            ],
        }

        return {
            **MODE_INFO,
            "status": "OK",
            "signal": False,
            "study": "RELATIVE_MOMENTUM_ENTRY_DELAY_DIAGNOSTIC",
            "days": days,
            "selected_coin_count": len(selected),
            "successful_coin_count": len(successful),
            "failed_coin_count": len(failed),
            "entry_hypotheses_frozen_from_v12_v13": [
                "Z_GE_1.0",
                "Z_GE_1.0_AND_TOP_10_PCT",
                "Z_GE_1.0_AND_TOP_20_PCT",
            ],
            "signal_observation": "COMPLETED_5M_CANDLE",
            "baseline_entry": "NEXT_5M_OPEN_AFTER_COMPLETED_SIGNAL",
            "entry_delays_minutes": list(V14_ENTRY_DELAYS_MINUTES),
            "hold_minutes_after_each_entry": V14_HOLD_MINUTES,
            "round_trip_cost_pct": ROUND_TRIP_COST_PCT,
            "same_symbol_signal_cooldown_minutes": 60,
            "results": {
                name: delay_report_v14(events)
                for name, events in hypotheses.items()
            },
            "failed": failed,
            "generated_utc": utc_now(),
            "interpretation_note": (
                "V14 changes only entry timing. The signal definition, "
                "relative-momentum thresholds, cross-sectional ranking, "
                "cost and signal cooldown are frozen. Look for a coherent "
                "improvement with delay in both discovery and reference; "
                "do not select a delay from one favorable cell."
            ),
        }

    except Exception as e:
        return {
            **MODE_INFO,
            "status": "ERROR",
            "signal": False,
            "error": str(e),
            "generated_utc": utc_now(),
        }

V15_WAIT_MINUTES = 60
V15_HOLD_MINUTES = 120


def classify_post_rise_behavior_v15(candles, signal_i):
    """
    Observe the 60 minutes AFTER the original next-open reference.
    No entry decision is made until that observation window is complete.

    Categories are descriptive and fixed before seeing V15 results:
    - CONTINUED_UP: price kept advancing during the wait.
    - SHALLOW_CONSOLIDATION: stayed near signal price with limited drawdown.
    - PULLBACK_RECOVERY: meaningful pullback, then recovered near/above signal.
    - PULLBACK_UNRECOVERED: meaningful pullback and remained below signal.
    """
    start_idx = signal_i + 1
    end_idx = start_idx + V15_WAIT_MINUTES // 5

    signal_close = candles[signal_i]["close"]
    window = candles[start_idx:end_idx + 1]
    if len(window) < (V15_WAIT_MINUTES // 5 + 1):
        return None

    end_price = candles[end_idx]["open"]
    highs = [c["high"] for c in window]
    lows = [c["low"] for c in window]

    end_change = pct_change(signal_close, end_price)
    max_up = pct_change(signal_close, max(highs))
    max_down = pct_change(signal_close, min(lows))

    # Fixed descriptive thresholds; not optimized from results.
    if end_change >= 0.75:
        label = "CONTINUED_UP"
    elif max_down > -0.75 and -0.50 <= end_change < 0.75:
        label = "SHALLOW_CONSOLIDATION"
    elif max_down <= -0.75 and end_change >= -0.25:
        label = "PULLBACK_RECOVERY"
    else:
        label = "PULLBACK_UNRECOVERED"

    return {
        "behavior": label,
        "wait_end_change_pct": end_change,
        "wait_max_up_pct": max_up,
        "wait_max_down_pct": max_down,
        "entry_idx": end_idx,
    }


def relative_candidates_v15(candles, symbol):
    """
    Frozen V12 relative-momentum signal.
    Observe post-rise behavior for 60m, then enter at the observation-window
    end OPEN and hold 120m. Cost remains 0.15%.
    """
    rows = []
    ret30, mus, sigmas = precompute_rolling_volatility_v12(candles, 288)
    hold_bars = V15_HOLD_MINUTES // 5

    for i in range(294, len(candles) - (V15_WAIT_MINUTES // 5) - hold_bars - 3):
        mom30 = ret30[i]
        if mom30 is None or mom30 <= 0:
            continue

        mu, sigma = mus[i], sigmas[i]
        if mu is None or sigma is None or sigma <= 0:
            continue

        z = (mom30 - mu) / sigma
        behavior = classify_post_rise_behavior_v15(candles, i)
        if behavior is None:
            continue

        entry_idx = behavior["entry_idx"]
        exit_idx = entry_idx + hold_bars
        entry_price = candles[entry_idx]["open"]
        exit_price = candles[exit_idx]["open"]
        gross = pct_change(entry_price, exit_price)
        net = gross - ROUND_TRIP_COST_PCT

        rows.append({
            "symbol": symbol,
            "signal_time_ms": candles[i]["close_time"],
            "entry_open_time": candles[entry_idx]["open_time"],
            "momentum_30m_pct": mom30,
            "relative_momentum_z": z,
            "behavior": behavior["behavior"],
            "wait_end_change_pct": behavior["wait_end_change_pct"],
            "wait_max_up_pct": behavior["wait_max_up_pct"],
            "wait_max_down_pct": behavior["wait_max_down_pct"],
            "net_120m_pct": net,
        })

    return rows



def relative_candidates_forward_live(candles, symbol):
    """
    Forward-safe version of the frozen relative-momentum + 60m behavior observation.
    IMPORTANT: does NOT require the future 120m exit candle to exist.
    It uses only data known by the paper-entry OPEN.
    """
    rows = []
    ret30, mus, sigmas = precompute_rolling_volatility_v12(candles, 288)
    wait_bars = V15_WAIT_MINUTES // 5

    # Need enough candles only through the 60m observation / entry checkpoint.
    for i in range(294, len(candles) - wait_bars - 1):
        mom30 = ret30[i]
        if mom30 is None or mom30 <= 0:
            continue

        mu, sigma = mus[i], sigmas[i]
        if mu is None or sigma is None or sigma <= 0:
            continue

        z = (mom30 - mu) / sigma
        behavior = classify_post_rise_behavior_v15(candles, i)
        if behavior is None:
            continue

        entry_idx = behavior["entry_idx"]
        if entry_idx >= len(candles):
            continue

        rows.append({
            "symbol": symbol,
            "signal_time_ms": candles[i]["close_time"],
            "entry_open_time": candles[entry_idx]["open_time"],
            "momentum_30m_pct": mom30,
            "relative_momentum_z": z,
            "behavior": behavior["behavior"],
            "wait_end_change_pct": behavior["wait_end_change_pct"],
            "wait_max_up_pct": behavior["wait_max_up_pct"],
            "wait_max_down_pct": behavior["wait_max_down_pct"],
        })

    return rows


def summarize_behavior_v15(events):
    vals = [e["net_120m_pct"] for e in events]
    if not vals:
        return {
            "n": 0,
            "mean_net_120m_pct": None,
            "median_net_120m_pct": None,
            "win_rate_pct": None,
            "profit_factor": None,
        }

    wins = [x for x in vals if x > 0]
    losses = [x for x in vals if x < 0]
    gp = sum(wins)
    gl = abs(sum(losses))
    pf = gp / gl if gl > 0 else None

    return {
        "n": len(vals),
        "mean_net_120m_pct": round(mean(vals), 4),
        "median_net_120m_pct": round(safe_median_v10(vals), 4),
        "win_rate_pct": round(len(wins) / len(vals) * 100, 2),
        "profit_factor": round(pf, 4) if pf is not None else None,
    }


def behavior_report_v15(events):
    ev = sorted(events, key=lambda e: e["entry_open_time"])
    cut = int(len(ev) * 2 / 3)
    dev, ref = ev[:cut], ev[cut:]

    labels = (
        "CONTINUED_UP",
        "SHALLOW_CONSOLIDATION",
        "PULLBACK_RECOVERY",
        "PULLBACK_UNRECOVERED",
    )

    report = {}
    for label in labels:
        a = [e for e in ev if e["behavior"] == label]
        d = [e for e in dev if e["behavior"] == label]
        r = [e for e in ref if e["behavior"] == label]
        report[label] = {
            "all": summarize_behavior_v15(a),
            "discovery_first_2_3": summarize_behavior_v15(d),
            "reference_last_1_3": summarize_behavior_v15(r),
        }
    return report


@app.get("/post-rise-behavior")
async def post_rise_behavior_v15(
    count: int = Query(default=10, ge=10, le=30),
    days: int = Query(default=90, ge=90, le=365),
):
    """
    V15: after a relative-momentum rise, observe 60m price behavior before
    entry. Tests whether continuation depends on consolidation/pullback shape.
    Research/paper only.
    """
    try:
        async with httpx.AsyncClient(timeout=httpx.Timeout(240.0)) as client:
            universe_data = await build_universe(client)
            selected = universe_data[:count]
            semaphore = asyncio.Semaphore(2)

            async def worker(item):
                async with semaphore:
                    try:
                        candles = await get_5m_candles_days(
                            client, item["symbol"], days
                        )
                        return {
                            "ok": True,
                            "symbol": item["symbol"],
                            "events": relative_candidates_v15(
                                candles, item["symbol"]
                            ),
                        }
                    except Exception as e:
                        return {
                            "ok": False,
                            "symbol": item["symbol"],
                            "error": str(e),
                        }

            results = await asyncio.gather(*[worker(x) for x in selected])

        successful = [x for x in results if x.get("ok")]
        failed = [x for x in results if not x.get("ok")]

        raw = []
        for item in successful:
            raw.extend(item["events"])

        # Same-time cross-sectional rank, frozen from V12-V14.
        by_time = {}
        for e in raw:
            by_time.setdefault(e["signal_time_ms"], []).append(e)

        ranked = []
        for group in by_time.values():
            if len(group) < 5:
                continue
            ordered = sorted(group, key=lambda x: x["relative_momentum_z"])
            n = len(ordered)
            for idx, e in enumerate(ordered):
                row = dict(e)
                row["cross_section_percentile"] = (
                    idx / (n - 1) if n > 1 else 1.0
                )
                ranked.append(row)

        # Frozen 60m signal cooldown.
        ranked.sort(key=lambda e: (e["symbol"], e["entry_open_time"]))
        cooled = []
        last_by_symbol = {}
        for e in ranked:
            t = e["entry_open_time"]
            last = last_by_symbol.get(e["symbol"])
            if last is None or t - last >= 60 * 60 * 1000:
                cooled.append(e)
                last_by_symbol[e["symbol"]] = t

        hypotheses = {
            "Z_GE_1.0": [
                e for e in cooled if e["relative_momentum_z"] >= 1.0
            ],
            "Z_GE_1.0_AND_TOP_10_PCT": [
                e for e in cooled
                if e["relative_momentum_z"] >= 1.0
                and e["cross_section_percentile"] >= 0.90
            ],
            "Z_GE_1.0_AND_TOP_20_PCT": [
                e for e in cooled
                if e["relative_momentum_z"] >= 1.0
                and e["cross_section_percentile"] >= 0.80
            ],
        }

        return {
            **MODE_INFO,
            "status": "OK",
            "signal": False,
            "study": "POST_RISE_BEHAVIOR_AFTER_RELATIVE_MOMENTUM",
            "days": days,
            "selected_coin_count": len(selected),
            "successful_coin_count": len(successful),
            "failed_coin_count": len(failed),
            "signal_hypotheses_frozen_from_v12_v14": list(hypotheses.keys()),
            "observation_wait_minutes": V15_WAIT_MINUTES,
            "entry": "OPEN_AT_END_OF_60M_OBSERVATION_WINDOW",
            "hold_minutes": V15_HOLD_MINUTES,
            "round_trip_cost_pct": ROUND_TRIP_COST_PCT,
            "behavior_definitions": {
                "CONTINUED_UP": "wait_end_change >= +0.75%",
                "SHALLOW_CONSOLIDATION": (
                    "max drawdown > -0.75% and wait end change between "
                    "-0.50% and +0.75%"
                ),
                "PULLBACK_RECOVERY": (
                    "max drawdown <= -0.75% and wait end change >= -0.25%"
                ),
                "PULLBACK_UNRECOVERED": "remaining meaningful pullback cases",
            },
            "results": {
                name: behavior_report_v15(events)
                for name, events in hypotheses.items()
            },
            "failed": failed,
            "generated_utc": utc_now(),
            "interpretation_note": (
                "V15 studies observed post-rise behavior rather than adding "
                "another momentum threshold. Categories are predefined. "
                "A behavior is interesting only if discovery and reference "
                "show similar direction with adequate sample size; isolated "
                "positive cells should not be promoted."
            ),
        }

    except Exception as e:
        return {
            **MODE_INFO,
            "status": "ERROR",
            "signal": False,
            "error": str(e),
            "generated_utc": utc_now(),
        }

V16_WAIT_MINUTES = 60
V16_HOLD_MINUTES = 120


def summarize_v16(events):
    vals = [e["net_120m_pct"] for e in events]
    if not vals:
        return {
            "n": 0, "mean_net_pct": None, "median_net_pct": None,
            "win_rate_pct": None, "profit_factor": None
        }
    wins = [x for x in vals if x > 0]
    losses = [x for x in vals if x < 0]
    gp, gl = sum(wins), abs(sum(losses))
    return {
        "n": len(vals),
        "mean_net_pct": round(mean(vals), 4),
        "median_net_pct": round(safe_median_v10(vals), 4),
        "win_rate_pct": round(100 * len(wins) / len(vals), 2),
        "profit_factor": round(gp / gl, 4) if gl > 0 else None,
        "best_net_pct": round(max(vals), 4),
        "worst_net_pct": round(min(vals), 4),
    }


def chronological_blocks_v16(events):
    ev = sorted(events, key=lambda e: e["entry_open_time"])
    n = len(ev)
    if n == 0:
        return {}
    q1 = n // 4
    q2 = n // 2
    q3 = (3 * n) // 4
    return {
        "Q1_OLDEST": summarize_v16(ev[:q1]),
        "Q2": summarize_v16(ev[q1:q2]),
        "Q3": summarize_v16(ev[q2:q3]),
        "Q4_NEWEST": summarize_v16(ev[q3:]),
    }


def breadth_v16(events):
    by_symbol = {}
    for e in events:
        by_symbol.setdefault(e["symbol"], []).append(e)

    rows = []
    for symbol, arr in by_symbol.items():
        sm = summarize_v16(arr)
        rows.append({"symbol": symbol, **sm})

    rows.sort(key=lambda x: (-x["n"], x["symbol"]))
    positive_mean = sum(1 for x in rows if x["mean_net_pct"] is not None and x["mean_net_pct"] > 0)
    pf_above_1 = sum(1 for x in rows if x["profit_factor"] is not None and x["profit_factor"] > 1)

    return {
        "coin_count": len(rows),
        "coins_positive_mean": positive_mean,
        "coins_pf_above_1": pf_above_1,
        "per_coin": rows,
    }


@app.get("/v16-validate")
async def v16_validate(
    count: int = Query(default=30, ge=10, le=30),
    days: int = Query(default=365, ge=90, le=365),
):
    """
    V16 long validation. No threshold optimization.

    Frozen primary candidate:
      Z >= 1
      cross-sectional top 20%
      observe 60m
      behavior = CONTINUED_UP (wait end >= +0.75%)
      enter at end-of-wait OPEN
      hold 120m
      cost 0.15%

    PULLBACK_UNRECOVERED is retained as a secondary/control candidate.
    """
    try:
        async with httpx.AsyncClient(timeout=httpx.Timeout(300.0)) as client:
            universe_data = await build_universe(client)
            selected = universe_data[:count]
            semaphore = asyncio.Semaphore(2)

            async def worker(item):
                async with semaphore:
                    try:
                        candles = await get_5m_candles_days(client, item["symbol"], days)
                        return {
                            "ok": True,
                            "symbol": item["symbol"],
                            "events": relative_candidates_v15(candles, item["symbol"]),
                        }
                    except Exception as e:
                        return {"ok": False, "symbol": item["symbol"], "error": str(e)}

            results = await asyncio.gather(*[worker(x) for x in selected])

        successful = [x for x in results if x.get("ok")]
        failed = [x for x in results if not x.get("ok")]

        raw = []
        for item in successful:
            raw.extend(item["events"])

        # Cross-sectional percentile exactly as V15.
        by_time = {}
        for e in raw:
            by_time.setdefault(e["signal_time_ms"], []).append(e)

        ranked = []
        for group in by_time.values():
            if len(group) < 5:
                continue
            ordered = sorted(group, key=lambda x: x["relative_momentum_z"])
            n = len(ordered)
            for idx, e in enumerate(ordered):
                row = dict(e)
                row["cross_section_percentile"] = idx / (n - 1) if n > 1 else 1.0
                ranked.append(row)

        # Same frozen 60m signal cooldown.
        ranked.sort(key=lambda e: (e["symbol"], e["entry_open_time"]))
        cooled = []
        last_by_symbol = {}
        for e in ranked:
            t = e["entry_open_time"]
            last = last_by_symbol.get(e["symbol"])
            if last is None or t - last >= 60 * 60 * 1000:
                cooled.append(e)
                last_by_symbol[e["symbol"]] = t

        top20 = [
            e for e in cooled
            if e["relative_momentum_z"] >= 1.0
            and e["cross_section_percentile"] >= 0.80
        ]

        continued = [e for e in top20 if e["behavior"] == "CONTINUED_UP"]
        pullback_unrecovered = [
            e for e in top20 if e["behavior"] == "PULLBACK_UNRECOVERED"
        ]

        def package(events):
            ev = sorted(events, key=lambda e: e["entry_open_time"])
            cut = int(len(ev) * 2 / 3)
            return {
                "all": summarize_v16(ev),
                "older_first_2_3": summarize_v16(ev[:cut]),
                "newest_last_1_3": summarize_v16(ev[cut:]),
                "chronological_quarters": chronological_blocks_v16(ev),
                "breadth": breadth_v16(ev),
            }

        return {
            **MODE_INFO,
            "status": "OK",
            "signal": False,
            "study": "V16_FROZEN_LONG_VALIDATION",
            "days": days,
            "selected_coin_count": len(selected),
            "successful_coin_count": len(successful),
            "failed_coin_count": len(failed),
            "frozen_common_rule": {
                "relative_momentum_z_min": 1.0,
                "cross_section_percentile_min": 0.80,
                "observation_wait_minutes": V16_WAIT_MINUTES,
                "entry": "OPEN_AT_END_OF_60M_OBSERVATION_WINDOW",
                "hold_minutes": V16_HOLD_MINUTES,
                "round_trip_cost_pct": ROUND_TRIP_COST_PCT,
                "same_symbol_signal_cooldown_minutes": 60,
            },
            "primary_candidate": {
                "name": "CONTINUED_UP",
                "definition": "wait_end_change >= +0.75%",
                "validation": package(continued),
            },
            "secondary_control": {
                "name": "PULLBACK_UNRECOVERED",
                "definition": "same frozen V15 category; no retuning",
                "validation": package(pullback_unrecovered),
            },
            "failed": failed,
            "generated_utc": utc_now(),
            "interpretation_note": (
                "V16 is validation, not optimization. No V15 thresholds are changed. "
                "Inspect full-period performance, oldest/newest consistency, four "
                "chronological blocks, and coin breadth. A favorable aggregate driven "
                "by a few coins or one period should not be promoted to forward paper."
            ),
        }

    except Exception as e:
        return {
            **MODE_INFO,
            "status": "ERROR",
            "signal": False,
            "error": str(e),
            "generated_utc": utc_now(),
        }

@app.get("/v16-validate-light")
async def v16_validate_light():
    """
    Render-friendly V16 validation.
    Frozen strategy rules are unchanged.
    Only workload is reduced to 10 coins / 180 days.
    """
    return await v16_validate(count=10, days=180)

# =========================
# V17 MARKET REGIME DIAGNOSTIC
# =========================

async def fetch_btc_5m_for_v17(client, days):
    return await get_5m_candles_days(client, "BTCUSDT", days)


def btc_regime_at_v17(btc, t_ms):
    # Find last completed BTC candle before/at event time.
    lo, hi = 0, len(btc) - 1
    idx = None
    while lo <= hi:
        mid = (lo + hi) // 2
        if btc[mid]["close_time"] <= t_ms:
            idx = mid
            lo = mid + 1
        else:
            hi = mid - 1

    if idx is None or idx < 288:
        return None

    def r(bars):
        j = idx - bars
        if j < 0:
            return None
        return pct_change(btc[j]["close"], btc[idx]["close"])

    r1h = r(12)
    r4h = r(48)
    r24h = r(288)

    if r4h is None or r24h is None:
        return None

    if r4h > 0 and r24h > 0:
        trend = "BTC_BULL"
    elif r4h < 0 and r24h < 0:
        trend = "BTC_BEAR"
    else:
        trend = "BTC_MIXED"

    return {
        "btc_1h_pct": r1h,
        "btc_4h_pct": r4h,
        "btc_24h_pct": r24h,
        "btc_trend": trend,
    }


def summarize_v17(events):
    vals = [e["net_120m_pct"] for e in events]
    if not vals:
        return {"n": 0, "mean_net_pct": None, "median_net_pct": None,
                "win_rate_pct": None, "profit_factor": None}
    wins = [x for x in vals if x > 0]
    losses = [x for x in vals if x < 0]
    gp = sum(wins)
    gl = abs(sum(losses))
    return {
        "n": len(vals),
        "mean_net_pct": round(mean(vals), 4),
        "median_net_pct": round(safe_median_v10(vals), 4),
        "win_rate_pct": round(100 * len(wins) / len(vals), 2),
        "profit_factor": round(gp / gl, 4) if gl > 0 else None,
    }


@app.get("/v17-regime")
async def v17_regime(
    count: int = Query(default=10, ge=10, le=20),
    days: int = Query(default=180, ge=90, le=180),
):
    """
    Diagnostic only. V16 CONTINUED_UP entry rule remains frozen.
    V17 does NOT filter trades. It labels them by market regime.
    """
    try:
        async with httpx.AsyncClient(timeout=httpx.Timeout(300.0)) as client:
            universe_data = await build_universe(client)
            selected = universe_data[:count]
            semaphore = asyncio.Semaphore(2)

            async def worker(item):
                async with semaphore:
                    try:
                        candles = await get_5m_candles_days(client, item["symbol"], days)
                        return {"ok": True, "symbol": item["symbol"],
                                "events": relative_candidates_v15(candles, item["symbol"])}
                    except Exception as e:
                        return {"ok": False, "symbol": item["symbol"], "error": str(e)}

            coin_results, btc = await asyncio.gather(
                asyncio.gather(*[worker(x) for x in selected]),
                fetch_btc_5m_for_v17(client, days),
            )

        successful = [x for x in coin_results if x.get("ok")]
        failed = [x for x in coin_results if not x.get("ok")]

        raw = []
        for item in successful:
            raw.extend(item["events"])

        # Same cross-sectional ranking as V12-V16.
        by_time = {}
        for e in raw:
            by_time.setdefault(e["signal_time_ms"], []).append(e)

        ranked = []
        for group in by_time.values():
            if len(group) < 5:
                continue
            ordered = sorted(group, key=lambda x: x["relative_momentum_z"])
            n = len(ordered)
            for idx, e in enumerate(ordered):
                row = dict(e)
                row["cross_section_percentile"] = idx / (n - 1) if n > 1 else 1.0
                ranked.append(row)

        ranked.sort(key=lambda e: (e["symbol"], e["entry_open_time"]))
        cooled, last_by_symbol = [], {}
        for e in ranked:
            t = e["entry_open_time"]
            last = last_by_symbol.get(e["symbol"])
            if last is None or t - last >= 60 * 60 * 1000:
                cooled.append(e)
                last_by_symbol[e["symbol"]] = t

        # Frozen V16 primary candidate. No regime filter.
        events = [
            dict(e) for e in cooled
            if e["relative_momentum_z"] >= 1.0
            and e["cross_section_percentile"] >= 0.80
            and e["behavior"] == "CONTINUED_UP"
        ]

        # Cross-sectional breadth at each signal time:
        # fraction of available coins with positive 30m return.
        raw_by_time = {}
        for e in raw:
            raw_by_time.setdefault(e["signal_time_ms"], []).append(e)

        for e in events:
            peers = raw_by_time.get(e["signal_time_ms"], [])
            if peers:
                positive = sum(1 for x in peers if x["momentum_30m_pct"] > 0)
                e["alt_breadth_positive_pct"] = 100 * positive / len(peers)
                e["alt_market_mean_30m_pct"] = mean(
                    [x["momentum_30m_pct"] for x in peers]
                )
            else:
                e["alt_breadth_positive_pct"] = None
                e["alt_market_mean_30m_pct"] = None

            reg = btc_regime_at_v17(btc, e["signal_time_ms"])
            if reg:
                e.update(reg)
            else:
                e["btc_trend"] = "UNKNOWN"

        # Fixed descriptive regime buckets; no filtering.
        btc_groups = {}
        for label in ("BTC_BULL", "BTC_MIXED", "BTC_BEAR", "UNKNOWN"):
            btc_groups[label] = summarize_v17(
                [e for e in events if e.get("btc_trend") == label]
            )

        breadth_groups = {
            "BREADTH_LT_40": summarize_v17([
                e for e in events
                if e.get("alt_breadth_positive_pct") is not None
                and e["alt_breadth_positive_pct"] < 40
            ]),
            "BREADTH_40_TO_60": summarize_v17([
                e for e in events
                if e.get("alt_breadth_positive_pct") is not None
                and 40 <= e["alt_breadth_positive_pct"] < 60
            ]),
            "BREADTH_GE_60": summarize_v17([
                e for e in events
                if e.get("alt_breadth_positive_pct") is not None
                and e["alt_breadth_positive_pct"] >= 60
            ]),
        }

        alt_momentum_groups = {
            "ALT_MEAN_30M_LE_0": summarize_v17([
                e for e in events
                if e.get("alt_market_mean_30m_pct") is not None
                and e["alt_market_mean_30m_pct"] <= 0
            ]),
            "ALT_MEAN_30M_0_TO_0_5": summarize_v17([
                e for e in events
                if e.get("alt_market_mean_30m_pct") is not None
                and 0 < e["alt_market_mean_30m_pct"] < 0.5
            ]),
            "ALT_MEAN_30M_GE_0_5": summarize_v17([
                e for e in events
                if e.get("alt_market_mean_30m_pct") is not None
                and e["alt_market_mean_30m_pct"] >= 0.5
            ]),
        }

        # Chronological thirds, useful for checking whether a regime effect persists.
        ev = sorted(events, key=lambda e: e["entry_open_time"])
        n = len(ev)
        a, b = n // 3, (2 * n) // 3
        thirds = {
            "T1_OLDEST": summarize_v17(ev[:a]),
            "T2_MIDDLE": summarize_v17(ev[a:b]),
            "T3_NEWEST": summarize_v17(ev[b:]),
        }

        return {
            **MODE_INFO,
            "status": "OK",
            "signal": False,
            "study": "V17_MARKET_REGIME_DIAGNOSTIC",
            "days": days,
            "selected_coin_count": len(selected),
            "successful_coin_count": len(successful),
            "failed_coin_count": len(failed),
            "frozen_entry_rule": {
                "relative_momentum_z_min": 1.0,
                "cross_section_percentile_min": 0.80,
                "behavior": "CONTINUED_UP",
                "continued_up_definition": "wait_end_change >= +0.75%",
                "observation_wait_minutes": 60,
                "entry": "OPEN_AT_END_OF_60M_OBSERVATION_WINDOW",
                "hold_minutes": 120,
                "round_trip_cost_pct": ROUND_TRIP_COST_PCT,
            },
            "overall": summarize_v17(events),
            "btc_regime": btc_groups,
            "alt_breadth_regime": breadth_groups,
            "alt_market_30m_regime": alt_momentum_groups,
            "chronological_thirds": thirds,
            "event_count": len(events),
            "failed": failed,
            "generated_utc": utc_now(),
            "interpretation_note": (
                "V17 is diagnostic only. It does not use BTC trend, alt breadth, "
                "or alt-market momentum to accept/reject trades. Look for regime "
                "effects that are economically meaningful and sufficiently sampled. "
                "Do not promote a regime from a single favorable bucket."
            ),
        }

    except Exception as e:
        return {
            **MODE_INFO, "status": "ERROR", "signal": False,
            "error": str(e), "generated_utc": utc_now()
        }

# =========================
# V18 REGIME VALIDATION
# =========================

def split_thirds_v18(events):
    ev = sorted(events, key=lambda e: e["entry_open_time"])
    n = len(ev)
    a, b = n // 3, (2 * n) // 3
    return {
        "ALL": summarize_v17(ev),
        "T1_OLDEST": summarize_v17(ev[:a]),
        "T2_MIDDLE": summarize_v17(ev[a:b]),
        "T3_NEWEST": summarize_v17(ev[b:]),
    }


@app.get("/v18-regime-validate")
async def v18_regime_validate(
    count: int = Query(default=10, ge=10, le=20),
    days: int = Query(default=180, ge=90, le=180),
):
    """
    V18: frozen continuation signal + predefined regime validation.
    No trading. No regime threshold optimization.

    Important fix vs V17:
    true alt breadth is calculated from ALL selected coins' completed
    30m returns at each timestamp, not from positive-momentum candidates.
    """
    try:
        async with httpx.AsyncClient(timeout=httpx.Timeout(300.0)) as client:
            universe_data = await build_universe(client)
            selected = universe_data[:count]
            semaphore = asyncio.Semaphore(2)

            async def worker(item):
                async with semaphore:
                    try:
                        candles = await get_5m_candles_days(
                            client, item["symbol"], days
                        )
                        return {
                            "ok": True,
                            "symbol": item["symbol"],
                            "candles": candles,
                            "events": relative_candidates_v15(
                                candles, item["symbol"]
                            ),
                        }
                    except Exception as e:
                        return {
                            "ok": False,
                            "symbol": item["symbol"],
                            "error": str(e),
                        }

            coin_results, btc = await asyncio.gather(
                asyncio.gather(*[worker(x) for x in selected]),
                fetch_btc_5m_for_v17(client, days),
            )

        successful = [x for x in coin_results if x.get("ok")]
        failed = [x for x in coin_results if not x.get("ok")]

        raw = []
        for item in successful:
            raw.extend(item["events"])

        # Same frozen cross-sectional relative-momentum ranking.
        by_time = {}
        for e in raw:
            by_time.setdefault(e["signal_time_ms"], []).append(e)

        ranked = []
        for group in by_time.values():
            if len(group) < 5:
                continue
            ordered = sorted(group, key=lambda x: x["relative_momentum_z"])
            n = len(ordered)
            for idx, e in enumerate(ordered):
                row = dict(e)
                row["cross_section_percentile"] = (
                    idx / (n - 1) if n > 1 else 1.0
                )
                ranked.append(row)

        ranked.sort(key=lambda e: (e["symbol"], e["entry_open_time"]))
        cooled, last_by_symbol = [], {}
        for e in ranked:
            t = e["entry_open_time"]
            last = last_by_symbol.get(e["symbol"])
            if last is None or t - last >= 60 * 60 * 1000:
                cooled.append(e)
                last_by_symbol[e["symbol"]] = t

        events = [
            dict(e) for e in cooled
            if e["relative_momentum_z"] >= 1.0
            and e["cross_section_percentile"] >= 0.80
            and e["behavior"] == "CONTINUED_UP"
        ]

        # Build TRUE market snapshots from all successfully fetched coins.
        # Keyed by completed 5m candle close_time.
        snapshots = {}
        for item in successful:
            candles = item["candles"]
            for i in range(6, len(candles)):
                t = candles[i]["close_time"]
                mom30 = pct_change(candles[i - 6]["close"], candles[i]["close"])
                snapshots.setdefault(t, []).append(mom30)

        for e in events:
            vals = snapshots.get(e["signal_time_ms"], [])
            if vals:
                e["true_alt_breadth_positive_pct"] = (
                    100 * sum(1 for x in vals if x > 0) / len(vals)
                )
                e["true_alt_market_mean_30m_pct"] = mean(vals)
                e["breadth_coin_count"] = len(vals)
            else:
                e["true_alt_breadth_positive_pct"] = None
                e["true_alt_market_mean_30m_pct"] = None
                e["breadth_coin_count"] = 0

            reg = btc_regime_at_v17(btc, e["signal_time_ms"])
            if reg:
                e.update(reg)
            else:
                e["btc_trend"] = "UNKNOWN"

        # Predefined diagnostic/validation groups. No threshold search.
        groups = {
            "BTC_BULL": [
                e for e in events if e.get("btc_trend") == "BTC_BULL"
            ],
            "ALT_MEAN_30M_GE_0_5": [
                e for e in events
                if e.get("true_alt_market_mean_30m_pct") is not None
                and e["true_alt_market_mean_30m_pct"] >= 0.5
            ],
            "BTC_BULL_AND_ALT_GE_0_5": [
                e for e in events
                if e.get("btc_trend") == "BTC_BULL"
                and e.get("true_alt_market_mean_30m_pct") is not None
                and e["true_alt_market_mean_30m_pct"] >= 0.5
            ],
            "BTC_BULL_AND_ALT_LT_0_5": [
                e for e in events
                if e.get("btc_trend") == "BTC_BULL"
                and e.get("true_alt_market_mean_30m_pct") is not None
                and e["true_alt_market_mean_30m_pct"] < 0.5
            ],
        }

        true_breadth_groups = {
            "TRUE_BREADTH_LT_40": [
                e for e in events
                if e.get("true_alt_breadth_positive_pct") is not None
                and e["true_alt_breadth_positive_pct"] < 40
            ],
            "TRUE_BREADTH_40_TO_60": [
                e for e in events
                if e.get("true_alt_breadth_positive_pct") is not None
                and 40 <= e["true_alt_breadth_positive_pct"] < 60
            ],
            "TRUE_BREADTH_GE_60": [
                e for e in events
                if e.get("true_alt_breadth_positive_pct") is not None
                and e["true_alt_breadth_positive_pct"] >= 60
            ],
        }

        return {
            **MODE_INFO,
            "status": "OK",
            "signal": False,
            "study": "V18_FROZEN_REGIME_VALIDATION",
            "days": days,
            "selected_coin_count": len(selected),
            "successful_coin_count": len(successful),
            "failed_coin_count": len(failed),
            "frozen_entry_rule": {
                "relative_momentum_z_min": 1.0,
                "cross_section_percentile_min": 0.80,
                "behavior": "CONTINUED_UP",
                "continued_up_definition": "wait_end_change >= +0.75%",
                "observation_wait_minutes": 60,
                "entry": "OPEN_AT_END_OF_60M_OBSERVATION_WINDOW",
                "hold_minutes": 120,
                "round_trip_cost_pct": ROUND_TRIP_COST_PCT,
            },
            "v17_breadth_bug_fixed": True,
            "breadth_definition": (
                "percentage of ALL successfully fetched selected coins whose "
                "completed 30m return is > 0 at the signal timestamp"
            ),
            "overall": split_thirds_v18(events),
            "predefined_regime_groups": {
                name: split_thirds_v18(arr)
                for name, arr in groups.items()
            },
            "true_alt_breadth_groups": {
                name: split_thirds_v18(arr)
                for name, arr in true_breadth_groups.items()
            },
            "event_count": len(events),
            "failed": failed,
            "generated_utc": utc_now(),
            "interpretation_note": (
                "V18 validates predefined V17 regime observations without "
                "changing the frozen entry rule. BTC_BULL is especially useful "
                "only if direction is reasonably consistent across T1/T2/T3. "
                "The combined BTC+ALT group is descriptive and must not be "
                "promoted merely because one subgroup is strong. True breadth "
                "is now computed from all selected coins, fixing V17."
            ),
        }

    except Exception as e:
        return {
            **MODE_INFO,
            "status": "ERROR",
            "signal": False,
            "error": str(e),
            "generated_utc": utc_now(),
        }

# =========================
# V18 LIGHT â€” RENDER SAFE
# =========================
@app.get("/v18-light")
async def v18_light():
    """
    Same frozen V18 logic, reduced workload for Render Free.
    10 coins x 90 days. No strategy threshold changes.
    """
    return await v18_regime_validate(count=10, days=90)

# =========================
# V19 FROZEN REGIME BLOCK VALIDATION
# =========================

def block_summary_v19(events, block_days=30):
    """
    Calendar-style sequential blocks based on event timestamps.
    No thresholds are learned from these blocks.
    """
    if not events:
        return []

    ev = sorted(events, key=lambda e: e["entry_open_time"])
    start_ms = ev[0]["entry_open_time"]
    block_ms = block_days * 24 * 60 * 60 * 1000
    groups = {}

    for e in ev:
        k = int((e["entry_open_time"] - start_ms) // block_ms)
        groups.setdefault(k, []).append(e)

    rows = []
    for k in sorted(groups):
        arr = groups[k]
        rows.append({
            "block": k + 1,
            "block_days": block_days,
            "n": len(arr),
            **summarize_v17(arr),
        })
    return rows


@app.get("/v19-frozen-blocks")
async def v19_frozen_blocks(
    count: int = Query(default=10, ge=10, le=20),
    days: int = Query(default=90, ge=90, le=180),
):
    """
    V19: no optimization.

    Frozen candidate A:
      V16 CONTINUED_UP + BTC_BULL.

    Frozen candidate B:
      Candidate A + true selected-alt mean 30m return < +0.5%.

    Reports sequential 30-day blocks so the 90-day Render-safe run
    yields approximately three independent time blocks.
    """
    try:
        async with httpx.AsyncClient(timeout=httpx.Timeout(300.0)) as client:
            universe_data = await build_universe(client)
            selected = universe_data[:count]
            semaphore = asyncio.Semaphore(2)

            async def worker(item):
                async with semaphore:
                    try:
                        candles = await get_5m_candles_days(
                            client, item["symbol"], days
                        )
                        return {
                            "ok": True,
                            "symbol": item["symbol"],
                            "candles": candles,
                            "events": relative_candidates_v15(
                                candles, item["symbol"]
                            ),
                        }
                    except Exception as e:
                        return {
                            "ok": False,
                            "symbol": item["symbol"],
                            "error": str(e),
                        }

            coin_results, btc = await asyncio.gather(
                asyncio.gather(*[worker(x) for x in selected]),
                fetch_btc_5m_for_v17(client, days),
            )

        successful = [x for x in coin_results if x.get("ok")]
        failed = [x for x in coin_results if not x.get("ok")]

        raw = []
        for item in successful:
            raw.extend(item["events"])

        # Frozen relative-momentum ranking.
        by_time = {}
        for e in raw:
            by_time.setdefault(e["signal_time_ms"], []).append(e)

        ranked = []
        for group in by_time.values():
            if len(group) < 5:
                continue
            ordered = sorted(group, key=lambda x: x["relative_momentum_z"])
            n = len(ordered)
            for idx, e in enumerate(ordered):
                row = dict(e)
                row["cross_section_percentile"] = (
                    idx / (n - 1) if n > 1 else 1.0
                )
                ranked.append(row)

        # Frozen 60m same-symbol signal cooldown.
        ranked.sort(key=lambda e: (e["symbol"], e["entry_open_time"]))
        cooled = []
        last_by_symbol = {}
        for e in ranked:
            t = e["entry_open_time"]
            last = last_by_symbol.get(e["symbol"])
            if last is None or t - last >= 60 * 60 * 1000:
                cooled.append(e)
                last_by_symbol[e["symbol"]] = t

        base = [
            dict(e) for e in cooled
            if e["relative_momentum_z"] >= 1.0
            and e["cross_section_percentile"] >= 0.80
            and e["behavior"] == "CONTINUED_UP"
        ]

        # True market snapshot from all selected successfully fetched coins.
        snapshots = {}
        for item in successful:
            candles = item["candles"]
            for i in range(6, len(candles)):
                t = candles[i]["close_time"]
                mom30 = pct_change(
                    candles[i - 6]["close"], candles[i]["close"]
                )
                snapshots.setdefault(t, []).append(mom30)

        for e in base:
            vals = snapshots.get(e["signal_time_ms"], [])
            e["true_alt_market_mean_30m_pct"] = (
                mean(vals) if vals else None
            )
            reg = btc_regime_at_v17(btc, e["signal_time_ms"])
            if reg:
                e.update(reg)
            else:
                e["btc_trend"] = "UNKNOWN"

        candidate_a = [
            e for e in base if e.get("btc_trend") == "BTC_BULL"
        ]
        candidate_b = [
            e for e in candidate_a
            if e.get("true_alt_market_mean_30m_pct") is not None
            and e["true_alt_market_mean_30m_pct"] < 0.5
        ]

        def package(arr):
            return {
                "all": summarize_v17(arr),
                "sequential_30d_blocks": block_summary_v19(arr, 30),
            }

        return {
            **MODE_INFO,
            "status": "OK",
            "signal": False,
            "study": "V19_FROZEN_REGIME_BLOCK_VALIDATION",
            "days": days,
            "selected_coin_count": len(selected),
            "successful_coin_count": len(successful),
            "failed_coin_count": len(failed),
            "frozen_base_entry_rule": {
                "relative_momentum_z_min": 1.0,
                "cross_section_percentile_min": 0.80,
                "behavior": "CONTINUED_UP",
                "continued_up_definition": "wait_end_change >= +0.75%",
                "observation_wait_minutes": 60,
                "entry": "OPEN_AT_END_OF_60M_OBSERVATION_WINDOW",
                "hold_minutes": 120,
                "round_trip_cost_pct": ROUND_TRIP_COST_PCT,
            },
            "candidate_A_BTC_BULL": {
                "definition": (
                    "base rule + BTC 4h return > 0 and BTC 24h return > 0"
                ),
                "results": package(candidate_a),
            },
            "candidate_B_BTC_BULL_ALT_LT_0_5": {
                "definition": (
                    "candidate A + true selected-alt mean 30m return < +0.5%"
                ),
                "results": package(candidate_b),
            },
            "base_event_count_before_regime": len(base),
            "failed": failed,
            "generated_utc": utc_now(),
            "interpretation_note": (
                "V19 does not optimize thresholds. Candidate A and B are frozen "
                "from V18. Evaluate whether PF/mean remain directionally stable "
                "across sequential 30-day blocks. Small blocks should be treated "
                "as descriptive, not as proof of an edge."
            ),
        }

    except Exception as e:
        return {
            **MODE_INFO,
            "status": "ERROR",
            "signal": False,
            "error": str(e),
            "generated_utc": utc_now(),
        }

# =========================
# V20 FROZEN FORWARD PAPER
# =========================
# Research/paper only. No exchange order endpoint exists here.
# State is in-memory for this first forward-paper version; Render restart
# resets it. We will add persistent DB only after endpoint behavior is verified.

V20_STATE = {
    "open": {},
    "closed": [],
    "seen_signal_keys": set(),
    "started_utc": utc_now(),
}

V20_COST_PCT = 0.15
V20_HOLD_MS = 120 * 60 * 1000



# ============================================================
# V42 OPERASYONEL YARDIMCILAR (paper-only)
# ============================================================
SPREAD_MAX_PCT = float(os.getenv("SPREAD_MAX_PCT", "0.30"))
USE_LIVE_ENTRY = os.getenv("USE_LIVE_ENTRY", "1") == "1"
HOLD_FROM_LIVE_ENTRY = os.getenv("HOLD_FROM_LIVE_ENTRY", "1") == "1"
SHADOW_STOPS_PCT = (2.0, 3.0)
SEEN_KEY_MAX_AGE_MS = 7 * 24 * 60 * 60 * 1000


def alt_now_ms():
    return int(datetime.now(timezone.utc).timestamp() * 1000)


def alt_fmt_ms(ms):
    if not ms:
        return "-"
    return datetime.fromtimestamp(ms / 1000, tz=timezone.utc).strftime("%H:%M UTC")


async def v56_fetch_legacy_exit_candle(client, symbol, due_ms):
    """Fetch first completed 5m candle at/after due_ms, independent of universe membership."""
    try:
        raw = await get_json(
            client,
            "/api/v3/klines",
            params={
                "symbol": symbol,
                "interval": "5m",
                "startTime": int(due_ms),
                "limit": 3,
            },
        )
        now_ms = alt_now_ms()
        for k in raw:
            open_time = int(k[0])
            close_time = int(k[6])
            if open_time >= int(due_ms) and close_time < now_ms:
                return {
                    "open_time": open_time,
                    "open": float(k[1]),
                    "high": float(k[2]),
                    "low": float(k[3]),
                    "close": float(k[4]),
                    "volume": float(k[5]),
                    "close_time": close_time,
                }
    except Exception:
        return None
    return None


async def live_quote(client, symbol):
    """bookTicker: anlik bid/ask ve spread. Hata olursa None."""
    try:
        d = await get_json(client, "/api/v3/ticker/bookTicker", params={"symbol": symbol})
        bid = float(d["bidPrice"])
        ask = float(d["askPrice"])
        if bid <= 0 or ask <= 0:
            return None
        mid = (bid + ask) / 2.0
        return {"bid": bid, "ask": ask, "spread_pct": (ask - bid) / mid * 100.0}
    except Exception:
        return None


# =========================
# V43 LOW-LATENCY EXECUTION
# =========================
# Strategy thresholds are unchanged. This is an execution-quality guard:
# a signal detected too late is skipped instead of being entered many minutes
# after its theoretical checkpoint.
V43_EXECUTION_VERSION = "V43_LOW_LATENCY"
V43_MAX_ENTRY_DELAY_SECONDS = 120
V47_EXECUTION_VERSION = "V47_SEEN_KEY_FIX"
V47_STARTED_UTC = utc_now()
V47_SEEN_KEYS = {"V27": set(), "V32": set()}
V48_LAST_V32_RESULT = {"result": None, "captured_utc": None}
V51_V32_SCAN_HISTORY = []

# V96: bounded, read-only TOP3 snapshots. Never changes paper selection.
V96_TOP3_HISTORY = []
V96_TOP3_LAST_ERROR = None

def v96_record_top3(cohort, state):
    """Observe already-eligible candidates; never raise into the V32 scanner."""
    global V96_TOP3_LAST_ERROR
    try:
        for signal_ms, group in cohort.items():
            ranked_group = sorted(
                group,
                key=lambda e: (
                    e.get("wait_end_change_pct", 0.0),
                    e.get("relative_momentum_z", 0.0),
                    e.get("cross_section_percentile", 0.0),
                ),
                reverse=True,
            )
            candidates = []
            for rank, e in enumerate(ranked_group[:3], start=1):
                symbol = e["symbol"]
                entry_ms = int(e["entry_open_time"])
                key = f"{symbol}:{entry_ms}"
                prior = [
                    int(p.get("entry_open_time") or 0)
                    for p in state["closed"]
                    if p.get("symbol") == symbol
                ]
                candidates.append({
                    "rank": rank,
                    "symbol": symbol,
                    "key": key,
                    "entry_open_time": entry_ms,
                    "continuation_60m_pct": round(float(e["wait_end_change_pct"]), 4),
                    "relative_momentum_z": round(float(e["relative_momentum_z"]), 4),
                    "cross_section_percentile": round(float(e["cross_section_percentile"]), 4),
                    "seen_key": key in state["seen_signal_keys"],
                    "already_open": symbol in state["open"],
                    "cooldown_60m": bool(prior and entry_ms - max(prior) < 3600000),
                })
            record = {
                "captured_utc": utc_now(),
                "signal_time_ms": signal_ms,
                "eligible_in_cohort": len(group),
                "candidates": candidates,
            }
            V96_TOP3_HISTORY[:] = [
                row for row in V96_TOP3_HISTORY
                if row["signal_time_ms"] != signal_ms
            ]
            V96_TOP3_HISTORY.append(record)
            del V96_TOP3_HISTORY[:-100]
        V96_TOP3_LAST_ERROR = None
    except Exception as exc:
        V96_TOP3_LAST_ERROR = f"{type(exc).__name__}: {exc}"
        # Observation failures must never interrupt entry/exit/stop logic.

V43_STARTED_UTC = utc_now()

# =========================
# V55 RISK EXIT ENGINE
# =========================
# Entry strategy is unchanged. These are fixed prospective risk-management rules,
# not fitted/optimized from the current forward cohort.
V55_EXECUTION_VERSION = "V55_RISK_EXIT_ENGINE"
V55_HARD_STOP_PCT = 3.0
V55_TRAIL_ACTIVATE_PCT = 2.0
V55_TRAIL_DISTANCE_PCT = 1.5

async def apply_live_entry(client, pos, candle_open_price, hold_ms):
    """
    Paper girisini gercekte alinabilecek fiyata cevirir.
    Doner: (ok, reason). ok=False ise sinyal 'islenemez' (spread cok genis).
    """
    pos["candle_open_price"] = candle_open_price
    pos["entry_price"] = candle_open_price
    pos["live_entry"] = False
    pos["entry_live_ms"] = None

    q = await live_quote(client, pos["symbol"])
    if not q:
        return True, None  # canli fiyat alinamadi -> eski kurala geri don

    now_ms = alt_now_ms()
    pos["live_bid"] = q["bid"]
    pos["live_ask"] = q["ask"]
    pos["spread_pct"] = round(q["spread_pct"], 4)
    delay_reference_ms = int(pos.get("actionable_time_ms") or pos["entry_open_time"])
    pos["delay_reference_ms"] = delay_reference_ms
    pos["delay_reference"] = (
        "ACTIONABLE_AFTER_COMPLETED_WINDOW"
        if pos.get("actionable_time_ms")
        else "LEGACY_ENTRY_OPEN_TIME"
    )
    pos["entry_delay_seconds"] = round((now_ms - delay_reference_ms) / 1000.0)
    pos["entry_slippage_vs_candle_pct"] = round(pct_change(candle_open_price, q["ask"]), 4)
    pos["execution_version"] = "V50_ACTIONABLE_TIME"

    if pos["entry_delay_seconds"] > V43_MAX_ENTRY_DELAY_SECONDS:
        return False, (
            f"entry delay {pos['entry_delay_seconds']}s > "
            f"{V43_MAX_ENTRY_DELAY_SECONDS}s"
        )

    if q["spread_pct"] > SPREAD_MAX_PCT:
        return False, f"spread {q['spread_pct']:.3f}% > {SPREAD_MAX_PCT}%"

    if USE_LIVE_ENTRY:
        pos["entry_price"] = q["ask"]
        pos["live_entry"] = True
        pos["entry_live_ms"] = now_ms
        if HOLD_FROM_LIVE_ENTRY:
            pos["exit_due_time"] = max(int(pos["exit_due_time"]), now_ms + hold_ms)
    return True, None


def alt_log_skip(state, pos, reason):
    lst = state.setdefault("skipped", [])
    lst.append({
        "time_utc": utc_now(),
        "symbol": pos.get("symbol"),
        "reason": reason,
        "relative_momentum_z": pos.get("relative_momentum_z"),
    })
    del lst[:-200]


def shadow_stop_results(candles, closed, exit_candle, cost_pct):
    """
    GOLGE olcum: gercek cikisi degistirmez. 5m mum low'larina bakarak
    %2 / %3 stop olsaydi sonuc ne olurdu + MAE/MFE.
    Mum ici sira bilinmedigi icin stop dokunusu = stop gerceklesti (muhafazakar).
    """
    entry = float(closed["entry_price"])
    start_ms = int(closed.get("entry_live_ms") or closed["entry_open_time"])
    end_ms = int(exit_candle["open_time"])
    window = [c for c in candles if c["close_time"] > start_ms and c["open_time"] < end_ms]
    if not window or entry <= 0:
        return {}
    mae = min(pct_change(entry, c["low"]) for c in window)
    mfe = max(pct_change(entry, c["high"]) for c in window)
    out = {"mae_pct": round(mae, 4), "mfe_pct": round(mfe, 4)}
    base_net = float(closed["net_pct"])
    for stop in SHADOW_STOPS_PCT:
        hit = mae <= -stop
        key = str(stop).replace(".0", "")
        out[f"shadow_stop_{key}_hit"] = hit
        out[f"shadow_stop_{key}_net_pct"] = round((-stop - cost_pct) if hit else base_net, 4)
    return out


def prune_seen_keys(keys):
    """key = SYMBOL:entry_open_time_ms ; 7 gunden eskileri sil (yerinde)."""
    cutoff = alt_now_ms() - SEEN_KEY_MAX_AGE_MS
    for k in list(keys):
        try:
            if int(str(k).rsplit(":", 1)[1]) < cutoff:
                keys.discard(k)
        except Exception:
            pass
    return keys


async def alt_safe_send(msg, kind, symbol):
    try:
        res = await v22_telegram_send(msg)
        return {"type": kind, "symbol": symbol, **res}
    except Exception as e:
        return {"type": kind, "symbol": symbol, "sent": False, "error": str(e)}


def alt_entry_text(title, p, extra_lines, open_count):
    lines = [title, f"Coin: {p['symbol']}"]
    if p.get("live_entry"):
        lines.append(f"Canli giris (ask): {p['entry_price']}")
        lines.append(f"Mum acilisi (eski ref): {p.get('candle_open_price')} | fark: {p.get('entry_slippage_vs_candle_pct')}%")
    else:
        lines.append(f"Giris (mum acilisi, canli fiyat alinamadi): {p['entry_price']}")
    if p.get("spread_pct") is not None:
        lines.append(f"Spread: {p['spread_pct']}% | sinyal gecikmesi: {p.get('entry_delay_seconds')} sn")
    lines += extra_lines
    lines.append(f"Planli cikis: {alt_fmt_ms(p.get('exit_due_time'))} (120 dk)")
    lines.append(f"Acik paper pozisyon: {open_count}")
    lines.append("PAPER sinyal - gercek emir degildir.")
    return "\n".join(lines)


def alt_exit_text(title, p):
    lines = [
        title,
        f"Coin: {p['symbol']}",
        f"Giris: {p['entry_price']} | Cikis: {p['exit_price']}",
        f"Brut: {p['gross_pct']}% | Maliyet: {p['cost_pct']}% | NET: {p['net_pct']}%",
    ]
    if p.get("exit_reason"):
        lines.append(f"Neden: {p['exit_reason']}")
    if p.get("mae_pct") is not None:
        lines.append(f"En kotu: {p['mae_pct']}% | En iyi: {p['mfe_pct']}%")
        lines.append(f"%2 stop olsaydi: {p.get('shadow_stop_2_net_pct')}% | %3 stop olsaydi: {p.get('shadow_stop_3_net_pct')}%")
    lines.append("PAPER - gercek emir degildir.")
    return "\n".join(lines)


def alt_period_stats(closed, since_ms):
    rows = [x for x in closed if int(x.get("exit_open_time", 0)) >= since_ms]
    vals = [float(x.get("net_pct", 0.0)) for x in rows]
    if not vals:
        return {"count": 0}
    equity = 1.0
    for v in vals:
        equity *= 1.0 + v / 100.0
    return {
        "count": len(vals),
        "wins": sum(1 for v in vals if v > 0),
        "win_rate_pct": round(100.0 * sum(1 for v in vals if v > 0) / len(vals), 1),
        "mean_net_pct": round(mean(vals), 4),
        "compounded_pct": round((equity - 1.0) * 100.0, 4),
        "best_pct": round(max(vals), 4),
        "worst_pct": round(min(vals), 4),
    }


def v20_public_state():
    closed = V20_STATE["closed"]
    vals = [x["net_pct"] for x in closed]
    equity = 100.0
    peak = 100.0
    max_dd = 0.0
    for x in closed:
        equity *= (1.0 + x["net_pct"] / 100.0)
        peak = max(peak, equity)
        dd = (equity / peak - 1.0) * 100.0
        max_dd = min(max_dd, dd)

    return {
        "model": MODEL,
        "mode": "RESEARCH_PAPER_ONLY",
        "trading": False,
        "orders": False,
        "strategy": "V27_CANDIDATE_B_CLEAN_BROAD_ALTCOIN_FORWARD_PAPER",
        "started_utc": V20_STATE["started_utc"],
        "open_count": len(V20_STATE["open"]),
        "closed_count": len(closed),
        "closed_summary": summarize_v17([
            {"net_120m_pct": v} for v in vals
        ]) if vals else summarize_v17([]),
        "paper_equity_start": 100.0,
        "paper_equity": round(equity, 4),
        "paper_return_pct": round(equity - 100.0, 4),
        "max_drawdown_pct": round(max_dd, 4),
        "open_positions": list(V20_STATE["open"].values()),
        "recent_closed": closed[-20:][::-1],
    }



# =========================
# V44 FAST SHARED SNAPSHOT
# =========================
# The frozen V27/V32 strategy rules are unchanged.
# For live forward decisions we only need enough 5m history to compute:
# - 288-bar rolling Z history
# - the 30m return used by Z
# - the already-observed 60m continuation
# 400 completed 5m candles (~33h) safely cover that requirement.
# V27 and V32 share one snapshot so the same ~300 symbols are not downloaded twice.

V44_SNAPSHOT_CANDLES = 400
V44_SNAPSHOT_TTL_SECONDS = 10
V44_FETCH_CONCURRENCY = 60
_V44_SNAPSHOT_LOCK = None
_V44_SNAPSHOT = {
    "ts": 0.0,
    "good": None,
    "errors": [],
    "universe_count": 0,
    "fetch_seconds": None,
    "snapshot_utc": None,
}
V44_METRICS = {
    "v27_last_scan_seconds": None,
    "v27_last_scan_utc": None,
    "v32_last_scan_seconds": None,
    "v32_last_scan_utc": None,
}


async def v44_market_snapshot(client):
    import time as _t
    global _V44_SNAPSHOT_LOCK

    if _V44_SNAPSHOT_LOCK is None:
        _V44_SNAPSHOT_LOCK = asyncio.Lock()

    async with _V44_SNAPSHOT_LOCK:
        now = _t.time()
        if (
            _V44_SNAPSHOT["good"] is not None
            and now - _V44_SNAPSHOT["ts"] < V44_SNAPSHOT_TTL_SECONDS
        ):
            return (
                _V44_SNAPSHOT["good"],
                list(_V44_SNAPSHOT["errors"]),
                _V44_SNAPSHOT["universe_count"],
                True,
            )

        started = _t.perf_counter()
        universe = await build_universe(client)
        semaphore = asyncio.Semaphore(V44_FETCH_CONCURRENCY)

        async def fetch(item):
            async with semaphore:
                try:
                    candles = await get_completed_5m_candles(
                        client,
                        item["symbol"],
                        limit=V44_SNAPSHOT_CANDLES,
                    )
                    return item["symbol"], candles, None
                except Exception as e:
                    return item["symbol"], None, str(e)

        fetched = await asyncio.gather(*[fetch(x) for x in universe])
        good = {sym: c for sym, c, err in fetched if c}
        errors = [
            {"symbol": sym, "error": err}
            for sym, c, err in fetched
            if err
        ]

        # BTC is required by V27 even if universe filtering ever excludes it.
        if "BTCUSDT" not in good:
            try:
                good["BTCUSDT"] = await get_completed_5m_candles(
                    client, "BTCUSDT", limit=V44_SNAPSHOT_CANDLES
                )
            except Exception as e:
                errors.append({"symbol": "BTCUSDT", "error": str(e)})

        elapsed = _t.perf_counter() - started
        _V44_SNAPSHOT.update({
            "ts": _t.time(),
            "good": good,
            "errors": errors,
            "universe_count": len(universe),
            "fetch_seconds": round(elapsed, 3),
            "snapshot_utc": utc_now(),
        })
        return good, list(errors), len(universe), False


async def v20_scan_once():
    """
    Scan current completed candles for the exact frozen Candidate B.
    A signal is entered only once, at the current/next available 5m OPEN
    after the completed 60m continuation observation.
    """
    import time as _t
    scan_started = _t.perf_counter()
    async with httpx.AsyncClient(timeout=httpx.Timeout(120.0)) as client:
        good, errors, universe_count, snapshot_cache_hit = await v44_market_snapshot(client)

        btc = good.get("BTCUSDT")
        if btc is None:
            return {"status": "ERROR", "error": "BTC missing from V44 shared snapshot"}

        # Generate candidate observations for all selected alts.
        raw = []
        for sym, candles in good.items():
            if sym == "BTCUSDT":
                continue
            rows = relative_candidates_forward_live(candles, sym)
            if rows:
                raw.extend(rows)

        # Rank by same signal timestamp.
        by_time = {}
        for e in raw:
            by_time.setdefault(e["signal_time_ms"], []).append(e)

        ranked = []
        for group in by_time.values():
            if len(group) < 5:
                continue
            ordered = sorted(group, key=lambda x: x["relative_momentum_z"])
            n = len(ordered)
            for idx, e in enumerate(ordered):
                row = dict(e)
                row["cross_section_percentile"] = idx / (n - 1) if n > 1 else 1.0
                row["cohort_size"] = n
                ranked.append(row)

        # True selected-alt market mean at signal timestamp.
        snapshots = {}
        for sym, candles in good.items():
            if sym == "BTCUSDT":
                continue
            for i in range(6, len(candles)):
                t = candles[i]["close_time"]
                r30 = pct_change(candles[i - 6]["close"], candles[i]["close"])
                snapshots.setdefault(t, []).append(r30)

        now_candidates = []
        for e in ranked:
            if e["relative_momentum_z"] < 1.0:
                continue
            if e["cross_section_percentile"] < 0.80:
                continue
            if e["behavior"] != "CONTINUED_UP":
                continue

            reg = btc_regime_at_v17(btc, e["signal_time_ms"])
            if not reg or reg["btc_trend"] != "BTC_BULL":
                continue

            vals = snapshots.get(e["signal_time_ms"], [])
            if not vals:
                continue
            alt_mean = mean(vals)
            if alt_mean >= 0.5:
                continue

            row = dict(e)
            row["btc_4h_pct"] = reg["btc_4h_pct"]
            row["btc_24h_pct"] = reg["btc_24h_pct"]
            row["alt_market_mean_30m_pct"] = alt_mean
            now_candidates.append(row)

        # Only signals whose paper entry is very recent are eligible for forward entry.
        # This prevents historical rows from being inserted as if they were live.
        newest_open_ms = max(
            (c[-1]["open_time"] for c in good.values() if c), default=0
        )
        freshness_ms = 10 * 60 * 1000

        new_entries = []
        for e in now_candidates:
            if newest_open_ms - e["entry_open_time"] > freshness_ms:
                continue

            key = f'{e["symbol"]}:{e["entry_open_time"]}'
            if key in V20_STATE["seen_signal_keys"]:
                continue
            if e["symbol"] in V20_STATE["open"]:
                continue

            pos = {
                "key": key,
                "symbol": e["symbol"],
                "signal_time_ms": e["signal_time_ms"],
                "entry_open_time": e["entry_open_time"],
                "entry_price": None,
                "exit_due_time": e["entry_open_time"] + V20_HOLD_MS,
                "relative_momentum_z": round(e["relative_momentum_z"], 4),
                "cross_section_percentile": round(e["cross_section_percentile"], 4),
                "cohort_size": e.get("cohort_size"),
                "wait_end_change_pct": round(e["wait_end_change_pct"], 4),
                "btc_4h_pct": round(e["btc_4h_pct"], 4),
                "btc_24h_pct": round(e["btc_24h_pct"], 4),
                "alt_market_mean_30m_pct": round(e["alt_market_mean_30m_pct"], 4),
                "status": "OPEN_PAPER",
            }

            candles = good.get(e["symbol"], [])
            price = next(
                (c["open"] for c in candles if c["open_time"] == e["entry_open_time"]),
                None
            )
            if price is None:
                continue
            pos["entry_price"] = price

            ok_entry, skip_reason = await apply_live_entry(client, pos, price, V20_HOLD_MS)
            if not ok_entry:
                alt_log_skip(V20_STATE, pos, skip_reason)
                V20_STATE["seen_signal_keys"].add(key)
                continue

            V20_STATE["seen_signal_keys"].add(key)
            V20_STATE["open"][e["symbol"]] = pos
            new_entries.append(pos)

        # Close due paper positions using the first available 5m OPEN at/after due time.
        newly_closed = []
        for sym, pos in list(V20_STATE["open"].items()):
            candles = good.get(sym)
            if not candles:
                continue
            exit_candle = next(
                (c for c in candles if c["open_time"] >= pos["exit_due_time"]),
                None
            )
            if exit_candle is None:
                continue

            exit_price = exit_candle["open"]
            gross = pct_change(pos["entry_price"], exit_price)
            net = gross - V20_COST_PCT
            closed = {
                **pos,
                "status": "CLOSED_PAPER",
                "exit_open_time": exit_candle["open_time"],
                "exit_price": exit_price,
                "gross_pct": round(gross, 4),
                "cost_pct": V20_COST_PCT,
                "net_pct": round(net, 4),
            }
            closed.update(shadow_stop_results(candles, closed, exit_candle, V20_COST_PCT))
            V20_STATE["closed"].append(closed)
            del V20_STATE["open"][sym]
            newly_closed.append(closed)

        V44_METRICS["v27_last_scan_seconds"] = round(_t.perf_counter() - scan_started, 3)
        V44_METRICS["v27_last_scan_utc"] = utc_now()

        return {
            "status": "OK",
            "selected_symbols": list(good.keys()),
            "new_entries": new_entries,
            "newly_closed": newly_closed,
            "eligible_candidate_rows_seen": len(now_candidates),
            "fetch_errors": errors,
        }


@app.get("/v20-forward-scan")
async def v20_forward_scan():
    result = await v20_scan_once()
    return {
        "model": MODEL,
        "mode": "RESEARCH_PAPER_ONLY",
        "trading": False,
        "orders": False,
        "signal": bool(result.get("new_entries")),
        "scan": result,
        "paper": v20_public_state(),
        "generated_utc": utc_now(),
    }


@app.get("/v20-forward-status")
async def v20_forward_status():
    return {
        "status": "OK",
        "signal": False,
        "paper": v20_public_state(),
        "note": (
            "In-memory forward-paper state. Render restart resets this first "
            "verification version. No exchange orders are sent."
        ),
        "generated_utc": utc_now(),
    }


# =========================
# V21 AUTO + POSTGRES PERSISTENCE
# =========================
# Candidate B remains FROZEN. This section changes operations only:
# automatic scanning + durable paper state. It does NOT place orders.

V21_SCAN_INTERVAL_SECONDS = 60
V21_DB_URL = os.getenv("DATABASE_URL", "").strip()
V21_AUTO_TASK = None
V21_LAST_SCAN = {
    "status": "NOT_RUN",
    "started_utc": None,
    "finished_utc": None,
    "error": None,
}


def v21_db_connect():
    if not V21_DB_URL:
        return None
    import psycopg
    return psycopg.connect(V21_DB_URL)


def v21_init_db():
    if not V21_DB_URL:
        return False
    with v21_db_connect() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                CREATE TABLE IF NOT EXISTS alt_v21_paper_state (
                    id INTEGER PRIMARY KEY,
                    payload JSONB NOT NULL,
                    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
                )
            """)
        conn.commit()
    return True


def v21_serializable_state():
    return {
        "open": V20_STATE["open"],
        "closed": V20_STATE["closed"],
        "seen_signal_keys": sorted(prune_seen_keys(V20_STATE["seen_signal_keys"])),
        "started_utc": V20_STATE["started_utc"],
    }


def v21_save_state():
    if not V21_DB_URL:
        return False
    payload = json.dumps(v21_serializable_state())
    with v21_db_connect() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO alt_v21_paper_state (id, payload, updated_at)
                VALUES (1, %s::jsonb, NOW())
                ON CONFLICT (id) DO UPDATE
                SET payload = EXCLUDED.payload,
                    updated_at = NOW()
            """, (payload,))
        conn.commit()
    return True


def v21_load_state():
    if not V21_DB_URL:
        return False
    with v21_db_connect() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT payload FROM alt_v21_paper_state WHERE id = 1"
            )
            row = cur.fetchone()
    if not row:
        v21_save_state()
        return True

    payload = row[0]
    if isinstance(payload, str):
        payload = json.loads(payload)

    V20_STATE["open"] = payload.get("open", {})
    V20_STATE["closed"] = payload.get("closed", [])
    V20_STATE["seen_signal_keys"] = set(
        payload.get("seen_signal_keys", [])
    )
    V20_STATE["started_utc"] = payload.get(
        "started_utc", V20_STATE["started_utc"]
    )
    return True


async def v21_run_and_persist():
    V21_LAST_SCAN["status"] = "RUNNING"
    V21_LAST_SCAN["started_utc"] = utc_now()
    V21_LAST_SCAN["error"] = None
    try:
        result = await v20_scan_once()
        if V21_DB_URL:
            v21_save_state()
        V21_LAST_SCAN["status"] = result.get("status", "OK")
        return result
    except Exception as e:
        V21_LAST_SCAN["status"] = "ERROR"
        V21_LAST_SCAN["error"] = str(e)
        return {"status": "ERROR", "error": str(e)}
    finally:
        V21_LAST_SCAN["finished_utc"] = utc_now()


async def v21_auto_loop():
    # Small delay lets the web service become healthy first.
    await asyncio.sleep(15)
    while True:
        await v21_run_and_persist()
        await asyncio.sleep(V21_SCAN_INTERVAL_SECONDS)


@app.on_event("startup")
async def v21_startup():
    global V21_AUTO_TASK
    try:
        if V21_DB_URL:
            v21_init_db()
            v21_load_state()
    except Exception as e:
        V21_LAST_SCAN["status"] = "DB_STARTUP_ERROR"
        V21_LAST_SCAN["error"] = str(e)

    if V21_AUTO_TASK is None or V21_AUTO_TASK.done():
        V21_AUTO_TASK = asyncio.create_task(v21_auto_loop())


@app.on_event("shutdown")
async def v21_shutdown():
    global V21_AUTO_TASK
    try:
        if V21_DB_URL:
            v21_save_state()
    except Exception:
        pass
    if V21_AUTO_TASK is not None:
        V21_AUTO_TASK.cancel()


@app.get("/v21-status")
async def v21_status():
    return {
        "model": MODEL,
        "mode": "RESEARCH_PAPER_ONLY",
        "trading": False,
        "orders": False,
        "strategy": "V27_CANDIDATE_B_CLEAN_BROAD_ALTCOIN_FORWARD_PAPER",
        "strategy_changed": True,
        "strategy_change": "Universe broadened further by lowering the 24h quote-volume floor from 5M USDT to 1M USDT; Candidate B signal thresholds unchanged.",
        "automation": {
            "enabled": True,
            "scan_interval_seconds": V21_SCAN_INTERVAL_SECONDS,
            "last_scan": V21_LAST_SCAN,
        },
        "persistence": {
            "database_configured": bool(V21_DB_URL),
            "backend": "POSTGRESQL" if V21_DB_URL else "MEMORY_ONLY",
        },
        "paper": v20_public_state(),
        "generated_utc": utc_now(),
    }


@app.get("/v21-scan-now")
async def v21_scan_now():
    result = await v21_run_and_persist()
    return {
        "model": MODEL,
        "mode": "RESEARCH_PAPER_ONLY",
        "trading": False,
        "orders": False,
        "signal": bool(result.get("new_entries")),
        "scan": result,
        "paper": v20_public_state(),
        "database_configured": bool(V21_DB_URL),
        "generated_utc": utc_now(),
    }


# =========================
# V22 TELEGRAM NOTIFICATIONS
# =========================
# Operational change only. Frozen Candidate B strategy is unchanged.
# Secrets are read from Render environment variables.

V22_TG_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "").strip()
V22_TG_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "").strip()


async def v22_telegram_send(message: str):
    if not V22_TG_TOKEN or not V22_TG_CHAT_ID:
        return {"sent": False, "reason": "telegram_not_configured"}

    url = f"https://api.telegram.org/bot{V22_TG_TOKEN}/sendMessage"
    async with httpx.AsyncClient(timeout=20.0) as client:
        r = await client.post(
            url,
            json={
                "chat_id": V22_TG_CHAT_ID,
                "text": message,
                "disable_web_page_preview": True,
            },
        )
        r.raise_for_status()
    return {"sent": True}


async def v22_run_notify_and_persist():
    V21_LAST_SCAN["status"] = "RUNNING"
    V21_LAST_SCAN["started_utc"] = utc_now()
    V21_LAST_SCAN["error"] = None

    try:
        result = await v20_scan_once()

        # V42: once state kaydi, sonra bildirim (Telegram hatasi state'i bozamaz).
        if V21_DB_URL:
            try:
                v21_save_state()
            except Exception as e:
                result["db_save_error"] = str(e)

        notifications = []
        open_count = len(V20_STATE["open"])

        for p in result.get("new_entries", []):
            msg = alt_entry_text(
                "ALT V27 PAPER GIRIS",
                p,
                [
                    f"Z: {p['relative_momentum_z']} | yuzdelik: {p['cross_section_percentile']} (kohort: {p.get('cohort_size')})",
                    f"60dk devam: {p['wait_end_change_pct']}%",
                    f"BTC 4s: {p['btc_4h_pct']}% | BTC 24s: {p['btc_24h_pct']}%",
                    f"ALT ort 30dk: {p['alt_market_mean_30m_pct']}%",
                ],
                open_count,
            )
            notifications.append(await alt_safe_send(msg, "ENTRY", p["symbol"]))

        for p in result.get("newly_closed", []):
            msg = alt_exit_text("ALT V27 PAPER CIKIS", p)
            notifications.append(await alt_safe_send(msg, "EXIT", p["symbol"]))

        V21_LAST_SCAN["status"] = result.get("status", "OK")
        result["telegram_notifications"] = notifications
        return result

    except Exception as e:
        V21_LAST_SCAN["status"] = "ERROR"
        V21_LAST_SCAN["error"] = str(e)
        return {"status": "ERROR", "error": str(e)}
    finally:
        V21_LAST_SCAN["finished_utc"] = utc_now()


async def v22_auto_loop():
    await asyncio.sleep(15)
    while True:
        await v22_run_notify_and_persist()
        await asyncio.sleep(V21_SCAN_INTERVAL_SECONDS)


# Replace the V21 startup task with the V22 notification-aware loop.
@app.on_event("startup")
async def v22_startup():
    global V21_AUTO_TASK
    # V21 startup may already have created its loop. Cancel it so there is
    # exactly one automatic scanner.
    if V21_AUTO_TASK is not None and not V21_AUTO_TASK.done():
        V21_AUTO_TASK.cancel()

    try:
        if V21_DB_URL:
            v21_init_db()
            v21_load_state()
    except Exception as e:
        V21_LAST_SCAN["status"] = "DB_STARTUP_ERROR"
        V21_LAST_SCAN["error"] = str(e)

    V21_AUTO_TASK = asyncio.create_task(v22_auto_loop())


@app.get("/v22-status")
async def v22_status():
    return {
        "model": MODEL,
        "mode": "RESEARCH_PAPER_ONLY",
        "trading": False,
        "orders": False,
        "strategy": "V27_CANDIDATE_B_CLEAN_BROAD_ALTCOIN_FORWARD_PAPER",
        "strategy_changed": True,
        "strategy_change": "Universe broadened further by lowering the 24h quote-volume floor from 5M USDT to 1M USDT; Candidate B signal thresholds unchanged.",
        "automation": {
            "enabled": True,
            "scan_interval_seconds": V21_SCAN_INTERVAL_SECONDS,
            "last_scan": V21_LAST_SCAN,
        },
        "persistence": {
            "database_configured": bool(V21_DB_URL),
            "backend": "POSTGRESQL" if V21_DB_URL else "MEMORY_ONLY",
        },
        "telegram": {
            "configured": bool(V22_TG_TOKEN and V22_TG_CHAT_ID),
            "entry_notifications": True,
            "exit_notifications": True,
        },
        "paper": v20_public_state(),
        "generated_utc": utc_now(),
    }


@app.get("/v22-telegram-test")
async def v22_telegram_test():
    result = await v22_telegram_send(
        "ALT Momentum V22 test OK\n"
        "Forward paper notifications are connected.\n"
        "Trading: FALSE | Orders: FALSE"
    )
    return {
        "status": "OK" if result.get("sent") else "NOT_CONFIGURED",
        "telegram": result,
        "trading": False,
        "orders": False,
        "generated_utc": utc_now(),
    }


@app.get("/v22-scan-now")
async def v22_scan_now():
    result = await v22_run_notify_and_persist()
    return {
        "model": MODEL,
        "mode": "RESEARCH_PAPER_ONLY",
        "trading": False,
        "orders": False,
        "signal": bool(result.get("new_entries")),
        "scan": result,
        "paper": v20_public_state(),
        "generated_utc": utc_now(),
    }


@app.get("/v23-status")
async def v23_status():
    return {
        "model": MODEL,
        "mode": "RESEARCH_PAPER_ONLY",
        "trading": False,
        "orders": False,
        "strategy": "V27_CANDIDATE_B_CLEAN_BROAD_ALTCOIN_FORWARD_PAPER",
        "strategy_changed": True,
        "strategy_change": (
            "Universe expanded from 10 coins to all eligible dynamic Binance "
            "USDT spot coins. Candidate B thresholds are unchanged, but "
            "cross-sectional Top 20% ranking and ALT market mean are now "
            "computed over the expanded universe."
        ),
        "universe": {
            "mode": "ALL_ELIGIBLE_DYNAMIC",
            "fixed_coin_count": False,
            "note": (
                "Existing build_universe liquidity/safety exclusions remain; "
                "there is no top-10 cap."
            ),
        },
        "automation": {
            "enabled": True,
            "scan_interval_seconds": V21_SCAN_INTERVAL_SECONDS,
            "last_scan": V21_LAST_SCAN,
        },
        "persistence": {
            "database_configured": bool(V21_DB_URL),
            "backend": "POSTGRESQL" if V21_DB_URL else "MEMORY_ONLY",
        },
        "telegram": {
            "configured": bool(V22_TG_TOKEN and V22_TG_CHAT_ID),
            "entry_notifications": True,
            "exit_notifications": True,
        },
        "paper": v20_public_state(),
        "generated_utc": utc_now(),
    }



@app.get("/v24-status")
async def v24_status():
    return {
        "model": MODEL,
        "mode": "RESEARCH_PAPER_ONLY",
        "trading": False,
        "orders": False,
        "strategy": "V27_CANDIDATE_B_CLEAN_BROAD_ALTCOIN_FORWARD_PAPER",
        "strategy_changed": True,
        "strategy_change": (
            "24h quote-volume floor lowered from 5M to 1M USDT. "
            "Candidate B signal thresholds are unchanged."
        ),
        "universe": {
            "mode": "BROAD_DYNAMIC",
            "min_quote_volume_usdt_24h": MIN_QUOTE_VOLUME_USDT,
            "fixed_coin_count": False,
            "note": (
                "All Binance USDT spot symbols passing the existing eligibility "
                "exclusions and >=1M USDT 24h quote volume are scanned."
            ),
        },
        "automation": {
            "enabled": True,
            "scan_interval_seconds": V21_SCAN_INTERVAL_SECONDS,
            "last_scan": V21_LAST_SCAN,
        },
        "persistence": {
            "database_configured": bool(V21_DB_URL),
            "backend": "POSTGRESQL" if V21_DB_URL else "MEMORY_ONLY",
        },
        "telegram": {
            "configured": bool(V22_TG_TOKEN and V22_TG_CHAT_ID),
            "entry_notifications": True,
            "exit_notifications": True,
        },
        "paper": v20_public_state(),
        "generated_utc": utc_now(),
    }


@app.get("/v25-status")
async def v25_status():
    return {
        "model": MODEL,
        "mode": "RESEARCH_PAPER_ONLY",
        "trading": False,
        "orders": False,
        "strategy": "V27_CANDIDATE_B_CLEAN_BROAD_ALTCOIN_FORWARD_PAPER",
        "strategy_changed": True,
        "strategy_change": (
            "24h quote-volume floor lowered from 1M to 250k USDT. "
            "Candidate B signal thresholds are unchanged."
        ),
        "universe": {
            "mode": "VERY_BROAD_DYNAMIC",
            "min_quote_volume_usdt_24h": MIN_QUOTE_VOLUME_USDT,
            "fixed_coin_count": False,
            "note": (
                "All Binance USDT spot symbols passing the existing eligibility "
                "exclusions and >=250k USDT 24h quote volume are scanned."
            ),
        },
        "automation": {
            "enabled": True,
            "scan_interval_seconds": V21_SCAN_INTERVAL_SECONDS,
            "last_scan": V21_LAST_SCAN,
        },
        "persistence": {
            "database_configured": bool(V21_DB_URL),
            "backend": "POSTGRESQL" if V21_DB_URL else "MEMORY_ONLY",
        },
        "telegram": {
            "configured": bool(V22_TG_TOKEN and V22_TG_CHAT_ID),
            "entry_notifications": True,
            "exit_notifications": True,
        },
        "paper": v20_public_state(),
        "generated_utc": utc_now(),
    }



@app.get("/v26-status")
async def v26_status():
    return {
        "model": MODEL,
        "mode": "RESEARCH_PAPER_ONLY",
        "trading": False,
        "orders": False,
        "strategy": "V27_CANDIDATE_B_CLEAN_BROAD_ALTCOIN_FORWARD_PAPER",
        "strategy_changed": True,
        "strategy_change": (
            "250k USDT 24h liquidity floor retained. Clearly non-altcoin "
            "cash-like/stable-like and tokenized-equity bases observed in V25 "
            "are excluded. Candidate B thresholds unchanged."
        ),
        "universe": {
            "mode": "MAX_BROAD_ALTCOIN_DYNAMIC",
            "min_quote_volume_usdt_24h": MIN_QUOTE_VOLUME_USDT,
            "fixed_coin_count": False,
            "extra_non_alt_base_exclusions": sorted(V26_NON_ALT_BASE_EXCLUSIONS),
        },
        "automation": {
            "enabled": True,
            "scan_interval_seconds": V21_SCAN_INTERVAL_SECONDS,
            "last_scan": V21_LAST_SCAN,
        },
        "persistence": {
            "database_configured": bool(V21_DB_URL),
            "backend": "POSTGRESQL" if V21_DB_URL else "MEMORY_ONLY",
        },
        "telegram": {
            "configured": bool(V22_TG_TOKEN and V22_TG_CHAT_ID),
            "entry_notifications": True,
            "exit_notifications": True,
        },
        "paper": v20_public_state(),
        "generated_utc": utc_now(),
    }


@app.get("/v27-status")
async def v27_status():
    return {
        "model": MODEL,
        "mode": "RESEARCH_PAPER_ONLY",
        "trading": False,
        "orders": False,
        "strategy": "V27_CANDIDATE_B_CLEAN_BROAD_ALTCOIN_FORWARD_PAPER",
        "strategy_changed": True,
        "strategy_change": (
            "Corrected Binance tokenized equity/index base exclusions (e.g. "
            "TSLAB, AAPLB, NVDAB, GOOGLB, QQQB). 250k USDT liquidity floor "
            "and Candidate B signal thresholds are unchanged."
        ),
        "universe": {
            "mode": "CLEAN_BROAD_ALTCOIN_DYNAMIC",
            "min_quote_volume_usdt_24h": MIN_QUOTE_VOLUME_USDT,
            "fixed_coin_count": False,
            "extra_non_alt_base_exclusions": sorted(V26_NON_ALT_BASE_EXCLUSIONS),
        },
        "automation": {
            "enabled": True,
            "scan_interval_seconds": V21_SCAN_INTERVAL_SECONDS,
            "last_scan": V21_LAST_SCAN,
        },
        "persistence": {
            "database_configured": bool(V21_DB_URL),
            "backend": "POSTGRESQL" if V21_DB_URL else "MEMORY_ONLY",
        },
        "telegram": {
            "configured": bool(V22_TG_TOKEN and V22_TG_CHAT_ID),
            "entry_notifications": True,
            "exit_notifications": True,
        },
        "paper": v20_public_state(),
        "generated_utc": utc_now(),
    }

# ============================================================
# V28 CHALLENGER DIAGNOSTIC â€” SIGNAL FUNNEL ONLY
# V27 forward paper remains unchanged.
# No orders, no live trading, no V28 paper entries yet.
# ============================================================

V28_COST_PCT = 0.15


def v28_pf(values):
    wins = [x for x in values if x > 0]
    losses = [x for x in values if x < 0]
    if not losses:
        return None if wins else 0.0
    return sum(wins) / abs(sum(losses))


def v28_stats(rows, field):
    vals = [float(r[field]) for r in rows if r.get(field) is not None]
    if not vals:
        return {"n": 0, "mean_net_pct": None, "median_net_pct": None, "win_rate_pct": None, "profit_factor": None}
    return {
        "n": len(vals),
        "mean_net_pct": round(mean(vals), 4),
        "median_net_pct": round(median(vals), 4),
        "win_rate_pct": round(sum(1 for x in vals if x > 0) / len(vals) * 100, 2),
        "profit_factor": round(v28_pf(vals), 4) if v28_pf(vals) is not None else None,
    }


def v28_base_signals(candles, symbol):
    """Frozen relative-momentum base signal, without requiring 60m continuation."""
    rows = []
    ret30, mus, sigmas = precompute_rolling_volatility_v12(candles, 288)
    # Need 60m observation + 120m future after the 60m checkpoint.
    for i in range(294, len(candles) - 12 - 24 - 3):
        mom30 = ret30[i]
        if mom30 is None or mom30 <= 0:
            continue
        mu, sigma = mus[i], sigmas[i]
        if mu is None or sigma is None or sigma <= 0:
            continue
        z = (mom30 - mu) / sigma
        signal_close = candles[i]["close"]

        def checkpoint(minutes):
            idx = i + 1 + minutes // 5
            return idx, pct_change(signal_close, candles[idx]["open"])

        i15, c15 = checkpoint(15)
        i30, c30 = checkpoint(30)
        i60, c60 = checkpoint(60)

        # For each possible decision point: enter at that checkpoint OPEN,
        # then measure 60m and 120m from entry, net of fixed 0.15% cost.
        def future(entry_idx, bars):
            exit_idx = entry_idx + bars
            if exit_idx >= len(candles):
                return None
            return pct_change(candles[entry_idx]["open"], candles[exit_idx]["open"]) - V28_COST_PCT

        rows.append({
            "symbol": symbol,
            "signal_time_ms": candles[i]["close_time"],
            "momentum_30m_pct": mom30,
            "relative_momentum_z": z,
            "cont_15m_pct": c15,
            "cont_30m_pct": c30,
            "cont_60m_pct": c60,
            "net15_entry_60m_pct": future(i15, 12),
            "net15_entry_120m_pct": future(i15, 24),
            "net30_entry_60m_pct": future(i30, 12),
            "net30_entry_120m_pct": future(i30, 24),
            "net60_entry_60m_pct": future(i60, 12),
            "net60_entry_120m_pct": future(i60, 24),
        })
    return rows


@app.get("/v28-funnel")
async def v28_funnel(days: int = Query(default=3, ge=2, le=7)):
    """
    Diagnostic only. V27 is untouched.
    Shows exactly where Candidate-B frequency collapses and compares
    15m/30m/60m continuation checkpoints without creating a new strategy.
    """
    try:
        async with httpx.AsyncClient(timeout=httpx.Timeout(180.0)) as client:
            universe = await build_universe(client)
            semaphore = asyncio.Semaphore(8)

            async def fetch(item):
                async with semaphore:
                    try:
                        c = await get_5m_candles_days(client, item["symbol"], days)
                        return item["symbol"], c, None
                    except Exception as e:
                        return item["symbol"], None, str(e)

            fetched = await asyncio.gather(*[fetch(x) for x in universe])
            good = {s: c for s, c, err in fetched if c}
            errors = [{"symbol": s, "error": err} for s, c, err in fetched if err]
            btc = good.get("BTCUSDT")
            if btc is None:
                btc = await get_5m_candles_days(client, "BTCUSDT", days)

        raw = []
        for sym, candles in good.items():
            if sym == "BTCUSDT":
                continue
            raw.extend(v28_base_signals(candles, sym))

        # Cross-sectional rank at the signal timestamp.
        by_time = {}
        for e in raw:
            by_time.setdefault(e["signal_time_ms"], []).append(e)
        ranked = []
        for group in by_time.values():
            if len(group) < 5:
                continue
            ordered = sorted(group, key=lambda x: x["relative_momentum_z"])
            n = len(ordered)
            for idx, e in enumerate(ordered):
                r = dict(e)
                r["cross_section_percentile"] = idx / (n - 1) if n > 1 else 1.0
                ranked.append(r)

        # True selected-alt 30m market mean at each timestamp.
        snapshots = {}
        for sym, candles in good.items():
            if sym == "BTCUSDT":
                continue
            for i in range(6, len(candles)):
                t = candles[i]["close_time"]
                snapshots.setdefault(t, []).append(pct_change(candles[i-6]["close"], candles[i]["close"]))

        stages = {}
        stages["01_positive_30m"] = ranked
        stages["02_z_ge_1"] = [e for e in stages["01_positive_30m"] if e["relative_momentum_z"] >= 1.0]
        stages["03_top20"] = [e for e in stages["02_z_ge_1"] if e["cross_section_percentile"] >= 0.80]

        bull = []
        for e in stages["03_top20"]:
            reg = btc_regime_at_v17(btc, e["signal_time_ms"])
            if reg and reg["btc_trend"] == "BTC_BULL":
                x = dict(e)
                x["btc_4h_pct"] = reg["btc_4h_pct"]
                x["btc_24h_pct"] = reg["btc_24h_pct"]
                bull.append(x)
        stages["04_btc_bull"] = bull

        alt_ok = []
        for e in stages["04_btc_bull"]:
            vals = snapshots.get(e["signal_time_ms"], [])
            if vals and mean(vals) < 0.5:
                x = dict(e)
                x["alt_market_mean_30m_pct"] = mean(vals)
                alt_ok.append(x)
        stages["05_alt_mean_lt_0_5"] = alt_ok

        # Diagnostic continuation checkpoints. Same +0.75% threshold at each checkpoint
        # so we can isolate the effect of waiting time itself.
        stages["06_cont15_ge_0_75"] = [e for e in alt_ok if e["cont_15m_pct"] >= 0.75]
        stages["07_cont30_ge_0_75"] = [e for e in alt_ok if e["cont_30m_pct"] >= 0.75]
        stages["08_v27_cont60_ge_0_75"] = [e for e in alt_ok if e["cont_60m_pct"] >= 0.75]

        counts = {k: len(v) for k, v in stages.items()}
        base_n = max(1, counts["01_positive_30m"])
        funnel = []
        previous = None
        for k, rows in stages.items():
            n = len(rows)
            funnel.append({
                "stage": k,
                "n": n,
                "pct_of_initial": round(n / base_n * 100, 2),
                "pct_of_previous": None if previous is None or previous == 0 else round(n / previous * 100, 2),
            })
            # continuation branches are parallel from stage 05, not sequential
            if k in {"06_cont15_ge_0_75", "07_cont30_ge_0_75", "08_v27_cont60_ge_0_75"}:
                previous = counts["05_alt_mean_lt_0_5"]
            else:
                previous = n

        comparison = {
            "15m_confirmation": {
                "condition": "continuation >= +0.75% by 15m",
                "entry_count": len(stages["06_cont15_ge_0_75"]),
                "net_60m": v28_stats(stages["06_cont15_ge_0_75"], "net15_entry_60m_pct"),
                "net_120m": v28_stats(stages["06_cont15_ge_0_75"], "net15_entry_120m_pct"),
            },
            "30m_confirmation": {
                "condition": "continuation >= +0.75% by 30m",
                "entry_count": len(stages["07_cont30_ge_0_75"]),
                "net_60m": v28_stats(stages["07_cont30_ge_0_75"], "net30_entry_60m_pct"),
                "net_120m": v28_stats(stages["07_cont30_ge_0_75"], "net30_entry_120m_pct"),
            },
            "60m_confirmation_V27": {
                "condition": "continuation >= +0.75% by 60m",
                "entry_count": len(stages["08_v27_cont60_ge_0_75"]),
                "net_60m": v28_stats(stages["08_v27_cont60_ge_0_75"], "net60_entry_60m_pct"),
                "net_120m": v28_stats(stages["08_v27_cont60_ge_0_75"], "net60_entry_120m_pct"),
            },
        }

        return {
            **MODE_INFO,
            "status": "OK",
            "diagnostic": "V28_SIGNAL_FUNNEL_CHALLENGER_RESEARCH",
            "v27_forward_untouched": True,
            "v28_trading": False,
            "v28_orders": False,
            "days": days,
            "universe_size": len(universe),
            "symbols_fetched": len(good),
            "fetch_errors": errors,
            "funnel": funnel,
            "confirmation_comparison": comparison,
            "important_note": "15m/30m/60m branches are parallel diagnostics from the same Candidate-B pre-continuation pool; this endpoint does not select or deploy V28.",
            "generated_utc": utc_now(),
        }
    except Exception as e:
        return {
            **MODE_INFO,
            "status": "ERROR",
            "diagnostic": "V28_SIGNAL_FUNNEL_CHALLENGER_RESEARCH",
            "v27_forward_untouched": True,
            "error": str(e),
            "generated_utc": utc_now(),
        }

# ============================================================
# V28 BTC-INDEPENDENT CHALLENGER DIAGNOSTIC
# V27 forward-paper logic/state is NOT changed by this endpoint.
# ============================================================

def v28_cd60(rows):
    """Keep at most one event per symbol in any rolling 60-minute window."""
    by_symbol = {}
    for r in rows:
        by_symbol.setdefault(r["symbol"], []).append(r)
    kept = []
    gap = 60 * 60 * 1000
    for sym, items in by_symbol.items():
        items = sorted(items, key=lambda x: x["signal_time_ms"])
        last = None
        for r in items:
            t = r["signal_time_ms"]
            if last is None or t - last >= gap:
                kept.append(r)
                last = t
    return sorted(kept, key=lambda x: x["signal_time_ms"])


def v28_group_summary(rows):
    independent = v28_cd60(rows)
    days_seen = len({datetime.fromtimestamp(r["signal_time_ms"] / 1000, tz=timezone.utc).date().isoformat() for r in independent})
    return {
        "raw_event_count": len(rows),
        "independent_entry_count_cd60": len(independent),
        "active_days": days_seen,
        "entries_per_active_day": round(len(independent) / days_seen, 2) if days_seen else 0,
        "net_60m": v28_stats(independent, "net60_entry_60m_pct"),
        "net_120m": v28_stats(independent, "net60_entry_120m_pct"),
    }


@app.get("/v28-btc-independent")
async def v28_btc_independent(days: int = Query(default=3, ge=2, le=7)):
    """
    Challenger diagnostic only.
    Frozen core: positive 30m, z>=1, Top20, 60m continuation>=+0.75%,
    entry at 60m checkpoint OPEN, 60/120m future net of 0.15% cost.
    Compares NO BTC FILTER with BTC_BULL/MIXED/BEAR. V27 remains untouched.
    """
    try:
        async with httpx.AsyncClient(timeout=httpx.Timeout(180.0)) as client:
            universe = await build_universe(client)
            semaphore = asyncio.Semaphore(8)

            async def fetch(item):
                async with semaphore:
                    try:
                        c = await get_5m_candles_days(client, item["symbol"], days)
                        return item["symbol"], c, None
                    except Exception as e:
                        return item["symbol"], None, str(e)

            fetched = await asyncio.gather(*[fetch(x) for x in universe])
            good = {s: c for s, c, err in fetched if c}
            errors = [{"symbol": s, "error": err} for s, c, err in fetched if err]
            btc = await get_5m_candles_days(client, "BTCUSDT", days)

        raw = []
        for sym, candles in good.items():
            raw.extend(v28_base_signals(candles, sym))

        # Cross-sectional rank at each completed signal timestamp.
        by_time = {}
        for e in raw:
            by_time.setdefault(e["signal_time_ms"], []).append(e)
        ranked = []
        for group in by_time.values():
            if len(group) < 5:
                continue
            ordered = sorted(group, key=lambda x: x["relative_momentum_z"])
            n = len(ordered)
            for idx, e in enumerate(ordered):
                x = dict(e)
                x["cross_section_percentile"] = idx / (n - 1) if n > 1 else 1.0
                ranked.append(x)

        # True selected-alt 30m mean at signal time (same population concept as V27 diagnostics).
        snapshots = {}
        for sym, candles in good.items():
            for i in range(6, len(candles)):
                t = candles[i]["close_time"]
                snapshots.setdefault(t, []).append(pct_change(candles[i-6]["close"], candles[i]["close"]))

        core = []
        for e in ranked:
            if e["relative_momentum_z"] < 1.0:
                continue
            if e["cross_section_percentile"] < 0.80:
                continue
            if e["cont_60m_pct"] < 0.75:
                continue
            vals = snapshots.get(e["signal_time_ms"], [])
            if not vals:
                continue
            alt_mean = mean(vals)
            if alt_mean >= 0.5:
                continue
            x = dict(e)
            x["alt_market_mean_30m_pct"] = alt_mean
            reg = btc_regime_at_v17(btc, e["signal_time_ms"])
            if reg:
                x["btc_trend"] = reg["btc_trend"]
                x["btc_4h_pct"] = reg["btc_4h_pct"]
                x["btc_24h_pct"] = reg["btc_24h_pct"]
            else:
                x["btc_trend"] = "UNKNOWN"
            core.append(x)

        groups = {
            "NO_BTC_FILTER_V28_CHALLENGER": core,
            "BTC_BULL_V27_STYLE": [x for x in core if x["btc_trend"] == "BTC_BULL"],
            "BTC_MIXED": [x for x in core if x["btc_trend"] == "BTC_MIXED"],
            "BTC_BEAR": [x for x in core if x["btc_trend"] == "BTC_BEAR"],
            "BTC_UNKNOWN": [x for x in core if x["btc_trend"] == "UNKNOWN"],
        }

        return {
            **MODE_INFO,
            "status": "OK",
            "diagnostic": "V28_BTC_INDEPENDENT_CHALLENGER",
            "v27_forward_untouched": True,
            "strategy_changed": False,
            "trading": False,
            "orders": False,
            "days": days,
            "universe_size": len(universe),
            "symbols_fetched": len(good),
            "fetch_errors": errors,
            "frozen_core": {
                "relative_momentum_z_min": 1.0,
                "cross_section_percentile_min": 0.80,
                "continuation_60m_min_pct": 0.75,
                "alt_market_mean_30m_max_pct": 0.5,
                "entry": "60m checkpoint OPEN",
                "round_trip_cost_pct": V28_COST_PCT,
                "same_symbol_cooldown_minutes": 60,
            },
            "comparison": {k: v28_group_summary(v) for k, v in groups.items()},
            "decision_rule": "Do not deploy from event count alone. Prefer BTC-independent only if independent CD60 sample preserves/improves net mean, median and PF across regimes while materially increasing entries.",
            "generated_utc": utc_now(),
        }
    except Exception as e:
        return {
            **MODE_INFO,
            "status": "ERROR",
            "diagnostic": "V28_BTC_INDEPENDENT_CHALLENGER",
            "v27_forward_untouched": True,
            "error": str(e),
            "generated_utc": utc_now(),
        }


# ============================================================
# V28 LIGHT: 5-DAY BLOCK BTC-INDEPENDENT VALIDATION
# Render-Free friendly. Research only. V27 forward untouched.
# ============================================================

async def v28_get_5m_candles_window(client, symbol, days_ago=0, window_days=5):
    now_ms = int(datetime.now(timezone.utc).timestamp() * 1000)
    day_ms = 24 * 60 * 60 * 1000
    end_ms = now_ms - (days_ago * day_ms)
    start_ms = end_ms - (window_days * day_ms)

    cursor_end = end_ms
    by_open = {}
    max_pages = max(3, int((window_days * 288) / 1000) + 3)

    for _ in range(max_pages):
        raw = await get_json(
            client,
            "/api/v3/klines",
            params={
                "symbol": symbol,
                "interval": "5m",
                "limit": 1000,
                "endTime": cursor_end,
            },
        )
        if not raw:
            break

        oldest = int(raw[0][0])
        for k in raw:
            ot = int(k[0])
            ct = int(k[6])
            if ct >= end_ms or ot < start_ms:
                continue
            by_open[ot] = {
                "open_time": ot,
                "open": float(k[1]),
                "high": float(k[2]),
                "low": float(k[3]),
                "close": float(k[4]),
                "volume": float(k[5]),
                "close_time": ct,
            }

        if oldest <= start_ms:
            break
        cursor_end = oldest - 1
        await asyncio.sleep(0.02)

    return sorted(by_open.values(), key=lambda x: x["open_time"])


@app.get("/v28-btc-block")
async def v28_btc_block(
    days_ago: int = Query(default=0, ge=0, le=25),
    window_days: int = Query(default=5, ge=3, le=5),
):
    """
    One lightweight historical block.
    Frozen comparison:
      - V27 style: core + BTC_BULL
      - V28 challenger: same core, no BTC filter
    """
    try:
        async with httpx.AsyncClient(timeout=httpx.Timeout(180.0)) as client:
            universe = await build_universe(client)
            semaphore = asyncio.Semaphore(10)

            async def fetch(item):
                async with semaphore:
                    try:
                        c = await v28_get_5m_candles_window(
                            client, item["symbol"], days_ago, window_days
                        )
                        return item["symbol"], c, None
                    except Exception as e:
                        return item["symbol"], None, str(e)

            fetched = await asyncio.gather(*[fetch(x) for x in universe])
            good = {s: c for s, c, err in fetched if c}
            errors = [{"symbol": s, "error": err} for s, c, err in fetched if err]

            # BTC needs enough history before block start for 24h regime.
            btc_days = min(30, days_ago + window_days + 2)
            btc_full = await get_5m_candles_days(client, "BTCUSDT", btc_days)

        raw = []
        for sym, candles in good.items():
            raw.extend(v28_base_signals(candles, sym))

        by_time = {}
        for e in raw:
            by_time.setdefault(e["signal_time_ms"], []).append(e)

        ranked = []
        for group in by_time.values():
            if len(group) < 5:
                continue
            ordered = sorted(group, key=lambda x: x["relative_momentum_z"])
            n = len(ordered)
            for idx, e in enumerate(ordered):
                x = dict(e)
                x["cross_section_percentile"] = idx / (n - 1) if n > 1 else 1.0
                ranked.append(x)

        snapshots = {}
        for sym, candles in good.items():
            for i in range(6, len(candles)):
                t = candles[i]["close_time"]
                snapshots.setdefault(t, []).append(
                    pct_change(candles[i-6]["close"], candles[i]["close"])
                )

        core = []
        for e in ranked:
            if e["relative_momentum_z"] < 1.0:
                continue
            if e["cross_section_percentile"] < 0.80:
                continue
            if e["cont_60m_pct"] < 0.75:
                continue

            vals = snapshots.get(e["signal_time_ms"], [])
            if not vals:
                continue
            alt_mean = mean(vals)
            if alt_mean >= 0.5:
                continue

            x = dict(e)
            x["alt_market_mean_30m_pct"] = alt_mean
            reg = btc_regime_at_v17(btc_full, e["signal_time_ms"])
            if reg:
                x["btc_trend"] = reg["btc_trend"]
                x["btc_4h_pct"] = reg["btc_4h_pct"]
                x["btc_24h_pct"] = reg["btc_24h_pct"]
            else:
                x["btc_trend"] = "UNKNOWN"
            core.append(x)

        no_btc = core
        bull = [x for x in core if x["btc_trend"] == "BTC_BULL"]
        mixed = [x for x in core if x["btc_trend"] == "BTC_MIXED"]
        bear = [x for x in core if x["btc_trend"] == "BTC_BEAR"]

        indep = v28_cd60(no_btc)
        by_t = {}
        for r in indep:
            by_t[r["signal_time_ms"]] = by_t.get(r["signal_time_ms"], 0) + 1
        conc = list(by_t.values())

        now_ms = int(datetime.now(timezone.utc).timestamp() * 1000)
        day_ms = 24 * 60 * 60 * 1000
        end_ms = now_ms - days_ago * day_ms
        start_ms = end_ms - window_days * day_ms

        return {
            **MODE_INFO,
            "status": "OK",
            "diagnostic": "V28_LIGHT_5D_BTC_INDEPENDENT_BLOCK",
            "v27_forward_untouched": True,
            "strategy_changed": False,
            "trading": False,
            "orders": False,
            "days_ago": days_ago,
            "window_days": window_days,
            "window_start_utc": datetime.fromtimestamp(start_ms/1000, tz=timezone.utc).isoformat(),
            "window_end_utc": datetime.fromtimestamp(end_ms/1000, tz=timezone.utc).isoformat(),
            "universe_size": len(universe),
            "symbols_fetched": len(good),
            "fetch_errors": errors,
            "frozen_core": {
                "relative_momentum_z_min": 1.0,
                "cross_section_percentile_min": 0.80,
                "continuation_60m_min_pct": 0.75,
                "alt_market_mean_30m_max_pct": 0.5,
                "entry": "60m checkpoint OPEN",
                "round_trip_cost_pct": V28_COST_PCT,
                "same_symbol_cooldown_minutes": 60,
            },
            "comparison": {
                "NO_BTC_FILTER_V28": v28_group_summary(no_btc),
                "BTC_BULL_V27_STYLE": v28_group_summary(bull),
                "BTC_MIXED": v28_group_summary(mixed),
                "BTC_BEAR": v28_group_summary(bear),
            },
            "signal_concentration_no_btc": {
                "distinct_signal_times": len(conc),
                "max_same_timestamp_entries": max(conc) if conc else 0,
                "mean_same_timestamp_entries": round(mean(conc), 2) if conc else 0,
                "timestamps_with_ge_5_entries": sum(1 for x in conc if x >= 5),
                "timestamps_with_ge_10_entries": sum(1 for x in conc if x >= 10),
            },
            "generated_utc": utc_now(),
        }

    except Exception as e:
        return {
            **MODE_INFO,
            "status": "ERROR",
            "diagnostic": "V28_LIGHT_5D_BTC_INDEPENDENT_BLOCK",
            "v27_forward_untouched": True,
            "days_ago": days_ago,
            "error": str(e),
            "generated_utc": utc_now(),
        }



# ============================================================
# V29 TOP-N CONTINUATION RANKING â€” LIGHT 3-DAY BLOCK
# Research only. V27 forward paper remains untouched.
# Ranking uses information already known at the 60m decision point.
# ============================================================

def v29_select_topn_with_cd60(rows, top_n=None):
    """
    Chronological paper selection.
    At each signal timestamp:
      1) remove symbols still inside their 60m cooldown,
      2) rank remaining candidates by observed 60m continuation,
         then z-score, then cross-sectional percentile,
      3) keep ALL or Top-N.
    No future-return field is used in ranking.
    """
    by_time = {}
    for r in rows:
        by_time.setdefault(r["signal_time_ms"], []).append(r)

    last_selected = {}
    gap = 60 * 60 * 1000
    selected = []

    for t in sorted(by_time):
        available = []
        for r in by_time[t]:
            last = last_selected.get(r["symbol"])
            if last is None or t - last >= gap:
                available.append(r)

        ordered = sorted(
            available,
            key=lambda r: (
                float(r.get("cont_60m_pct", 0.0)),
                float(r.get("relative_momentum_z", 0.0)),
                float(r.get("cross_section_percentile", 0.0)),
            ),
            reverse=True,
        )

        chosen = ordered if top_n is None else ordered[:top_n]
        for r in chosen:
            selected.append(r)
            last_selected[r["symbol"]] = t

    return selected


def v29_selected_summary(rows):
    days_seen = len({
        datetime.fromtimestamp(r["signal_time_ms"] / 1000, tz=timezone.utc).date().isoformat()
        for r in rows
    })
    return {
        "entry_count": len(rows),
        "active_days": days_seen,
        "entries_per_active_day": round(len(rows) / days_seen, 2) if days_seen else 0,
        "net_60m": v28_stats(rows, "net60_entry_60m_pct"),
        "net_120m": v28_stats(rows, "net60_entry_120m_pct"),
    }


@app.get("/v29-topn-block")
async def v29_topn_block(
    days_ago: int = Query(default=0, ge=0, le=27),
    window_days: int = Query(default=3, ge=2, le=3),
):
    """
    Render-Free friendly V29 challenger.

    Frozen Candidate-B core:
      z >= 1
      cross-sectional percentile >= 0.80
      observed 60m continuation >= +0.75%
      selected-alt mean 30m < +0.5%
      entry = 60m checkpoint OPEN
      fixed cost = 0.15%
      same-symbol cooldown = 60m

    Main challenger has NO BTC filter.
    Compare ALL vs Top1 vs Top3 vs Top5 at each timestamp.
    BTC_BULL ALL is retained only as the V27-style control.
    """
    try:
        async with httpx.AsyncClient(timeout=httpx.Timeout(180.0)) as client:
            universe = await build_universe(client)
            semaphore = asyncio.Semaphore(10)

            async def fetch(item):
                async with semaphore:
                    try:
                        c = await v28_get_5m_candles_window(
                            client, item["symbol"], days_ago, window_days
                        )
                        return item["symbol"], c, None
                    except Exception as e:
                        return item["symbol"], None, str(e)

            fetched = await asyncio.gather(*[fetch(x) for x in universe])
            good = {s: c for s, c, err in fetched if c}
            errors = [{"symbol": s, "error": err} for s, c, err in fetched if err]

            btc_days = min(30, days_ago + window_days + 2)
            btc_full = await get_5m_candles_days(client, "BTCUSDT", btc_days)

        raw = []
        for sym, candles in good.items():
            raw.extend(v28_base_signals(candles, sym))

        # Cross-sectional z rank at each signal timestamp.
        by_time = {}
        for e in raw:
            by_time.setdefault(e["signal_time_ms"], []).append(e)

        ranked = []
        for group in by_time.values():
            if len(group) < 5:
                continue
            ordered = sorted(group, key=lambda x: x["relative_momentum_z"])
            n = len(ordered)
            for idx, e in enumerate(ordered):
                x = dict(e)
                x["cross_section_percentile"] = idx / (n - 1) if n > 1 else 1.0
                ranked.append(x)

        # True selected-alt 30m market mean at the same signal timestamp.
        snapshots = {}
        for sym, candles in good.items():
            for i in range(6, len(candles)):
                t = candles[i]["close_time"]
                snapshots.setdefault(t, []).append(
                    pct_change(candles[i-6]["close"], candles[i]["close"])
                )

        core = []
        for e in ranked:
            if e["relative_momentum_z"] < 1.0:
                continue
            if e["cross_section_percentile"] < 0.80:
                continue
            if e["cont_60m_pct"] < 0.75:
                continue

            vals = snapshots.get(e["signal_time_ms"], [])
            if not vals:
                continue
            alt_mean = mean(vals)
            if alt_mean >= 0.5:
                continue

            x = dict(e)
            x["alt_market_mean_30m_pct"] = alt_mean
            reg = btc_regime_at_v17(btc_full, e["signal_time_ms"])
            x["btc_trend"] = reg["btc_trend"] if reg else "UNKNOWN"
            core.append(x)

        all_sel = v29_select_topn_with_cd60(core, None)
        top1 = v29_select_topn_with_cd60(core, 1)
        top3 = v29_select_topn_with_cd60(core, 3)
        top5 = v29_select_topn_with_cd60(core, 5)

        bull_core = [r for r in core if r.get("btc_trend") == "BTC_BULL"]
        bull_all = v29_select_topn_with_cd60(bull_core, None)

        # Concentration before Top-N selection.
        candidate_counts = {}
        for r in core:
            candidate_counts[r["signal_time_ms"]] = candidate_counts.get(r["signal_time_ms"], 0) + 1
        cc = list(candidate_counts.values())

        now_ms = int(datetime.now(timezone.utc).timestamp() * 1000)
        day_ms = 24 * 60 * 60 * 1000
        end_ms = now_ms - days_ago * day_ms
        start_ms = end_ms - window_days * day_ms

        return {
            **MODE_INFO,
            "status": "OK",
            "diagnostic": "V29_TOPN_CONTINUATION_RANKING_LIGHT_BLOCK",
            "v27_forward_untouched": True,
            "strategy_changed": False,
            "trading": False,
            "orders": False,
            "days_ago": days_ago,
            "window_days": window_days,
            "window_start_utc": datetime.fromtimestamp(start_ms/1000, tz=timezone.utc).isoformat(),
            "window_end_utc": datetime.fromtimestamp(end_ms/1000, tz=timezone.utc).isoformat(),
            "universe_size": len(universe),
            "symbols_fetched": len(good),
            "fetch_errors": errors,
            "frozen_core": {
                "relative_momentum_z_min": 1.0,
                "cross_section_percentile_min": 0.80,
                "continuation_60m_min_pct": 0.75,
                "alt_market_mean_30m_max_pct": 0.5,
                "btc_filter_for_v29": "NONE",
                "entry": "60m checkpoint OPEN",
                "round_trip_cost_pct": V28_COST_PCT,
                "same_symbol_cooldown_minutes": 60,
            },
            "ranking_rule": {
                "primary": "observed_continuation_60m_pct_DESC",
                "tie_break_1": "relative_momentum_z_DESC",
                "tie_break_2": "cross_section_percentile_DESC",
                "lookahead_used": False,
            },
            "comparison": {
                "V29_ALL_NO_BTC_FILTER": v29_selected_summary(all_sel),
                "V29_TOP1": v29_selected_summary(top1),
                "V29_TOP3": v29_selected_summary(top3),
                "V29_TOP5": v29_selected_summary(top5),
                "V27_STYLE_BTC_BULL_ALL_CONTROL": v29_selected_summary(bull_all),
            },
            "candidate_concentration": {
                "distinct_signal_times": len(cc),
                "max_candidates_same_timestamp": max(cc) if cc else 0,
                "mean_candidates_same_timestamp": round(mean(cc), 2) if cc else 0,
                "timestamps_with_ge_5_candidates": sum(1 for x in cc if x >= 5),
                "timestamps_with_ge_10_candidates": sum(1 for x in cc if x >= 10),
            },
            "important_note": (
                "Top-N ranking uses only data known by the 60m decision point. "
                "This endpoint is diagnostic only and does not modify V27 forward state."
            ),
            "generated_utc": utc_now(),
        }

    except Exception as e:
        return {
            **MODE_INFO,
            "status": "ERROR",
            "diagnostic": "V29_TOPN_CONTINUATION_RANKING_LIGHT_BLOCK",
            "v27_forward_untouched": True,
            "days_ago": days_ago,
            "error": str(e),
            "generated_utc": utc_now(),
        }



# ============================================================
# V30 TOP-1 CONFIRMATION WINDOW CHALLENGER: 30 / 45 / 60 MIN
# Research only. V27 forward paper remains untouched.
# Threshold stays +0.75% for every window; exit stays +120m.
# ============================================================

def v30_build_window_rows(base_ranked, candles_by_symbol, snapshots, btc_full, confirm_minutes):
    steps = confirm_minutes // 5
    out = []
    for e in base_ranked:
        if e["relative_momentum_z"] < 1.0:
            continue
        if e["cross_section_percentile"] < 0.80:
            continue

        candles = candles_by_symbol.get(e["symbol"])
        if not candles:
            continue

        # Find the candle whose close_time equals the original signal timestamp.
        idx = None
        for j, c in enumerate(candles):
            if c["close_time"] == e["signal_time_ms"]:
                idx = j
                break
        if idx is None or idx + steps + 24 >= len(candles):
            continue

        signal_close = candles[idx]["close"]
        checkpoint_open = candles[idx + steps + 1]["open"]
        cont_pct = pct_change(signal_close, checkpoint_open)
        if cont_pct < 0.75:
            continue

        vals = snapshots.get(e["signal_time_ms"], [])
        if not vals:
            continue
        alt_mean = mean(vals)
        if alt_mean >= 0.5:
            continue

        # Fixed +120m exit from the chosen confirmation entry.
        exit_open = candles[idx + steps + 1 + 24]["open"]
        gross120 = pct_change(checkpoint_open, exit_open)
        net120 = gross120 - V28_COST_PCT

        x = dict(e)
        x["confirmation_minutes"] = confirm_minutes
        x["cont_60m_pct"] = cont_pct  # reused by V29 ranking helper; now means chosen window continuation
        x["entry_open_time_ms"] = candles[idx + steps + 1]["open_time"]
        x["entry_price"] = checkpoint_open
        x["net60_entry_120m_pct"] = net120
        x["alt_market_mean_30m_pct"] = alt_mean
        reg = btc_regime_at_v17(btc_full, e["signal_time_ms"])
        x["btc_trend"] = reg["btc_trend"] if reg else "UNKNOWN"
        out.append(x)
    return out


@app.get("/v30-confirmation-top1")
async def v30_confirmation_top1(
    days_ago: int = Query(default=0, ge=0, le=27),
    window_days: int = Query(default=3, ge=2, le=3),
):
    try:
        async with httpx.AsyncClient(timeout=httpx.Timeout(180.0)) as client:
            universe = await build_universe(client)
            semaphore = asyncio.Semaphore(10)

            async def fetch(item):
                async with semaphore:
                    try:
                        c = await v28_get_5m_candles_window(
                            client, item["symbol"], days_ago, window_days
                        )
                        return item["symbol"], c, None
                    except Exception as e:
                        return item["symbol"], None, str(e)

            fetched = await asyncio.gather(*[fetch(x) for x in universe])
            good = {sym: c for sym, c, err in fetched if c}
            errors = [{"symbol": sym, "error": err} for sym, c, err in fetched if err]
            btc_days = min(30, days_ago + window_days + 2)
            btc_full = await get_5m_candles_days(client, "BTCUSDT", btc_days)

        raw = []
        for sym, candles in good.items():
            raw.extend(v28_base_signals(candles, sym))

        by_time = {}
        for e in raw:
            by_time.setdefault(e["signal_time_ms"], []).append(e)

        ranked = []
        for group in by_time.values():
            if len(group) < 5:
                continue
            ordered = sorted(group, key=lambda x: x["relative_momentum_z"])
            n = len(ordered)
            for idx, e in enumerate(ordered):
                x = dict(e)
                x["cross_section_percentile"] = idx / (n - 1) if n > 1 else 1.0
                ranked.append(x)

        snapshots = {}
        for sym, candles in good.items():
            for i in range(6, len(candles)):
                t = candles[i]["close_time"]
                snapshots.setdefault(t, []).append(
                    pct_change(candles[i-6]["close"], candles[i]["close"])
                )

        comparisons = {}
        for mins in (30, 45, 60):
            rows = v30_build_window_rows(ranked, good, snapshots, btc_full, mins)
            top1 = v29_select_topn_with_cd60(rows, 1)
            comparisons[f"TOP1_CONFIRM_{mins}M"] = {
                "entry_count": len(top1),
                "active_days": len({
                    datetime.fromtimestamp(r["signal_time_ms"]/1000, tz=timezone.utc).date().isoformat()
                    for r in top1
                }),
                "net_120m": v28_stats(top1, "net60_entry_120m_pct"),
            }

        return {
            **MODE_INFO,
            "status": "OK",
            "diagnostic": "V30_TOP1_CONFIRMATION_30_45_60",
            "v27_forward_untouched": True,
            "v29_forward_untouched": True,
            "strategy_changed": False,
            "trading": False,
            "orders": False,
            "days_ago": days_ago,
            "window_days": window_days,
            "universe_size": len(universe),
            "symbols_fetched": len(good),
            "fetch_errors": errors,
            "frozen_rules": {
                "relative_momentum_z_min": 1.0,
                "cross_section_percentile_min": 0.80,
                "continuation_threshold_pct": 0.75,
                "alt_market_mean_30m_max_pct": 0.5,
                "btc_filter": "NONE",
                "selection": "TOP1 by observed continuation at decision time",
                "same_symbol_cooldown_minutes": 60,
                "round_trip_cost_pct": V28_COST_PCT,
                "exit_after_entry_minutes": 120,
                "lookahead_used_for_selection": False,
            },
            "comparison": comparisons,
            "important_note": (
                "Only confirmation duration changes: 30m vs 45m vs 60m. "
                "The +0.75% threshold and 120m post-entry exit are identical."
            ),
            "generated_utc": utc_now(),
        }
    except Exception as e:
        return {
            **MODE_INFO,
            "status": "ERROR",
            "diagnostic": "V30_TOP1_CONFIRMATION_30_45_60",
            "v27_forward_untouched": True,
            "error": str(e),
            "generated_utc": utc_now(),
        }



# ============================================================
# V31 REGIME DIAGNOSTIC â€” TOP1 / 60M FROZEN CHALLENGER
# Research only. No V27/V29/V30 forward logic is changed.
# All regime variables are known at the entry decision time.
# ============================================================

def v31_bucket(x, cuts, labels):
    for cut, label in zip(cuts, labels):
        if x < cut:
            return label
    return labels[-1]

def v31_group_summary(rows):
    return {
        "n": len(rows),
        "net_120m": v28_stats(rows, "net60_entry_120m_pct"),
    }

@app.get("/v31-regime-block")
async def v31_regime_block(
    days_ago: int = Query(default=0, ge=0, le=27),
    window_days: int = Query(default=3, ge=2, le=3),
):
    """
    Diagnostic only: explain why frozen V29 Top1/60m works in some blocks
    and fails in others. No threshold is selected here.
    """
    try:
        async with httpx.AsyncClient(timeout=httpx.Timeout(180.0)) as client:
            universe = await build_universe(client)
            semaphore = asyncio.Semaphore(10)

            async def fetch(item):
                async with semaphore:
                    try:
                        c = await v28_get_5m_candles_window(
                            client, item["symbol"], days_ago, window_days
                        )
                        return item["symbol"], c, None
                    except Exception as e:
                        return item["symbol"], None, str(e)

            fetched = await asyncio.gather(*[fetch(x) for x in universe])
            good = {sym: c for sym, c, err in fetched if c}
            errors = [{"symbol": sym, "error": err} for sym, c, err in fetched if err]

            btc_days = min(30, days_ago + window_days + 2)
            btc_full = await get_5m_candles_days(client, "BTCUSDT", btc_days)

        raw = []
        for sym, candles in good.items():
            raw.extend(v28_base_signals(candles, sym))

        # Cross-sectional z percentile exactly as in V29/V30.
        by_time = {}
        for e in raw:
            by_time.setdefault(e["signal_time_ms"], []).append(e)

        ranked = []
        for group in by_time.values():
            if len(group) < 5:
                continue
            ordered = sorted(group, key=lambda x: x["relative_momentum_z"])
            n = len(ordered)
            for idx, e in enumerate(ordered):
                x = dict(e)
                x["cross_section_percentile"] = idx / (n - 1) if n > 1 else 1.0
                ranked.append(x)

        # Market snapshot at original signal time: only contemporaneous/past info.
        snapshots = {}
        for sym, candles in good.items():
            for i in range(6, len(candles)):
                t = candles[i]["close_time"]
                r30 = pct_change(candles[i-6]["close"], candles[i]["close"])
                snapshots.setdefault(t, []).append(r30)

        # Frozen 60m confirmation challenger.
        rows = v30_build_window_rows(ranked, good, snapshots, btc_full, 60)

        # Candidate concentration at each decision cohort, before Top1.
        cohort = {}
        for r in rows:
            cohort.setdefault(r["signal_time_ms"], []).append(r)

        # Add regime descriptors known by the 60m decision point.
        enriched = []
        for r in rows:
            x = dict(r)
            vals = snapshots.get(r["signal_time_ms"], [])
            if vals:
                pos = sum(1 for v in vals if v > 0)
                x["breadth_positive30_pct"] = 100.0 * pos / len(vals)
                x["alt_mean30_pct"] = mean(vals)
                x["alt_abs_mean30_pct"] = mean([abs(v) for v in vals])
            else:
                x["breadth_positive30_pct"] = 0.0
                x["alt_mean30_pct"] = 0.0
                x["alt_abs_mean30_pct"] = 0.0

            same = cohort.get(r["signal_time_ms"], [])
            x["eligible_candidates_same_timestamp"] = len(same)
            x["mean_candidate_continuation_pct"] = mean(
                [q["cont_60m_pct"] for q in same]
            ) if same else 0.0

            reg = btc_regime_at_v17(btc_full, r["signal_time_ms"])
            x["btc_trend"] = reg["btc_trend"] if reg else "UNKNOWN"
            x["btc_4h_pct"] = reg.get("btc_4h_pct", 0.0) if reg else 0.0
            x["btc_24h_pct"] = reg.get("btc_24h_pct", 0.0) if reg else 0.0
            enriched.append(x)

        top1 = v29_select_topn_with_cd60(enriched, 1)

        # Predeclared coarse descriptive buckets; not strategy filters.
        groups = {
            "BTC_REGIME": {},
            "ALT_BREADTH_POSITIVE30": {},
            "ALT_MEAN30": {},
            "SIGNAL_CONCENTRATION": {},
            "TOP1_CONTINUATION_STRENGTH": {},
        }

        for r in top1:
            groups["BTC_REGIME"].setdefault(r["btc_trend"], []).append(r)

            b = v31_bucket(
                r["breadth_positive30_pct"],
                [40.0, 60.0, 101.0],
                ["LT40", "40_TO_LT60", "GE60"],
            )
            groups["ALT_BREADTH_POSITIVE30"].setdefault(b, []).append(r)

            a = v31_bucket(
                r["alt_mean30_pct"],
                [0.0, 0.25, 0.5, 999.0],
                ["LT0", "0_TO_LT0_25", "0_25_TO_LT0_5", "GE0_5"],
            )
            groups["ALT_MEAN30"].setdefault(a, []).append(r)

            c = v31_bucket(
                r["eligible_candidates_same_timestamp"],
                [2, 5, 10, 10**9],
                ["1", "2_TO_4", "5_TO_9", "GE10"],
            )
            groups["SIGNAL_CONCENTRATION"].setdefault(c, []).append(r)

            m = v31_bucket(
                r["cont_60m_pct"],
                [1.0, 1.5, 2.5, 10**9],
                ["0_75_TO_LT1", "1_TO_LT1_5", "1_5_TO_LT2_5", "GE2_5"],
            )
            groups["TOP1_CONTINUATION_STRENGTH"].setdefault(m, []).append(r)

        summarized = {
            name: {k: v31_group_summary(v) for k, v in buckets.items()}
            for name, buckets in groups.items()
        }

        # Whole-block descriptors, useful for comparing good vs bad 3-day blocks.
        all_snap_vals = []
        for vals in snapshots.values():
            all_snap_vals.extend(vals)

        return {
            **MODE_INFO,
            "status": "OK",
            "diagnostic": "V31_REGIME_DIAGNOSTIC_TOP1_60M",
            "v27_forward_untouched": True,
            "v29_forward_untouched": True,
            "v30_untouched": True,
            "strategy_changed": False,
            "trading": False,
            "orders": False,
            "days_ago": days_ago,
            "window_days": window_days,
            "universe_size": len(universe),
            "symbols_fetched": len(good),
            "fetch_errors": errors,
            "frozen_challenger": {
                "relative_momentum_z_min": 1.0,
                "cross_section_percentile_min": 0.80,
                "confirmation_minutes": 60,
                "continuation_threshold_pct": 0.75,
                "alt_market_mean_30m_max_pct": 0.5,
                "btc_filter": "NONE",
                "selection": "TOP1",
                "same_symbol_cooldown_minutes": 60,
                "round_trip_cost_pct": V28_COST_PCT,
                "exit_after_entry_minutes": 120,
            },
            "top1_overall": v31_group_summary(top1),
            "block_context": {
                "top1_entries": len(top1),
                "distinct_signal_timestamps": len({r["signal_time_ms"] for r in top1}),
                "mean_breadth_positive30_pct_at_entries": round(
                    mean([r["breadth_positive30_pct"] for r in top1]), 4
                ) if top1 else None,
                "mean_alt30_pct_at_entries": round(
                    mean([r["alt_mean30_pct"] for r in top1]), 4
                ) if top1 else None,
                "mean_signal_concentration_at_entries": round(
                    mean([r["eligible_candidates_same_timestamp"] for r in top1]), 4
                ) if top1 else None,
                "mean_top1_continuation_pct": round(
                    mean([r["cont_60m_pct"] for r in top1]), 4
                ) if top1 else None,
            },
            "regime_slices": summarized,
            "interpretation_rule": (
                "Diagnostic only. Do not choose a new filter from one block. "
                "Compare the same predeclared slices across all 3-day blocks; "
                "a useful regime variable must separate good/bad performance repeatedly."
            ),
            "lookahead_in_regime_variables": False,
            "generated_utc": utc_now(),
        }
    except Exception as e:
        return {
            **MODE_INFO,
            "status": "ERROR",
            "diagnostic": "V31_REGIME_DIAGNOSTIC_TOP1_60M",
            "v27_forward_untouched": True,
            "error": str(e),
            "generated_utc": utc_now(),
        }



# ============================================================
# V32 FORWARD CHALLENGER â€” FROZEN AFTER V31
# BTC-independent + Top1 + 60m confirmation + 120m hold
# Separate PostgreSQL state/table. V27 state/rules untouched.
# Research paper only; no trading/orders.
# ============================================================

V32_STATE = {
    "open": {},
    "closed": [],
    "seen_signal_keys": set(),
    "started_utc": utc_now(),
    "v59_counterfactual_pending": {},
    "v59_counterfactual_done": [],
}
V57_LEGACY_QUARANTINE = []
V32_LAST_SCAN = {
    "status": "NOT_RUN",
    "started_utc": None,
    "finished_utc": None,
    "error": None,
}
V32_AUTO_TASK = None
V32_COST_PCT = 0.15
V32_HOLD_MS = 120 * 60 * 1000
V32_SCAN_INTERVAL_SECONDS = 30
def v32_db_init():
    if not V21_DB_URL:
        return
    with v21_db_connect() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                CREATE TABLE IF NOT EXISTS alt_v32_paper_state (
                    id INTEGER PRIMARY KEY,
                    payload JSONB NOT NULL,
                    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
                )
            """)
        with conn.cursor() as cur:
            cur.execute("""
                CREATE TABLE IF NOT EXISTS alt_v54_scan_history (
                    id BIGSERIAL PRIMARY KEY,
                    captured_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    payload JSONB NOT NULL
                )
            """)
        conn.commit()

def v54_persist_scan_history(row):
    if not V21_DB_URL:
        return
    payload = json.dumps(row)
    with v21_db_connect() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO alt_v54_scan_history (captured_at, payload)
                VALUES (NOW(), %s::jsonb)
            """, (payload,))
            cur.execute("""
                DELETE FROM alt_v54_scan_history
                WHERE id NOT IN (
                    SELECT id FROM alt_v54_scan_history
                    ORDER BY id DESC
                    LIMIT 100
                )
            """)
        conn.commit()

def v54_load_scan_history(limit=20):
    if not V21_DB_URL:
        return []
    with v21_db_connect() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT payload
                FROM alt_v54_scan_history
                ORDER BY id DESC
                LIMIT %s
            """, (int(limit),))
            rows = cur.fetchall()
    out = []
    for row in reversed(rows):
        payload = row[0] if isinstance(row[0], dict) else json.loads(row[0])
        out.append(payload)
    return out

def v32_serializable_state():
    return {
        "open": V32_STATE["open"],
        "closed": V32_STATE["closed"],
        "seen_signal_keys": sorted(prune_seen_keys(V32_STATE["seen_signal_keys"])),
        "started_utc": V32_STATE["started_utc"],
        "v59_counterfactual_pending": V32_STATE.get("v59_counterfactual_pending", {}),
        "v59_counterfactual_done": V32_STATE.get("v59_counterfactual_done", []),
    }

def v32_save_state():
    if not V21_DB_URL:
        return
    payload = json.dumps(v32_serializable_state())
    with v21_db_connect() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO alt_v32_paper_state (id, payload, updated_at)
                VALUES (1, %s::jsonb, NOW())
                ON CONFLICT (id) DO UPDATE
                SET payload = EXCLUDED.payload, updated_at = NOW()
            """, (payload,))
        conn.commit()

def v32_load_state():
    if not V21_DB_URL:
        return
    with v21_db_connect() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT payload FROM alt_v32_paper_state WHERE id = 1")
            row = cur.fetchone()
    if not row:
        v32_save_state()
        return
    payload = row[0] if isinstance(row[0], dict) else json.loads(row[0])
    V32_STATE["open"] = payload.get("open", {})
    V32_STATE["closed"] = payload.get("closed", [])
    V32_STATE["seen_signal_keys"] = set(payload.get("seen_signal_keys", []))
    V32_STATE["started_utc"] = payload.get("started_utc", V32_STATE["started_utc"])
    V32_STATE["v59_counterfactual_pending"] = payload.get("v59_counterfactual_pending", {})
    V32_STATE["v59_counterfactual_done"] = payload.get("v59_counterfactual_done", [])

def v32_public_state():
    closed = V32_STATE["closed"]
    equity = 100.0
    peak = 100.0
    max_dd = 0.0
    for x in closed:
        equity *= (1.0 + float(x.get("net_pct", 0.0)) / 100.0)
        peak = max(peak, equity)
        if peak > 0:
            max_dd = min(max_dd, (equity / peak - 1.0) * 100.0)
    wins = sum(1 for x in closed if float(x.get("net_pct", 0.0)) > 0)
    return {
        "started_utc": V32_STATE["started_utc"],
        "open_count": len(V32_STATE["open"]),
        "closed_count": len(closed),
        "wins": wins,
        "win_rate_pct": round(100.0 * wins / len(closed), 2) if closed else None,
        "paper_equity": round(equity, 4),
        "max_drawdown_pct": round(max_dd, 4),
        "open_positions": list(V32_STATE["open"].values()),
        "recent_closed": closed[-20:],
    }

async def v53_market_snapshot_including_current(client):
    """V32-only snapshot including the current/in-progress 5m candle.
    The current candle OPEN is observable at the checkpoint and is the frozen
    historical paper-entry reference. V27 continues to use its completed-candle snapshot.
    """
    universe = await build_universe(client)
    semaphore = asyncio.Semaphore(V44_FETCH_CONCURRENCY)

    async def fetch(item):
        async with semaphore:
            try:
                candles = await v37_get_klines_including_current(
                    client, item["symbol"], V44_SNAPSHOT_CANDLES
                )
                return item["symbol"], candles, None
            except Exception as e:
                return item["symbol"], None, str(e)

    fetched = await asyncio.gather(*[fetch(x) for x in universe])
    good = {sym: c for sym, c, err in fetched if c}
    errors = [{"symbol": sym, "error": err} for sym, c, err in fetched if err]
    return good, errors, len(universe)


def v53_latest_checkpoint_row(candles, symbol):
    """Build the newest causal checkpoint row.

    Historical V15 semantics are signal_i -> start i+1 -> checkpoint i+13.
    Therefore, when the current/in-progress candle is entry_idx, signal_i is
    entry_idx-13. Only the current candle OPEN is used from the in-progress bar.
    """
    wait_bars = V15_WAIT_MINUTES // 5
    entry_idx = len(candles) - 1
    signal_i = entry_idx - (wait_bars + 1)
    if signal_i < 294 or entry_idx <= 0:
        return None

    completed = candles[:entry_idx]
    ret30, mus, sigmas = precompute_rolling_volatility_v12(completed, 288)
    if signal_i >= len(ret30):
        return None
    mom30 = ret30[signal_i]
    mu, sigma = mus[signal_i], sigmas[signal_i]
    if mom30 is None or mom30 <= 0 or mu is None or sigma is None or sigma <= 0:
        return None

    z = (mom30 - mu) / sigma
    signal_close = candles[signal_i]["close"]
    checkpoint_open = candles[entry_idx]["open"]
    continuation = pct_change(signal_close, checkpoint_open)
    return {
        "symbol": symbol,
        "signal_time_ms": candles[signal_i]["close_time"],
        "entry_open_time": candles[entry_idx]["open_time"],
        "momentum_30m_pct": mom30,
        "relative_momentum_z": z,
        "behavior": "CONTINUED_UP" if continuation >= 0.75 else "OTHER",
        "wait_end_change_pct": continuation,
        "checkpoint_open": checkpoint_open,
    }


async def v32_scan_once():
    import time as _t
    scan_started = _t.perf_counter()
    async with httpx.AsyncClient(timeout=httpx.Timeout(120.0)) as client:
        # V53: V32 alone uses the current/in-progress 5m OPEN. V27 is untouched.
        good, errors, universe_count = await v53_market_snapshot_including_current(client)

        raw = []
        for sym, candles in good.items():
            if sym == "BTCUSDT":
                continue
            row = v53_latest_checkpoint_row(candles, sym)
            if row:
                raw.append(row)

        # Frozen cross-sectional percentile at the original signal timestamp.
        by_time = {}
        for e in raw:
            by_time.setdefault(e["signal_time_ms"], []).append(e)

        ranked = []
        for group in by_time.values():
            if len(group) < 5:
                continue
            ordered = sorted(group, key=lambda x: x["relative_momentum_z"])
            n = len(ordered)
            for idx, e in enumerate(ordered):
                row = dict(e)
                row["cross_section_percentile"] = idx / (n - 1) if n > 1 else 1.0
                row["cohort_size"] = n
                ranked.append(row)

        # Frozen selected-alt 30m mean at the ORIGINAL signal timestamp.
        snapshots = {}
        for sym, candles in good.items():
            if sym == "BTCUSDT":
                continue
            # Exclude the current/in-progress candle from market-history calculations.
            completed = candles[:-1]
            for i in range(6, len(completed)):
                t = completed[i]["close_time"]
                snapshots.setdefault(t, []).append(
                    pct_change(completed[i - 6]["close"], completed[i]["close"])
                )

        eligible = []
        for e in ranked:
            if e["relative_momentum_z"] < 1.0:
                continue
            if e["cross_section_percentile"] < 0.80:
                continue
            if e["behavior"] != "CONTINUED_UP":
                continue
            vals = snapshots.get(e["signal_time_ms"], [])
            if not vals:
                continue
            alt_mean = mean(vals)
            if alt_mean >= 0.5:
                continue
            row = dict(e)
            row["alt_market_mean_30m_pct"] = alt_mean
            eligible.append(row)

        # Frozen TOP1 selection per original signal cohort.
        cohort = {}
        for e in eligible:
            cohort.setdefault(e["signal_time_ms"], []).append(e)
        top1_rows = []
        for group in cohort.values():
            group = sorted(
                group,
                key=lambda x: (
                    x.get("wait_end_change_pct", 0.0),
                    x.get("relative_momentum_z", 0.0),
                    x.get("cross_section_percentile", 0.0),
                ),
                reverse=True,
            )
            if group:
                top1_rows.append(group[0])

        # V96 observation: cohort is now defined; safe even if observer fails.
        v96_record_top3(cohort, V32_STATE)

        now_ms_for_freshness = alt_now_ms()
        freshness_ms = 10 * 60 * 1000
        entry_funnel = {
            "top1_total": len(top1_rows), "fresh_last_10m": 0,
            "rejected_not_fresh": 0, "rejected_seen_key": 0,
            "rejected_symbol_already_open": 0, "rejected_cooldown": 0,
            "rejected_missing_entry_candle": 0, "quote_or_entry_attempted": 0,
            "rejected_delay_gt_120s": 0, "rejected_spread": 0,
            "rejected_other_entry_reason": 0, "accepted_entries": 0,
            "rejection_samples": [],
        }

        def _v46_reject(kind, e, reason=None, pos=None):
            entry_funnel[kind] += 1
            if len(entry_funnel["rejection_samples"]) < 25:
                row = {
                    "symbol": e.get("symbol"), "entry_open_time": e.get("entry_open_time"),
                    "z": round(float(e.get("relative_momentum_z", 0.0)), 4),
                    "percentile": round(float(e.get("cross_section_percentile", 0.0)), 4),
                    "continuation_60m_pct": round(float(e.get("wait_end_change_pct", 0.0)), 4),
                    "stage": kind,
                }
                if reason: row["reason"] = reason
                if pos and pos.get("entry_delay_seconds") is not None:
                    row["entry_delay_seconds"] = pos.get("entry_delay_seconds")
                if pos and pos.get("spread_pct") is not None:
                    row["spread_pct"] = pos.get("spread_pct")
                entry_funnel["rejection_samples"].append(row)

        new_entries = []
        for e in top1_rows:
            if now_ms_for_freshness - e["entry_open_time"] > freshness_ms:
                _v46_reject("rejected_not_fresh", e, "older than 10m freshness window")
                continue
            entry_funnel["fresh_last_10m"] += 1
            key = f'{e["symbol"]}:{e["entry_open_time"]}'
            if key in V32_STATE["seen_signal_keys"]:
                _v46_reject("rejected_seen_key", e, "key already processed in V47")
                continue
            if e["symbol"] in V32_STATE["open"]:
                _v46_reject("rejected_symbol_already_open", e, "symbol already open")
                continue

            prior_times = [int(x.get("entry_open_time", 0)) for x in V32_STATE["closed"] if x.get("symbol") == e["symbol"]]
            if prior_times and e["entry_open_time"] - max(prior_times) < 60 * 60 * 1000:
                _v46_reject("rejected_cooldown", e, "same-symbol cooldown <60m")
                continue

            price = e.get("checkpoint_open")
            if price is None:
                _v46_reject("rejected_missing_entry_candle", e, "current checkpoint open not found")
                continue

            pos = {
                "key": key, "strategy": "V32_TOP1_60M_NO_BTC_FORWARD_CHALLENGER",
                "symbol": e["symbol"], "signal_time_ms": e["signal_time_ms"],
                "entry_open_time": e["entry_open_time"], "entry_price": price,
                "exit_due_time": e["entry_open_time"] + V32_HOLD_MS,
                "relative_momentum_z": round(e["relative_momentum_z"], 4),
                "cross_section_percentile": round(e["cross_section_percentile"], 4),
                "cohort_size": e.get("cohort_size"),
                "continuation_60m_pct": round(e["wait_end_change_pct"], 4),
                "alt_market_mean_30m_pct": round(e["alt_market_mean_30m_pct"], 4),
                "status": "OPEN_PAPER", "execution_version": "V53_CAUSAL_CHECKPOINT_OPEN",
            }
            entry_funnel["quote_or_entry_attempted"] += 1
            ok_entry, skip_reason = await apply_live_entry(client, pos, price, V32_HOLD_MS)
            pos["execution_version"] = V55_EXECUTION_VERSION
            pos.update(v61_entry_context(good.get(e["symbol"]), good.get("BTCUSDT"), int(e["entry_open_time"]), pos.get("entry_price") or price))
            # V94: observational V62-style filter, no effect on entry/exit.
            pos.update(v94_candidate_tag(good.get(e["symbol"]), int(e["entry_open_time"]), pos.get("spread_pct"), pos.get("entry_live_ms")))
            # V95: observational momentum/volume filter; never blocks the paper entry.
            pos.update(v95_momentum_volume_tag(pos))
            # V87: informational V86 score only; never blocks the paper entry.
            pos.update(v87_quality_at_entry(good, e))
            pos["risk_engine"] = {
                "hard_stop_pct": V55_HARD_STOP_PCT,
                "trail_activate_pct": V55_TRAIL_ACTIVATE_PCT,
                "trail_distance_pct": V55_TRAIL_DISTANCE_PCT,
                "max_hold_minutes": 120,
            }
            pos["peak_bid"] = pos.get("live_bid") or pos.get("entry_price")
            pos["peak_gain_pct"] = round(pct_change(pos["entry_price"], pos["peak_bid"]), 4)
            pos["trailing_active"] = False
            if not ok_entry:
                reason_text = str(skip_reason or "")
                if "entry delay" in reason_text:
                    _v46_reject("rejected_delay_gt_120s", e, reason_text, pos)
                elif "spread" in reason_text:
                    _v46_reject("rejected_spread", e, reason_text, pos)
                else:
                    _v46_reject("rejected_other_entry_reason", e, reason_text, pos)
                alt_log_skip(V32_STATE, pos, skip_reason)
                V32_STATE["seen_signal_keys"].add(key)
                V47_SEEN_KEYS["V32"].add(key)
                continue

            V32_STATE["seen_signal_keys"].add(key)
            V47_SEEN_KEYS["V32"].add(key)
            V32_STATE["open"][e["symbol"]] = pos
            new_entries.append(pos)
            entry_funnel["accepted_entries"] += 1

        newly_closed = []
        for sym, pos in list(V32_STATE["open"].items()):
            candles_all = good.get(sym)
            is_v55 = pos.get("execution_version") == V55_EXECUTION_VERSION

            # V56 legacy cleanup:
            # a legacy position may disappear from the current volume-filtered universe.
            # If it is overdue, fetch its due candle directly instead of leaving it OPEN forever.
            if not candles_all and not is_v55 and alt_now_ms() >= int(pos.get("exit_due_time", 0)):
                legacy_exit_candle = await v56_fetch_legacy_exit_candle(
                    client, sym, int(pos["exit_due_time"])
                )
                if legacy_exit_candle is not None:
                    exit_price = legacy_exit_candle["open"]
                    gross = pct_change(pos["entry_price"], exit_price)
                    net = gross - V32_COST_PCT
                    closed = {
                        **pos,
                        "status": "CLOSED_PAPER",
                        "exit_open_time": legacy_exit_candle["open_time"],
                        "exit_price": exit_price,
                        "gross_pct": round(gross, 4),
                        "cost_pct": V32_COST_PCT,
                        "net_pct": round(net, 4),
                        "exit_reason": "V56_LEGACY_OVERDUE_CLEANUP",
                        "exit_execution_version": "V56_UNIVERSE_LEGACY_CLEANUP",
                    }
                    V32_STATE["closed"].append(closed)
                    del V32_STATE["open"][sym]
                    newly_closed.append(closed)
                else:
                    quarantined = {
                        **pos,
                        "status": "QUARANTINED_LEGACY_ORPHAN",
                        "quarantined_utc": utc_now(),
                        "quarantine_reason": "OVERDUE_LEGACY_NO_RECOVERABLE_EXIT_CANDLE",
                        "excluded_from_closed_pnl": True,
                        "quarantine_version": "V57_LEGACY_QUARANTINE",
                    }
                    V57_LEGACY_QUARANTINE.append(quarantined)
                    del V32_STATE["open"][sym]
                continue

            if not candles_all:
                continue
            candles = candles_all[:-1]

            # V55 applies only prospectively to positions opened by V55.
            if pos.get("execution_version") == V55_EXECUTION_VERSION:
                q = await live_quote(client, sym)
                if V32_STATE["open"].get(sym) is not pos:
                    continue  # V61: hizli izleyici bu pozisyonu zaten kapatti
                now_ms = alt_now_ms()
                exit_price = None
                exit_reason = None

                if q:
                    bid = float(q["bid"])
                    peak = max(float(pos.get("peak_bid") or pos["entry_price"]), bid)
                    pos["peak_bid"] = peak
                    pos["peak_gain_pct"] = round(pct_change(pos["entry_price"], peak), 4)

                    if pos["peak_gain_pct"] >= V55_TRAIL_ACTIVATE_PCT:
                        pos["trailing_active"] = True

                    hard_stop_price = float(pos["entry_price"]) * (1.0 - V55_HARD_STOP_PCT / 100.0)
                    pos["hard_stop_price"] = hard_stop_price

                    if bid <= hard_stop_price:
                        exit_price = bid
                        exit_reason = "HARD_STOP"
                    elif pos.get("trailing_active"):
                        trail_stop = peak * (1.0 - V55_TRAIL_DISTANCE_PCT / 100.0)
                        pos["trailing_stop_price"] = trail_stop
                        if bid <= trail_stop:
                            exit_price = bid
                            exit_reason = "TRAILING_STOP"

                    if exit_price is None and now_ms >= int(pos["exit_due_time"]):
                        exit_price = bid
                        exit_reason = "TIME_STOP_120M"

                # Quote failure fallback: preserve the max-120m close ability.
                if exit_price is None and now_ms >= int(pos["exit_due_time"]):
                    exit_candle = next((c for c in candles if c["open_time"] >= pos["exit_due_time"]), None)
                    if exit_candle is not None:
                        exit_price = exit_candle["open"]
                        exit_reason = "TIME_STOP_120M_FALLBACK"

                if exit_price is None:
                    continue

                gross = pct_change(pos["entry_price"], exit_price)
                net = gross - V32_COST_PCT
                closed = {
                    **pos,
                    "status": "CLOSED_PAPER",
                    "exit_open_time": now_ms,
                    "exit_price": exit_price,
                    "gross_pct": round(gross, 4),
                    "cost_pct": V32_COST_PCT,
                    "net_pct": round(net, 4),
                    "exit_reason": exit_reason,
                    "exit_execution_version": V55_EXECUTION_VERSION,
                }
                closed.update(v61_exit_diagnostics(closed, "SCAN"))
                V32_STATE["closed"].append(closed)
                del V32_STATE["open"][sym]
                newly_closed.append(closed)
                v59_register_counterfactual(closed)
                continue

            # Legacy V53 and earlier positions: do not retroactively change their exit rule.
            exit_candle = next((c for c in candles if c["open_time"] >= pos["exit_due_time"]), None)
            if exit_candle is None:
                continue
            exit_price = exit_candle["open"]
            gross = pct_change(pos["entry_price"], exit_price)
            net = gross - V32_COST_PCT
            closed = {**pos, "status": "CLOSED_PAPER", "exit_open_time": exit_candle["open_time"],
                      "exit_price": exit_price, "gross_pct": round(gross, 4),
                      "cost_pct": V32_COST_PCT, "net_pct": round(net, 4),
                      "exit_reason": "LEGACY_TIME_STOP_120M"}
            closed.update(shadow_stop_results(candles, closed, exit_candle, V32_COST_PCT))
            V32_STATE["closed"].append(closed)
            del V32_STATE["open"][sym]
            newly_closed.append(closed)

        V44_METRICS["v32_last_scan_seconds"] = round(_t.perf_counter() - scan_started, 3)
        V44_METRICS["v32_last_scan_utc"] = utc_now()
        return {
            "status": "OK", "universe_size": universe_count, "symbols_fetched": len(good),
            "eligible_rows_seen": len(eligible), "top1_rows_seen": len(top1_rows),
            "entry_funnel_v46": entry_funnel, "new_entries": new_entries,
            "newly_closed": newly_closed, "fetch_errors": errors,
            "execution_architecture": V55_EXECUTION_VERSION,
        }


# ============================================================
# V87 â€” V86 FORWARD QUALITY TAG (OBSERVATIONAL ONLY)
# Adds frozen V86 0..4 quality score to NEW V32 paper entries
# and Telegram ENTRY messages. Does NOT gate entries/exits.
# ============================================================
V87_V86_THRESHOLDS={
    "alt30":0.454094,
    "btc30":0.232574,
    "disp30":0.607174,
    "breakout_change_pct":0.807852,
}

def v87_quality_at_entry(good, e):
    """Causal V86 quality at the paper-entry timestamp; no future candles."""
    import bisect
    try:
        ts=int(e["entry_open_time"])
        btc=good.get("BTCUSDT") or []
        bt=[int(x["open_time"]) for x in btc]
        bi=bisect.bisect_left(bt,ts)-1
        if bi<6:
            return {"v86_score":None,"v86_label":"VERI_YETERSIZ"}

        btc30=pct_change(float(btc[bi-6]["close"]),float(btc[bi]["close"]))
        alt30s=[]
        for sym,candles in good.items():
            if sym=="BTCUSDT" or not candles:
                continue
            tt=[int(x["open_time"]) for x in candles]
            j=bisect.bisect_left(tt,ts)-1
            if j>=6:
                alt30s.append(pct_change(float(candles[j-6]["close"]),float(candles[j]["close"])))
        if len(alt30s)<2:
            return {"v86_score":None,"v86_label":"VERI_YETERSIZ"}

        alt30=sum(alt30s)/len(alt30s)
        disp30=statistics.pstdev(alt30s)
        breakout=float(e.get("breakout_change_pct") if e.get("breakout_change_pct") is not None
                       else e.get("breakout_change") if e.get("breakout_change") is not None
                       else 0.0)
        vals={"alt30":alt30,"btc30":btc30,"disp30":disp30,
              "breakout_change_pct":breakout}
        passed={k:bool(vals[k]>=V87_V86_THRESHOLDS[k]) for k in V87_V86_THRESHOLDS}
        score=sum(passed.values())
        label="YUKSEK" if score>=3 else ("ORTA" if score==2 else "DUSUK")
        return {
            "v86_score":score,"v86_label":label,
            "v86_factors":{k:round(vals[k],6) for k in vals},
            "v86_pass":passed,"v86_version":"V86_FROZEN_4_FACTOR",
            "v86_observational_only":True,
        }
    except Exception as ex:
        return {"v86_score":None,"v86_label":"HATA","v86_quality_error":str(ex),
                "v86_observational_only":True}

def v87_quality_lines(p):
    q=p.get("v86_score")
    if q is None:
        return [f"V86 Kalite: {p.get('v86_label','VERI_YETERSIZ')} (giris filtresi degil)"]
    chk=p.get("v86_pass") or {}
    names=[("disp30","DISP30"),("alt30","ALT30"),("btc30","BTC30"),
           ("breakout_change_pct","BREAKOUT")]
    detail=" | ".join(f"{label}:{'OK' if chk.get(k) else 'X'}" for k,label in names)
    return [f"V86 Kalite: {q}/4 - {p.get('v86_label')} (gozlemsel)",detail]

async def v32_notify(result):
    notes = []
    open_count = len(V32_STATE["open"])

    for p in result.get("new_entries", []):
        msg = alt_entry_text(
            "ALT V32 PAPER GIRIS (Top1, BTC filtresi yok)",
            p,
            [
                f"Z: {p['relative_momentum_z']} | yuzdelik: {p['cross_section_percentile']} (kohort: {p.get('cohort_size')})",
                f"60dk devam: {p['continuation_60m_pct']}%",
                f"ALT ort 30dk: {p['alt_market_mean_30m_pct']}%",
                *v87_quality_lines(p),
                f"Sert stop: {p['entry_price'] * (1 - V55_HARD_STOP_PCT / 100):.6g} (-%{V55_HARD_STOP_PCT}) | trailing: +%{V55_TRAIL_ACTIVATE_PCT} sonrasi tepeden %{V55_TRAIL_DISTANCE_PCT}",
            ],
            open_count,
        )
        notes.append(await alt_safe_send(msg, "ENTRY", p["symbol"]))

    for p in result.get("newly_closed", []):
        notes.append(await alt_safe_send(alt_exit_text("ALT V32 PAPER CIKIS", p), "EXIT", p["symbol"]))
    return notes


async def v32_run_once():
    V32_LAST_SCAN["status"] = "RUNNING"
    V32_LAST_SCAN["started_utc"] = utc_now()
    V32_LAST_SCAN["finished_utc"] = None
    V32_LAST_SCAN["error"] = None
    try:
        result = await v32_scan_once()
        # V48: retain the exact latest automatic/manual scan result for read-only visibility.
        V48_LAST_V32_RESULT["result"] = result
        V48_LAST_V32_RESULT["captured_utc"] = utc_now()
        _v51_captured = V48_LAST_V32_RESULT["captured_utc"]
        _v51_funnel = result.get("entry_funnel_v46") or {}
        _v54_row = {
            "captured_utc": _v51_captured,
            "status": result.get("status"),
            "universe_size": result.get("universe_size"),
            "eligible_rows_seen": result.get("eligible_rows_seen"),
            "top1_rows_seen": result.get("top1_rows_seen"),
            "entry_funnel": _v51_funnel,
            "new_entries": result.get("new_entries", []),
            "newly_closed_count": len(result.get("newly_closed", [])),
            "fetch_errors_count": len(result.get("fetch_errors", [])),
        }
        V51_V32_SCAN_HISTORY.append(_v54_row)
        del V51_V32_SCAN_HISTORY[:-20]
        if V21_DB_URL:
            v54_persist_scan_history(_v54_row)
        # V59: resolve due 120m counterfactual benchmarks before persistent save.
        result["v59_counterfactual_completed"] = await v59_process_counterfactuals()
        # V43 reliability: persist state before external notification.
        if V21_DB_URL:
            v32_save_state()
        notes = await v32_notify(result)
        result["telegram_notifications"] = notes
        V32_LAST_SCAN["status"] = result.get("status", "OK")
        return result
    except Exception as e:
        V32_LAST_SCAN["status"] = "ERROR"
        V32_LAST_SCAN["error"] = str(e)
        err = {"status": "ERROR", "error": str(e)}
        V48_LAST_V32_RESULT["result"] = err
        V48_LAST_V32_RESULT["captured_utc"] = utc_now()
        _v54_error_row = {
            "captured_utc": V48_LAST_V32_RESULT["captured_utc"],
            "status": "ERROR",
            "error": str(e),
        }
        V51_V32_SCAN_HISTORY.append(_v54_error_row)
        del V51_V32_SCAN_HISTORY[:-20]
        try:
            if V21_DB_URL:
                v54_persist_scan_history(_v54_error_row)
        except Exception:
            pass
        return err
    finally:
        V32_LAST_SCAN["finished_utc"] = utc_now()

async def v32_auto_loop():
    # Offset from V27 loop so the two broad-universe scans do not start together.
    await asyncio.sleep(30)
    while True:
        await v32_run_once()
        await asyncio.sleep(V32_SCAN_INTERVAL_SECONDS)

@app.on_event("startup")
async def v32_startup():
    global V32_AUTO_TASK
    try:
        if V21_DB_URL:
            v32_db_init()
            v32_load_state()
            V51_V32_SCAN_HISTORY[:] = v54_load_scan_history(20)
    except Exception as e:
        V32_LAST_SCAN["status"] = "DB_STARTUP_ERROR"
        V32_LAST_SCAN["error"] = str(e)
    V32_AUTO_TASK = asyncio.create_task(v32_auto_loop())

@app.on_event("shutdown")
async def v32_shutdown():
    try:
        if V21_DB_URL:
            v32_save_state()
    except Exception:
        pass

@app.get("/v32-status")
async def v32_status():
    return {
        **MODE_INFO,
        "status": "OK",
        "strategy": "V32_TOP1_60M_NO_BTC_FORWARD_CHALLENGER",
        "research_only": True,
        "trading": False,
        "orders": False,
        "v27_forward_untouched": True,
        "frozen_rules": {
            "relative_momentum_z_min": 1.0,
            "cross_section_percentile_min": 0.80,
            "confirmation_minutes": 60,
            "continuation_threshold_pct": 0.75,
            "alt_market_mean_30m_max_pct": 0.5,
            "btc_filter": "NONE",
            "selection": "TOP1 by observed 60m continuation",
            "same_symbol_cooldown_minutes": 60,
            "round_trip_cost_pct": V32_COST_PCT,
            "exit_after_entry_minutes": 120,
        },
        "automation": {
            "enabled": True,
            "interval_seconds": V32_SCAN_INTERVAL_SECONDS,
            "startup_offset_seconds": 120,
            "last_scan": V32_LAST_SCAN,
        },
        "database": {
            "configured": bool(V21_DB_URL),
            "backend": "POSTGRESQL" if V21_DB_URL else "MEMORY_ONLY",
            "table": "alt_v32_paper_state",
        },
        "telegram": {
            "configured": bool(V22_TG_TOKEN and V22_TG_CHAT_ID),
            "entry_exit_notifications": True,
        },
        "paper": v32_public_state(),
        "generated_utc": utc_now(),
    }

@app.get("/v32-scan-now")
async def v32_scan_now():
    result = await v32_run_once()
    return {
        **MODE_INFO,
        "strategy": "V32_TOP1_60M_NO_BTC_FORWARD_CHALLENGER",
        "v27_forward_untouched": True,
        **result,
        "paper": v32_public_state(),
        "generated_utc": utc_now(),
    }



# ============================================================
# V33 READ-ONLY FORWARD COMPARISON PANEL
# Reads V27 + V32 paper state only. Does not scan, trade, or mutate strategy state.
# ============================================================

def v33_stats_from_closed(closed):
    vals = [float(x.get("net_pct", 0.0)) for x in closed]
    n = len(vals)
    if not n:
        return {
            "closed_count": 0, "wins": 0, "losses": 0, "win_rate_pct": None,
            "mean_net_pct": None, "median_net_pct": None, "profit_factor": None,
            "compound_return_pct": 0.0, "max_drawdown_pct": 0.0,
        }
    wins = sum(v > 0 for v in vals)
    losses = sum(v <= 0 for v in vals)
    gp = sum(v for v in vals if v > 0)
    gl = -sum(v for v in vals if v < 0)
    equity = 100.0
    peak = 100.0
    max_dd = 0.0
    for v in vals:
        equity *= 1.0 + v / 100.0
        peak = max(peak, equity)
        if peak > 0:
            max_dd = min(max_dd, (equity / peak - 1.0) * 100.0)
    ordered = sorted(vals)
    mid = n // 2
    med = ordered[mid] if n % 2 else (ordered[mid-1] + ordered[mid]) / 2.0
    return {
        "closed_count": n,
        "wins": wins,
        "losses": losses,
        "win_rate_pct": round(100.0 * wins / n, 2),
        "mean_net_pct": round(sum(vals) / n, 4),
        "median_net_pct": round(med, 4),
        "profit_factor": round(gp / gl, 4) if gl > 0 else (None if gp == 0 else "INF"),
        "compound_return_pct": round(equity - 100.0, 4),
        "max_drawdown_pct": round(max_dd, 4),
    }

def v33_v27_snapshot():
    # Existing V27/V21 state is read only.
    st = V20_STATE
    return {
        "label": "V27_BTC_BULL_CANDIDATE_B",
        "started_utc": st.get("started_utc"),
        "open_count": len(st.get("open", {})),
        "stats": v33_stats_from_closed(st.get("closed", [])),
        "open_positions": list(st.get("open", {}).values()),
        "recent_closed": st.get("closed", [])[-10:],
    }

def v33_v32_snapshot():
    st = V32_STATE
    return {
        "label": "V32_NO_BTC_TOP1_60M",
        "started_utc": st.get("started_utc"),
        "open_count": len(st.get("open", {})),
        "stats": v33_stats_from_closed(st.get("closed", [])),
        "open_positions": list(st.get("open", {}).values()),
        "recent_closed": st.get("closed", [])[-10:],
    }

@app.get("/v33-comparison")
async def v33_comparison():
    a = v33_v27_snapshot()
    b = v33_v32_snapshot()
    sa, sb = a["stats"], b["stats"]
    return {
        **MODE_INFO,
        "status": "OK",
        "panel": "V33_READ_ONLY_FORWARD_COMPARISON",
        "research_only": True,
        "trading": False,
        "orders": False,
        "mutates_v27": False,
        "mutates_v32": False,
        "v27": a,
        "v32": b,
        "difference_v32_minus_v27": {
            "closed_count": sb["closed_count"] - sa["closed_count"],
            "mean_net_pct": (
                round(sb["mean_net_pct"] - sa["mean_net_pct"], 4)
                if sb["mean_net_pct"] is not None and sa["mean_net_pct"] is not None else None
            ),
            "median_net_pct": (
                round(sb["median_net_pct"] - sa["median_net_pct"], 4)
                if sb["median_net_pct"] is not None and sa["median_net_pct"] is not None else None
            ),
            "win_rate_pct_points": (
                round(sb["win_rate_pct"] - sa["win_rate_pct"], 2)
                if sb["win_rate_pct"] is not None and sa["win_rate_pct"] is not None else None
            ),
            "compound_return_pct_points": round(
                sb["compound_return_pct"] - sa["compound_return_pct"], 4
            ),
        },
        "comparison_note": (
            "Descriptive forward-paper comparison only. Do not rank strategies from a very small "
            "number of closed trades. V27 and V32 have different selection rules and may have "
            "different trade counts."
        ),
        "generated_utc": utc_now(),
    }


# ============================================================
# V34 READ-ONLY WINNER / LOSER DIAGNOSTIC
# No strategy mutation, no scans, no orders.
# ============================================================

def v34_num(x):
    try:
        return float(x)
    except (TypeError, ValueError):
        return None

def v34_group_stats(rows, fields):
    out = {"n": len(rows)}
    for field in fields:
        vals = [v34_num(r.get(field)) for r in rows]
        vals = [v for v in vals if v is not None]
        if not vals:
            out[field] = {"mean": None, "median": None}
            continue
        vals2 = sorted(vals)
        n = len(vals2)
        med = vals2[n//2] if n % 2 else (vals2[n//2-1] + vals2[n//2]) / 2
        out[field] = {
            "mean": round(sum(vals)/len(vals), 4),
            "median": round(med, 4),
        }
    return out

def v34_diag(rows, continuation_field):
    closed = [r for r in rows if r.get("status") == "CLOSED_PAPER" and v34_num(r.get("net_pct")) is not None]
    winners = [r for r in closed if float(r["net_pct"]) > 0]
    losers = [r for r in closed if float(r["net_pct"]) <= 0]

    fields = [
        "relative_momentum_z",
        "cross_section_percentile",
        continuation_field,
        "alt_market_mean_30m_pct",
        "net_pct",
    ]
    # V27 has BTC fields; V32 intentionally does not use/store them.
    if any("btc_4h_pct" in r for r in closed):
        fields += ["btc_4h_pct", "btc_24h_pct"]

    return {
        "closed_count": len(closed),
        "winner_count": len(winners),
        "loser_or_flat_count": len(losers),
        "winner_stats": v34_group_stats(winners, fields),
        "loser_or_flat_stats": v34_group_stats(losers, fields),
        "warning": "Descriptive only. Small samples and outliers can dominate means; do not change thresholds from this panel alone."
    }

@app.get("/v34-diagnostic")
async def v34_diagnostic():
    return {
        "model": "ALT-MOMENTUM-V1",
        "mode": "RESEARCH_PAPER_ONLY",
        "status": "OK",
        "panel": "V34_READ_ONLY_WINNER_LOSER_DIAGNOSTIC",
        "trading": False,
        "orders": False,
        "mutates_v27": False,
        "mutates_v32": False,
        "runs_extra_market_scan": False,
        "v27": v34_diag(V20_STATE.get("closed", []), "wait_end_change_pct"),
        "v32": v34_diag(V32_STATE.get("closed", []), "continuation_60m_pct"),
        "interpretation_note": "Compare winner vs loser distributions only after enough forward-paper trades accumulate. This endpoint reads existing paper state and does not alter frozen rules."
    }


# ============================================================
# V35 FORWARD PULLBACK-ENTRY CHALLENGER (READS NEW V27 SIGNALS)
# Research/paper only. V27/V32 rules and state are not modified.
# Four predeclared variants share the same live reference-price observation:
#   30s / -0.25%, 30s / -0.50%, 60s / -0.25%, 60s / -0.50%
# A paper limit is considered filled when the sampled live Binance price is
# at or below the limit. Filled positions are held 120 minutes from fill.
# Fixed 0.15% round-trip research cost; no slippage/queue model.
# ============================================================

V35_VARIANTS = {
    "PB_30S_025": {"window_seconds": 30, "pullback_pct": 0.25},
    "PB_30S_050": {"window_seconds": 30, "pullback_pct": 0.50},
    "PB_60S_025": {"window_seconds": 60, "pullback_pct": 0.25},
    "PB_60S_050": {"window_seconds": 60, "pullback_pct": 0.50},
}
V35_COST_PCT = 0.15
V35_HOLD_SECONDS = 120 * 60
V35_POLL_SECONDS = 1.0
V35_DB_TABLE = "alt_v35_pullback_state"
V35_TASK = None
V35_LOCK = asyncio.Lock()
V35_STATE = {
    "started_utc": utc_now(),
    "seen_v27_keys": set(),
    "experiments": {},
    "last_error": None,
    "last_watch_utc": None,
}


def v35_ms_now():
    return int(datetime.now(timezone.utc).timestamp() * 1000)


def v35_iso_from_ms(ms):
    return datetime.fromtimestamp(ms / 1000.0, tz=timezone.utc).isoformat()


async def v35_live_price(client, symbol):
    data = await get_json(client, "/api/v3/ticker/price", params={"symbol": symbol})
    return float(data["price"])


def v35_db_init():
    if not V21_DB_URL:
        return
    with v21_db_connect() as conn:
        with conn.cursor() as cur:
            cur.execute(f"""
                CREATE TABLE IF NOT EXISTS {V35_DB_TABLE} (
                    id INTEGER PRIMARY KEY,
                    payload JSONB NOT NULL,
                    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
                )
            """)
        conn.commit()


def v35_serializable_state():
    return {
        "started_utc": V35_STATE["started_utc"],
        "seen_v27_keys": sorted(V35_STATE["seen_v27_keys"]),
        "experiments": V35_STATE["experiments"],
        "last_error": V35_STATE["last_error"],
        "last_watch_utc": V35_STATE["last_watch_utc"],
    }


def v35_save_state():
    if not V21_DB_URL:
        return
    payload = json.dumps(v35_serializable_state())
    with v21_db_connect() as conn:
        with conn.cursor() as cur:
            cur.execute(f"""
                INSERT INTO {V35_DB_TABLE} (id, payload, updated_at)
                VALUES (1, %s::jsonb, NOW())
                ON CONFLICT (id) DO UPDATE
                SET payload = EXCLUDED.payload, updated_at = NOW()
            """, (payload,))
        conn.commit()


def v35_load_state():
    if not V21_DB_URL:
        return False
    with v21_db_connect() as conn:
        with conn.cursor() as cur:
            cur.execute(f"SELECT payload FROM {V35_DB_TABLE} WHERE id = 1")
            row = cur.fetchone()
    if not row:
        return False
    payload = row[0]
    if isinstance(payload, str):
        payload = json.loads(payload)
    V35_STATE["started_utc"] = payload.get("started_utc", V35_STATE["started_utc"])
    V35_STATE["seen_v27_keys"] = set(payload.get("seen_v27_keys", []))
    V35_STATE["experiments"] = payload.get("experiments", {})
    V35_STATE["last_error"] = payload.get("last_error")
    V35_STATE["last_watch_utc"] = payload.get("last_watch_utc")
    return True


def v35_source_rows():
    rows = []
    rows.extend(V20_STATE.get("open", {}).values())
    rows.extend(V20_STATE.get("closed", []))
    return rows


def v35_source_by_key():
    return {str(x.get("key")): x for x in v35_source_rows() if x.get("key")}


async def v35_start_observation(source_pos):
    """Observe one NEW V27 forward signal for 60 seconds; no exchange order."""
    source_key = str(source_pos.get("key"))
    symbol = source_pos.get("symbol")
    if not source_key or not symbol:
        return

    async with V35_LOCK:
        if source_key in V35_STATE["experiments"]:
            return

    try:
        async with httpx.AsyncClient(timeout=httpx.Timeout(15.0)) as client:
            reference_price = await v35_live_price(client, symbol)
            start_ms = v35_ms_now()

            exp = {
                "source_v27_key": source_key,
                "symbol": symbol,
                "source_v27_entry_price": source_pos.get("entry_price"),
                "reference_price": reference_price,
                "observation_start_ms": start_ms,
                "observation_start_utc": v35_iso_from_ms(start_ms),
                "min_sampled_price": reference_price,
                "sample_count": 1,
                "observation_complete": False,
                "variants": {},
            }
            for name, cfg in V35_VARIANTS.items():
                exp["variants"][name] = {
                    **cfg,
                    "limit_price": reference_price * (1.0 - cfg["pullback_pct"] / 100.0),
                    "status": "WAITING_LIMIT_PAPER",
                    "fill_time_ms": None,
                    "fill_time_utc": None,
                    "fill_price": None,
                    "exit_due_ms": None,
                    "exit_time_ms": None,
                    "exit_time_utc": None,
                    "exit_price": None,
                    "gross_pct": None,
                    "cost_pct": V35_COST_PCT,
                    "net_pct": None,
                }

            async with V35_LOCK:
                V35_STATE["experiments"][source_key] = exp
            try:
                v35_save_state()
            except Exception:
                pass

            while True:
                elapsed = (v35_ms_now() - start_ms) / 1000.0
                if elapsed >= 60.0:
                    break
                await asyncio.sleep(V35_POLL_SECONDS)
                try:
                    price = await v35_live_price(client, symbol)
                except Exception:
                    continue
                now_ms = v35_ms_now()
                elapsed = (now_ms - start_ms) / 1000.0
                exp["sample_count"] += 1
                exp["min_sampled_price"] = min(exp["min_sampled_price"], price)

                for name, cfg in V35_VARIANTS.items():
                    v = exp["variants"][name]
                    if v["status"] != "WAITING_LIMIT_PAPER":
                        continue
                    if elapsed <= cfg["window_seconds"] and price <= v["limit_price"]:
                        v["status"] = "OPEN_PAPER"
                        v["fill_time_ms"] = now_ms
                        v["fill_time_utc"] = v35_iso_from_ms(now_ms)
                        # Paper limit fill at the declared limit, not at sampled lower price.
                        v["fill_price"] = v["limit_price"]
                        v["exit_due_ms"] = now_ms + V35_HOLD_SECONDS * 1000

                # Close the 30-second windows as soon as their window expires.
                if elapsed >= 30.0:
                    for name, cfg in V35_VARIANTS.items():
                        v = exp["variants"][name]
                        if cfg["window_seconds"] == 30 and v["status"] == "WAITING_LIMIT_PAPER":
                            v["status"] = "NO_FILL_CANCELLED"

            for name, cfg in V35_VARIANTS.items():
                v = exp["variants"][name]
                if v["status"] == "WAITING_LIMIT_PAPER":
                    v["status"] = "NO_FILL_CANCELLED"
            exp["observation_complete"] = True
            exp["observation_end_ms"] = v35_ms_now()
            exp["observation_end_utc"] = v35_iso_from_ms(exp["observation_end_ms"])
            try:
                v35_save_state()
            except Exception:
                pass
    except Exception as e:
        V35_STATE["last_error"] = f"observation {symbol}: {e}"
        try:
            v35_save_state()
        except Exception:
            pass


async def v35_close_due_positions():
    now_ms = v35_ms_now()
    due = []
    for exp in V35_STATE["experiments"].values():
        for name, v in exp.get("variants", {}).items():
            if v.get("status") == "OPEN_PAPER" and v.get("exit_due_ms") and now_ms >= int(v["exit_due_ms"]):
                due.append((exp, name, v))
    if not due:
        return 0

    closed_count = 0
    async with httpx.AsyncClient(timeout=httpx.Timeout(15.0)) as client:
        for exp, name, v in due:
            try:
                exit_price = await v35_live_price(client, exp["symbol"])
                exit_ms = v35_ms_now()
                gross = pct_change(float(v["fill_price"]), exit_price)
                net = gross - V35_COST_PCT
                v["status"] = "CLOSED_PAPER"
                v["exit_time_ms"] = exit_ms
                v["exit_time_utc"] = v35_iso_from_ms(exit_ms)
                v["exit_price"] = exit_price
                v["gross_pct"] = round(gross, 6)
                v["net_pct"] = round(net, 6)
                closed_count += 1
            except Exception as e:
                V35_STATE["last_error"] = f"exit {exp.get('symbol')} {name}: {e}"
    if closed_count:
        try:
            v35_save_state()
        except Exception:
            pass
    return closed_count


def v35_variant_stats(name):
    rows = []
    no_fill = 0
    waiting = 0
    open_count = 0
    source = v35_source_by_key()
    missed_source_nets = []

    for key, exp in V35_STATE["experiments"].items():
        v = exp.get("variants", {}).get(name)
        if not v:
            continue
        status = v.get("status")
        if status == "CLOSED_PAPER" and v.get("net_pct") is not None:
            rows.append(float(v["net_pct"]))
        elif status == "NO_FILL_CANCELLED":
            no_fill += 1
            src = source.get(key)
            if src and src.get("status") == "CLOSED_PAPER" and src.get("net_pct") is not None:
                missed_source_nets.append(float(src["net_pct"]))
        elif status == "OPEN_PAPER":
            open_count += 1
        else:
            waiting += 1

    fills_total = len(rows) + open_count
    decided = fills_total + no_fill
    wins = sum(x > 0 for x in rows)
    losses = sum(x <= 0 for x in rows)
    gp = sum(x for x in rows if x > 0)
    gl = -sum(x for x in rows if x < 0)
    vals_sorted = sorted(rows)
    med = None
    if vals_sorted:
        n = len(vals_sorted)
        med = vals_sorted[n//2] if n % 2 else (vals_sorted[n//2-1] + vals_sorted[n//2]) / 2.0

    return {
        "variant": name,
        **V35_VARIANTS[name],
        "signals_observed": len(V35_STATE["experiments"]),
        "decided_fill_or_no_fill": decided,
        "filled_total": fills_total,
        "fill_rate_pct": round(100.0 * fills_total / decided, 2) if decided else None,
        "no_fill_count": no_fill,
        "waiting_count": waiting,
        "open_count": open_count,
        "closed_count": len(rows),
        "wins": wins,
        "losses_or_flat": losses,
        "win_rate_pct": round(100.0 * wins / len(rows), 2) if rows else None,
        "mean_net_pct": round(mean(rows), 4) if rows else None,
        "median_net_pct": round(med, 4) if med is not None else None,
        "profit_factor": round(gp / gl, 4) if gl > 0 else (None if gp == 0 else "INF"),
        "missed_v27_closed_count": len(missed_source_nets),
        "missed_v27_mean_net_pct": round(mean(missed_source_nets), 4) if missed_source_nets else None,
        "missed_v27_win_rate_pct": round(100.0 * sum(x > 0 for x in missed_source_nets) / len(missed_source_nets), 2) if missed_source_nets else None,
    }


async def v35_watch_loop():
    # Baseline existing V27 history on first install so V35 starts cleanly forward.
    loaded = False
    try:
        if V21_DB_URL:
            v35_db_init()
            loaded = v35_load_state()
    except Exception as e:
        V35_STATE["last_error"] = f"DB startup: {e}"

    if not loaded:
        for row in v35_source_rows():
            if row.get("key"):
                V35_STATE["seen_v27_keys"].add(str(row["key"]))
        try:
            v35_save_state()
        except Exception:
            pass

    while True:
        try:
            V35_STATE["last_watch_utc"] = utc_now()
            # Only genuinely new V27 OPEN signals trigger the sub-minute experiment.
            for pos in list(V20_STATE.get("open", {}).values()):
                key = str(pos.get("key")) if pos.get("key") else None
                if not key or key in V35_STATE["seen_v27_keys"]:
                    continue
                V35_STATE["seen_v27_keys"].add(key)
                asyncio.create_task(v35_start_observation(dict(pos)))
            await v35_close_due_positions()
        except Exception as e:
            V35_STATE["last_error"] = f"watch loop: {e}"
        await asyncio.sleep(1.0)


@app.on_event("startup")
async def v35_startup():
    global V35_TASK
    V35_TASK = asyncio.create_task(v35_watch_loop())


@app.on_event("shutdown")
async def v35_shutdown():
    try:
        if V21_DB_URL:
            v35_save_state()
    except Exception:
        pass


@app.get("/v35-pullback-status")
async def v35_pullback_status():
    recent = sorted(
        V35_STATE["experiments"].values(),
        key=lambda x: x.get("observation_start_ms", 0),
        reverse=True,
    )[:20]
    return {
        "model": MODEL,
        "mode": "RESEARCH_PAPER_ONLY",
        "status": "OK",
        "panel": "V35_FORWARD_PULLBACK_ENTRY_CHALLENGER",
        "trading": False,
        "orders": False,
        "mutates_v27": False,
        "mutates_v32": False,
        "source_signal": "NEW V27 forward-paper entries only",
        "reference_price": "live Binance ticker price when V35 detects the new V27 entry",
        "sampling": "approximately 1 second REST sampling; a touch between samples can be missed",
        "exit_rule": "120 minutes from actual paper fill; live ticker sampled at/after due time",
        "round_trip_cost_pct": V35_COST_PCT,
        "slippage_queue_model": False,
        "started_utc": V35_STATE["started_utc"],
        "last_watch_utc": V35_STATE["last_watch_utc"],
        "last_error": V35_STATE["last_error"],
        "database_configured": bool(V21_DB_URL),
        "variants": {name: v35_variant_stats(name) for name in V35_VARIANTS},
        "recent_experiments": recent,
        "important_note": (
            "This is a fresh forward execution experiment. It must not be backfilled by simply adding "
            "0.25%/0.50% to old V27 returns because no-fill selection changes the trade set."
        ),
        "generated_utc": utc_now(),
    }


# ============================================================
# V36 READ-ONLY PAIRED COMPARISON PANEL
# Compares the same V27 source signal with V35 30s/-0.25% and 60s/-0.25%.
# No strategy/state mutation. Research/paper only.
# ============================================================

V36_VARIANTS = ("PB_30S_025", "PB_60S_025")


def v36_basic_stats(values):
    vals = [float(x) for x in values if x is not None]
    if not vals:
        return {
            "count": 0,
            "wins": 0,
            "losses_or_flat": 0,
            "win_rate_pct": None,
            "mean_net_pct": None,
            "median_net_pct": None,
            "profit_factor": None,
        }
    wins = [x for x in vals if x > 0]
    losses = [x for x in vals if x <= 0]
    gp = sum(wins)
    gl = -sum(x for x in vals if x < 0)
    return {
        "count": len(vals),
        "wins": len(wins),
        "losses_or_flat": len(losses),
        "win_rate_pct": round(100.0 * len(wins) / len(vals), 2),
        "mean_net_pct": round(mean(vals), 4),
        "median_net_pct": round(median(vals), 4),
        "profit_factor": round(gp / gl, 4) if gl > 0 else (None if gp == 0 else "INF"),
    }


def v36_variant_report(name, source):
    paired_filled = []
    no_fill_source = []
    pending_source = 0
    fills_total = 0
    no_fill_total = 0
    waiting_total = 0

    for key, exp in V35_STATE.get("experiments", {}).items():
        v = exp.get("variants", {}).get(name)
        if not v:
            continue
        status = v.get("status")
        src = source.get(str(key))

        if status in ("OPEN_PAPER", "CLOSED_PAPER"):
            fills_total += 1
        elif status == "NO_FILL_CANCELLED":
            no_fill_total += 1
        else:
            waiting_total += 1

        # Strict paired outcome: both V35 fill and its exact source V27 trade are closed.
        if (
            status == "CLOSED_PAPER"
            and v.get("net_pct") is not None
            and src
            and src.get("status") == "CLOSED_PAPER"
            and src.get("net_pct") is not None
        ):
            v35_net = float(v["net_pct"])
            v27_net = float(src["net_pct"])
            paired_filled.append({
                "key": str(key),
                "symbol": exp.get("symbol"),
                "v27_net_pct": v27_net,
                "v35_net_pct": v35_net,
                "v35_minus_v27_pct_points": v35_net - v27_net,
            })

        if status == "NO_FILL_CANCELLED":
            if src and src.get("status") == "CLOSED_PAPER" and src.get("net_pct") is not None:
                v27_net = float(src["net_pct"])
                no_fill_source.append({
                    "key": str(key),
                    "symbol": exp.get("symbol"),
                    "v27_net_pct": v27_net,
                })
            else:
                pending_source += 1

    paired_v27 = [x["v27_net_pct"] for x in paired_filled]
    paired_v35 = [x["v35_net_pct"] for x in paired_filled]
    improvements = [x["v35_minus_v27_pct_points"] for x in paired_filled]
    no_fill_nets = [x["v27_net_pct"] for x in no_fill_source]

    avoided_bad = sum(x < 0 for x in no_fill_nets)
    missed_good = sum(x > 0 for x in no_fill_nets)
    missed_flat = sum(x == 0 for x in no_fill_nets)

    decided = fills_total + no_fill_total
    return {
        "variant": name,
        **V35_VARIANTS[name],
        "signals_observed": len(V35_STATE.get("experiments", {})),
        "decided_fill_or_no_fill": decided,
        "filled_total": fills_total,
        "fill_rate_pct": round(100.0 * fills_total / decided, 2) if decided else None,
        "no_fill_total": no_fill_total,
        "waiting_total": waiting_total,
        "strict_paired_closed_count": len(paired_filled),
        "paired_same_source": {
            "v27_immediate_entry": v36_basic_stats(paired_v27),
            "v35_pullback_entry": v36_basic_stats(paired_v35),
            "v35_minus_v27_mean_pct_points": round(mean(improvements), 4) if improvements else None,
            "v35_minus_v27_median_pct_points": round(median(improvements), 4) if improvements else None,
            "v35_better_trade_count": sum(x > 0 for x in improvements),
            "v35_worse_trade_count": sum(x < 0 for x in improvements),
            "equal_trade_count": sum(x == 0 for x in improvements),
        },
        "no_fill_source_v27_outcomes": {
            "closed_source_count": len(no_fill_source),
            "source_still_pending_count": pending_source,
            "v27_stats": v36_basic_stats(no_fill_nets),
            "bad_v27_trades_avoided_count": avoided_bad,
            "good_v27_trades_missed_count": missed_good,
            "flat_v27_trades_missed_count": missed_flat,
            "bad_avoided_share_pct": round(100.0 * avoided_bad / len(no_fill_nets), 2) if no_fill_nets else None,
            "good_missed_share_pct": round(100.0 * missed_good / len(no_fill_nets), 2) if no_fill_nets else None,
        },
        "recent_strict_pairs": paired_filled[-10:],
    }


def v36_reference_timing_report():
    gaps = []
    latencies = []
    for exp in V35_STATE.get("experiments", {}).values():
        ref = exp.get("reference_price")
        src_entry = exp.get("source_v27_entry_price")
        if ref is not None and src_entry not in (None, 0):
            gaps.append(pct_change(float(src_entry), float(ref)))

        key = str(exp.get("source_v27_key") or "")
        start_ms = exp.get("observation_start_ms")
        # V27 key format is symbol:entry_open_time_ms.
        try:
            entry_ms = int(key.rsplit(":", 1)[1])
            if start_ms is not None:
                latencies.append((int(start_ms) - entry_ms) / 1000.0)
        except Exception:
            pass

    return {
        "reference_vs_v27_entry_gap_pct": {
            "count": len(gaps),
            "mean_pct": round(mean(gaps), 4) if gaps else None,
            "median_pct": round(median(gaps), 4) if gaps else None,
            "min_pct": round(min(gaps), 4) if gaps else None,
            "max_pct": round(max(gaps), 4) if gaps else None,
        },
        "v35_detection_latency_seconds_from_v27_entry_open_time": {
            "count": len(latencies),
            "mean_seconds": round(mean(latencies), 2) if latencies else None,
            "median_seconds": round(median(latencies), 2) if latencies else None,
            "min_seconds": round(min(latencies), 2) if latencies else None,
            "max_seconds": round(max(latencies), 2) if latencies else None,
        },
        "note": (
            "V35 limit levels are based on the live reference price when V35 detects the V27 entry, "
            "not on V27's theoretical 5m entry-open price. This diagnostic quantifies that timing/reference gap."
        ),
    }


@app.get("/v36-paired-comparison")
async def v36_paired_comparison():
    source = v35_source_by_key()
    return {
        "model": MODEL,
        "mode": "RESEARCH_PAPER_ONLY",
        "status": "OK",
        "panel": "V36_READ_ONLY_PAIRED_V27_V35_COMPARISON",
        "trading": False,
        "orders": False,
        "mutates_v27": False,
        "mutates_v32": False,
        "mutates_v35": False,
        "comparison": "V27 immediate paper entry vs V35 pullback entry on the exact same source signal",
        "variants": {name: v36_variant_report(name, source) for name in V36_VARIANTS},
        "reference_timing_diagnostic": v36_reference_timing_report(),
        "interpretation_guardrails": [
            "Strict paired results include only cases where both the V27 source and V35 filled trade are closed.",
            "No-fill analysis counts a bad V27 source as avoided and a good V27 source as missed; it does not invent a V35 return for an unfilled order.",
            "V35 exits use a live ticker sample at/after 120 minutes from actual fill, while V27 uses its own frozen paper exit model; paired differences therefore include entry timing and execution-model differences.",
            "Fixed research cost is 0.15%; slippage and queue position are not modeled.",
            "Approximately 1-second REST sampling can miss brief limit touches.",
        ],
        "v35_started_utc": V35_STATE.get("started_utc"),
        "generated_utc": utc_now(),
    }

# ============================================================
# V37 CLEAN FORWARD PULLBACK CHALLENGER
# Separate paper/research state. Does not mutate V27/V32/V35.
# Goal: remove the ~5-10 minute V35 detection delay by evaluating the
# frozen V27 CONTINUED_UP checkpoint from the CURRENT 5m candle OPEN.
# Then place a paper limit 0.25% below the live reference for 60 seconds.
# ============================================================

V37_DB_TABLE = "alt_v37_realtime_pullback_state"
V37_PULLBACK_PCT = 0.25
V37_WINDOW_SECONDS = 60
V37_HOLD_SECONDS = 120 * 60
V37_COST_PCT = 0.15
V37_POLL_SECONDS = 1.0
V37_SCAN_INTERVAL_SECONDS = 60
V37_LOCK = asyncio.Lock()
V37_STATE = {
    "started_utc": utc_now(),
    "seen_keys": set(),
    "experiments": {},
    "last_scan_started_utc": None,
    "last_scan_finished_utc": None,
    "last_scan_seconds": None,
    "last_universe_size": 0,
    "last_candidate_count": 0,
    "last_error": None,
}


def v37_serializable_state():
    return {
        **{k: v for k, v in V37_STATE.items() if k != "seen_keys"},
        "seen_keys": sorted(V37_STATE["seen_keys"]),
    }


def v37_db_init():
    if not V21_DB_URL:
        return
    with v21_db_connect() as conn:
        with conn.cursor() as cur:
            cur.execute(f"""
                CREATE TABLE IF NOT EXISTS {V37_DB_TABLE} (
                    id INTEGER PRIMARY KEY,
                    payload JSONB NOT NULL,
                    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
                )
            """)
        conn.commit()


def v37_save_state():
    if not V21_DB_URL:
        return
    payload = json.dumps(v37_serializable_state())
    with v21_db_connect() as conn:
        with conn.cursor() as cur:
            cur.execute(f"""
                INSERT INTO {V37_DB_TABLE} (id, payload, updated_at)
                VALUES (1, %s::jsonb, NOW())
                ON CONFLICT (id) DO UPDATE
                SET payload = EXCLUDED.payload, updated_at = NOW()
            """, (payload,))
        conn.commit()


def v37_load_state():
    if not V21_DB_URL:
        return False
    with v21_db_connect() as conn:
        with conn.cursor() as cur:
            cur.execute(f"SELECT payload FROM {V37_DB_TABLE} WHERE id = 1")
            row = cur.fetchone()
    if not row:
        return False
    p = row[0]
    if isinstance(p, str):
        p = json.loads(p)
    for k in V37_STATE:
        if k == "seen_keys":
            V37_STATE[k] = set(p.get(k, []))
        elif k in p:
            V37_STATE[k] = p[k]
    return True


async def v37_get_klines_including_current(client, symbol, limit=400):
    raw = await get_json(client, "/api/v3/klines", params={
        "symbol": symbol, "interval": "5m", "limit": limit
    })
    return [{
        "open_time": int(k[0]), "open": float(k[1]), "high": float(k[2]),
        "low": float(k[3]), "close": float(k[4]), "volume": float(k[5]),
        "close_time": int(k[6]),
    } for k in raw]


def v37_latest_candidate(candles, symbol):
    """Evaluate only the newest current-5m checkpoint, without waiting for it to close."""
    if len(candles) < 310:
        return None
    # Last row is the current/in-progress 5m candle. Its OPEN is known now.
    entry_idx = len(candles) - 1
    signal_i = entry_idx - (V15_WAIT_MINUTES // 5)
    if signal_i < 294:
        return None

    completed = candles[:entry_idx]  # excludes current candle for rolling history
    ret30, mus, sigmas = precompute_rolling_volatility_v12(completed, 288)
    if signal_i >= len(ret30):
        return None
    mom30 = ret30[signal_i]
    mu, sigma = mus[signal_i], sigmas[signal_i]
    if mom30 is None or mom30 <= 0 or mu is None or sigma is None or sigma <= 0:
        return None
    z = (mom30 - mu) / sigma
    signal_close = candles[signal_i]["close"]
    checkpoint_open = candles[entry_idx]["open"]
    continuation = pct_change(signal_close, checkpoint_open)
    if continuation < 0.75:
        return None
    return {
        "symbol": symbol,
        "signal_time_ms": candles[signal_i]["close_time"],
        "entry_open_time": candles[entry_idx]["open_time"],
        "relative_momentum_z": z,
        "wait_end_change_pct": continuation,
        "checkpoint_open": checkpoint_open,
    }


async def v37_observe_limit(candidate):
    key = f'{candidate["symbol"]}:{candidate["entry_open_time"]}'
    symbol = candidate["symbol"]
    async with V37_LOCK:
        if key in V37_STATE["experiments"]:
            return
    try:
        async with httpx.AsyncClient(timeout=httpx.Timeout(15.0)) as client:
            ref = await v35_live_price(client, symbol)
            start_ms = v35_ms_now()
            limit_price = ref * (1.0 - V37_PULLBACK_PCT / 100.0)
            latency_s = (start_ms - int(candidate["entry_open_time"])) / 1000.0
            exp = {
                "key": key, "symbol": symbol,
                "entry_open_time": candidate["entry_open_time"],
                "checkpoint_open": candidate["checkpoint_open"],
                "reference_price": ref,
                "observation_start_ms": start_ms,
                "observation_start_utc": v35_iso_from_ms(start_ms),
                "detection_latency_seconds": round(latency_s, 3),
                "relative_momentum_z": round(candidate["relative_momentum_z"], 4),
                "cross_section_percentile": round(candidate["cross_section_percentile"], 4),
                "wait_end_change_pct": round(candidate["wait_end_change_pct"], 4),
                "btc_4h_pct": round(candidate["btc_4h_pct"], 4),
                "btc_24h_pct": round(candidate["btc_24h_pct"], 4),
                "alt_market_mean_30m_pct": round(candidate["alt_market_mean_30m_pct"], 4),
                "pullback_pct": V37_PULLBACK_PCT,
                "window_seconds": V37_WINDOW_SECONDS,
                "limit_price": limit_price,
                "min_sampled_price": ref,
                "sample_count": 1,
                "status": "WAITING_LIMIT_PAPER",
                "fill_time_ms": None, "fill_time_utc": None, "fill_price": None,
                "exit_due_ms": None, "exit_time_ms": None, "exit_time_utc": None,
                "exit_price": None, "gross_pct": None, "cost_pct": V37_COST_PCT,
                "net_pct": None,
            }
            async with V37_LOCK:
                V37_STATE["experiments"][key] = exp
            try: v37_save_state()
            except Exception: pass

            while (v35_ms_now() - start_ms) / 1000.0 < V37_WINDOW_SECONDS:
                await asyncio.sleep(V37_POLL_SECONDS)
                try:
                    price = await v35_live_price(client, symbol)
                except Exception:
                    continue
                exp["sample_count"] += 1
                exp["min_sampled_price"] = min(exp["min_sampled_price"], price)
                if price <= limit_price:
                    now_ms = v35_ms_now()
                    exp["status"] = "OPEN_PAPER"
                    exp["fill_time_ms"] = now_ms
                    exp["fill_time_utc"] = v35_iso_from_ms(now_ms)
                    exp["fill_price"] = limit_price
                    exp["exit_due_ms"] = now_ms + V37_HOLD_SECONDS * 1000
                    break
            if exp["status"] == "WAITING_LIMIT_PAPER":
                exp["status"] = "NO_FILL_CANCELLED"
            try: v37_save_state()
            except Exception: pass
    except Exception as e:
        V37_STATE["last_error"] = f"observation {symbol}: {e}"


async def v37_close_due():
    now_ms = v35_ms_now()
    due = [x for x in V37_STATE["experiments"].values()
           if x.get("status") == "OPEN_PAPER" and x.get("exit_due_ms") and now_ms >= int(x["exit_due_ms"])]
    if not due:
        return
    async with httpx.AsyncClient(timeout=httpx.Timeout(15.0)) as client:
        for x in due:
            try:
                px = await v35_live_price(client, x["symbol"])
                t = v35_ms_now()
                gross = pct_change(float(x["fill_price"]), px)
                x["status"] = "CLOSED_PAPER"
                x["exit_time_ms"] = t
                x["exit_time_utc"] = v35_iso_from_ms(t)
                x["exit_price"] = px
                x["gross_pct"] = round(gross, 6)
                x["net_pct"] = round(gross - V37_COST_PCT, 6)
            except Exception as e:
                V37_STATE["last_error"] = f"exit {x.get('symbol')}: {e}"
    try: v37_save_state()
    except Exception: pass


async def v37_scan_once():
    started_ms = v35_ms_now()
    V37_STATE["last_scan_started_utc"] = utc_now()
    async with httpx.AsyncClient(timeout=httpx.Timeout(60.0)) as client:
        universe = await build_universe(client)
        V37_STATE["last_universe_size"] = len(universe)
        semaphore = asyncio.Semaphore(12)
        async def fetch(item):
            async with semaphore:
                try:
                    c = await v37_get_klines_including_current(client, item["symbol"], 400)
                    return item["symbol"], c
                except Exception:
                    return item["symbol"], None
        fetched = await asyncio.gather(*[fetch(x) for x in universe])
        good = {s: c for s, c in fetched if c}
        if "BTCUSDT" not in good:
            good["BTCUSDT"] = await v37_get_klines_including_current(client, "BTCUSDT", 400)

        raw = []
        for sym, candles in good.items():
            if sym == "BTCUSDT":
                continue
            e = v37_latest_candidate(candles, sym)
            if e:
                raw.append(e)

        # Cross-sectional percentile for the same original signal timestamp.
        by_time = {}
        for e in raw:
            by_time.setdefault(e["signal_time_ms"], []).append(e)
        ranked = []
        for group in by_time.values():
            if len(group) < 5:
                continue
            ordered = sorted(group, key=lambda x: x["relative_momentum_z"])
            n = len(ordered)
            for idx, e in enumerate(ordered):
                x = dict(e)
                x["cross_section_percentile"] = idx / (n - 1) if n > 1 else 1.0
                ranked.append(x)

        btc = good["BTCUSDT"]
        accepted = []
        for e in ranked:
            if e["relative_momentum_z"] < 1.0 or e["cross_section_percentile"] < 0.80:
                continue
            reg = btc_regime_at_v17(btc, e["signal_time_ms"])
            if not reg or reg["btc_trend"] != "BTC_BULL":
                continue
            vals = []
            for sym, c in good.items():
                if sym == "BTCUSDT":
                    continue
                # Find original signal candle by close_time; compute its 30m move.
                idx = next((j for j, q in enumerate(c) if q["close_time"] == e["signal_time_ms"]), None)
                if idx is not None and idx >= 6:
                    vals.append(pct_change(c[idx-6]["close"], c[idx]["close"]))
            if not vals:
                continue
            alt_mean = mean(vals)
            if alt_mean >= 0.5:
                continue
            x = dict(e)
            x["btc_4h_pct"] = reg["btc_4h_pct"]
            x["btc_24h_pct"] = reg["btc_24h_pct"]
            x["alt_market_mean_30m_pct"] = alt_mean
            accepted.append(x)

        V37_STATE["last_candidate_count"] = len(accepted)
        for e in accepted:
            key = f'{e["symbol"]}:{e["entry_open_time"]}'
            if key in V37_STATE["seen_keys"]:
                continue
            V37_STATE["seen_keys"].add(key)
            asyncio.create_task(v37_observe_limit(e))

    finished_ms = v35_ms_now()
    V37_STATE["last_scan_finished_utc"] = utc_now()
    V37_STATE["last_scan_seconds"] = round((finished_ms - started_ms) / 1000.0, 3)
    try: v37_save_state()
    except Exception: pass


async def v37_loop():
    loaded = False
    try:
        if V21_DB_URL:
            v37_db_init(); loaded = v37_load_state()
    except Exception as e:
        V37_STATE["last_error"] = f"DB startup: {e}"
    await asyncio.sleep(20)
    while True:
        try:
            # V43: V37 discovery scanning is retired to free Binance/API capacity
            # for the main V27/V32 low-latency forward branches.
            # Existing V37 paper positions are still allowed to close normally.
            await v37_close_due()
        except Exception as e:
            V37_STATE["last_error"] = f"close-only loop: {e}"
        await asyncio.sleep(V37_SCAN_INTERVAL_SECONDS)


@app.on_event("startup")
async def v37_startup():
    global V37_TASK
    V37_TASK = asyncio.create_task(v37_loop())


@app.on_event("shutdown")
async def v37_shutdown():
    try: v37_save_state()
    except Exception: pass


def v37_stats():
    rows = list(V37_STATE["experiments"].values())
    closed = [float(x["net_pct"]) for x in rows if x.get("status") == "CLOSED_PAPER" and x.get("net_pct") is not None]
    fills = [x for x in rows if x.get("status") in ("OPEN_PAPER", "CLOSED_PAPER")]
    nofills = [x for x in rows if x.get("status") == "NO_FILL_CANCELLED"]
    waiting = [x for x in rows if x.get("status") == "WAITING_LIMIT_PAPER"]
    lat = [float(x["detection_latency_seconds"]) for x in rows if x.get("detection_latency_seconds") is not None]
    gp = sum(x for x in closed if x > 0); gl = -sum(x for x in closed if x < 0)
    return {
        "signals_observed": len(rows), "filled_total": len(fills),
        "fill_rate_pct": round(100*len(fills)/len(rows),2) if rows else None,
        "no_fill_total": len(nofills), "waiting_total": len(waiting),
        "closed_count": len(closed), "win_rate_pct": round(100*sum(x>0 for x in closed)/len(closed),2) if closed else None,
        "mean_net_pct": round(mean(closed),4) if closed else None,
        "median_net_pct": round(median(closed),4) if closed else None,
        "profit_factor": round(gp/gl,4) if gl>0 else (None if gp==0 else "INF"),
        "detection_latency_seconds": {
            "count": len(lat), "mean": round(mean(lat),2) if lat else None,
            "median": round(median(lat),2) if lat else None,
            "min": round(min(lat),2) if lat else None, "max": round(max(lat),2) if lat else None,
        },
    }


@app.get("/v37-status")
async def v37_status():
    recent = sorted(V37_STATE["experiments"].values(), key=lambda x: x.get("observation_start_ms",0), reverse=True)[:20]
    return {
        **MODE_INFO, "status": "OK", "panel": "V37_CLEAN_REALTIME_PULLBACK_CHALLENGER",
        "trading": False, "orders": False,
        "mutates_v27": False, "mutates_v32": False, "mutates_v35": False,
        "frozen_rules": {
            "source_logic": "V27 Candidate B, evaluated at current 5m checkpoint OPEN",
            "pullback_pct": V37_PULLBACK_PCT, "limit_window_seconds": V37_WINDOW_SECONDS,
            "hold_minutes_from_fill": 120, "round_trip_cost_pct": V37_COST_PCT,
        },
        "scanner": {
            "interval_seconds": V37_SCAN_INTERVAL_SECONDS,
            "last_scan_started_utc": V37_STATE["last_scan_started_utc"],
            "last_scan_finished_utc": V37_STATE["last_scan_finished_utc"],
            "last_scan_seconds": V37_STATE["last_scan_seconds"],
            "last_universe_size": V37_STATE["last_universe_size"],
            "last_candidate_count": V37_STATE["last_candidate_count"],
            "last_error": V37_STATE["last_error"],
        },
        "stats": v37_stats(), "started_utc": V37_STATE["started_utc"],
        "recent_experiments": recent,
        "limitations": [
            "Paper/research only; no exchange orders.",
            "Current 5m candle OPEN is used only as the already-known 60m checkpoint price; future current-candle high/low/close are not used.",
            "Limit touch uses approximately 1-second REST sampling and can miss brief touches.",
            "Exit uses live ticker at/after 120 minutes from actual paper fill.",
            "Fixed 0.15% cost; slippage and queue position are not modeled.",
            "The key validation metric is V37 detection latency; results should not be interpreted until latency is materially below V35's ~460 seconds."
        ],
        "generated_utc": utc_now(),
    }

# ============================================================
# V38 READ-ONLY >= +2% SEPARATOR DIAGNOSTIC
# No strategy mutation. No market scan. No orders.
# Compares CLOSED_PAPER trades with net >= +2% vs net < +2%.
# V27, V32 and V37 are analyzed separately to avoid pseudo-replication.
# ============================================================

def v38_float(x):
    try:
        v = float(x)
        return v if math.isfinite(v) else None
    except (TypeError, ValueError):
        return None

def v38_quantile(vals, q):
    xs = sorted(vals)
    if not xs: return None
    if len(xs) == 1: return xs[0]
    pos = (len(xs)-1)*q
    lo, hi = int(math.floor(pos)), int(math.ceil(pos))
    if lo == hi: return xs[lo]
    return xs[lo] + (xs[hi]-xs[lo])*(pos-lo)

def v38_desc(vals):
    xs = [float(x) for x in vals if x is not None]
    if not xs:
        return {"n":0,"mean":None,"median":None,"q1":None,"q3":None,"iqr":None}
    q1, q3 = v38_quantile(xs,.25), v38_quantile(xs,.75)
    return {"n":len(xs),"mean":round(sum(xs)/len(xs),4),"median":round(v38_quantile(xs,.5),4),
            "q1":round(q1,4),"q3":round(q3,4),"iqr":round(q3-q1,4)}

def v38_mann_whitney(a,b):
    # Rank-sum with average ranks, tie-corrected normal approximation.
    # Cliff's delta gives direction/effect magnitude independent of p-value.
    a=[float(x) for x in a if x is not None]; b=[float(x) for x in b if x is not None]
    n1,n2=len(a),len(b)
    if n1==0 or n2==0:
        return {"u":None,"p_two_sided_approx":None,"cliffs_delta":None,"prob_superiority":None,"warning":"one group empty"}
    tagged=[(x,0) for x in a]+[(x,1) for x in b]
    tagged.sort(key=lambda z:z[0])
    ranks=[0.0]*len(tagged); tie_sizes=[]; i=0
    while i<len(tagged):
        j=i+1
        while j<len(tagged) and tagged[j][0]==tagged[i][0]: j+=1
        r=((i+1)+j)/2.0
        for k in range(i,j): ranks[k]=r
        if j-i>1: tie_sizes.append(j-i)
        i=j
    r1=sum(r for r,t in zip(ranks,tagged) if t[1]==0)
    u1=r1-n1*(n1+1)/2.0
    u2=n1*n2-u1
    u=min(u1,u2)
    N=n1+n2
    tie_term=sum(t**3-t for t in tie_sizes)
    var_u=n1*n2/12.0*((N+1)-(tie_term/(N*(N-1)) if N>1 else 0))
    z=(u1-n1*n2/2.0)/math.sqrt(var_u) if var_u>0 else 0.0
    p=math.erfc(abs(z)/math.sqrt(2.0))
    # delta >0 means >=2% group tends to have higher feature values.
    gt=sum(x>y for x in a for y in b); lt=sum(x<y for x in a for y in b)
    delta=(gt-lt)/(n1*n2)
    ps=(delta+1)/2.0
    return {"u":round(u,3),"p_two_sided_approx":round(p,6),"cliffs_delta":round(delta,4),
            "prob_superiority":round(ps,4),
            "warning":"Approximate p-value; interpret with effect size and sample counts, especially when >=2% group is small."}

def v38_dataset(rows, fields):
    closed=[r for r in rows if r.get("status")=="CLOSED_PAPER" and v38_float(r.get("net_pct")) is not None]
    hi=[r for r in closed if float(r["net_pct"])>=2.0]
    lo=[r for r in closed if float(r["net_pct"])<2.0]
    tests={}
    for field in fields:
        av=[v38_float(r.get(field)) for r in hi]; av=[x for x in av if x is not None]
        bv=[v38_float(r.get(field)) for r in lo]; bv=[x for x in bv if x is not None]
        mw=v38_mann_whitney(av,bv)
        tests[field]={"ge_2pct":v38_desc(av),"lt_2pct":v38_desc(bv),**mw,
                      "direction":"HIGHER_IN_GE_2" if mw.get("cliffs_delta") is not None and mw["cliffs_delta"]>0 else
                                  ("LOWER_IN_GE_2" if mw.get("cliffs_delta") is not None and mw["cliffs_delta"]<0 else "NO_DIRECTION")}
    ranked=sorted(
        [{"field":k,"cliffs_delta":v.get("cliffs_delta"),"abs_cliffs_delta":abs(v.get("cliffs_delta")) if v.get("cliffs_delta") is not None else None,
          "p_two_sided_approx":v.get("p_two_sided_approx"),"direction":v.get("direction")} for k,v in tests.items()],
        key=lambda x:(x["abs_cliffs_delta"] is not None, x["abs_cliffs_delta"] or -1), reverse=True)
    return {"closed_count":len(closed),"ge_2pct_count":len(hi),"lt_2pct_count":len(lo),
            "ge_2pct_rate":round(100*len(hi)/len(closed),2) if closed else None,
            "tests":tests,"ranked_by_abs_effect_size":ranked}

@app.get("/v38-ge2-separator")
async def v38_ge2_separator():
    common=["relative_momentum_z","cross_section_percentile","alt_market_mean_30m_pct","btc_4h_pct","btc_24h_pct"]
    v27_fields=common+["wait_end_change_pct"]
    v32_fields=["relative_momentum_z","cross_section_percentile","alt_market_mean_30m_pct","continuation_60m_pct"]
    v37_fields=common+["wait_end_change_pct","detection_latency_seconds"]
    return {
        "model":MODEL,"mode":"RESEARCH_PAPER_ONLY","status":"OK",
        "panel":"V38_READ_ONLY_GE2_SEPARATOR_DIAGNOSTIC","trading":False,"orders":False,
        "threshold_definition":"GE_2: net_pct >= +2.0%; LT_2: net_pct < +2.0% (includes 0..2 and negatives)",
        "mutates_v27":False,"mutates_v32":False,"mutates_v35":False,"mutates_v37":False,
        "runs_extra_market_scan":False,
        "v27":v38_dataset(V20_STATE.get("closed",[]),v27_fields),
        "v32":v38_dataset(V32_STATE.get("closed",[]),v32_fields),
        "v37":v38_dataset(list(V37_STATE.get("experiments",{}).values()),v37_fields),
        "v35_note":"Not pooled into V27/V32/V37 because V35 variants reuse V27 source signals and are not independent observations; use V36 paired comparison for V35.",
        "interpretation":"Look first for a sizable Cliff's delta with the SAME direction across independent forward branches, then consider p-values. This is diagnostic only; do not create a threshold from one small subgroup.",
        "generated_utc":utc_now(),
    }


# ============================================================
# V39 READ-ONLY CONTINUATION QUARTILE DIAGNOSTIC
# No strategy mutation. No market scan. No orders.
# Tests whether stronger 60m continuation is associated with
# monotonically better forward outcomes in V27 and V32.
# ============================================================

def v39_pf(values):
    wins = [x for x in values if x > 0]
    losses = [x for x in values if x < 0]
    gp = sum(wins)
    gl = -sum(losses)
    if gl > 0:
        return round(gp / gl, 4)
    return None if gp == 0 else "INF"


def v39_quartiles(rows, continuation_field):
    clean = []
    for r in rows:
        if r.get("status") != "CLOSED_PAPER":
            continue
        net = v38_float(r.get("net_pct"))
        cont = v38_float(r.get(continuation_field))
        if net is None or cont is None:
            continue
        clean.append({"net": net, "cont": cont})

    clean.sort(key=lambda x: x["cont"])
    n = len(clean)
    if n == 0:
        return {"n": 0, "continuation_field": continuation_field, "quartiles": [], "monotonic_checks": None}

    # Rank-based quartiles keep group sizes as balanced as possible and avoid
    # optimizing any numerical continuation threshold on this same sample.
    groups = [[] for _ in range(4)]
    for i, row in enumerate(clean):
        q = min(3, (i * 4) // n)
        groups[q].append(row)

    result = []
    for idx, g in enumerate(groups, start=1):
        nets = [x["net"] for x in g]
        conts = [x["cont"] for x in g]
        wins = sum(x > 0 for x in nets)
        ge2 = sum(x >= 2.0 for x in nets)
        result.append({
            "quartile": f"Q{idx}",
            "n": len(g),
            "continuation_min_pct": round(min(conts), 4) if conts else None,
            "continuation_max_pct": round(max(conts), 4) if conts else None,
            "continuation_median_pct": round(median(conts), 4) if conts else None,
            "ge_2pct_count": ge2,
            "ge_2pct_rate": round(100.0 * ge2 / len(g), 2) if g else None,
            "win_rate_pct": round(100.0 * wins / len(g), 2) if g else None,
            "mean_net_pct": round(mean(nets), 4) if nets else None,
            "median_net_pct": round(median(nets), 4) if nets else None,
            "profit_factor": v39_pf(nets),
        })

    def nondecreasing(field):
        vals = [x[field] for x in result]
        if any(v is None or isinstance(v, str) for v in vals):
            return None
        return all(vals[i] <= vals[i+1] for i in range(len(vals)-1))

    return {
        "n": n,
        "continuation_field": continuation_field,
        "quartile_method": "rank-based Q1..Q4; approximately equal counts; no optimized numeric threshold",
        "quartiles": result,
        "monotonic_checks": {
            "ge_2pct_rate_nondecreasing_Q1_to_Q4": nondecreasing("ge_2pct_rate"),
            "win_rate_nondecreasing_Q1_to_Q4": nondecreasing("win_rate_pct"),
            "mean_net_nondecreasing_Q1_to_Q4": nondecreasing("mean_net_pct"),
            "median_net_nondecreasing_Q1_to_Q4": nondecreasing("median_net_pct"),
            "profit_factor_nondecreasing_Q1_to_Q4": nondecreasing("profit_factor"),
        },
    }


@app.get("/v39-continuation-quartiles")
async def v39_continuation_quartiles():
    v27 = v39_quartiles(V20_STATE.get("closed", []), "wait_end_change_pct")
    v32 = v39_quartiles(V32_STATE.get("closed", []), "continuation_60m_pct")
    return {
        "model": MODEL,
        "mode": "RESEARCH_PAPER_ONLY",
        "status": "OK",
        "panel": "V39_READ_ONLY_CONTINUATION_QUARTILE_DIAGNOSTIC",
        "trading": False,
        "orders": False,
        "mutates_v27": False,
        "mutates_v32": False,
        "mutates_v35": False,
        "mutates_v37": False,
        "mutates_v38": False,
        "runs_extra_market_scan": False,
        "question": "Does stronger observed 60m continuation show a graded Q1->Q4 improvement in forward outcomes?",
        "v27": v27,
        "v32": v32,
        "interpretation_guardrail": "Diagnostic only. A monotonic pattern across both independent forward branches is more persuasive than one favorable quartile or one p-value; do not derive a new threshold from this panel alone.",
        "generated_utc": utc_now(),
    }

# ============================================================
# V40 - READ-ONLY Z x CONTINUATION TERTILE MATRIX DIAGNOSTIC
# ============================================================

def v40_num(x):
    try:
        return float(x)
    except (TypeError, ValueError):
        return None


def v40_tertile_labels(rows, field):
    valid = []
    for r in rows:
        v = v40_num(r.get(field))
        if v is not None:
            valid.append((v, r))
    valid.sort(key=lambda x: x[0])
    n = len(valid)
    out = {}
    labels = ["LOW", "MID", "HIGH"]
    for i, (v, r) in enumerate(valid):
        # Rank-based tertiles; deterministic and no numeric threshold optimization.
        bucket = min(2, (i * 3) // max(1, n))
        out[id(r)] = labels[bucket]
    return out


def v40_cell_stats(rows, continuation_field):
    if not rows:
        return {
            "n": 0,
            "ge_2pct_count": 0,
            "ge_2pct_rate": None,
            "win_rate_pct": None,
            "mean_net_pct": None,
            "median_net_pct": None,
            "profit_factor": None,
            "z_median": None,
            "continuation_median_pct": None,
            "alt_market_mean30_median_pct": None,
        }
    nets = [v40_num(r.get("net_pct")) for r in rows]
    nets = [x for x in nets if x is not None]
    zvals = [v40_num(r.get("relative_momentum_z")) for r in rows]
    zvals = [x for x in zvals if x is not None]
    cont = [v40_num(r.get(continuation_field)) for r in rows]
    cont = [x for x in cont if x is not None]
    alt = [v40_num(r.get("alt_market_mean_30m_pct")) for r in rows]
    alt = [x for x in alt if x is not None]
    wins = [x for x in nets if x > 0]
    losses = [x for x in nets if x < 0]
    gp = sum(wins)
    gl = abs(sum(losses))
    pf = (gp / gl) if gl > 0 else (None if gp > 0 else 0.0)
    ge2 = sum(1 for x in nets if x >= 2.0)
    return {
        "n": len(nets),
        "ge_2pct_count": ge2,
        "ge_2pct_rate": round(100.0 * ge2 / len(nets), 2) if nets else None,
        "win_rate_pct": round(100.0 * len(wins) / len(nets), 2) if nets else None,
        "mean_net_pct": round(mean(nets), 4) if nets else None,
        "median_net_pct": round(median(nets), 4) if nets else None,
        "profit_factor": round(pf, 4) if pf is not None else None,
        "z_median": round(median(zvals), 4) if zvals else None,
        "continuation_median_pct": round(median(cont), 4) if cont else None,
        "alt_market_mean30_median_pct": round(median(alt), 4) if alt else None,
    }


def v40_matrix(rows, continuation_field):
    usable = [r for r in rows if v40_num(r.get("net_pct")) is not None and v40_num(r.get("relative_momentum_z")) is not None and v40_num(r.get(continuation_field)) is not None]
    zlab = v40_tertile_labels(usable, "relative_momentum_z")
    clab = v40_tertile_labels(usable, continuation_field)
    order = ["LOW", "MID", "HIGH"]
    cells = []
    for c in order:
        for z in order:
            subset = [r for r in usable if clab.get(id(r)) == c and zlab.get(id(r)) == z]
            cells.append({
                "continuation_tertile": c,
                "z_tertile": z,
                **v40_cell_stats(subset, continuation_field),
            })
    # Descriptive ranking only, not a selection rule.
    ranked = sorted(
        cells,
        key=lambda x: (
            -1 if x["profit_factor"] is None else x["profit_factor"],
            -999 if x["median_net_pct"] is None else x["median_net_pct"],
        ),
        reverse=True,
    )
    return {
        "n": len(usable),
        "continuation_field": continuation_field,
        "binning": "Independent rank-based tertiles for Z and continuation; no optimized numeric thresholds.",
        "cells": cells,
        "descriptive_rank_by_profit_factor": ranked,
    }


@app.get("/v40-z-continuation-matrix")
async def v40_z_continuation_matrix():
    v27_rows = list(V20_STATE.get("closed", []))
    v32_rows = list(V32_STATE.get("closed", []))
    return {
        "model": MODEL,
        "mode": "RESEARCH_PAPER_ONLY",
        "status": "OK",
        "panel": "V40_READ_ONLY_Z_X_CONTINUATION_TERTILE_MATRIX",
        "trading": False,
        "orders": False,
        "mutates_v27": False,
        "mutates_v32": False,
        "mutates_v35": False,
        "mutates_v37": False,
        "mutates_v38": False,
        "mutates_v39": False,
        "runs_extra_market_scan": False,
        "question": "Are outcomes better when observed 60m continuation is strong but relative-momentum Z is not excessively high?",
        "v27": v40_matrix(v27_rows, "wait_end_change_pct"),
        "v32": v40_matrix(v32_rows, "continuation_60m_pct"),
        "interpretation_guardrail": "Exploratory interaction diagnostic only. Look for the same broad cell pattern in V27 and V32, adequate cell counts, positive median/mean and PF>1 together. Do not derive a new trading threshold from the best cell alone.",
        "generated_utc": utc_now(),
    }


# ============================================================
# V41 - PROSPECTIVE OVEREXTENSION VALIDATION (READ-ONLY)
# ============================================================
# Predeclared after V40:
# Test whether the HIGH-Z tertile is prospectively worse than
# LOW/MID-Z among NEW closed V27 and V32 trades.
#
# IMPORTANT:
# - V41 does not block or alter any trade.
# - Baseline is fixed at first V41 initialization.
# - Z cutpoints are frozen from the PRE-V41 closed sample.
# - Only trades not present at baseline are included later.
# - V27 and V32 are analyzed separately.
# ============================================================

V41_STATE = {
    "initialized": False,
    "initialized_utc": None,
    "v27_baseline_keys": [],
    "v32_baseline_keys": [],
    "v27_high_z_cut": None,
    "v32_high_z_cut": None,
}

def _v41_trade_key(t):
    return "|".join([
        str(t.get("symbol", "")),
        str(t.get("entry_open_time", t.get("entry_time_utc", ""))),
        str(t.get("exit_time_utc", "")),
    ])

def _v41_num(t, *fields):
    for field in fields:
        value = t.get(field)
        if value is not None:
            try:
                return float(value)
            except Exception:
                pass
    return None

def _v41_z(t):
    return _v41_num(t, "relative_momentum_z", "z", "z_score")

def _v41_cont_v27(t):
    return _v41_num(t, "wait_end_change_pct", "continuation_60m_pct")

def _v41_cont_v32(t):
    return _v41_num(t, "continuation_60m_pct", "wait_end_change_pct")

def _v41_rank_high_cut(trades):
    vals = sorted(v for v in (_v41_z(t) for t in trades) if v is not None)
    if not vals:
        return None
    # HIGH = upper third. Freeze this numeric cut at V41 initialization.
    idx = max(0, min(len(vals) - 1, (2 * len(vals)) // 3))
    return float(vals[idx])

def _v41_summary(rows):
    values = [_v41_num(x, "net_pct") for x in rows]
    values = [x for x in values if x is not None]
    if not values:
        return {
            "n": 0, "wins": 0, "losses": 0, "win_rate_pct": None,
            "ge_2pct_count": 0, "ge_2pct_rate": None,
            "mean_net_pct": None, "median_net_pct": None,
            "profit_factor": None,
        }
    wins = [x for x in values if x > 0]
    losses = [x for x in values if x < 0]
    gp = sum(wins)
    gl = abs(sum(losses))
    pf = (gp / gl) if gl > 0 else (None if gp > 0 else 0.0)
    ge2 = sum(1 for x in values if x >= 2.0)
    return {
        "n": len(values),
        "wins": len(wins),
        "losses": len(losses),
        "win_rate_pct": round(len(wins) / len(values) * 100.0, 2),
        "ge_2pct_count": ge2,
        "ge_2pct_rate": round(ge2 / len(values) * 100.0, 2),
        "mean_net_pct": round(mean(values), 4),
        "median_net_pct": round(median(values), 4),
        "profit_factor": round(pf, 4) if pf is not None else None,
    }

def _v41_branch_report(current, baseline_keys, high_cut, cont_fn):
    baseline = set(baseline_keys)
    new_rows = [t for t in current if _v41_trade_key(t) not in baseline]
    usable = [t for t in new_rows if _v41_z(t) is not None and _v41_num(t, "net_pct") is not None]
    high = [t for t in usable if _v41_z(t) >= high_cut] if high_cut is not None else []
    control = [t for t in usable if _v41_z(t) < high_cut] if high_cut is not None else []

    def cont_median(rows):
        vals = [cont_fn(t) for t in rows]
        vals = [v for v in vals if v is not None]
        return round(median(vals), 4) if vals else None

    hs = _v41_summary(high)
    cs = _v41_summary(control)

    return {
        "new_closed_since_v41_baseline": len(new_rows),
        "usable_with_z_and_net": len(usable),
        "frozen_high_z_cut": round(high_cut, 6) if high_cut is not None else None,
        "HIGH_Z_overextended_candidate": {
            **hs,
            "continuation_median_pct": cont_median(high),
        },
        "LOW_MID_Z_control": {
            **cs,
            "continuation_median_pct": cont_median(control),
        },
        "prospective_difference_HIGH_minus_CONTROL": {
            "mean_net_pct_points": (
                round(hs["mean_net_pct"] - cs["mean_net_pct"], 4)
                if hs["mean_net_pct"] is not None and cs["mean_net_pct"] is not None else None
            ),
            "median_net_pct_points": (
                round(hs["median_net_pct"] - cs["median_net_pct"], 4)
                if hs["median_net_pct"] is not None and cs["median_net_pct"] is not None else None
            ),
            "win_rate_pct_points": (
                round(hs["win_rate_pct"] - cs["win_rate_pct"], 2)
                if hs["win_rate_pct"] is not None and cs["win_rate_pct"] is not None else None
            ),
            "ge_2pct_rate_points": (
                round(hs["ge_2pct_rate"] - cs["ge_2pct_rate"], 2)
                if hs["ge_2pct_rate"] is not None and cs["ge_2pct_rate"] is not None else None
            ),
        },
    }

def _v41_db_init():
    """Create a separate V41 baseline table. Does not alter V27/V32 tables."""
    if not V21_DB_URL:
        return False
    with v21_db_connect() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                CREATE TABLE IF NOT EXISTS alt_v41_validation_state (
                    id INTEGER PRIMARY KEY,
                    payload JSONB NOT NULL,
                    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
                )
            """)
        conn.commit()
    return True


def _v41_save_baseline():
    if not V21_DB_URL:
        return False
    payload = json.dumps({
        "initialized": bool(V41_STATE["initialized"]),
        "initialized_utc": V41_STATE["initialized_utc"],
        "v27_baseline_keys": V41_STATE["v27_baseline_keys"],
        "v32_baseline_keys": V41_STATE["v32_baseline_keys"],
        "v27_high_z_cut": V41_STATE["v27_high_z_cut"],
        "v32_high_z_cut": V41_STATE["v32_high_z_cut"],
    })
    with v21_db_connect() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO alt_v41_validation_state (id, payload, updated_at)
                VALUES (1, %s::jsonb, NOW())
                ON CONFLICT (id) DO UPDATE
                SET payload = EXCLUDED.payload,
                    updated_at = NOW()
            """, (payload,))
        conn.commit()
    return True


def _v41_load_baseline():
    if not V21_DB_URL:
        return False
    _v41_db_init()
    with v21_db_connect() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT payload FROM alt_v41_validation_state WHERE id = 1")
            row = cur.fetchone()
    if not row:
        return False
    payload = row[0]
    if isinstance(payload, str):
        payload = json.loads(payload)
    V41_STATE["initialized"] = bool(payload.get("initialized", False))
    V41_STATE["initialized_utc"] = payload.get("initialized_utc")
    V41_STATE["v27_baseline_keys"] = list(payload.get("v27_baseline_keys", []))
    V41_STATE["v32_baseline_keys"] = list(payload.get("v32_baseline_keys", []))
    V41_STATE["v27_high_z_cut"] = payload.get("v27_high_z_cut")
    V41_STATE["v32_high_z_cut"] = payload.get("v32_high_z_cut")
    return bool(V41_STATE["initialized"])


def _v41_initialize_if_needed():
    if V41_STATE["initialized"]:
        return

    # First try to restore the frozen prospective baseline after a restart/deploy.
    try:
        if V21_DB_URL and _v41_load_baseline():
            return
    except Exception:
        # Endpoint remains available; if DB is temporarily unavailable we do NOT
        # overwrite an existing DB baseline here.
        raise

    v27_closed = list(V20_STATE.get("closed", []))
    v32_closed = list(V32_STATE.get("closed", []))

    V41_STATE["v27_baseline_keys"] = [_v41_trade_key(t) for t in v27_closed]
    V41_STATE["v32_baseline_keys"] = [_v41_trade_key(t) for t in v32_closed]
    V41_STATE["v27_high_z_cut"] = _v41_rank_high_cut(v27_closed)
    V41_STATE["v32_high_z_cut"] = _v41_rank_high_cut(v32_closed)
    V41_STATE["initialized_utc"] = utc_now()
    V41_STATE["initialized"] = True

    if V21_DB_URL:
        _v41_save_baseline()

@app.get("/v41-prospective-overextension")
async def v41_prospective_overextension():
    _v41_initialize_if_needed()

    v27_closed = list(V20_STATE.get("closed", []))
    v32_closed = list(V32_STATE.get("closed", []))

    return {
        **MODE_INFO,
        "status": "OK",
        "panel": "V41_PROSPECTIVE_OVEREXTENSION_VALIDATION",
        "trading": False,
        "orders": False,
        "mutates_v27": False,
        "mutates_v32": False,
        "mutates_v35": False,
        "mutates_v37": False,
        "mutates_v38": False,
        "mutates_v39": False,
        "mutates_v40": False,
        "runs_extra_market_scan": False,
        "predeclared_hypothesis": (
            "Among NEW forward closed trades after the V41 baseline, "
            "the frozen upper-third Z group (HIGH-Z / overextended candidate) "
            "will have worse outcomes than the LOW/MID-Z control group."
        ),
        "baseline": {
            "initialized_utc": V41_STATE["initialized_utc"],
            "v27_closed_at_baseline": len(V41_STATE["v27_baseline_keys"]),
            "v32_closed_at_baseline": len(V41_STATE["v32_baseline_keys"]),
            "persistent_postgresql": bool(V21_DB_URL),
            "database_table": "alt_v41_validation_state" if V21_DB_URL else None,
            "cut_method": (
                "Upper-third Z cut frozen from pre-V41 closed trades at first endpoint initialization; "
                "future trades do not move the cut."
            ),
        },
        "v27": _v41_branch_report(
            v27_closed,
            V41_STATE["v27_baseline_keys"],
            V41_STATE["v27_high_z_cut"],
            _v41_cont_v27,
        ),
        "v32": _v41_branch_report(
            v32_closed,
            V41_STATE["v32_baseline_keys"],
            V41_STATE["v32_high_z_cut"],
            _v41_cont_v32,
        ),
        "interpretation_guardrail": (
            "Prospective diagnostic only. Do not alter V27/V32 entry logic from early V41 results. "
            "Require adequate NEW sample size and broadly consistent deterioration in HIGH-Z across both branches."
        ),
        "generated_utc": utc_now(),
    }


# ============================================================
# V42: CANLI GENEL BAKIS + GUNLUK OZET
# ============================================================
ALT_DAILY_HOUR_UTC = int(os.getenv("DAILY_SUMMARY_HOUR_UTC", "17"))
ALT_DAILY_LAST_SENT = {"date": None}


def alt_daily_summary_payload():
    since = alt_now_ms() - 24 * 60 * 60 * 1000
    return {
        "since_utc": datetime.fromtimestamp(since / 1000, tz=timezone.utc).isoformat(),
        "v27": {
            "last_24h": alt_period_stats(V20_STATE["closed"], since),
            "open_count": len(V20_STATE["open"]),
            "all_time": alt_period_stats(V20_STATE["closed"], 0),
        },
        "v32": {
            "last_24h": alt_period_stats(V32_STATE["closed"], since),
            "open_count": len(V32_STATE["open"]),
            "all_time": alt_period_stats(V32_STATE["closed"], 0),
        },
    }


def alt_daily_summary_text():
    d = alt_daily_summary_payload()

    def block(name, x):
        a = x["last_24h"]
        if a.get("count", 0) == 0:
            return f"{name}: son 24 saatte kapanan islem yok (acik: {x['open_count']})"
        return (
            f"{name}: {a['count']} islem | kazanma %{a['win_rate_pct']} | "
            f"ort net {a['mean_net_pct']}% | bilesik {a['compounded_pct']}% | "
            f"en iyi {a['best_pct']}% / en kotu {a['worst_pct']}% | acik: {x['open_count']}"
        )

    return "ALT GUNLUK OZET (paper)\n" + block("V27", d["v27"]) + "\n" + block("V32", d["v32"])


# Durable, cross-process daily notification reservation.
# Claim BEFORE sending: at-most-one attempt per UTC date, even after restarts.
# A failed/uncertain send is deliberately not retried automatically to avoid duplicates.
def _alt_daily_claim_once(day):
    conn = _v612_db_conn()
    if conn is None:
        raise RuntimeError("Daily summary skipped: PostgreSQL is not configured")
    try:
        with conn:
            with conn.cursor() as cur:
                cur.execute("""
                    CREATE TABLE IF NOT EXISTS alt_daily_summary_delivery (
                        summary_date DATE PRIMARY KEY,
                        state TEXT NOT NULL,
                        claimed_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                        updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                        details TEXT
                    )
                """)
                cur.execute("""
                    INSERT INTO alt_daily_summary_delivery (summary_date, state)
                    VALUES (%s, 'CLAIMED')
                    ON CONFLICT (summary_date) DO NOTHING
                    RETURNING summary_date
                """, (day,))
                return cur.fetchone() is not None
    finally:
        conn.close()


def _alt_daily_record_result(day, state, details):
    conn = _v612_db_conn()
    if conn is None:
        return
    try:
        with conn:
            with conn.cursor() as cur:
                cur.execute("""
                    UPDATE alt_daily_summary_delivery
                    SET state = %s, details = %s, updated_at = NOW()
                    WHERE summary_date = %s
                """, (state, str(details)[:1500], day))
    finally:
        conn.close()


async def alt_daily_summary_loop():
    await asyncio.sleep(30)
    while True:
        try:
            now = datetime.now(timezone.utc)
            today = now.date()
            if now.hour >= ALT_DAILY_HOUR_UTC:
                claimed = await asyncio.to_thread(_alt_daily_claim_once, today)
                if claimed:
                    try:
                        result = await alt_safe_send(alt_daily_summary_text(), "DAILY", "-")
                        sent = result.get("sent") is True
                        state = "SENT" if sent else "FAILED_OR_UNKNOWN"
                        await asyncio.to_thread(_alt_daily_record_result, today, state, result)
                        print(f"ALT_DAILY_SUMMARY {today} {state}", flush=True)
                    except Exception as exc:
                        print(f"ALT_DAILY_SUMMARY {today} FAILED_OR_UNKNOWN: {exc}", flush=True)
                ALT_DAILY_LAST_SENT["date"] = today.isoformat()
        except Exception as exc:
            # Fail closed: never send without a successful durable claim.
            print(f"ALT_DAILY_SUMMARY claim error: {exc}", flush=True)
        await asyncio.sleep(300)


@app.on_event("startup")
async def alt_daily_summary_startup():
    asyncio.create_task(alt_daily_summary_loop())

@app.get("/alt-daily-summary")
async def alt_daily_summary():
    return {**MODE_INFO, "status": "OK", "summary": alt_daily_summary_payload(), "generated_utc": utc_now()}


@app.get("/alt-live-overview")
async def alt_live_overview():
    return {
        **MODE_INFO,
        "status": "OK",
        "settings": {
            "spread_max_pct": SPREAD_MAX_PCT,
            "use_live_entry": USE_LIVE_ENTRY,
            "hold_from_live_entry": HOLD_FROM_LIVE_ENTRY,
            "shadow_stops_pct": list(SHADOW_STOPS_PCT),
            "universe_cache_ttl_seconds": UNIVERSE_CACHE_TTL_SECONDS,
            "daily_summary_hour_utc": ALT_DAILY_HOUR_UTC,
        },
        "v27": {
            "open": list(V20_STATE["open"].values()),
            "skipped_recent": V20_STATE.get("skipped", [])[-20:],
            "seen_keys": len(V20_STATE["seen_signal_keys"]),
        },
        "v32": {
            "open": list(V32_STATE["open"].values()),
            "skipped_recent": V32_STATE.get("skipped", [])[-20:],
            "seen_keys": len(V32_STATE["seen_signal_keys"]),
        },
        "generated_utc": utc_now(),
    }


# =========================
# V43 STATUS
# =========================
@app.get("/v43-status")
async def v43_status():
    def _new_v43_rows(state):
        rows = list(state.get("open", {}).values()) + list(state.get("closed", []))
        return [x for x in rows if x.get("execution_version") == V43_EXECUTION_VERSION]

    def _latency(rows):
        vals = [float(x["entry_delay_seconds"]) for x in rows
                if x.get("entry_delay_seconds") is not None]
        if not vals:
            return {"n": 0, "mean_seconds": None, "median_seconds": None,
                    "max_seconds": None}
        return {
            "n": len(vals),
            "mean_seconds": round(mean(vals), 2),
            "median_seconds": round(median(vals), 2),
            "max_seconds": round(max(vals), 2),
        }

    v27_rows = _new_v43_rows(V20_STATE)
    v32_rows = _new_v43_rows(V32_STATE)
    return {
        **MODE_INFO,
        "status": "OK",
        "panel": "V43_LOW_LATENCY_EXECUTION",
        "research_only": True,
        "trading": False,
        "orders": False,
        "strategy_thresholds_changed": False,
        "execution_changes": {
            "v27_scan_interval_seconds": V21_SCAN_INTERVAL_SECONDS,
            "v32_scan_interval_seconds": V32_SCAN_INTERVAL_SECONDS,
            "candle_fetch_concurrency": 30,
            "max_entry_delay_seconds": V43_MAX_ENTRY_DELAY_SECONDS,
            "late_signals": "SKIP_PAPER_ENTRY",
            "live_entry_price": "BEST_ASK",
            "spread_max_pct": SPREAD_MAX_PCT,
            "v37_new_scans": False,
            "v37_existing_positions": "CLOSE_ONLY",
            "v32_persist_before_telegram": True,
        },
        "v43_process_started_utc": V43_STARTED_UTC,
        "v27_v43_rows": len(v27_rows),
        "v27_latency": _latency(v27_rows),
        "v32_v43_rows": len(v32_rows),
        "v32_latency": _latency(v32_rows),
        "note": (
            "Only rows tagged V43_LOW_LATENCY belong to the clean V43 execution "
            "cohort. Pre-V43 open/closed rows remain in PostgreSQL but are not "
            "counted in these latency statistics."
        ),
        "generated_utc": utc_now(),
    }


# =========================
# V44 STATUS
# =========================
@app.get("/v44-status")
async def v44_status():
    import time as _t
    age = None
    if _V44_SNAPSHOT["good"] is not None:
        age = round(max(0.0, _t.time() - _V44_SNAPSHOT["ts"]), 2)

    return {
        **MODE_INFO,
        "status": "OK",
        "panel": "V44_FAST_SHARED_MARKET_SNAPSHOT",
        "research_only": True,
        "trading": False,
        "orders": False,
        "strategy_thresholds_changed": False,
        "v43_late_entry_guard_still_active_seconds": V43_MAX_ENTRY_DELAY_SECONDS,
        "snapshot": {
            "candles_per_symbol": V44_SNAPSHOT_CANDLES,
            "approx_history_hours": round(V44_SNAPSHOT_CANDLES * 5 / 60, 1),
            "fetch_concurrency": V44_FETCH_CONCURRENCY,
            "ttl_seconds": V44_SNAPSHOT_TTL_SECONDS,
            "universe_count": _V44_SNAPSHOT["universe_count"],
            "symbols_fetched_ok": (
                len(_V44_SNAPSHOT["good"])
                if _V44_SNAPSHOT["good"] is not None else 0
            ),
            "error_count": len(_V44_SNAPSHOT["errors"]),
            "fetch_seconds": _V44_SNAPSHOT["fetch_seconds"],
            "snapshot_utc": _V44_SNAPSHOT["snapshot_utc"],
            "snapshot_age_seconds": age,
        },
        "scan_timing": dict(V44_METRICS),
        "design": (
            "V27 and V32 share one 400-candle snapshot. This replaces separate "
            "3-day full-universe downloads; entry thresholds and ranking rules "
            "are unchanged."
        ),
        "generated_utc": utc_now(),
    }


# =========================
# V45 READ-ONLY FUNNEL DIAGNOSTIC
# =========================
V45_VERSION = "V45_DIAGNOSTIC_FIX"


def _v45_rank_rows(good):
    raw = []
    for sym, candles in good.items():
        if sym == "BTCUSDT":
            continue
        rows = relative_candidates_forward_live(candles, sym)
        if rows:
            raw.extend(rows)

    by_time = {}
    for e in raw:
        by_time.setdefault(e["signal_time_ms"], []).append(e)

    ranked = []
    for group in by_time.values():
        if len(group) < 5:
            continue
        ordered = sorted(group, key=lambda x: x["relative_momentum_z"])
        n = len(ordered)
        for idx, e in enumerate(ordered):
            row = dict(e)
            row["cross_section_percentile"] = idx / (n - 1) if n > 1 else 1.0
            row["cohort_size"] = n
            ranked.append(row)
    return raw, ranked


def _v45_alt_snapshots(good):
    snapshots = {}
    for sym, candles in good.items():
        if sym == "BTCUSDT":
            continue
        for i in range(6, len(candles)):
            t = candles[i]["close_time"]
            snapshots.setdefault(t, []).append(
                pct_change(candles[i - 6]["close"], candles[i]["close"])
            )
    return snapshots


def _v45_recent(rows, newest_open_ms, minutes=10):
    freshness_ms = minutes * 60 * 1000
    return [
        e for e in rows
        if newest_open_ms - e["entry_open_time"] <= freshness_ms
    ]


@app.get("/v45-diagnostic")
async def v45_diagnostic():
    import time as _t
    started = _t.perf_counter()

    async with httpx.AsyncClient(timeout=httpx.Timeout(120.0)) as client:
        good, errors, universe_count, cache_hit = await v44_market_snapshot(client)

    btc = good.get("BTCUSDT")
    raw, ranked = _v45_rank_rows(good)
    snapshots = _v45_alt_snapshots(good)

    newest_open_ms = max(
        (c[-1]["open_time"] for c in good.values() if c),
        default=0,
    )

    z_ok = [e for e in ranked if e["relative_momentum_z"] >= 1.0]
    top20_ok = [e for e in z_ok if e["cross_section_percentile"] >= 0.80]
    continued_ok = [e for e in top20_ok if e["behavior"] == "CONTINUED_UP"]

    alt_ok = []
    for e in continued_ok:
        vals = snapshots.get(e["signal_time_ms"], [])
        if not vals:
            continue
        alt_mean = mean(vals)
        if alt_mean < 0.5:
            row = dict(e)
            row["alt_market_mean_30m_pct"] = alt_mean
            alt_ok.append(row)

    v27_btc_ok = []
    if btc:
        for e in alt_ok:
            reg = btc_regime_at_v17(btc, e["signal_time_ms"])
            if reg and reg["btc_trend"] == "BTC_BULL":
                row = dict(e)
                row["btc_4h_pct"] = reg["btc_4h_pct"]
                row["btc_24h_pct"] = reg["btc_24h_pct"]
                v27_btc_ok.append(row)

    v27_recent = _v45_recent(v27_btc_ok, newest_open_ms)
    v32_recent_pre_top1 = _v45_recent(alt_ok, newest_open_ms)

    # Exact V32 Top1 per original signal timestamp.
    cohort = {}
    for e in alt_ok:
        cohort.setdefault(e["signal_time_ms"], []).append(e)

    v32_top1 = []
    for group in cohort.values():
        ordered = sorted(
            group,
            key=lambda x: (
                x.get("wait_end_change_pct", 0.0),
                x.get("relative_momentum_z", 0.0),
                x.get("cross_section_percentile", 0.0),
            ),
            reverse=True,
        )
        if ordered:
            v32_top1.append(ordered[0])

    v32_recent = _v45_recent(v32_top1, newest_open_ms)

    def sample(rows, limit=10):
        out = []
        for e in sorted(rows, key=lambda x: x["entry_open_time"], reverse=True)[:limit]:
            out.append({
                "symbol": e["symbol"],
                "entry_open_time": e["entry_open_time"],
                "z": round(e["relative_momentum_z"], 4),
                "percentile": round(e["cross_section_percentile"], 4),
                "continuation_60m_pct": round(e["wait_end_change_pct"], 4),
                "alt_mean_30m_pct": (
                    round(e["alt_market_mean_30m_pct"], 4)
                    if "alt_market_mean_30m_pct" in e else None
                ),
            })
        return out

    return {
        **MODE_INFO,
        "status": "OK",
        "panel": V45_VERSION,
        "research_only": True,
        "trading": False,
        "orders": False,
        "strategy_thresholds_changed": False,
        "v32_len_function_bug_fixed": True,
        "lookback": {
            "candles_per_symbol": V44_SNAPSHOT_CANDLES,
            "note": (
                "400 completed 5m candles are retained because the frozen Z calculation "
                "uses a 288-bar rolling window plus 30m/60m observation bars; this is "
                "sufficient for live forward detection and preserves V44 speed."
            ),
        },
        "snapshot": {
            "universe_count": universe_count,
            "symbols_fetched_ok": len(good),
            "fetch_errors": len(errors),
            "cache_hit": cache_hit,
        },
        "funnel_all_observable_rows": {
            "raw_relative_rows": len(raw),
            "ranked_rows": len(ranked),
            "z_ge_1": len(z_ok),
            "top20_percentile": len(top20_ok),
            "continued_up": len(continued_ok),
            "alt_mean_lt_0_5": len(alt_ok),
            "v27_btc_bull": len(v27_btc_ok),
            "v32_top1": len(v32_top1),
        },
        "fresh_last_10m": {
            "v27_after_all_strategy_filters": len(v27_recent),
            "v32_before_top1": len(v32_recent_pre_top1),
            "v32_after_top1": len(v32_recent),
        },
        "recent_v27_candidates": sample(v27_recent),
        "recent_v32_candidates": sample(v32_recent),
        "diagnostic_seconds": round(_t.perf_counter() - started, 3),
        "note": (
            "Read-only diagnostic: no paper positions are opened or closed here. "
            "The <=120s live-entry guard remains in the real V27/V32 scanners."
        ),
        "generated_utc": utc_now(),
    }


@app.get("/v46-status")
async def v46_status():
    return {
        **MODE_INFO,
        "status": "OK",
        "panel": "V46_ENTRY_REJECTION_FUNNEL",
        "research_only": True,
        "trading": False,
        "orders": False,
        "strategy_thresholds_changed": False,
        "late_entry_guard_seconds": V43_MAX_ENTRY_DELAY_SECONDS,
        "spread_max_pct": SPREAD_MAX_PCT,
        "diagnostic_location": "/v32-scan-now -> entry_funnel_v46",
        "note": "V46 adds counters only; V27/V32 strategy decisions are unchanged.",
        "generated_utc": utc_now(),
    }


@app.get("/v47-status")
async def v47_status():
    return {
        **MODE_INFO,
        "status": "OK",
        "panel": "V47_SEEN_KEY_FIX",
        "research_only": True,
        "trading": False,
        "orders": False,
        "strategy_thresholds_changed": False,
        "late_entry_guard_seconds": V43_MAX_ENTRY_DELAY_SECONDS,
        "spread_max_pct": SPREAD_MAX_PCT,
        "v47_started_utc": V47_STARTED_UTC,
        "v47_seen_counts": {
            "v27": len(V47_SEEN_KEYS["V27"]),
            "v32": len(V47_SEEN_KEYS["V32"]),
        },
        "legacy_seen_keys_preserved": True,
        "note": (
            "V32 duplicate prevention uses a clean V47 seen namespace so legacy "
            "pre-V47 seen keys cannot block fresh V47 entry attempts. A V47 key "
            "is consumed only after a terminal live-entry decision. Strategy "
            "thresholds and the <=120s guard are unchanged."
        ),
        "generated_utc": utc_now(),
    }


@app.get("/v48-status")
async def v48_status():
    r = V48_LAST_V32_RESULT.get("result") or {}
    funnel = r.get("entry_funnel_v46")
    return {
        **MODE_INFO,
        "status": "OK",
        "panel": "V48_AUTO_SCAN_VISIBILITY",
        "research_only": True,
        "trading": False,
        "orders": False,
        "strategy_thresholds_changed": False,
        "late_entry_guard_seconds": V43_MAX_ENTRY_DELAY_SECONDS,
        "spread_max_pct": SPREAD_MAX_PCT,
        "v47_seen_counts": {
            "v27": len(V47_SEEN_KEYS["V27"]),
            "v32": len(V47_SEEN_KEYS["V32"]),
        },
        "last_v32_scan": {
            "captured_utc": V48_LAST_V32_RESULT.get("captured_utc"),
            "scan_status": r.get("status"),
            "universe_size": r.get("universe_size"),
            "eligible_rows_seen": r.get("eligible_rows_seen"),
            "top1_rows_seen": r.get("top1_rows_seen"),
            "entry_funnel": funnel,
            "new_entries_count": len(r.get("new_entries") or []),
            "new_entries": r.get("new_entries") or [],
            "fetch_errors": r.get("fetch_errors") or [],
        },
        "note": "Read-only latest V32 scan visibility, including automatic scans. Strategy and paper execution rules unchanged.",
        "generated_utc": utc_now(),
    }


@app.get("/v49-timing-diagnostic")
async def v49_timing_diagnostic():
    import time

    t0 = time.perf_counter()
    started_utc = utc_now()
    now_ms = int(time.time() * 1000)

    async with httpx.AsyncClient(timeout=30.0) as client:
        t_snap0 = time.perf_counter()
        good, errors, universe_count, cache_hit = await v44_market_snapshot(client)
        t_snap1 = time.perf_counter()

        if not good:
            return {
                **MODE_INFO,
                "status": "ERROR",
                "panel": "V49_TIMING_DIAGNOSTIC",
                "error": "No snapshot data",
                "fetch_errors": errors,
            }

        # Same V32 forward candidate construction and cross-sectional ranking.
        t_calc0 = time.perf_counter()
        all_rows = []
        for symbol, candles in good.items():
            if symbol == "BTCUSDT":
                continue
            all_rows.extend(relative_candidates_forward_live(candles, symbol))

        by_time = {}
        for e in all_rows:
            by_time.setdefault(e["signal_time_ms"], []).append(e)
        ranked = []
        for group in by_time.values():
            if len(group) < 5:
                continue
            ordered = sorted(group, key=lambda x: x["relative_momentum_z"])
            n = len(ordered)
            for idx, e in enumerate(ordered):
                row = dict(e)
                row["cross_section_percentile"] = idx / (n - 1) if n > 1 else 1.0
                row["cohort_size"] = n
                ranked.append(row)
        alt_snaps = {}
        for sym, candles in good.items():
            if sym == "BTCUSDT":
                continue
            for i in range(6, len(candles)):
                t = candles[i]["close_time"]
                alt_snaps.setdefault(t, []).append(
                    pct_change(candles[i - 6]["close"], candles[i]["close"])
                )

        eligible = []
        for e in ranked:
            if e.get("relative_momentum_z", -999) < 1.0:
                continue
            if e.get("cross_section_percentile", 0) < 0.80:
                continue
            if e.get("behavior") != "CONTINUED_UP":
                continue
            vals = alt_snaps.get(e.get("signal_time_ms"), [])
            if not vals:
                continue
            alt_mean = mean(vals)
            if alt_mean >= 0.5:
                continue
            row = dict(e)
            row["alt_market_mean_30m_pct"] = alt_mean
            eligible.append(row)

        # Same V32 Top1 rule per original signal timestamp.
        grouped = {}
        for e in eligible:
            grouped.setdefault(e["signal_time_ms"], []).append(e)
        top1 = []
        for _, rows in grouped.items():
            rows = sorted(
                rows,
                key=lambda x: (
                    float(x.get("wait_end_change_pct", -999)),
                    float(x.get("relative_momentum_z", -999)),
                    float(x.get("cross_section_percentile", -999)),
                ),
                reverse=True,
            )
            if rows:
                top1.append(rows[0])
        top1.sort(key=lambda x: x["entry_open_time"])
        t_calc1 = time.perf_counter()

        newest_completed_open_ms = max(
            (c[-1]["open_time"] for c in good.values() if c),
            default=None,
        )

        recent = []
        for e in top1[-12:]:
            entry_ms = int(e["entry_open_time"])
            signal_ms = int(e.get("signal_time_ms") or 0)
            delay_now = (now_ms - entry_ms) / 1000.0
            recent.append({
                "symbol": e["symbol"],
                "signal_time_ms": signal_ms,
                "entry_open_time": entry_ms,
                "signal_to_entry_open_seconds": round((entry_ms - signal_ms) / 1000.0, 3) if signal_ms else None,
                "entry_open_to_request_now_seconds": round(delay_now, 3),
                "z": round(float(e.get("relative_momentum_z", 0.0)), 4),
                "percentile": round(float(e.get("cross_section_percentile", 0.0)), 4),
                "continuation_60m_pct": round(float(e.get("wait_end_change_pct", 0.0)), 4),
            })

        # Fetch a live quote only for the newest Top1 candidate, read-only.
        quote_timing = None
        if top1:
            e = top1[-1]
            q0 = time.perf_counter()
            q = await live_quote(client, e["symbol"])
            q1 = time.perf_counter()
            quote_now_ms = int(time.time() * 1000)
            quote_timing = {
                "symbol": e["symbol"],
                "entry_open_time": e["entry_open_time"],
                "quote_seconds": round(q1 - q0, 4),
                "delay_at_quote_seconds": round((quote_now_ms - int(e["entry_open_time"])) / 1000.0, 3),
                "quote": q,
            }

    t1 = time.perf_counter()
    return {
        **MODE_INFO,
        "status": "OK",
        "panel": "V49_TIMING_DIAGNOSTIC",
        "research_only": True,
        "trading": False,
        "orders": False,
        "strategy_thresholds_changed": False,
        "late_entry_guard_seconds": V43_MAX_ENTRY_DELAY_SECONDS,
        "timing_seconds": {
            "snapshot": round(t_snap1 - t_snap0, 4),
            "candidate_and_ranking": round(t_calc1 - t_calc0, 4),
            "total": round(t1 - t0, 4),
        },
        "snapshot": {
            "cache_hit": cache_hit,
            "universe_count": universe_count,
            "symbols_ok": len(good),
            "fetch_errors_count": len(errors),
            "newest_completed_open_ms": newest_completed_open_ms,
        },
        "counts": {
            "raw_relative_rows": len(all_rows),
            "ranked_rows": len(ranked),
            "eligible_rows": len(eligible),
            "top1_rows": len(top1),
        },
        "recent_top1_timing": recent,
        "newest_top1_live_quote_timing": quote_timing,
        "interpretation_hint": (
            "Compare entry_open_to_request_now_seconds and delay_at_quote_seconds with "
            "the <=120s guard. signal_to_entry_open_seconds shows how entry_open_time "
            "is positioned relative to the original signal timestamp."
        ),
        "started_utc": started_utc,
        "generated_utc": utc_now(),
    }


@app.get("/v50-status")
async def v50_status():
    return {
        **MODE_INFO,
        "status": "OK",
        "panel": "V50_ACTIONABLE_TIME_FIX",
        "research_only": True,
        "trading": False,
        "orders": False,
        "strategy_thresholds_changed": False,
        "late_entry_guard_seconds": V43_MAX_ENTRY_DELAY_SECONDS,
        "spread_max_pct": SPREAD_MAX_PCT,
        "historical_entry_open_time_preserved": True,
        "delay_reference": "actionable_time_ms = observation checkpoint candle close_time + 1ms",
        "note": "Completed-candle forward timing correction only; frozen strategy thresholds unchanged.",
        "generated_utc": utc_now(),
    }



@app.get("/v96-top3-observe")
async def v96_top3_observe():
    return {
        **MODE_INFO,
        "status": "OK",
        "panel": "V96_TOP3_OBSERVATIONAL",
        "research_only": True,
        "trading": False,
        "orders": False,
        "strategy_thresholds_changed": False,
        "history_count": len(V96_TOP3_HISTORY),
        "history": list(V96_TOP3_HISTORY),
        "observer_error": V96_TOP3_LAST_ERROR,
        "note": "Observation only. No additional market calls, no fallback entries. RAM resets on restart.",
        "generated_utc": utc_now(),
    }

@app.get("/v51-status")
async def v51_status():
    history = list(V51_V32_SCAN_HISTORY)
    attempted = 0
    delay_rejected = 0
    spread_rejected = 0
    accepted = 0
    fresh = 0
    for row in history:
        f = row.get("entry_funnel") or {}
        fresh += int(f.get("fresh_last_10m", 0) or 0)
        attempted += int(f.get("quote_or_entry_attempted", 0) or 0)
        delay_rejected += int(f.get("rejected_delay_gt_120s", 0) or 0)
        spread_rejected += int(f.get("rejected_spread", 0) or 0)
        accepted += int(f.get("accepted_entries", 0) or 0)

    return {
        **MODE_INFO,
        "status": "OK",
        "panel": "V51_V32_LAST_20_SCAN_HISTORY",
        "research_only": True,
        "trading": False,
        "orders": False,
        "strategy_thresholds_changed": False,
        "late_entry_guard_seconds": V43_MAX_ENTRY_DELAY_SECONDS,
        "history_count": len(history),
        "aggregate_last_20": {
            "fresh_candidates": fresh,
            "quote_or_entry_attempted": attempted,
            "rejected_delay_gt_120s": delay_rejected,
            "rejected_spread": spread_rejected,
            "accepted_entries": accepted,
        },
        "history": history,
        "note": "Read-only RAM history. Keeps the last 20 completed V32 scans so an entry/rejection cannot disappear on the next scan.",
        "generated_utc": utc_now(),
    }


@app.get("/v52-status")
async def v52_status():
    return {
        **MODE_INFO,
        "status": "OK",
        "panel": "V52_LOW_LATENCY_CADENCE",
        "research_only": True,
        "trading": False,
        "orders": False,
        "strategy_thresholds_changed": False,
        "late_entry_guard_seconds": V43_MAX_ENTRY_DELAY_SECONDS,
        "v32_scan_interval_seconds": V32_SCAN_INTERVAL_SECONDS,
        "shared_snapshot_ttl_seconds": V44_SNAPSHOT_TTL_SECONDS,
        "note": "Execution cadence only: V32 30s cycle and 10s snapshot TTL. Signal thresholds and 120s guard unchanged.",
        "generated_utc": utc_now(),
    }


@app.get("/v53-status")
async def v53_status():
    return {
        **MODE_INFO,
        "status": "OK",
        "panel": "V53_CAUSAL_CHECKPOINT_OPEN",
        "research_only": True,
        "trading": False,
        "orders": False,
        "strategy_thresholds_changed": False,
        "late_entry_guard_seconds": V43_MAX_ENTRY_DELAY_SECONDS,
        "v32_scan_interval_seconds": V32_SCAN_INTERVAL_SECONDS,
        "checkpoint_source": "current/in-progress 5m candle OPEN",
        "historical_alignment": "signal_i -> checkpoint entry_idx = signal_i + 13",
        "v27_untouched": True,
        "note": "V32 forward architecture now evaluates the frozen CONTINUED_UP checkpoint from the observable current 5m OPEN; no wait for checkpoint candle close.",
        "generated_utc": utc_now(),
    }


@app.get("/v54-status")
async def v54_status():
    db_history = []
    db_error = None
    try:
        db_history = v54_load_scan_history(20) if V21_DB_URL else list(V51_V32_SCAN_HISTORY)
    except Exception as e:
        db_error = str(e)
        db_history = list(V51_V32_SCAN_HISTORY)

    latest = db_history[-1] if db_history else None
    return {
        **MODE_INFO,
        "status": "OK" if db_error is None else "DEGRADED",
        "panel": "V54_PERSISTENT_FORWARD_ENGINE",
        "research_only": True,
        "trading": False,
        "orders": False,
        "strategy_thresholds_changed": False,
        "late_entry_guard_seconds": V43_MAX_ENTRY_DELAY_SECONDS,
        "execution_version": "V53_CAUSAL_CHECKPOINT_OPEN",
        "persistent_v32_state": bool(V21_DB_URL),
        "persistent_seen_keys": len(V32_STATE.get("seen_signal_keys", set())),
        "open_positions": len(V32_STATE.get("open", {})),
        "closed_positions": len(V32_STATE.get("closed", [])),
        "persistent_history_count": len(db_history),
        "latest_scan": latest,
        "last_scan_runtime": V32_LAST_SCAN,
        "db_error": db_error,
        "note": "V53 signal logic unchanged. V54 persists V32 seen keys/state plus the latest scan heartbeat/history across Render restarts.",
        "generated_utc": utc_now(),
    }


@app.get("/v55-status")
async def v55_status():
    open_v55 = [p for p in V32_STATE.get("open", {}).values()
                if p.get("execution_version") == V55_EXECUTION_VERSION]
    closed_v55 = [p for p in V32_STATE.get("closed", [])
                  if p.get("execution_version") == V55_EXECUTION_VERSION]
    reasons = {}
    for p in closed_v55:
        r = p.get("exit_reason", "UNKNOWN")
        reasons[r] = reasons.get(r, 0) + 1

    return {
        **MODE_INFO,
        "status": "OK",
        "panel": "V55_RISK_EXIT_ENGINE",
        "research_only": True,
        "trading": False,
        "orders": False,
        "entry_strategy_thresholds_changed": False,
        "entry_architecture": "V53_CAUSAL_CHECKPOINT_OPEN",
        "risk_exit_rules": {
            "hard_stop_pct": V55_HARD_STOP_PCT,
            "trailing_activation_gain_pct": V55_TRAIL_ACTIVATE_PCT,
            "trailing_distance_from_peak_pct": V55_TRAIL_DISTANCE_PCT,
            "maximum_hold_minutes": 120,
            "paper_exit_price": "live bid when available",
        },
        "legacy_v53_positions_untouched": True,
        "v55_open_count": len(open_v55),
        "v55_closed_count": len(closed_v55),
        "v55_exit_reasons": reasons,
        "v55_open_positions": open_v55,
        "last_scan_runtime": V32_LAST_SCAN,
        "persistent_seen_keys": len(V32_STATE.get("seen_signal_keys", set())),
        "db_configured": bool(V21_DB_URL),
        "note": "Prospective V55 only: same frozen V53 entry logic; hard stop + activated trailing stop + 120m maximum hold. Existing V53 positions keep their legacy exit rule.",
        "generated_utc": utc_now(),
    }


@app.get("/v56-status")
async def v56_status():
    overdue_legacy = []
    now_ms = alt_now_ms()
    for p in V32_STATE.get("open", {}).values():
        if p.get("execution_version") != V55_EXECUTION_VERSION and now_ms >= int(p.get("exit_due_time", 0)):
            overdue_legacy.append({
                "symbol": p.get("symbol"),
                "key": p.get("key"),
                "exit_due_time": p.get("exit_due_time"),
            })

    universe_preview = []
    universe_error = None
    try:
        async with httpx.AsyncClient() as client:
            u = await build_universe(client)
            universe_preview = [x["symbol"] for x in u if x.get("base_asset") in {"AMD", "AMDB", "MVLL", "MVLLB"}]
            universe_count = len(u)
    except Exception as e:
        universe_count = None
        universe_error = str(e)

    return {
        **MODE_INFO,
        "status": "OK",
        "panel": "V56_UNIVERSE_LEGACY_CLEANUP",
        "research_only": True,
        "trading": False,
        "orders": False,
        "entry_strategy_thresholds_changed": False,
        "v55_risk_rules_changed": False,
        "new_non_alt_exclusions": ["AMD", "AMDB", "MVLL", "MVLLB"],
        "excluded_symbols_still_in_universe": universe_preview,
        "current_universe_count": universe_count,
        "universe_error": universe_error,
        "overdue_legacy_open_count": len(overdue_legacy),
        "overdue_legacy_open_positions": overdue_legacy,
        "v32_open_count": len(V32_STATE.get("open", {})),
        "v32_closed_count": len(V32_STATE.get("closed", [])),
        "last_scan_runtime": V32_LAST_SCAN,
        "db_configured": bool(V21_DB_URL),
        "note": "V56 changes only universe hygiene and overdue legacy cleanup. V55 hard stop/trailing/time-stop rules remain unchanged.",
        "generated_utc": utc_now(),
    }


@app.get("/v57-status")
async def v57_status():
    now_ms = alt_now_ms()
    overdue_legacy = []
    for p in V32_STATE.get("open", {}).values():
        if p.get("execution_version") != V55_EXECUTION_VERSION and now_ms >= int(p.get("exit_due_time", 0)):
            overdue_legacy.append({
                "symbol": p.get("symbol"),
                "key": p.get("key"),
                "exit_due_time": p.get("exit_due_time"),
            })

    return {
        **MODE_INFO,
        "status": "OK",
        "panel": "V57_LEGACY_QUARANTINE",
        "research_only": True,
        "trading": False,
        "orders": False,
        "entry_strategy_thresholds_changed": False,
        "v55_risk_rules_changed": False,
        "v56_universe_exclusions_retained": ["AMD", "AMDB", "MVLL", "MVLLB"],
        "overdue_legacy_open_count": len(overdue_legacy),
        "overdue_legacy_open_positions": overdue_legacy,
        "runtime_quarantine_count": len(V57_LEGACY_QUARANTINE),
        "runtime_quarantine": V57_LEGACY_QUARANTINE[-20:],
        "v32_open_count": len(V32_STATE.get("open", {})),
        "v32_closed_count": len(V32_STATE.get("closed", [])),
        "last_scan_runtime": V32_LAST_SCAN,
        "db_configured": bool(V21_DB_URL),
        "note": "Unrecoverable overdue pre-V55 legacy positions are removed from active-open state without inventing P/L and recorded in an audit quarantine. V55 entry/risk rules are unchanged.",
        "generated_utc": utc_now(),
    }



def v59_register_counterfactual(closed):
    if closed.get("execution_version") != V55_EXECUTION_VERSION:
        return
    if closed.get("exit_reason") not in ("HARD_STOP", "TRAILING_STOP"):
        return
    key = closed.get("key")
    if not key or closed.get("exit_due_time") is None or closed.get("entry_price") is None:
        return
    pending = V32_STATE.setdefault("v59_counterfactual_pending", {})
    done = V32_STATE.setdefault("v59_counterfactual_done", [])
    if key in pending or any(x.get("key") == key for x in done):
        return
    pending[key] = {
        "key": key,
        "symbol": closed.get("symbol"),
        "entry_price": closed.get("entry_price"),
        "entry_live_ms": closed.get("entry_live_ms"),
        "entry_open_time": closed.get("entry_open_time"),
        "exit_due_time": closed.get("exit_due_time"),
        "actual_exit_reason": closed.get("exit_reason"),
        "actual_exit_price": closed.get("exit_price"),
        "actual_net_pct": closed.get("net_pct"),
        "registered_utc": utc_now(),
    }

async def v59_process_counterfactuals():
    pending = V32_STATE.setdefault("v59_counterfactual_pending", {})
    done = V32_STATE.setdefault("v59_counterfactual_done", [])
    if not pending:
        return []
    now_ms = alt_now_ms()
    completed = []
    async with httpx.AsyncClient(timeout=20.0) as client:
        for key, item in list(pending.items()):
            due = int(item.get("exit_due_time", 0))
            if not due or now_ms < due:
                continue
            candle = await v56_fetch_legacy_exit_candle(client, item["symbol"], due)
            if candle is None:
                continue
            cf_price = candle["open"]
            gross = pct_change(float(item["entry_price"]), cf_price)
            cf_net = gross - V32_COST_PCT
            actual = float(item.get("actual_net_pct", 0.0))
            row = {
                **item,
                "counterfactual_exit_open_time": candle["open_time"],
                "counterfactual_exit_price": cf_price,
                "counterfactual_gross_pct": round(gross, 4),
                "counterfactual_net_pct": round(cf_net, 4),
                "risk_engine_advantage_pct": round(actual - cf_net, 4),
                "benchmark": "HOLD_TO_ORIGINAL_120M_DUE",
                "completed_utc": utc_now(),
                "version": "V59_COUNTERFACTUAL_EXIT",
            }
            done.append(row)
            del pending[key]
            completed.append(row)
    if len(done) > 500:
        del done[:-500]
    return completed

def v58_safe_stats(values):
    vals = [float(x) for x in values if x is not None]
    if not vals:
        return {"n":0,"wins":0,"losses":0,"win_rate_pct":None,"mean_net_pct":None,
                "median_net_pct":None,"profit_factor":None,"sum_net_pct":0.0,
                "best_net_pct":None,"worst_net_pct":None}
    a = sorted(vals); n = len(a); m = n // 2
    median = a[m] if n % 2 else (a[m-1] + a[m]) / 2
    wins = sum(x > 0 for x in vals); losses = sum(x < 0 for x in vals)
    gp = sum(x for x in vals if x > 0); gl = abs(sum(x for x in vals if x < 0))
    pf = gp / gl if gl > 0 else ("INF" if gp > 0 else None)
    return {
        "n":n,"wins":wins,"losses":losses,"win_rate_pct":round(100*wins/n,2),
        "mean_net_pct":round(sum(vals)/n,4),"median_net_pct":round(median,4),
        "profit_factor":round(pf,4) if isinstance(pf,(int,float)) else pf,
        "sum_net_pct":round(sum(vals),4),"best_net_pct":round(max(vals),4),
        "worst_net_pct":round(min(vals),4)
    }

@app.get("/v58-analysis")
async def v58_analysis():
    closed_all = list(V32_STATE.get("closed", []))
    v55_closed = [p for p in closed_all
                  if p.get("execution_version") == V55_EXECUTION_VERSION
                  and p.get("net_pct") is not None]
    v55_open = [p for p in V32_STATE.get("open", {}).values()
                if p.get("execution_version") == V55_EXECUTION_VERSION]

    by_reason = {}
    for reason in ("HARD_STOP","TRAILING_STOP","TIME_STOP_120M"):
        q = [p for p in v55_closed if p.get("exit_reason") == reason]
        by_reason[reason] = v58_safe_stats([p.get("net_pct") for p in q])

    def avg(field):
        v = [float(p[field]) for p in v55_closed if p.get(field) is not None]
        return round(sum(v)/len(v),4) if v else None

    return {
        **MODE_INFO,
        "status":"OK",
        "panel":"V58_READ_ONLY_ANALYSIS",
        "research_only":True,
        "trading":False,
        "orders":False,
        "read_only":True,
        "strategy_changed":False,
        "entry_rules_changed":False,
        "risk_exit_rules_changed":False,
        "v56_universe_cleanup_retained":True,
        "v57_legacy_quarantine_retained":True,
        "v55_forward":{
            "closed_count":len(v55_closed),
            "open_count":len(v55_open),
            "overall":v58_safe_stats([p.get("net_pct") for p in v55_closed]),
            "by_exit_reason":by_reason,
            "mean_entry_delay_seconds":avg("entry_delay_seconds"),
            "mean_entry_slippage_vs_candle_pct":avg("entry_slippage_vs_candle_pct"),
            "mean_continuation_60m_pct":avg("continuation_60m_pct"),
            "mean_relative_momentum_z":avg("relative_momentum_z")
        },
        "sample_guidance":{
            "minimum_first_review_closed":30,
            "preferred_review_closed":50,
            "ready_for_first_review":len(v55_closed)>=30,
            "ready_for_preferred_review":len(v55_closed)>=50
        },
        "data_integrity":{
            "quarantined_legacy_excluded_from_v55_stats":True,
            "legacy_pre_v55_excluded_from_v55_stats":True,
            "only_execution_version":V55_EXECUTION_VERSION
        },
        "last_scan_runtime":V32_LAST_SCAN,
        "generated_utc":utc_now()
    }


@app.get("/v59-analysis")
async def v59_analysis():
    pending = list(V32_STATE.get("v59_counterfactual_pending", {}).values())
    done = list(V32_STATE.get("v59_counterfactual_done", []))

    early_closed = [
        p for p in V32_STATE.get("closed", [])
        if p.get("execution_version") == V55_EXECUTION_VERSION
        and p.get("exit_reason") in ("HARD_STOP", "TRAILING_STOP")
    ]
    tracked = {x.get("key") for x in pending} | {x.get("key") for x in done}
    old_untracked = [p.get("key") for p in early_closed if p.get("key") not in tracked]

    def st(rows, field):
        return v58_safe_stats([r.get(field) for r in rows if r.get(field) is not None])

    by_reason = {}
    for reason in ("HARD_STOP", "TRAILING_STOP"):
        q = [r for r in done if r.get("actual_exit_reason") == reason]
        by_reason[reason] = {
            "n": len(q),
            "actual_risk_exit": st(q, "actual_net_pct"),
            "counterfactual_120m": st(q, "counterfactual_net_pct"),
            "mean_risk_engine_advantage_pct": (
                round(sum(float(r["risk_engine_advantage_pct"]) for r in q) / len(q), 4)
                if q else None
            ),
        }

    return {
        **MODE_INFO,
        "status": "OK",
        "panel": "V59_COUNTERFACTUAL_EXIT_ANALYSIS",
        "research_only": True,
        "trading": False,
        "orders": False,
        "read_only_analysis": True,
        "strategy_changed": False,
        "entry_rules_changed": False,
        "risk_exit_rules_changed": False,
        "benchmark": "Prospective HARD_STOP/TRAILING_STOP actual result versus same entry held to original 120m due time.",
        "pending_count": len(pending),
        "completed_count": len(done),
        "overall": {
            "actual_risk_exit": st(done, "actual_net_pct"),
            "counterfactual_120m": st(done, "counterfactual_net_pct"),
            "mean_risk_engine_advantage_pct": (
                round(sum(float(r["risk_engine_advantage_pct"]) for r in done) / len(done), 4)
                if done else None
            ),
        },
        "by_actual_exit_reason": by_reason,
        "pending": pending[-20:],
        "recent_completed": done[-20:],
        "pre_v59_early_exits_not_tracked_count": len(old_untracked),
        "pre_v59_early_exits_not_tracked_keys": old_untracked[-20:],
        "data_integrity_note": "Prospective only; no historical benchmark price is fabricated. V59 does not change entry or exit behavior.",
        "last_scan_runtime": V32_LAST_SCAN,
        "generated_utc": utc_now(),
    }


# ============================================================
# V60 - READ-ONLY ENTRY QUALITY / OVEREXTENSION RESEARCH
# No trading-rule changes. Uses only CLOSED V55 forward trades.
# Predeclared descriptive bands; not optimized thresholds.
# ============================================================

def v60_band(x, cuts, labels):
    if x is None:
        return "MISSING"
    x = float(x)
    for cut, label in zip(cuts, labels):
        if x < cut:
            return label
    return labels[-1]

def v60_group(rows, field, cuts, labels):
    groups = {label: [] for label in labels}
    groups["MISSING"] = []
    for r in rows:
        groups[v60_band(r.get(field), cuts, labels)].append(r)
    out = {}
    for label, q in groups.items():
        if not q:
            continue
        out[label] = {
            **v58_safe_stats([r.get("net_pct") for r in q]),
            "mean_z": round(sum(float(r["relative_momentum_z"]) for r in q if r.get("relative_momentum_z") is not None) /
                            max(1, sum(r.get("relative_momentum_z") is not None for r in q)), 4),
            "mean_continuation_60m_pct": round(sum(float(r["continuation_60m_pct"]) for r in q if r.get("continuation_60m_pct") is not None) /
                                               max(1, sum(r.get("continuation_60m_pct") is not None for r in q)), 4),
        }
    return out

@app.get("/v60-entry-quality")
async def v60_entry_quality():
    rows = [
        p for p in V32_STATE.get("closed", [])
        if p.get("execution_version") == V55_EXECUTION_VERSION
        and p.get("net_pct") is not None
    ]

    z_groups = v60_group(
        rows, "relative_momentum_z",
        [2.0, 3.0, 5.0],
        ["Z_1_TO_LT2", "Z_2_TO_LT3", "Z_3_TO_LT5", "Z_5_PLUS"]
    )
    cont_groups = v60_group(
        rows, "continuation_60m_pct",
        [1.5, 3.0, 5.0],
        ["CONT_LT1_5", "CONT_1_5_TO_LT3", "CONT_3_TO_LT5", "CONT_5_PLUS"]
    )

    cells = {}
    for r in rows:
        zb = v60_band(r.get("relative_momentum_z"), [2.0,3.0,5.0],
                      ["Z_1_TO_LT2","Z_2_TO_LT3","Z_3_TO_LT5","Z_5_PLUS"])
        cb = v60_band(r.get("continuation_60m_pct"), [1.5,3.0,5.0],
                      ["CONT_LT1_5","CONT_1_5_TO_LT3","CONT_3_TO_LT5","CONT_5_PLUS"])
        cells.setdefault(f"{zb}__{cb}", []).append(r)

    matrix = {
        k: v58_safe_stats([r.get("net_pct") for r in q])
        for k, q in cells.items()
    }

    winners = [r for r in rows if float(r.get("net_pct",0)) > 0]
    losers = [r for r in rows if float(r.get("net_pct",0)) <= 0]

    def means(q):
        def avg(field):
            vals=[float(r[field]) for r in q if r.get(field) is not None]
            return round(sum(vals)/len(vals),4) if vals else None
        return {
            "n":len(q),
            "mean_z":avg("relative_momentum_z"),
            "mean_continuation_60m_pct":avg("continuation_60m_pct"),
            "mean_percentile":avg("cross_section_percentile"),
            "mean_alt_market_30m_pct":avg("alt_market_mean_30m_pct"),
            "mean_entry_delay_seconds":avg("entry_delay_seconds"),
            "mean_slippage_pct":avg("entry_slippage_vs_candle_pct"),
        }

    return {
        **MODE_INFO,
        "status":"OK",
        "panel":"V60_READ_ONLY_ENTRY_QUALITY_RESEARCH",
        "research_only":True,
        "trading":False,
        "orders":False,
        "strategy_changed":False,
        "risk_rules_changed":False,
        "sample_size":len(rows),
        "overall":v58_safe_stats([r.get("net_pct") for r in rows]),
        "winner_vs_loser_features":{
            "winners":means(winners),
            "losers":means(losers)
        },
        "z_bands":z_groups,
        "continuation_bands":cont_groups,
        "z_x_continuation_cells":matrix,
        "guardrail":"Descriptive only. Bands were declared before viewing V60 output. Do not promote a filter from a tiny cell. Require larger forward sample and repeated directional pattern.",
        "next_review_at_closed":[30,50,100],
        "generated_utc":utc_now()
    }


# ============================================================
# V61
# ============================================================
import bisect
import random
from array import array

V61_ADMIN_KEY = os.getenv("V61_ADMIN_KEY", "").strip()
V61_FAST_ENABLED = os.getenv("V61_FAST_STOP_MONITOR", "1") == "1"
V61_FAST_INTERVAL_SECONDS = float(os.getenv("V61_FAST_STOP_INTERVAL_SECONDS", "3"))
V61_FAST = {
    "enabled": V61_FAST_ENABLED,
    "interval_seconds": V61_FAST_INTERVAL_SECONDS,
    "started_utc": None, "last_tick_utc": None, "ticks": 0, "errors": 0,
    "last_error": None, "positions_watched": 0, "closed_by_watcher": 0,
    "last_quote_latency_ms": None, "recent_closes": [],
}
V61_FAST_TASK = None


def _v61_check_key(key):
    if V61_ADMIN_KEY and key != V61_ADMIN_KEY:
        raise HTTPException(status_code=403, detail="Yetkisiz: key gerekli.")


# ---------------- giris baglami / cikis tanisi ----------------
def v61_entry_context(candles, btc_candles, entry_open_ms, entry_price):
    """Girisin verildigi ANDA bilinen bilgiler (ileri bakma yok)."""
    out = {}
    try:
        if not candles:
            return out
        prior = [c for c in candles if c["open_time"] < entry_open_ms]
        if len(prior) < 60:
            return out
        last12, last3 = prior[-12:], prior[-3:]
        prev = prior[-51:-3]
        hi = max(c["high"] for c in last12)
        lo = min(c["low"] for c in last12)
        ep = float(entry_price)
        out["ctx_dist_from_60m_high_pct"] = round(pct_change(hi, ep), 4)
        out["ctx_range_pos_60m"] = round((ep - lo) / (hi - lo), 4) if hi > lo else None
        v_recent = sum(c["volume"] for c in last3) / 3.0
        v_base = (sum(c["volume"] for c in prev) / len(prev)) if prev else 0.0
        out["ctx_vol_ratio_15m_vs_4h"] = round(v_recent / v_base, 3) if v_base > 0 else None
        out["ctx_up_candles_of_12"] = sum(1 for c in last12 if c["close"] > c["open"])
        trs = [(c["high"] - c["low"]) / c["close"] * 100 for c in last12 if c["close"] > 0]
        out["ctx_range_pct_5m_avg_1h"] = round(sum(trs) / len(trs), 4) if trs else None
        out["ctx_ret_30m_pct"] = round(pct_change(prior[-7]["close"], prior[-1]["close"]), 4)
        out["ctx_ret_60m_pct"] = round(pct_change(prior[-13]["close"], prior[-1]["close"]), 4)
        if len(prior) >= 49:
            out["ctx_ret_4h_pct"] = round(pct_change(prior[-49]["close"], prior[-1]["close"]), 4)
        if btc_candles:
            bp = [c for c in btc_candles if c["open_time"] < entry_open_ms]
            if len(bp) >= 49:
                out["ctx_btc_30m_pct"] = round(pct_change(bp[-7]["close"], bp[-1]["close"]), 4)
                out["ctx_btc_60m_pct"] = round(pct_change(bp[-13]["close"], bp[-1]["close"]), 4)
                out["ctx_btc_4h_pct"] = round(pct_change(bp[-49]["close"], bp[-1]["close"]), 4)
    except Exception:
        pass
    return out


def v61_exit_diagnostics(closed, monitor):
    out = {"stop_monitor": monitor}
    try:
        entry = float(closed["entry_price"])
        ep = float(closed["exit_price"])
        t0 = int(closed.get("entry_live_ms") or closed.get("entry_open_time") or 0)
        t1 = int(closed.get("exit_open_time") or 0)
        if t0 and t1 >= t0:
            out["hold_minutes"] = round((t1 - t0) / 60000.0, 2)
        reason = closed.get("exit_reason")
        if reason == "HARD_STOP":
            lvl = float(closed.get("hard_stop_price") or entry * (1 - V55_HARD_STOP_PCT / 100.0))
            out["stop_overshoot_pct"] = round((lvl - ep) / entry * 100.0, 4)
        elif reason == "TRAILING_STOP" and closed.get("trailing_stop_price"):
            lvl = float(closed["trailing_stop_price"])
            out["stop_overshoot_pct"] = round((lvl - ep) / entry * 100.0, 4)
    except Exception:
        pass
    return out


# ---------------- hizli stop izleyici ----------------
async def v61_fast_stop_once(client):
    open_pos = {
        sym: p for sym, p in V32_STATE["open"].items()
        if p.get("execution_version") == V55_EXECUTION_VERSION
    }
    V61_FAST["positions_watched"] = len(open_pos)
    if not open_pos:
        return []

    import time as _t
    t0 = _t.perf_counter()

    # V61.1: Bir gecersiz/legacy sembol tum toplu bookTicker istegini 400'e
    # dusurmesin. Ilk 400'de grubu ikiye bolerek sorunlu sembol(ler)i buluruz;
    # bunlari sadece FAST monitor icin RAM'de atlariz. Ana scanner/state'e dokunulmaz.
    if "invalid_quote_symbols" not in V61_FAST:
        V61_FAST["invalid_quote_symbols"] = []

    invalid = set(V61_FAST.get("invalid_quote_symbols") or [])
    syms = [sym for sym in open_pos if sym not in invalid]

    async def _fetch_bookticker_resilient(symbols):
        if not symbols:
            return []
        try:
            return await get_json(
                client, "/api/v3/ticker/bookTicker",
                params={"symbols": json.dumps(symbols, separators=(",", ":"))},
            )
        except httpx.HTTPStatusError as exc:
            if exc.response.status_code != 400:
                raise
            if len(symbols) == 1:
                bad = symbols[0]
                invalid.add(bad)
                return []
            mid = len(symbols) // 2
            left, right = await asyncio.gather(
                _fetch_bookticker_resilient(symbols[:mid]),
                _fetch_bookticker_resilient(symbols[mid:]),
            )
            return left + right

    raw = await _fetch_bookticker_resilient(syms)
    V61_FAST["invalid_quote_symbols"] = sorted(invalid)
    V61_FAST["last_quote_latency_ms"] = round((_t.perf_counter() - t0) * 1000, 1)
    quotes = {d["symbol"]: d for d in raw}
    now_ms = alt_now_ms()
    closed_now = []

    for sym, pos in open_pos.items():
        if V32_STATE["open"].get(sym) is not pos:
            continue
        d = quotes.get(sym)
        if not d:
            continue
        bid = float(d["bidPrice"])
        if bid <= 0:
            continue
        entry = float(pos["entry_price"])
        peak = max(float(pos.get("peak_bid") or entry), bid)
        pos["peak_bid"] = peak
        pos["peak_gain_pct"] = round(pct_change(entry, peak), 4)
        if pos["peak_gain_pct"] >= V55_TRAIL_ACTIVATE_PCT:
            pos["trailing_active"] = True
        hard = entry * (1.0 - V55_HARD_STOP_PCT / 100.0)
        pos["hard_stop_price"] = hard

        reason = None
        if bid <= hard:
            reason = "HARD_STOP"
        elif pos.get("trailing_active"):
            ts = peak * (1.0 - V55_TRAIL_DISTANCE_PCT / 100.0)
            pos["trailing_stop_price"] = ts
            if bid <= ts:
                reason = "TRAILING_STOP"
        if reason is None:
            continue   # zaman stop'u mevcut tarama dongusunde kalir (kural ayni)

        gross = pct_change(entry, bid)
        net = gross - V32_COST_PCT
        closed = {
            **pos,
            "status": "CLOSED_PAPER",
            "exit_open_time": now_ms,
            "exit_price": bid,
            "gross_pct": round(gross, 4),
            "cost_pct": V32_COST_PCT,
            "net_pct": round(net, 4),
            "exit_reason": reason,
            "exit_execution_version": V55_EXECUTION_VERSION,
        }
        closed.update(v61_exit_diagnostics(closed, "FAST_3S"))
        v59_register_counterfactual(closed)
        V32_STATE["closed"].append(closed)
        del V32_STATE["open"][sym]
        closed_now.append(closed)

    if closed_now:
        V61_FAST["closed_by_watcher"] += len(closed_now)
        V61_FAST["recent_closes"] = (V61_FAST["recent_closes"] + [
            {"symbol": c["symbol"], "reason": c["exit_reason"], "net_pct": c["net_pct"],
             "stop_overshoot_pct": c.get("stop_overshoot_pct"), "utc": utc_now()}
            for c in closed_now
        ])[-20:]
        try:
            if V21_DB_URL:
                v32_save_state()          # once kayit, sonra bildirim
        except Exception as exc:
            V61_FAST["last_error"] = f"save: {exc}"
        for c in closed_now:
            await alt_safe_send(
                alt_exit_text("ALT V32 PAPER CIKIS (hizli stop izleyici)", c), "EXIT", c["symbol"]
            )
    return closed_now


async def v61_fast_loop():
    V61_FAST["started_utc"] = utc_now()
    await asyncio.sleep(20)
    while True:
        try:
            async with httpx.AsyncClient(timeout=httpx.Timeout(8.0)) as client:
                for _ in range(200):
                    try:
                        await v61_fast_stop_once(client)
                        V61_FAST["ticks"] += 1
                    except Exception as exc:
                        V61_FAST["errors"] += 1
                        V61_FAST["last_error"] = str(exc)
                    V61_FAST["last_tick_utc"] = utc_now()
                    await asyncio.sleep(V61_FAST_INTERVAL_SECONDS)
        except Exception as exc:
            V61_FAST["errors"] += 1
            V61_FAST["last_error"] = str(exc)
            await asyncio.sleep(5)


@app.on_event("startup")
async def v61_startup():
    global V61_FAST_TASK
    if V61_FAST_ENABLED and V61_FAST_TASK is None:
        V61_FAST_TASK = asyncio.create_task(v61_fast_loop())


@app.get("/v61-status")
async def v61_status():
    return {
        **MODE_INFO,
        "status": "OK",
        "panel": "V61_STATUS",
        "entry_rules_changed": False,
        "risk_rules_changed": False,
        "fast_stop_monitor": {k: v for k, v in V61_FAST.items()},
        "fast_stop_fix": "V61.1_RESILIENT_BOOKTICKER_BAD_SYMBOL_ISOLATION",
        "stop_rules": {
            "hard_stop_pct": V55_HARD_STOP_PCT,
            "trail_activate_pct": V55_TRAIL_ACTIVATE_PCT,
            "trail_distance_pct": V55_TRAIL_DISTANCE_PCT,
            "time_stop_minutes": 120,
        },
        "note": "V55 satirlarinda stop_monitor alani FAST_3S (hizli izleyici) ya da SCAN (~60 sn tarama) ya da yok (V61 oncesi). Karsilastirirken bu alana gore ayir.",
        "generated_utc": utc_now(),
    }


# ============================================================
# /v61-stop-review  (salt-okunur, state'e DOKUNMAZ)
# ============================================================
V61_REVIEW_CACHE = {}


async def v61_fetch_window(client, symbol, start_ms, bars):
    raw = await get_json(
        client, "/api/v3/klines",
        params={"symbol": symbol, "interval": "5m", "startTime": int(start_ms), "limit": int(min(bars, 200))},
    )
    now_ms = alt_now_ms()
    out = []
    for k in raw:
        if int(k[6]) < now_ms:
            out.append({
                "open_time": int(k[0]), "open": float(k[1]), "high": float(k[2]),
                "low": float(k[3]), "close": float(k[4]), "volume": float(k[5]),
                "close_time": int(k[6]),
            })
    return out


async def v61_review_trade(client, p):
    key = p.get("key")
    if key in V61_REVIEW_CACHE:
        return V61_REVIEW_CACHE[key]

    sym = p["symbol"]
    entry = float(p["entry_price"])
    t_entry = int(p.get("entry_live_ms") or p["entry_open_time"])
    open_bar = int(p["entry_open_time"])
    due = int(p["exit_due_time"])
    t_exit = int(p["exit_open_time"])
    bars = int((max(due, t_exit) - open_bar) / 300000) + 6

    row = {
        "key": key, "symbol": sym, "exit_reason": p.get("exit_reason"),
        "entry_utc": datetime.fromtimestamp(t_entry / 1000, tz=timezone.utc).strftime("%m-%d %H:%M:%S"),
        "actual_net_pct": p.get("net_pct"),
        "hold_minutes": p.get("hold_minutes") or round((t_exit - t_entry) / 60000.0, 1),
        "stop_monitor": p.get("stop_monitor", "LEGACY"),
        "stop_overshoot_pct": p.get("stop_overshoot_pct"),
        "peak_gain_pct": p.get("peak_gain_pct"),
        "entry_delay_seconds": p.get("entry_delay_seconds"),
        "spread_pct": p.get("spread_pct"),
        "z": p.get("relative_momentum_z"),
        "continuation_60m_pct": p.get("continuation_60m_pct"),
    }
    if row["stop_overshoot_pct"] is None and p.get("exit_reason") == "HARD_STOP" and p.get("hard_stop_price"):
        row["stop_overshoot_pct"] = round((float(p["hard_stop_price"]) - float(p["exit_price"])) / entry * 100.0, 4)

    try:
        win = await v61_fetch_window(client, sym, open_bar, bars)
        btc = await v61_fetch_window(client, "BTCUSDT", open_bar, bars)
    except Exception as exc:
        row["error"] = str(exc)
        return row

    held = [c for c in win if c["close_time"] > t_entry and c["open_time"] < t_exit]
    full = [c for c in win if c["close_time"] > t_entry and c["open_time"] < due]
    if held:
        row["mae_during_hold_pct"] = round(pct_change(entry, min(c["low"] for c in held)), 4)
        row["mfe_during_hold_pct"] = round(pct_change(entry, max(c["high"] for c in held)), 4)
    if full:
        low_c = min(full, key=lambda c: c["low"])
        row["mae_to_due_pct"] = round(pct_change(entry, low_c["low"]), 4)
        row["mfe_to_due_pct"] = round(pct_change(entry, max(c["high"] for c in full)), 4)
        row["minutes_to_lowest"] = round((low_c["open_time"] - t_entry) / 60000.0, 1)

    cf = next((c for c in win if c["open_time"] >= due), None)
    if cf is not None and p.get("exit_reason") in ("HARD_STOP", "TRAILING_STOP"):
        cf_net = pct_change(entry, cf["open"]) - V32_COST_PCT
        row["counterfactual_120m_net_pct"] = round(cf_net, 4)
        row["stop_advantage_pct"] = round(float(p["net_pct"]) - cf_net, 4)   # + ise stop yardim etti
        after = [c for c in win if c["close_time"] > t_exit and c["open_time"] < due]
        if after:
            row["post_exit_low_pct_vs_exit"] = round(pct_change(float(p["exit_price"]), min(c["low"] for c in after)), 4)
            row["post_exit_high_pct_vs_exit"] = round(pct_change(float(p["exit_price"]), max(c["high"] for c in after)), 4)

    b_in = [c for c in btc if c["close_time"] > t_entry]
    b_done = [c for c in btc if c["close_time"] <= t_exit]
    if b_in and b_done:
        ref = b_in[0]["open"]
        row["btc_pct_during_hold"] = round(pct_change(ref, b_done[-1]["close"]), 4)
        b_hold = [c for c in btc if c["close_time"] > t_entry and c["open_time"] < t_exit]
        if b_hold:
            row["btc_mae_during_hold_pct"] = round(pct_change(ref, min(c["low"] for c in b_hold)), 4)

    for k in ("ctx_dist_from_60m_high_pct", "ctx_range_pos_60m", "ctx_vol_ratio_15m_vs_4h",
              "ctx_up_candles_of_12", "ctx_range_pct_5m_avg_1h", "ctx_ret_30m_pct",
              "ctx_ret_60m_pct", "ctx_btc_30m_pct", "ctx_btc_60m_pct"):
        if p.get(k) is not None:
            row[k] = p[k]

    if cf is not None or p.get("exit_reason") not in ("HARD_STOP", "TRAILING_STOP"):
        V61_REVIEW_CACHE[key] = row
    return row


@app.get("/v61-stop-review")
async def v61_stop_review():
    closed = [
        p for p in V32_STATE.get("closed", [])
        if p.get("execution_version") == V55_EXECUTION_VERSION and p.get("net_pct") is not None
    ]
    closed.sort(key=lambda p: int(p.get("entry_live_ms") or p.get("entry_open_time") or 0))
    closed = closed[-80:]

    rows = []
    async with httpx.AsyncClient(timeout=httpx.Timeout(30.0)) as client:
        for p in closed:
            rows.append(await v61_review_trade(client, p))

    by_key = {p.get("key"): p for p in closed}

    # --- ust uste binme ve kume ---
    spans = []
    for r in rows:
        p = by_key.get(r["key"], {})
        a = int(p.get("entry_live_ms") or p.get("entry_open_time") or 0)
        b = int(p.get("exit_open_time") or a)
        spans.append((a, b))
    for i, r in enumerate(rows):
        a, b = spans[i]
        r["overlapping_other_trades"] = sum(
            1 for j, (c, d) in enumerate(spans) if j != i and c < b and d > a
        )

    hours = {}
    for r in rows:
        h = r["entry_utc"][:8]   # "MM-DD HH"
        hours.setdefault(h, []).append(r)
    by_entry_hour = {
        h: {
            "n": len(v),
            "mean_net_pct": round(sum(float(x["actual_net_pct"]) for x in v) / len(v), 4),
            "hard_stops": sum(1 for x in v if x["exit_reason"] == "HARD_STOP"),
            "symbols": [x["symbol"] for x in v],
        }
        for h, v in sorted(hours.items())
    }
    total_hs = sum(1 for r in rows if r["exit_reason"] == "HARD_STOP")
    top_hour_hs = max((v["hard_stops"] for v in by_entry_hour.values()), default=0)

    # --- ayni sembol tekrar giris ---
    by_sym = {}
    for r in rows:
        by_sym.setdefault(r["symbol"], []).append(r)
    reentries = []
    for sym, lst in by_sym.items():
        if len(lst) < 2:
            continue
        seq = []
        prev_exit = None
        for r in lst:
            p = by_key.get(r["key"], {})
            a = int(p.get("entry_live_ms") or p.get("entry_open_time") or 0)
            seq.append({
                "entry_utc": r["entry_utc"], "reason": r["exit_reason"], "net_pct": r["actual_net_pct"],
                "gap_min_since_prev_exit": round((a - prev_exit) / 60000.0, 1) if prev_exit else None,
            })
            prev_exit = int(p.get("exit_open_time") or a)
        reentries.append({"symbol": sym, "sequence": seq})

    # --- cikis nedenine gore ---
    by_reason = {}
    for reason in ("HARD_STOP", "TRAILING_STOP", "TIME_STOP_120M"):
        q = [r for r in rows if r["exit_reason"] == reason]
        entry = {"actual": v58_safe_stats([r["actual_net_pct"] for r in q])}
        cf = [r for r in q if r.get("counterfactual_120m_net_pct") is not None]
        if cf:
            entry["counterfactual_120m"] = v58_safe_stats([r["counterfactual_120m_net_pct"] for r in cf])
            entry["n_with_counterfactual"] = len(cf)
            entry["stop_helped_count"] = sum(1 for r in cf if r["stop_advantage_pct"] > 0)
            entry["stop_hurt_count"] = sum(1 for r in cf if r["stop_advantage_pct"] < 0)
            entry["sum_stop_advantage_pct"] = round(sum(r["stop_advantage_pct"] for r in cf), 4)
        by_reason[reason] = entry

    hs = [r for r in rows if r["exit_reason"] == "HARD_STOP" and r.get("stop_overshoot_pct") is not None]
    overshoot = {
        "n": len(hs),
        "mean_pct": round(sum(r["stop_overshoot_pct"] for r in hs) / len(hs), 4) if hs else None,
        "max_pct": round(max(r["stop_overshoot_pct"] for r in hs), 4) if hs else None,
        "by_monitor": {
            m: {
                "n": len([r for r in hs if r["stop_monitor"] == m]),
                "mean_pct": round(sum(r["stop_overshoot_pct"] for r in hs if r["stop_monitor"] == m)
                                  / max(1, len([r for r in hs if r["stop_monitor"] == m])), 4),
            }
            for m in sorted({r["stop_monitor"] for r in hs})
        },
    }

    # --- BTC baglami ---
    def avg(lst, f):
        v = [r[f] for r in lst if r.get(f) is not None]
        return round(sum(v) / len(v), 4) if v else None
    hs_rows = [r for r in rows if r["exit_reason"] == "HARD_STOP"]
    other = [r for r in rows if r["exit_reason"] != "HARD_STOP"]
    btc_ctx = {
        "hard_stop_trades": {"n": len(hs_rows), "mean_btc_pct_during_hold": avg(hs_rows, "btc_pct_during_hold"),
                             "mean_btc_mae_pct": avg(hs_rows, "btc_mae_during_hold_pct")},
        "other_trades": {"n": len(other), "mean_btc_pct_during_hold": avg(other, "btc_pct_during_hold"),
                         "mean_btc_mae_pct": avg(other, "btc_mae_during_hold_pct")},
    }

    # --- giris baglami (sadece V61 sonrasi satirlar) ---
    ctx_rows = [r for r in rows if r.get("ctx_range_pos_60m") is not None]
    win = [r for r in ctx_rows if float(r["actual_net_pct"]) > 0]
    lose = [r for r in ctx_rows if float(r["actual_net_pct"]) <= 0]
    ctx_fields = ["ctx_dist_from_60m_high_pct", "ctx_range_pos_60m", "ctx_vol_ratio_15m_vs_4h",
                  "ctx_up_candles_of_12", "ctx_range_pct_5m_avg_1h", "ctx_ret_30m_pct",
                  "ctx_ret_60m_pct", "ctx_btc_30m_pct", "ctx_btc_60m_pct"]
    ctx_compare = {
        "n_with_context": len(ctx_rows),
        "winners": {"n": len(win), **{f: avg(win, f) for f in ctx_fields}},
        "losers": {"n": len(lose), **{f: avg(lose, f) for f in ctx_fields}},
        "note": "ctx_* alanlari V61'den sonra acilan islemlerde var; eski islemlerde bos.",
    }

    # --- butunluk ---
    integrity = []
    for r in rows:
        base = r["symbol"][:-4] if r["symbol"].endswith("USDT") else r["symbol"]
        if base in V26_NON_ALT_BASE_EXCLUSIONS:
            integrity.append({"symbol": r["symbol"], "entry_utc": r["entry_utc"],
                              "issue": "Sembol su an dislanan non-alt listesinde; V56 oncesi giris olabilir. V55 istatistiklerinden cikarmayi dusun."})

    entry_times = [spans[i][0] for i in range(len(rows)) if spans[i][0]]
    return {
        **MODE_INFO,
        "status": "OK",
        "panel": "V61_STOP_REVIEW",
        "read_only": True,
        "strategy_changed": False,
        "sample": {
            "closed_v55": len(rows),
            "first_entry_utc": datetime.fromtimestamp(min(entry_times) / 1000, tz=timezone.utc).isoformat() if entry_times else None,
            "last_entry_utc": datetime.fromtimestamp(max(entry_times) / 1000, tz=timezone.utc).isoformat() if entry_times else None,
            "distinct_entry_hours": len(by_entry_hour),
            "distinct_entry_days": len({h[:5] for h in by_entry_hour}),
        },
        "overall": v58_safe_stats([r["actual_net_pct"] for r in rows]),
        "by_exit_reason": by_reason,
        "hard_stop_overshoot": overshoot,
        "clustering": {
            "by_entry_hour": by_entry_hour,
            "hard_stops_total": total_hs,
            "hard_stops_in_busiest_hour": top_hour_hs,
            "max_overlapping_trades": max((r["overlapping_other_trades"] for r in rows), default=0),
        },
        "same_symbol_reentries": reentries,
        "btc_context": btc_ctx,
        "entry_context_comparison": ctx_compare,
        "integrity_flags": integrity,
        "trades": rows,
        "how_to_read": [
            "stop_advantage_pct > 0: stop 120 dk beklemekten iyiydi; < 0: stop zarar ettirdi.",
            "stop_helped_count vs stop_hurt_count: stopun yardim ettigi/zarar ettirdigi islem sayisi.",
            "mae_to_due_pct: giristen 120 dk'ya kadarki en kotu seviye; mfe_to_due_pct en iyi seviye.",
            "overlapping_other_trades yuksekse sonuclar bagimsiz gozlem degildir.",
            "Betimleyicidir; kucuk orneklerden filtre cikarma.",
        ],
        "generated_utc": utc_now(),
    }


# ============================================================
# /v61-event-study  (gecmis veri, arka plan gorevi, paper-only)
# ============================================================
V61_STUDY = {"status": "IDLE", "progress": {}, "params": None, "result": None,
             "error": None, "started_utc": None, "finished_utc": None}
V61_STUDY_TASK = None

V61_EXIT_CONFIGS = [
    {"name": "V55_stop3.0_trail2.0/1.5_120m", "stop": 3.0, "act": 2.0, "dist": 1.5, "hold": 120},
    {"name": "TIME_ONLY_120m", "stop": None, "act": None, "dist": None, "hold": 120},
    {"name": "stop3.0_notrail_120m", "stop": 3.0, "act": None, "dist": None, "hold": 120},
    {"name": "stop2.0_trail2.0/1.5_120m", "stop": 2.0, "act": 2.0, "dist": 1.5, "hold": 120},
    {"name": "stop5.0_trail3.0/2.0_120m", "stop": 5.0, "act": 3.0, "dist": 2.0, "hold": 120},
    {"name": "V55_stop3.0_trail2.0/1.5_60m", "stop": 3.0, "act": 2.0, "dist": 1.5, "hold": 60},
]


def v61_simulate(opens, highs, lows, entry_idx, cfg, entry_slip, exit_slip, stop_slip, cost):
    """
    5m mumlarla yol simulasyonu (muhafazakar):
    - Stop dokunusu mum low'una gore; mum acilisi seviyenin altindaysa acilista cikilir.
    - Ayni mumda trailing icin once high (zirve) sonra low varsayilir.
    Doner: (net_pct, reason, exit_idx) ya da None (veri yetmiyor).
    """
    n = len(opens)
    exit_bar = entry_idx + cfg["hold"] // 5
    if exit_bar >= n:
        return None
    entry = opens[entry_idx] * (1.0 + entry_slip / 100.0)
    stop_level = entry * (1.0 - cfg["stop"] / 100.0) if cfg["stop"] else None
    peak = entry
    trailing = False
    for j in range(entry_idx, exit_bar):
        lo, hi, op = lows[j], highs[j], opens[j]
        if stop_level is not None and lo <= stop_level:
            px = min(stop_level, op) * (1.0 - stop_slip / 100.0)
            return ((px / entry - 1.0) * 100.0 - cost, "HARD_STOP", j)
        if cfg["act"]:
            prev_peak, prev_trailing = peak, trailing
            if hi > peak:
                peak = hi
            if peak >= entry * (1.0 + cfg["act"] / 100.0):
                trailing = True
            if trailing:
                ts = peak * (1.0 - cfg["dist"] / 100.0)
                if lo <= ts:
                    # mum acilisi onceki zirveye gore zaten seviyenin altindaysa acilista cikilir
                    if prev_trailing and op <= prev_peak * (1.0 - cfg["dist"] / 100.0):
                        px = op
                    else:
                        px = ts
                    px *= (1.0 - stop_slip / 100.0)
                    return ((px / entry - 1.0) * 100.0 - cost, "TRAILING_STOP", j)
    px = opens[exit_bar] * (1.0 - exit_slip / 100.0)
    return ((px / entry - 1.0) * 100.0 - cost, "TIME_STOP", exit_bar)


def v61_stats(trades, rng):
    """trades: list of (net, day, reason). Gun-bazli bootstrap guven araligi."""
    nets = [t[0] for t in trades]
    n = len(nets)
    if n == 0:
        return {"n": 0}
    wins = [x for x in nets if x > 0]
    losses = [x for x in nets if x <= 0]
    gp, gl = sum(wins), abs(sum(losses))
    srt = sorted(nets)
    med = srt[n // 2] if n % 2 else (srt[n // 2 - 1] + srt[n // 2]) / 2
    days = {}
    for net, day, _ in trades:
        d = days.setdefault(day, [0.0, 0])
        d[0] += net
        d[1] += 1
    dl = list(days.values())
    ci = None
    if len(dl) >= 5:
        means = []
        for _ in range(1000):
            samp = [dl[rng.randrange(len(dl))] for _ in dl]
            c = sum(x[1] for x in samp)
            means.append(sum(x[0] for x in samp) / c if c else 0.0)
        means.sort()
        ci = [round(means[24], 3), round(means[974], 3)]
    reasons = {}
    for _, _, r in trades:
        reasons[r] = reasons.get(r, 0) + 1
    return {
        "n": n, "n_days": len(dl),
        "mean_net_pct": round(sum(nets) / n, 4),
        "median_net_pct": round(med, 4),
        "win_rate_pct": round(100.0 * len(wins) / n, 1),
        "profit_factor": round(gp / gl, 3) if gl > 0 else None,
        "mean_win_pct": round(gp / len(wins), 3) if wins else None,
        "mean_loss_pct": round(-gl / len(losses), 3) if losses else None,
        "mean_net_ci95_day_bootstrap": ci,
        "exit_reasons": reasons,
    }


def v61_extract_symbol(sym, sym_idx, candles, alt_acc, conf_by_slot, early_by_slot, store):
    n = len(candles)
    store[sym_idx] = {
        "sym": sym, "n": n,
        "t": array("q", [c["open_time"] for c in candles]),
        "o": array("d", [c["open"] for c in candles]),
        "h": array("d", [c["high"] for c in candles]),
        "l": array("d", [c["low"] for c in candles]),
    }
    for i in range(6, n):
        ct = candles[i]["close_time"]
        ch = pct_change(candles[i - 6]["close"], candles[i]["close"])
        a = alt_acc.get(ct)
        if a is None:
            alt_acc[ct] = [ch, 1]
        else:
            a[0] += ch
            a[1] += 1

    ret30, mus, sigmas = precompute_rolling_volatility_v12(candles, 288)
    for i in range(294, n - 1):
        m = ret30[i]
        if m is None or m <= 0:
            continue
        mu, sg = mus[i], sigmas[i]
        if mu is None or sg is None or sg <= 0:
            continue
        early_by_slot.setdefault(candles[i]["close_time"], []).append(((m - mu) / sg, sym_idx, i + 1))

    for r in relative_candidates_forward_live(candles, sym):
        conf_by_slot.setdefault(r["signal_time_ms"], []).append((
            r["relative_momentum_z"], sym_idx, r["behavior"] == "CONTINUED_UP",
            r["wait_end_change_pct"], r["entry_open_time"],
        ))


def v61_build_entries(conf_by_slot, early_by_slot, alt_acc, store):
    """Her varyant icin [(entry_time_ms, sym_idx, entry_idx)] listesi."""
    def alt_ok(slot):
        a = alt_acc.get(slot)
        return a is not None and (a[0] / a[1]) < 0.5

    variants = {k: [] for k in (
        "CONF_TOP1_MAXCONT_(V32_replika)", "CONF_TOP1_MINCONT", "CONF_TOP1_MAXZ", "CONF_ALL",
        "EARLY_TOP1_MAXZ", "EARLY_ALL",
    )}

    def entry_idx_for(sym_idx, t_ms):
        arr = store[sym_idx]["t"]
        k = bisect.bisect_left(arr, t_ms)
        return k if k < len(arr) and arr[k] == t_ms else None

    for slot, rows in conf_by_slot.items():
        if len(rows) < 5 or not alt_ok(slot):
            continue
        ordered = sorted(rows, key=lambda x: x[0])
        nn = len(ordered)
        elig = []
        for idx, r in enumerate(ordered):
            pct = idx / (nn - 1)
            if r[0] >= 1.0 and pct >= 0.8 and r[2]:
                elig.append((r, pct))
        if not elig:
            continue

        def mk(r):
            ei = entry_idx_for(r[1], r[4])
            return (r[4], r[1], ei) if ei is not None else None

        top_max = max(elig, key=lambda x: (x[0][3], x[0][0], x[1]))[0]
        top_min = min(elig, key=lambda x: (x[0][3], -x[0][0]))[0]
        top_z = max(elig, key=lambda x: (x[0][0], x[1]))[0]
        for name, r in (("CONF_TOP1_MAXCONT_(V32_replika)", top_max), ("CONF_TOP1_MINCONT", top_min),
                        ("CONF_TOP1_MAXZ", top_z)):
            e = mk(r)
            if e:
                variants[name].append(e)
        for r, _ in elig:
            e = mk(r)
            if e:
                variants["CONF_ALL"].append(e)

    for slot, rows in early_by_slot.items():
        if len(rows) < 5 or not alt_ok(slot):
            continue
        ordered = sorted(rows, key=lambda x: x[0])
        nn = len(ordered)
        elig = [(r, idx / (nn - 1)) for idx, r in enumerate(ordered) if r[0] >= 1.0 and idx / (nn - 1) >= 0.8]
        if not elig:
            continue
        best = max(elig, key=lambda x: (x[0][0], x[1]))[0]
        for name, picks in (("EARLY_TOP1_MAXZ", [best]), ("EARLY_ALL", [r for r, _ in elig])):
            for r in picks:
                arr = store[r[1]]["t"]
                if r[2] < len(arr):
                    variants[name].append((arr[r[2]], r[1], r[2]))

    for v in variants.values():
        v.sort(key=lambda x: x[0])
    return variants


def v61_run_variants(variants, store, entry_slip, exit_slip, stop_slip, cost, n_random, btc_day):
    rng = random.Random(61)
    result = {"variants": {}, "baseline_random_entries": {}, "v32_replica_by_day": {}}

    def day_of(ms):
        return datetime.fromtimestamp(ms / 1000, tz=timezone.utc).strftime("%Y-%m-%d")

    for vname, entries in variants.items():
        result["variants"][vname] = {"n_signals": len(entries)}
        for cfg in V61_EXIT_CONFIGS:
            busy = {}
            trades = []
            for t_ms, si, ei in entries:
                if busy.get(si, 0) > t_ms:
                    continue
                d = store[si]
                sim = v61_simulate(d["o"], d["h"], d["l"], ei, cfg, entry_slip, exit_slip, stop_slip, cost)
                if sim is None:
                    continue
                net, reason, xi = sim
                busy[si] = d["t"][min(xi, d["n"] - 1)]
                trades.append((net, day_of(t_ms), reason))
            result["variants"][vname][cfg["name"]] = v61_stats(trades, rng)
            if vname.startswith("CONF_TOP1_MAXCONT") and cfg["name"].startswith("V55_stop3.0_trail2.0/1.5_120m"):
                by_day = {}
                for net, day, _ in trades:
                    x = by_day.setdefault(day, [0.0, 0])
                    x[0] += net
                    x[1] += 1
                result["v32_replica_by_day"] = [
                    {"day": day, "n": x[1], "mean_net_pct": round(x[0] / x[1], 3),
                     "btc_day_pct": btc_day.get(day)}
                    for day, x in sorted(by_day.items())
                ]

    # rastgele giris tabani: ayni cikis + maliyet modeliyle sinyalsiz giris
    syms = list(store.keys())
    for cfg in V61_EXIT_CONFIGS:
        trades = []
        hold_bars = cfg["hold"] // 5
        tries = 0
        while len(trades) < n_random and tries < n_random * 4:
            tries += 1
            si = rng.choice(syms)
            d = store[si]
            if d["n"] < 294 + hold_bars + 5:
                continue
            ei = rng.randrange(294, d["n"] - hold_bars - 2)
            sim = v61_simulate(d["o"], d["h"], d["l"], ei, cfg, entry_slip, exit_slip, stop_slip, cost)
            if sim is None:
                continue
            trades.append((sim[0], day_of(d["t"][ei]), sim[1]))
        result["baseline_random_entries"][cfg["name"]] = v61_stats(trades, rng)
    return result


async def v61_study_run(n_symbols, days, entry_slip, stop_slip, cost):
    V61_STUDY.update(status="RUNNING", started_utc=utc_now(), finished_utc=None, error=None,
                     result=None, progress={"stage": "universe"})
    try:
        async with httpx.AsyncClient(timeout=httpx.Timeout(60.0)) as client:
            uni = await build_universe(client)
            syms = [u["symbol"] for u in uni if u["symbol"] != "BTCUSDT"][:n_symbols]
            sem = asyncio.Semaphore(6)

            async def fetch(sym):
                async with sem:
                    await asyncio.sleep(0.05)
                    try:
                        return sym, await get_5m_candles_days(client, sym, days), None
                    except Exception as exc:
                        return sym, None, str(exc)

            alt_acc, conf_by_slot, early_by_slot, store = {}, {}, {}, {}
            errors = []
            done = 0
            # Bellek icin 6'serli partiler: bir parti islenip silinmeden yenisi cekilmez.
            for start in range(0, len(syms), 6):
                batch = await asyncio.gather(*[fetch(x) for x in syms[start:start + 6]])
                for sym, candles, err in batch:
                    done += 1
                    V61_STUDY["progress"] = {"stage": "fetch+features", "done": done, "total": len(syms)}
                    if err or not candles or len(candles) < 400:
                        errors.append({"symbol": sym, "error": err or "yetersiz veri"})
                        continue
                    await asyncio.to_thread(
                        v61_extract_symbol, sym, len(store), candles, alt_acc, conf_by_slot, early_by_slot, store
                    )
                del batch, candles

            btc_day = {}
            try:
                btc = await get_5m_candles_days(client, "BTCUSDT", days)
                first, last = {}, {}
                for c in btc:
                    dkey = datetime.fromtimestamp(c["open_time"] / 1000, tz=timezone.utc).strftime("%Y-%m-%d")
                    first.setdefault(dkey, c["open"])
                    last[dkey] = c["close"]
                btc_day = {k: round(pct_change(first[k], last[k]), 2) for k in first}
            except Exception:
                pass

        if not store:
            raise RuntimeError("Hicbir sembol icin veri alinamadi.")

        V61_STUDY["progress"] = {"stage": "simulate"}

        def compute():
            variants = v61_build_entries(conf_by_slot, early_by_slot, alt_acc, store)
            return v61_run_variants(variants, store, entry_slip, entry_slip, stop_slip, cost, 3000, btc_day)

        res = await asyncio.to_thread(compute)
        t_all = [store[i]["t"][0] for i in store] + [store[i]["t"][-1] for i in store]
        res["data"] = {
            "symbols_used": len(store), "symbols_failed": errors[:20],
            "bars_per_symbol_approx": int(sum(store[i]["n"] for i in store) / len(store)),
            "period_utc": [
                datetime.fromtimestamp(min(t_all) / 1000, tz=timezone.utc).isoformat(),
                datetime.fromtimestamp(max(t_all) / 1000, tz=timezone.utc).isoformat(),
            ],
            "btc_day_pct": btc_day,
        }
        res["assumptions"] = {
            "entry_slippage_pct": entry_slip, "exit_slippage_pct": entry_slip,
            "stop_extra_slippage_pct": stop_slip, "round_trip_cost_pct": cost,
            "same_symbol_one_position_at_a_time": True,
            "alt_filter": "tum evren 30dk ortalama degisim < %0.5 (V32 ile ayni)",
            "signal_rule": "z>=1, yuzdelik>=0.80 (CONF_*: + CONTINUED_UP onayi; EARLY_*: onay yok, sonraki mum acilisinda giris)",
            "intra_bar_order": "muhafazakar: sert stop low'a gore, trailing icin once high sonra low",
        }
        res["caveats"] = [
            "Sembol listesi BUGUNKU 24s hacme gore secildi (hayatta kalma yanliligi): sonuclar iyimser olabilir.",
            "Gecmis bid/ask yok; slipaj/yayilma sabit varsayimlarla modellendi (assumptions'a bak, parametreleri degistirip tekrar calistir).",
            "Kisa donem (varsayilan 45 gun) tek bir rejim olabilir; n_days ve CI'ya bak, n_days kucukse sonuc zayiftir.",
            "baseline_random_entries: sinyalsiz rastgele giriste ayni cikis/maliyet modeli. Sinyal bunu belirgin gecmiyorsa kenar yok demektir.",
            "Cok sayida varyant x cikis denendi: en iyi gorunen hucreyi secmek cok-karsilastirma yanliligidir. Once V32_replika satirina ve baseline'a bak.",
        ]
        V61_STUDY.update(status="DONE", result=res, finished_utc=utc_now(), progress={"stage": "done"})
    except Exception as exc:
        V61_STUDY.update(status="ERROR", error=str(exc), finished_utc=utc_now())


@app.get("/v61-event-study-start")
async def v61_event_study_start(
    symbols: int = Query(default=50, ge=20, le=80),
    days: int = Query(default=40, ge=14, le=60),
    entry_slip_pct: float = Query(default=0.10, ge=0.0, le=1.0),
    stop_slip_pct: float = Query(default=0.30, ge=0.0, le=2.0),
    cost_pct: float = Query(default=0.15, ge=0.0, le=1.0),
    key: str = "",
):
    global V61_STUDY_TASK
    _v61_check_key(key)
    if V61_STUDY["status"] == "RUNNING":
        return {"status": "ALREADY_RUNNING", "progress": V61_STUDY["progress"]}
    V61_STUDY["params"] = {"symbols": symbols, "days": days, "entry_slip_pct": entry_slip_pct,
                           "stop_slip_pct": stop_slip_pct, "cost_pct": cost_pct}
    V61_STUDY_TASK = asyncio.create_task(v61_study_run(symbols, days, entry_slip_pct, stop_slip_pct, cost_pct))
    return {"status": "STARTED", "params": V61_STUDY["params"],
            "next": "Birkac dakika sonra /v61-event-study sonucu oku."}


@app.get("/v61-event-study")
async def v61_event_study():
    return {**MODE_INFO, "panel": "V61_EVENT_STUDY", "research_only": True,
            "study_status": V61_STUDY["status"], "progress": V61_STUDY["progress"],
            "params": V61_STUDY["params"], "started_utc": V61_STUDY["started_utc"],
            "finished_utc": V61_STUDY["finished_utc"], "error": V61_STUDY["error"],
            "result": V61_STUDY["result"]}


# ============================================================
# V61.2 â€” PERSISTENT SAFE EVENT-STUDY CONTROL PLANE
# ============================================================
V612_JOB = {
    "status": "IDLE",
    "params": None,
    "progress": {},
    "started_utc": None,
    "updated_utc": None,
    "finished_utc": None,
    "error": None,
    "result": None,
}
V612_TASK = None
V612_BATCH_SIZE = 5
V612_PAUSE_SECONDS = 2.0

def _v612_now():
    return datetime.now(timezone.utc).isoformat()

def _v612_db_conn():
    # Reuse the same configured PostgreSQL URL already used by the paper state.
    url = (
        os.getenv("DATABASE_URL")
        or os.getenv("POSTGRES_URL")
        or os.getenv("V7_DATABASE_URL")
    )
    if not url:
        return None
    return psycopg.connect(url)

def v612_db_init():
    conn = _v612_db_conn()
    if conn is None:
        return False
    try:
        with conn:
            with conn.cursor() as cur:
                cur.execute("""
                    CREATE TABLE IF NOT EXISTS alt_v612_eventstudy_state (
                        id INTEGER PRIMARY KEY,
                        payload JSONB NOT NULL,
                        updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
                    )
                """)
        return True
    finally:
        conn.close()

def v612_save():
    V612_JOB["updated_utc"] = _v612_now()
    conn = _v612_db_conn()
    if conn is None:
        return False
    try:
        with conn:
            with conn.cursor() as cur:
                cur.execute("""
                    INSERT INTO alt_v612_eventstudy_state(id, payload, updated_at)
                    VALUES (1, %s::jsonb, NOW())
                    ON CONFLICT (id) DO UPDATE
                    SET payload = EXCLUDED.payload, updated_at = NOW()
                """, (json.dumps(V612_JOB, ensure_ascii=False, default=str),))
        return True
    finally:
        conn.close()

def v612_load():
    conn = _v612_db_conn()
    if conn is None:
        return False
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT payload FROM alt_v612_eventstudy_state WHERE id=1")
            row = cur.fetchone()
        if row and isinstance(row[0], dict):
            V612_JOB.clear()
            V612_JOB.update(row[0])
            # A task cannot survive process restart. Mark it resumable rather than
            # pretending it is still running.
            if V612_JOB.get("status") == "RUNNING":
                V612_JOB["status"] = "PAUSED_RESTART"
                V612_JOB["error"] = None
            return True
        return False
    finally:
        conn.close()

async def _v612_run_existing_study(params):
    """
    Low-risk wrapper. It uses the existing V61 study function if discoverable.
    We do not duplicate or silently alter its statistical logic here.
    """
    global V612_TASK
    V612_JOB.update({
        "status": "RUNNING",
        "params": params,
        "progress": {"stage": "starting"},
        "started_utc": V612_JOB.get("started_utc") or _v612_now(),
        "finished_utc": None,
        "error": None,
        "result": None,
    })
    v612_save()
    try:
        # Locate the existing event-study coroutine/function by common names.
        candidates = [
            "v61_study_run",  # actual worker name in the user's V61 source
            "v61_event_study_worker",
            "v61_run_event_study",
            "v61_event_study_run",
            "_v61_event_study_worker",
        ]
        fn = None
        for name in candidates:
            obj = globals().get(name)
            if callable(obj):
                fn = obj
                break

        if fn is None:
            V612_JOB["status"] = "COMPATIBILITY_ERROR"
            V612_JOB["error"] = (
                "Existing V61 event-study worker function name was not found. "
                "Active scanner/FAST_3S is unaffected."
            )
            v612_save()
            return

        V612_JOB["progress"] = {
            "stage": "running_existing_engine",
            "batch_size_target": V612_BATCH_SIZE,
            "note": "Persistent control plane active; strategy is unchanged."
        }
        v612_save()

        # V61's real worker signature is positional:
        # v61_study_run(n_symbols, days, entry_slip, stop_slip, cost)
        if getattr(fn, "__name__", "") == "v61_study_run":
            out = fn(
                params["symbols"],
                params["days"],
                params["entry_slip_pct"],
                params["stop_slip_pct"],
                params["cost_pct"],
            )
        else:
            try:
                out = fn(**params)
            except TypeError:
                out = fn(params)
        if inspect.isawaitable(out):
            out = await out

        # Some existing workers store their result in a global state and return None.
        if out is None:
            old_state = (
                globals().get("V61_STUDY")
                or globals().get("V61_EVENT_STUDY")
                or globals().get("V61_EVENT_STUDY_STATE")
            )
            if isinstance(old_state, dict):
                out = old_state.get("result")
                # Mirror the real worker's terminal state/error.
                if out is None and old_state.get("status") == "ERROR":
                    raise RuntimeError(old_state.get("error") or "V61 study worker failed")

        V612_JOB["result"] = out
        V612_JOB["status"] = "DONE" if out is not None else "DONE_NO_RESULT"
        V612_JOB["progress"] = {"stage": "done"}
        V612_JOB["finished_utc"] = _v612_now()
        v612_save()
    except asyncio.CancelledError:
        V612_JOB["status"] = "PAUSED_RESTART"
        V612_JOB["progress"] = {"stage": "interrupted"}
        v612_save()
        raise
    except Exception as exc:
        V612_JOB["status"] = "ERROR"
        V612_JOB["error"] = f"{type(exc).__name__}: {exc}"
        V612_JOB["finished_utc"] = _v612_now()
        v612_save()

@app.on_event("startup")
async def v612_startup():
    try:
        v612_db_init()
        v612_load()
    except Exception as exc:
        V612_JOB["status"] = "DB_ERROR"
        V612_JOB["error"] = f"{type(exc).__name__}: {exc}"

@app.get("/v61-2-status")
async def v612_status():
    return {
        "model": "ALT-MOMENTUM-V1",
        "mode": "RESEARCH_PAPER_ONLY",
        "trading": False,
        "orders": False,
        "status": "OK",
        "panel": "V61_2_SAFE_EVENT_STUDY",
        "strategy_changed": False,
        "risk_changed": False,
        "fast_stop_changed": False,
        "persistent_state": True,
        "batch_size_target": V612_BATCH_SIZE,
        "job": V612_JOB,
        "generated_utc": _v612_now(),
    }

@app.get("/v61-2-event-study-start")
async def v612_start(
    symbols: int = Query(20, ge=5, le=50),
    days: int = Query(20, ge=5, le=40),
    entry_slip_pct: float = Query(0.10, ge=0, le=2),
    stop_slip_pct: float = Query(0.30, ge=0, le=5),
    cost_pct: float = Query(0.15, ge=0, le=2),
):
    global V612_TASK
    if V612_TASK is not None and not V612_TASK.done():
        return {"status": "ALREADY_RUNNING", "job": V612_JOB}

    params = {
        "symbols": int(symbols),
        "days": int(days),
        "entry_slip_pct": float(entry_slip_pct),
        "stop_slip_pct": float(stop_slip_pct),
        "cost_pct": float(cost_pct),
    }
    V612_JOB["started_utc"] = _v612_now()
    V612_TASK = asyncio.create_task(_v612_run_existing_study(params))
    return {
        "status": "STARTED",
        "params": params,
        "note": "V61.2 persistent research wrapper; active V61.1 strategy unchanged.",
    }


# ============================================================
# V62 SHADOW CHALLENGER â€” RESEARCH ONLY
# Active V61.1/V55 entries, FAST_3S exits and paper state are untouched.
# Goal: compare timing, not promote a new strategy.
# ============================================================
V62_STUDY = {"status":"IDLE","progress":{},"params":None,"result":None,
             "error":None,"started_utc":None,"finished_utc":None}
V62_TASK = None

# Predeclared before looking at V62 results; do not optimize from a tiny sample.
V62_PULLBACK_MIN_PCT = 0.25
V62_PULLBACK_MAX_PCT = 1.50
V62_PULLBACK_WINDOW_BARS = 6       # 30m after EARLY entry point
V62_ATR_LOOKBACK = 14
V62_ATR_STOP_MULT = 1.5
V62_ATR_TRAIL_MULT = 2.0
V62_ATR_ACTIVATE_R = 1.0
V62_MAX_HOLD_BARS = 24             # 120m


def v62_pullback_reaccel_entries(early_entries, store):
    """Already-risen EARLY_TOP1 -> micro pullback -> reacceleration -> next-bar open.
    This is NOT dip buying: source signal must already be an EARLY momentum candidate.
    Reacceleration is causal: positive candle closes above previous candle high.
    """
    out=[]
    for _, si, ei in early_entries:
        d=store[si]
        o,h,l,c,t=d['o'],d['h'],d['l'],d['c'],d['t']
        if ei < 2 or ei+2 >= d['n']:
            continue
        running_high=max(h[ei-1], h[ei])
        pullback_seen=False
        end=min(d['n']-2, ei+V62_PULLBACK_WINDOW_BARS)
        for j in range(ei, end+1):
            running_high=max(running_high,h[j])
            dd=(l[j]/running_high-1.0)*100.0
            if -V62_PULLBACK_MAX_PCT <= dd <= -V62_PULLBACK_MIN_PCT:
                pullback_seen=True
            # After a qualifying pullback, require an actual reacceleration candle.
            if pullback_seen and j>=1 and c[j] > o[j] and c[j] > h[j-1]:
                entry_idx=j+1
                if entry_idx < d['n']:
                    out.append((t[entry_idx],si,entry_idx))
                break
    out.sort(key=lambda x:x[0])
    return out


def v62_atr_pct(d, entry_idx):
    if entry_idx < V62_ATR_LOOKBACK+1:
        return None
    vals=[]
    for j in range(entry_idx-V62_ATR_LOOKBACK, entry_idx):
        prev=d['c'][j-1]
        tr=max(d['h'][j]-d['l'][j], abs(d['h'][j]-prev), abs(d['l'][j]-prev))
        if prev>0:
            vals.append(tr/prev*100.0)
    return sum(vals)/len(vals) if vals else None


def v62_simulate_atr(d, entry_idx, entry_slip, exit_slip, stop_slip, cost):
    atr=v62_atr_pct(d,entry_idx)
    if atr is None or atr<=0:
        return None
    if entry_idx+V62_MAX_HOLD_BARS >= d['n']:
        return None
    entry=d['o'][entry_idx]*(1+entry_slip/100.0)
    risk=max(0.35, atr*V62_ATR_STOP_MULT)  # floor only prevents microscopic stops
    trail_dist=max(0.35, atr*V62_ATR_TRAIL_MULT)
    activate=risk*V62_ATR_ACTIVATE_R
    hard=entry*(1-risk/100.0)
    peak=entry
    trailing=False
    for j in range(entry_idx, entry_idx+V62_MAX_HOLD_BARS):
        op,hi,lo=d['o'][j],d['h'][j],d['l'][j]
        if lo<=hard:
            px=min(hard,op)*(1-stop_slip/100.0)
            return ((px/entry-1)*100.0-cost,'ATR_HARD_STOP',j,atr,risk,trail_dist)
        peak=max(peak,hi)
        if peak>=entry*(1+activate/100.0):
            trailing=True
        if trailing:
            ts=peak*(1-trail_dist/100.0)
            if lo<=ts:
                px=min(ts,op)*(1-stop_slip/100.0)
                return ((px/entry-1)*100.0-cost,'ATR_TRAILING_STOP',j,atr,risk,trail_dist)
    xi=entry_idx+V62_MAX_HOLD_BARS
    px=d['o'][xi]*(1-exit_slip/100.0)
    return ((px/entry-1)*100.0-cost,'ATR_TIME_120M',xi,atr,risk,trail_dist)


def v62_run_entry_comparison(variants, store, entry_slip, stop_slip, cost):
    rng=random.Random(62)
    configs=[
        {"name":"TIME_ONLY_120m","stop":None,"act":None,"dist":None,"hold":120},
        {"name":"V55_FIXED","stop":3.0,"act":2.0,"dist":1.5,"hold":120},
    ]
    out={}
    for name,entries in variants.items():
        out[name]={"n_signals":len(entries)}
        for cfg in configs:
            busy={}; trades=[]
            for t_ms,si,ei in entries:
                if busy.get(si,0)>t_ms: continue
                sim=v61_simulate(store[si]['o'],store[si]['h'],store[si]['l'],ei,cfg,
                                 entry_slip,entry_slip,stop_slip,cost)
                if sim is None: continue
                net,reason,xi=sim
                busy[si]=store[si]['t'][min(xi,store[si]['n']-1)]
                day=datetime.fromtimestamp(t_ms/1000,tz=timezone.utc).strftime('%Y-%m-%d')
                trades.append((net,day,reason))
            out[name][cfg['name']]=v61_stats(trades,rng)
        busy={}; trades=[]; atr_meta=[]
        for t_ms,si,ei in entries:
            if busy.get(si,0)>t_ms: continue
            sim=v62_simulate_atr(store[si],ei,entry_slip,entry_slip,stop_slip,cost)
            if sim is None: continue
            net,reason,xi,atr,risk,dist=sim
            busy[si]=store[si]['t'][min(xi,store[si]['n']-1)]
            day=datetime.fromtimestamp(t_ms/1000,tz=timezone.utc).strftime('%Y-%m-%d')
            trades.append((net,day,reason)); atr_meta.append((atr,risk,dist))
        z=v61_stats(trades,rng)
        if atr_meta:
            z['mean_atr_pct']=round(sum(x[0] for x in atr_meta)/len(atr_meta),4)
            z['mean_initial_risk_pct']=round(sum(x[1] for x in atr_meta)/len(atr_meta),4)
            z['mean_trail_distance_pct']=round(sum(x[2] for x in atr_meta)/len(atr_meta),4)
        out[name]['ATR_DYNAMIC_PREDECLARED']=z
    return out


async def v62_study_run(n_symbols,days,entry_slip,stop_slip,cost):
    V62_STUDY.update(status='RUNNING',params={"symbols":n_symbols,"days":days,
        "entry_slip_pct":entry_slip,"stop_slip_pct":stop_slip,"cost_pct":cost},
        progress={"stage":"universe"},started_utc=utc_now(),finished_utc=None,error=None,result=None)
    try:
        async with httpx.AsyncClient(timeout=httpx.Timeout(60.0)) as client:
            uni=await build_universe(client)
            syms=[u['symbol'] for u in uni if u['symbol']!='BTCUSDT'][:n_symbols]
            sem=asyncio.Semaphore(6)
            async def fetch(sym):
                async with sem:
                    try: return sym,await get_5m_candles_days(client,sym,days),None
                    except Exception as exc: return sym,None,str(exc)
            alt_acc,conf_by_slot,early_by_slot,store={},{},{},{}
            errors=[]; done=0
            for start in range(0,len(syms),6):
                batch=await asyncio.gather(*[fetch(x) for x in syms[start:start+6]])
                for sym,candles,err in batch:
                    done+=1; V62_STUDY['progress']={"stage":"fetch+features","done":done,"total":len(syms)}
                    if err or not candles or len(candles)<400:
                        errors.append({"symbol":sym,"error":err or 'yetersiz veri'}); continue
                    # V61 extractor plus close array needed for ATR.
                    si=len(store)
                    await asyncio.to_thread(v61_extract_symbol,sym,si,candles,alt_acc,conf_by_slot,early_by_slot,store)
                    store[si]['c']=array('d',[c['close'] for c in candles])
        if not store: raise RuntimeError('Hicbir sembol icin veri alinamadi.')
        V62_STUDY['progress']={"stage":"simulate"}
        def compute():
            base=v61_build_entries(conf_by_slot,early_by_slot,alt_acc,store)
            variants={
                'CONF_60M_TOP1_MAXCONT':base['CONF_TOP1_MAXCONT_(V32_replika)'],
                'EARLY_TOP1_MAXZ':base['EARLY_TOP1_MAXZ'],
            }
            variants['PULLBACK_REACCEL_TOP1']=v62_pullback_reaccel_entries(base['EARLY_TOP1_MAXZ'],store)
            return v62_run_entry_comparison(variants,store,entry_slip,stop_slip,cost)
        result=await asyncio.to_thread(compute)
        V62_STUDY.update(status='DONE',progress={"stage":"done"},finished_utc=utc_now(),result={
            'entry_comparison':result,
            'predeclared_pullback':{"min_pct":V62_PULLBACK_MIN_PCT,"max_pct":V62_PULLBACK_MAX_PCT,
                "window_minutes":V62_PULLBACK_WINDOW_BARS*5,
                "reacceleration":"bullish 5m candle close > previous candle high; entry next 5m open"},
            'predeclared_atr_exit':{"lookback_bars":V62_ATR_LOOKBACK,"stop_mult":V62_ATR_STOP_MULT,
                "trail_mult":V62_ATR_TRAIL_MULT,"activate_R":V62_ATR_ACTIVATE_R,"max_hold_minutes":120},
            'data':{"symbols_used":len(store),"symbols_failed":errors[:20]},
            'guardrails':["SHADOW/RESEARCH only; active V61.1 entries unchanged.",
                "Do not select the best cell from this small sample; require larger temporal validation.",
                "BTC regime is intentionally measured later, not used to suppress entries yet.",
                "WebSocket/real orders are intentionally not enabled by this research patch."],
        })
    except Exception as exc:
        V62_STUDY.update(status='ERROR',error=f'{type(exc).__name__}: {exc}',finished_utc=utc_now())


@app.get('/v62-shadow-start')
async def v62_shadow_start(symbols:int=Query(20,ge=20,le=50),days:int=Query(20,ge=14,le=40),
    entry_slip_pct:float=Query(0.10,ge=0,le=1),stop_slip_pct:float=Query(0.30,ge=0,le=2),
    cost_pct:float=Query(0.15,ge=0,le=1)):
    global V62_TASK
    if V62_TASK is not None and not V62_TASK.done():
        return {"status":"ALREADY_RUNNING","progress":V62_STUDY.get('progress')}
    V62_TASK=asyncio.create_task(v62_study_run(symbols,days,entry_slip_pct,stop_slip_pct,cost_pct))
    return {"status":"STARTED","paper_only":True,"active_strategy_changed":False,
            "compare":["CONF_60M","EARLY","PULLBACK_REACCEL"],
            "exits":["TIME120","V55_FIXED","ATR_DYNAMIC_PREDECLARED"]}


@app.get('/v62-shadow-status')
async def v62_shadow_status():
    return {**MODE_INFO,"status":"OK","panel":"V62_SHADOW_CHALLENGER",
        "trading":False,"orders":False,"active_strategy_changed":False,
        "active_risk_changed":False,"v61_fast_3s_changed":False,
        "study":V62_STUDY,"generated_utc":utc_now()}


# ============================================================
# V63 â€” PULLBACK/REACCEL EXIT VALIDATION (RESEARCH ONLY)
# Entry is frozen from V62. No active V61/V55/FAST_3S changes.
# Predeclared exits: TIME120, hard -3%, catastrophe -5%, late trailing.
# ============================================================
V63_STUDY={"status":"IDLE","progress":{},"params":None,"result":None,"error":None,
           "started_utc":None,"finished_utc":None}
V63_TASK=None

V63_EXITS=[
    {"name":"TIME_ONLY_120m","stop":None,"act":None,"dist":None,"hold":120},
    {"name":"HARD_STOP_3_TIME120","stop":3.0,"act":None,"dist":None,"hold":120},
    {"name":"CATASTROPHE_STOP_5_TIME120","stop":5.0,"act":None,"dist":None,"hold":120},
    {"name":"HARD3_LATE_TRAIL_ACT3_DIST2_TIME120","stop":3.0,"act":3.0,"dist":2.0,"hold":120},
]


def v63_compare(entries,store,entry_slip,stop_slip,cost):
    rng=random.Random(63); out={}
    for cfg in V63_EXITS:
        busy={}; trades=[]
        for t_ms,si,ei in entries:
            if busy.get(si,0)>t_ms: continue
            sim=v61_simulate(store[si]['o'],store[si]['h'],store[si]['l'],ei,cfg,
                             entry_slip,entry_slip,stop_slip,cost)
            if sim is None: continue
            net,reason,xi=sim
            busy[si]=store[si]['t'][min(xi,store[si]['n']-1)]
            day=datetime.fromtimestamp(t_ms/1000,tz=timezone.utc).strftime('%Y-%m-%d')
            trades.append((net,day,reason))
        out[cfg['name']]=v61_stats(trades,rng)
    return out


async def v63_study_run(n_symbols,days,entry_slip,stop_slip,cost):
    V63_STUDY.update(status='RUNNING',params={"symbols":n_symbols,"days":days,
        "entry_slip_pct":entry_slip,"stop_slip_pct":stop_slip,"cost_pct":cost},
        progress={"stage":"universe"},started_utc=utc_now(),finished_utc=None,error=None,result=None)
    try:
        async with httpx.AsyncClient(timeout=httpx.Timeout(60.0)) as client:
            uni=await build_universe(client)
            syms=[u['symbol'] for u in uni if u['symbol']!='BTCUSDT'][:n_symbols]
            sem=asyncio.Semaphore(6)
            async def fetch(sym):
                async with sem:
                    try:return sym,await get_5m_candles_days(client,sym,days),None
                    except Exception as exc:return sym,None,str(exc)
            alt_acc,conf_by_slot,early_by_slot,store={},{},{},{}
            errors=[];done=0
            for start in range(0,len(syms),6):
                batch=await asyncio.gather(*[fetch(x) for x in syms[start:start+6]])
                for sym,candles,err in batch:
                    done+=1;V63_STUDY['progress']={"stage":"fetch+features","done":done,"total":len(syms)}
                    if err or not candles or len(candles)<400:
                        errors.append({"symbol":sym,"error":err or 'yetersiz veri'});continue
                    si=len(store)
                    await asyncio.to_thread(v61_extract_symbol,sym,si,candles,alt_acc,conf_by_slot,early_by_slot,store)
                    store[si]['c']=array('d',[c['close'] for c in candles])
            if not store:raise RuntimeError('Hicbir sembol icin veri alinamadi.')
        V63_STUDY['progress']={"stage":"simulate"}
        def compute():
            base=v61_build_entries(conf_by_slot,early_by_slot,alt_acc,store)
            frozen=v62_pullback_reaccel_entries(base['EARLY_TOP1_MAXZ'],store)
            return len(frozen),v63_compare(frozen,store,entry_slip,stop_slip,cost)
        n_sig,res=await asyncio.to_thread(compute)
        V63_STUDY.update(status='DONE',progress={"stage":"done"},finished_utc=utc_now(),result={
            "entry":"V62_FROZEN_PULLBACK_REACCEL_TOP1",
            "entry_rules":{"source":"EARLY_TOP1_MAXZ","pullback_min_pct":V62_PULLBACK_MIN_PCT,
                "pullback_max_pct":V62_PULLBACK_MAX_PCT,"window_minutes":V62_PULLBACK_WINDOW_BARS*5,
                "reacceleration":"bullish 5m close > previous 5m high; entry next 5m open"},
            "n_signals":n_sig,"exit_comparison":res,
            "data":{"symbols_used":len(store),"symbols_failed":errors[:20]},
            "guardrails":["Research/shadow only; active strategy unchanged.",
                "Entry thresholds frozen before V63 results.",
                "Exit candidates predeclared before V63 results; do not tune to this sample.",
                "Primary question: can a catastrophe/hard stop protect downside without destroying TIME120 edge?"]})
    except Exception as exc:
        V63_STUDY.update(status='ERROR',error=f'{type(exc).__name__}: {exc}',finished_utc=utc_now())


@app.get('/v63-validation-start')
async def v63_validation_start(symbols:int=Query(30,ge=20,le=50),days:int=Query(30,ge=20,le=40),
    entry_slip_pct:float=Query(0.10,ge=0,le=1),stop_slip_pct:float=Query(0.30,ge=0,le=2),
    cost_pct:float=Query(0.15,ge=0,le=1)):
    global V63_TASK
    if V63_TASK is not None and not V63_TASK.done():
        return {"status":"ALREADY_RUNNING","progress":V63_STUDY.get('progress')}
    V63_TASK=asyncio.create_task(v63_study_run(symbols,days,entry_slip_pct,stop_slip_pct,cost_pct))
    return {"status":"STARTED","paper_only":True,"active_strategy_changed":False,
        "entry":"V62_FROZEN_PULLBACK_REACCEL_TOP1",
        "exits":[x['name'] for x in V63_EXITS]}


@app.get('/v63-validation-status')
async def v63_validation_status():
    return {**MODE_INFO,"status":"OK","panel":"V63_PULLBACK_EXIT_VALIDATION",
        "trading":False,"orders":False,"active_strategy_changed":False,
        "active_risk_changed":False,"v61_fast_3s_changed":False,
        "study":V63_STUDY,"generated_utc":utc_now()}


# ============================================================
# V64 â€” WINNER vs LOSER DIAGNOSTIC (RESEARCH ONLY)
# Frozen V62 pullback/reaccel entry + TIME120 control.
# Descriptive diagnostics only: NO filter promotion / NO strategy change.
# ============================================================
V64_STUDY={"status":"IDLE","progress":{},"params":None,"result":None,"error":None,
           "started_utc":None,"finished_utc":None}
V64_TASK=None


def v64_summary(vals):
    a=[float(x) for x in vals if x is not None and math.isfinite(float(x))]
    if not a:return {"n":0,"mean":None,"median":None}
    return {"n":len(a),"mean":round(sum(a)/len(a),6),"median":round(statistics.median(a),6)}


def v64_feature_row(candles, entry_i, signal_i, entry_px):
    # All features use information available no later than entry open.
    def pc(a,b): return ((b/a)-1.0)*100.0 if a else None
    sig_close=candles[signal_i]['close']
    eopen=candles[entry_i]['open']
    # pullback/reaccel geometry from the completed bars preceding entry
    prev=candles[entry_i-1]; prev2=candles[entry_i-2]
    look=candles[max(0,entry_i-6):entry_i]
    hi=max(x['high'] for x in look); lo=min(x['low'] for x in look)
    vol_now=prev.get('volume',0.0)
    vols=[x.get('volume',0.0) for x in candles[max(0,entry_i-21):entry_i-1]]
    vavg=(sum(vols)/len(vols)) if vols else 0.0
    ranges=[pc(x['low'],x['high']) for x in candles[max(0,entry_i-14):entry_i] if x['low']]
    return {
      'continuation_to_entry_pct':pc(sig_close,eopen),
      'distance_from_30m_high_pct':pc(hi,eopen),
      'range_position_30m':((eopen-lo)/(hi-lo)) if hi>lo else None,
      'reaccel_body_pct':pc(prev['open'],prev['close']),
      'reaccel_break_prev_high_pct':pc(prev2['high'],prev['close']),
      'volume_ratio_20':(vol_now/vavg) if vavg>0 else None,
      'mean_5m_range_pct_14':(sum(ranges)/len(ranges)) if ranges else None,
    }


def v64_compute(conf_by_slot,early_by_slot,alt_acc,store,candles_by_si,entry_slip,cost):
    base=v61_build_entries(conf_by_slot,early_by_slot,alt_acc,store)
    entries=v62_pullback_reaccel_entries(base['EARLY_TOP1_MAXZ'],store)
    busy={}; rows=[]
    cfg={"name":"TIME_ONLY_120m","stop":None,"act":None,"dist":None,"hold":120}
    for t_ms,si,ei in entries:
        if busy.get(si,0)>t_ms: continue
        sim=v61_simulate(store[si]['o'],store[si]['h'],store[si]['l'],ei,cfg,entry_slip,entry_slip,0.0,cost)
        if sim is None: continue
        net,reason,xi=sim; busy[si]=store[si]['t'][min(xi,store[si]['n']-1)]
        candles=candles_by_si[si]
        # V62 frozen pullback entry is downstream of an EARLY source; use nearest causal
        # reference 13 bars before entry for descriptive continuation geometry only.
        sig_i=max(0,ei-13)
        f=v64_feature_row(candles,ei,sig_i,store[si]['o'][ei])
        f.update(net_pct=float(net),winner=bool(net>0),day=datetime.fromtimestamp(t_ms/1000,tz=timezone.utc).strftime('%Y-%m-%d'))
        rows.append(f)
    feats=['continuation_to_entry_pct','distance_from_30m_high_pct','range_position_30m','reaccel_body_pct',
           'reaccel_break_prev_high_pct','volume_ratio_20','mean_5m_range_pct_14']
    groups={}
    for name,pred in [('WINNERS',lambda r:r['winner']),('LOSERS',lambda r:not r['winner'])]:
        rr=[r for r in rows if pred(r)]
        groups[name]={'n':len(rr),'mean_net_pct':round(sum(r['net_pct'] for r in rr)/len(rr),6) if rr else None,
                      'features':{f:v64_summary([r.get(f) for r in rr]) for f in feats}}
    comparison={}
    for f in feats:
        w=groups['WINNERS']['features'][f]['mean']; l=groups['LOSERS']['features'][f]['mean']
        comparison[f]={'winner_mean':w,'loser_mean':l,'difference':round(w-l,6) if w is not None and l is not None else None}
    # Temporal stability: same descriptive winner/loser feature means in first vs second half.
    days=sorted(set(r['day'] for r in rows)); cut=days[len(days)//2] if days else None
    temporal={}
    for label,rr in [('FIRST_HALF',[r for r in rows if cut and r['day']<cut]),('SECOND_HALF',[r for r in rows if cut and r['day']>=cut])]:
        temporal[label]={'n':len(rr),'win_rate_pct':round(100*sum(r['winner'] for r in rr)/len(rr),3) if rr else None,
          'feature_differences':{f:(round((sum(r[f] for r in rr if r['winner'] and r.get(f) is not None)/max(1,sum(1 for r in rr if r['winner'] and r.get(f) is not None)))-(sum(r[f] for r in rr if (not r['winner']) and r.get(f) is not None)/max(1,sum(1 for r in rr if (not r['winner']) and r.get(f) is not None))),6)) for f in feats}}
    return len(entries),len(rows),groups,comparison,temporal


async def v64_run(n_symbols,days,entry_slip,cost):
    V64_STUDY.update(status='RUNNING',params={'symbols':n_symbols,'days':days,'entry_slip_pct':entry_slip,'cost_pct':cost},
        progress={'stage':'universe'},result=None,error=None,started_utc=utc_now(),finished_utc=None)
    try:
        async with httpx.AsyncClient(timeout=httpx.Timeout(60.0)) as client:
            uni=await build_universe(client); syms=[u['symbol'] for u in uni if u['symbol']!='BTCUSDT'][:n_symbols]
            sem=asyncio.Semaphore(6)
            async def fetch(sym):
                async with sem:
                    try:return sym,await get_5m_candles_days(client,sym,days),None
                    except Exception as exc:return sym,None,str(exc)
            alt_acc,conf_by_slot,early_by_slot,store,candles_by_si={},{},{},{},{};errors=[];done=0
            for start in range(0,len(syms),6):
                batch=await asyncio.gather(*[fetch(x) for x in syms[start:start+6]])
                for sym,candles,err in batch:
                    done+=1;V64_STUDY['progress']={'stage':'fetch+features','done':done,'total':len(syms)}
                    if err or not candles or len(candles)<400:errors.append({'symbol':sym,'error':err or 'insufficient data'});continue
                    si=len(store);await asyncio.to_thread(v61_extract_symbol,sym,si,candles,alt_acc,conf_by_slot,early_by_slot,store)
                    store[si]['c']=array('d',[c['close'] for c in candles]);candles_by_si[si]=candles
        V64_STUDY['progress']={'stage':'diagnostic'}
        n_sig,n_rows,groups,comp,temp=await asyncio.to_thread(v64_compute,conf_by_slot,early_by_slot,alt_acc,store,candles_by_si,entry_slip,cost)
        V64_STUDY.update(status='DONE',progress={'stage':'done'},finished_utc=utc_now(),result={
          'entry':'V62_FROZEN_PULLBACK_REACCEL_TOP1','exit':'TIME_ONLY_120m_CONTROL','signals_before_overlap':n_sig,
          'analyzed_trades':n_rows,'winner_loser':groups,'feature_comparison':comp,'temporal_stability':temp,
          'data':{'symbols_used':len(store),'symbols_failed':errors[:20]},
          'guardrails':['Descriptive diagnostic only.','No feature threshold is promoted from this sample.',
            'Any apparent separator must be frozen first and tested out-of-sample in V65.',
            'Active V61/V55/FAST_3S strategy and risk are unchanged.']})
    except Exception as exc:V64_STUDY.update(status='ERROR',error=f'{type(exc).__name__}: {exc}',finished_utc=utc_now())

@app.get('/v64-diagnostic-start')
async def v64_diagnostic_start(symbols:int=Query(30,ge=20,le=50),days:int=Query(30,ge=20,le=40),entry_slip_pct:float=Query(.10,ge=0,le=1),cost_pct:float=Query(.15,ge=0,le=1)):
    global V64_TASK
    if V64_TASK is not None and not V64_TASK.done():return {'status':'ALREADY_RUNNING','progress':V64_STUDY.get('progress')}
    V64_TASK=asyncio.create_task(v64_run(symbols,days,entry_slip_pct,cost_pct))
    return {'status':'STARTED','paper_only':True,'active_strategy_changed':False,'purpose':'winner_vs_loser_diagnostic'}

@app.get('/v64-diagnostic-status')
async def v64_diagnostic_status():
    return {**MODE_INFO,'status':'OK','panel':'V64_WINNER_LOSER_DIAGNOSTIC','trading':False,'orders':False,
      'read_only_research':True,'active_strategy_changed':False,'active_risk_changed':False,'v61_fast_3s_changed':False,
      'study':V64_STUDY,'generated_utc':utc_now()}


BINANCE_BASE = "https://api.binance.com"

# ============================================================
# V65 â€” COMBINED EARLY BREAKOUT CHALLENGER (PAPER/RESEARCH ONLY)
# Adds: Binance server-time offset, immediate live ASK forward helper,
# and a predeclared early breakout challenger (>=0.50% 5m + volume ratio >=1.20).
# Existing V61/V55/FAST_3S control remains unchanged.
# ============================================================
V65_BREAKOUT_5M_PCT=0.50
V65_VOLUME_RATIO_MIN=1.20
V65_VOL_LOOKBACK=20
V65_STUDY={"status":"IDLE","progress":{},"params":None,"result":None,"error":None,"started_utc":None,"finished_utc":None}
V65_TASK=None
V65_TIME_OFFSET_MS=0
V65_TIME_SYNC_UTC=None

async def get_binance_time_offset(client=None):
    global V65_TIME_OFFSET_MS,V65_TIME_SYNC_UTC
    own=client is None
    c=client or httpx.AsyncClient(timeout=10.0)
    try:
        t0=int(time.time()*1000); r=await c.get(f'{BINANCE_BASE}/api/v3/time'); t1=int(time.time()*1000)
        r.raise_for_status(); server=int(r.json()['serverTime']); local_mid=(t0+t1)//2
        V65_TIME_OFFSET_MS=server-local_mid;V65_TIME_SYNC_UTC=utc_now()
        return V65_TIME_OFFSET_MS
    finally:
        if own: await c.aclose()

async def get_realtime_ask_price(symbol,client=None):
    own=client is None;c=client or httpx.AsyncClient(timeout=10.0)
    try:
        r=await c.get(f'{BINANCE_BASE}/api/v3/ticker/bookTicker',params={'symbol':symbol});r.raise_for_status()
        j=r.json();return {'symbol':symbol,'ask':float(j['askPrice']),'bid':float(j['bidPrice']),
          'server_adjusted_local_ms':int(time.time()*1000)+V65_TIME_OFFSET_MS,'offset_ms':V65_TIME_OFFSET_MS}
    finally:
        if own:await c.aclose()

def v65_early_entries(store):
    # Historical causal replica: trigger on first COMPLETED 5m candle satisfying
    # >=0.50% close/open and volume >=1.20x previous-20 average; enter next open.
    by_slot={}
    for si,d in store.items():
        o,h,l,c,v,t=d['o'],d['h'],d['l'],d['c'],d.get('v'),d['t']
        if v is None:continue
        for i in range(V65_VOL_LOOKBACK,len(o)-1):
            ch=((c[i]/o[i])-1)*100 if o[i] else 0
            av=sum(v[i-V65_VOL_LOOKBACK:i])/V65_VOL_LOOKBACK
            vr=(v[i]/av) if av>0 else 0
            if ch>=V65_BREAKOUT_5M_PCT and vr>=V65_VOLUME_RATIO_MIN:
                by_slot.setdefault(t[i],[]).append((ch,vr,si,i+1))
    out=[]
    for slot,a in by_slot.items():
        # Top1 earliest-breakout challenger; rank by 5m change, then volume ratio.
        ch,vr,si,ei=max(a,key=lambda x:(x[0],x[1]))
        out.append((slot,si,ei))
    return sorted(out)

def v65_compare(entries,store,entry_slip,cost):
    cfg={'name':'TIME_ONLY_120m','stop':None,'act':None,'dist':None,'hold':120};busy={};tr=[];rng=random.Random(65)
    for tm,si,ei in entries:
        if busy.get(si,0)>tm:continue
        sim=v61_simulate(store[si]['o'],store[si]['h'],store[si]['l'],ei,cfg,entry_slip,entry_slip,0.0,cost)
        if sim is None:continue
        net,reason,xi=sim;busy[si]=store[si]['t'][min(xi,store[si]['n']-1)]
        day=datetime.fromtimestamp(tm/1000,tz=timezone.utc).strftime('%Y-%m-%d');tr.append((net,day,reason))
    return v61_stats(tr,rng)

async def v65_run(n_symbols,days,entry_slip,cost):
    V65_STUDY.update(status='RUNNING',progress={'stage':'time_sync'},params={'symbols':n_symbols,'days':days,'entry_slip_pct':entry_slip,'cost_pct':cost},result=None,error=None,started_utc=utc_now(),finished_utc=None)
    try:
      async with httpx.AsyncClient(timeout=httpx.Timeout(60.0)) as client:
        try:
            off=await get_binance_time_offset(client)
            time_sync_status='OK'
            time_sync_error=None
        except Exception as exc:
            off=0
            time_sync_status='UNAVAILABLE_NON_BLOCKING'
            time_sync_error=f'{type(exc).__name__}: {exc}'
        uni=await build_universe(client);syms=[u['symbol'] for u in uni if u['symbol']!='BTCUSDT'][:n_symbols]
        sem=asyncio.Semaphore(6)
        async def fetch(sym):
          async with sem:
            try:return sym,await get_5m_candles_days(client,sym,days),None
            except Exception as e:return sym,None,str(e)
        alt_acc,conf,early,store={},{},{},{};errs=[];done=0
        for st in range(0,len(syms),6):
          for sym,candles,err in await asyncio.gather(*[fetch(x) for x in syms[st:st+6]]):
            done+=1;V65_STUDY['progress']={'stage':'fetch+features','done':done,'total':len(syms)}
            if err or not candles or len(candles)<400:errs.append({'symbol':sym,'error':err or 'insufficient'});continue
            si=len(store);await asyncio.to_thread(v61_extract_symbol,sym,si,candles,alt_acc,conf,early,store)
            store[si]['c']=array('d',[x['close'] for x in candles]);store[si]['v']=array('d',[x.get('volume',0.0) for x in candles])
      V65_STUDY['progress']={'stage':'compare'}
      def calc():
        base=v61_build_entries(conf,early,alt_acc,store)
        pb=v62_pullback_reaccel_entries(base['EARLY_TOP1_MAXZ'],store);bo=v65_early_entries(store)
        return len(pb),len(bo),v65_compare(pb,store,entry_slip,cost),v65_compare(bo,store,entry_slip,cost)
      npb,nbo,spb,sbo=await asyncio.to_thread(calc)
      V65_STUDY.update(status='DONE',progress={'stage':'done'},finished_utc=utc_now(),result={
        'binance_time_sync':{'status':time_sync_status,'offset_ms':off,'synced_utc':V65_TIME_SYNC_UTC,'error':time_sync_error},
        'forward_execution_design':'completed signal candle -> immediate live bookTicker ASK; no extra 5m wait in forward paper execution',
        'comparison':{
          'V62_PULLBACK_REACCEL_TIME120':{'raw_signals':npb,'stats':spb},
          'V65_EARLY_BREAKOUT_TIME120':{'raw_signals':nbo,'rules':{'5m_change_min_pct':V65_BREAKOUT_5M_PCT,'volume_ratio_min':V65_VOLUME_RATIO_MIN,'volume_lookback_bars':V65_VOL_LOOKBACK,'selection':'Top1 per completed 5m slot by change then volume'},'stats':sbo}},
        'data':{'symbols_used':len(store),'symbols_failed':errs[:20]},
        'guardrails':['Paper/research only; no real orders.','Early breakout thresholds predeclared before result.','Historical test enters next bar open as causal proxy; forward paper design uses immediate live ASK after completed signal candle.','Active V61/V55/FAST_3S control unchanged.']})
    except Exception as e:V65_STUDY.update(status='ERROR',error=f'{type(e).__name__}: {e}',finished_utc=utc_now())

@app.get('/v65-combined-start')
async def v65_combined_start(symbols:int=Query(30,ge=20,le=50),days:int=Query(30,ge=20,le=40),entry_slip_pct:float=Query(.10,ge=0,le=1),cost_pct:float=Query(.15,ge=0,le=1)):
    global V65_TASK
    if V65_TASK is not None and not V65_TASK.done():return {'status':'ALREADY_RUNNING','progress':V65_STUDY.get('progress')}
    V65_TASK=asyncio.create_task(v65_run(symbols,days,entry_slip_pct,cost_pct))
    return {'status':'STARTED','paper_only':True,'active_strategy_changed':False,'challenger':'EARLY_BREAKOUT_0.50_VOL1.20'}

@app.get('/v65-combined-status')
async def v65_combined_status():
    return {**MODE_INFO,'status':'OK','panel':'V65_COMBINED_EARLY_BREAKOUT','trading':False,'orders':False,'active_strategy_changed':False,'active_risk_changed':False,'v61_fast_3s_changed':False,'study':V65_STUDY,'generated_utc':utc_now()}

@app.get('/v65-time-sync')
async def v65_time_sync():
    try:off=await get_binance_time_offset();return {'status':'OK','offset_ms':off,'synced_utc':V65_TIME_SYNC_UTC}
    except Exception as e:return {'status':'UNAVAILABLE_NON_BLOCKING','offset_ms':0,'error':str(e),'note':'Render cannot access Binance /time; research/backtest may continue without server-time offset.'}


# ============================================================
# V66 â€” EARLY BREAKOUT: MICRO-CONFIRMATION + FAILURE EXIT STUDY
# Research only. Active V61/V55/FAST_3S remains unchanged.
# Frozen source breakout: >=0.50% completed 5m candle + volume ratio >=1.20.
# ============================================================
V66_STUDY={"status":"IDLE","progress":{},"params":None,"result":None,"error":None,"started_utc":None,"finished_utc":None}
V66_TASK=None

def v66_candidates(store):
    by_slot={}
    for si,d in store.items():
        o,c,v,t=d['o'],d['c'],d.get('v'),d['t']
        if v is None: continue
        for i in range(20,len(o)-3):
            ch=((c[i]/o[i])-1)*100 if o[i] else 0
            av=sum(v[i-20:i])/20;vr=(v[i]/av) if av>0 else 0
            if ch>=0.50 and vr>=1.20: by_slot.setdefault(t[i],[]).append((ch,vr,si,i))
    out=[]
    for tm,a in by_slot.items():
        ch,vr,si,i=max(a,key=lambda x:(x[0],x[1]));out.append((tm,si,i,ch,vr))
    return sorted(out)

def v66_exit(store,si,entry_i,entry_px,breakout_level,cost,entry_slip,mode):
    d=store[si];o,h,l,c=d['o'],d['h'],d['l'],d['c'];end=min(len(o)-1,entry_i+24)
    ep=entry_px*(1+entry_slip/100)
    xi=end;reason='TIME_STOP_120M';xp=o[end]
    for j in range(entry_i,end+1):
        # Failure exit is causal at completed 5m close; execute next bar open.
        if mode=='FAILURE_EXIT' and c[j] < breakout_level and j+1 < len(o):
            xi=j+1;xp=o[xi];reason='BREAKOUT_FAILURE';break
    net=((xp/ep)-1)*100-cost
    return net,reason,xi

def v66_stats_rows(rows):
    rng=random.Random(66);return v61_stats([(r[0],r[1],r[2]) for r in rows],rng)

def v66_compute(store,cost,entry_slip):
    cand=v66_candidates(store);models={'BASE_IMMEDIATE':[],'CONFIRM_5M':[],'CONFIRM_10M':[],'CONFIRM_5M_FAILURE_EXIT':[],'CONFIRM_10M_FAILURE_EXIT':[]}
    busy={k:{} for k in models}
    for tm,si,i,ch,vr in cand:
        d=store[si];o,h,l,c,t=d['o'],d['h'],d['l'],d['c'],d['t'];breakout=c[i]
        specs=[('BASE_IMMEDIATE',i+1,True,'TIME'),
               ('CONFIRM_5M',i+2,(i+1<len(c) and c[i+1]>breakout),'TIME'),
               ('CONFIRM_10M',i+3,(i+2<len(c) and c[i+1]>breakout and c[i+2]>c[i+1]),'TIME'),
               ('CONFIRM_5M_FAILURE_EXIT',i+2,(i+1<len(c) and c[i+1]>breakout),'FAILURE_EXIT'),
               ('CONFIRM_10M_FAILURE_EXIT',i+3,(i+2<len(c) and c[i+1]>breakout and c[i+2]>c[i+1]),'FAILURE_EXIT')]
        for name,ei,ok,mode in specs:
            if not ok or ei>=len(o):continue
            etm=t[ei]
            if busy[name].get(si,0)>etm:continue
            net,reason,xi=v66_exit(store,si,ei,o[ei],breakout,cost,entry_slip,mode)
            busy[name][si]=t[min(xi,len(t)-1)];day=datetime.fromtimestamp(etm/1000,tz=timezone.utc).strftime('%Y-%m-%d')
            models[name].append((net,day,reason))
    return len(cand),{k:v66_stats_rows(v) for k,v in models.items()}

async def v66_run(n_symbols,days,entry_slip,cost):
    V66_STUDY.update(status='RUNNING',progress={'stage':'universe'},params={'symbols':n_symbols,'days':days,'entry_slip_pct':entry_slip,'cost_pct':cost},result=None,error=None,started_utc=utc_now(),finished_utc=None)
    try:
      async with httpx.AsyncClient(timeout=httpx.Timeout(60.0)) as client:
        uni=await build_universe(client);syms=[u['symbol'] for u in uni if u['symbol']!='BTCUSDT'][:n_symbols];sem=asyncio.Semaphore(6)
        async def fetch(sym):
          async with sem:
            try:return sym,await get_5m_candles_days(client,sym,days),None
            except Exception as e:return sym,None,str(e)
        aa,cc,ee,store={},{},{},{};errs=[];done=0
        for st in range(0,len(syms),6):
          for sym,candles,err in await asyncio.gather(*[fetch(x) for x in syms[st:st+6]]):
            done+=1;V66_STUDY['progress']={'stage':'fetch','done':done,'total':len(syms)}
            if err or not candles or len(candles)<400:errs.append({'symbol':sym,'error':err or 'insufficient'});continue
            si=len(store);await asyncio.to_thread(v61_extract_symbol,sym,si,candles,aa,cc,ee,store)
            store[si]['c']=array('d',[x['close'] for x in candles]);store[si]['v']=array('d',[x.get('volume',0.0) for x in candles])
      V66_STUDY['progress']={'stage':'simulate'};raw,res=await asyncio.to_thread(v66_compute,store,cost,entry_slip)
      V66_STUDY.update(status='DONE',progress={'stage':'done'},finished_utc=utc_now(),result={'raw_breakouts':raw,'models':res,
        'rules':{'breakout':'completed 5m >= +0.50% and volume >=1.20x prior20','confirm_5m':'next completed 5m close remains above breakout close','confirm_10m':'two subsequent completed closes rising above breakout close','failure_exit':'after entry, first completed 5m close below breakout close -> exit next 5m open','max_hold_minutes':120},
        'data':{'symbols_used':len(store),'symbols_failed':errs[:20]},'guardrails':['Research only; no real orders.','Rules fixed before result.','No threshold optimization in this run.','Active V61/V55/FAST_3S unchanged.']})
    except Exception as e:V66_STUDY.update(status='ERROR',error=f'{type(e).__name__}: {e}',finished_utc=utc_now())

@app.get('/v66-start')
async def v66_start(symbols:int=Query(30,ge=20,le=50),days:int=Query(30,ge=20,le=40),entry_slip_pct:float=Query(.10,ge=0,le=1),cost_pct:float=Query(.15,ge=0,le=1)):
    global V66_TASK
    if V66_TASK is not None and not V66_TASK.done():return {'status':'ALREADY_RUNNING','progress':V66_STUDY.get('progress')}
    V66_TASK=asyncio.create_task(v66_run(symbols,days,entry_slip_pct,cost_pct));return {'status':'STARTED','paper_only':True,'models':['BASE_IMMEDIATE','CONFIRM_5M','CONFIRM_10M','CONFIRM_5M_FAILURE_EXIT','CONFIRM_10M_FAILURE_EXIT']}
@app.get('/v66-status')
async def v66_status():return {**MODE_INFO,'status':'OK','panel':'V66_BREAKOUT_FAILURE_TEST','trading':False,'orders':False,'active_strategy_changed':False,'active_risk_changed':False,'study':V66_STUDY,'generated_utc':utc_now()}


# ============================================================
# V67 â€” CONFIRM_10M POST-ENTRY WINNER/LOSER DIAGNOSTIC
# Frozen entry: +0.50% 5m breakout, volume >=1.20x, 10m confirmation.
# Exit label/control: TIME120. Diagnostic only; no strategy change.
# ============================================================
V67_STUDY={"status":"IDLE","progress":{},"params":None,"result":None,"error":None,"started_utc":None,"finished_utc":None}
V67_TASK=None
V67_HORIZONS=[5,10,15,20,30]

def v67_summ(a):
    a=[float(x) for x in a if x is not None and math.isfinite(float(x))]
    return {'n':len(a),'mean':round(sum(a)/len(a),5) if a else None,'median':round(statistics.median(a),5) if a else None}

def v67_compute(store,cost,entry_slip):
    cand=v66_candidates(store);rows=[];busy={}
    for tm,si,i,ch,vr in cand:
        d=store[si];o,h,l,c,t=d['o'],d['h'],d['l'],d['c'],d['t'];breakout=c[i]
        if i+3>=len(o) or not (c[i+1]>breakout and c[i+2]>c[i+1]):continue
        ei=i+3;et=t[ei]
        if busy.get(si,0)>et:continue
        cfg={'name':'TIME_ONLY_120m','stop':None,'act':None,'dist':None,'hold':120}
        sim=v61_simulate(o,h,l,ei,cfg,entry_slip,entry_slip,0.0,cost)
        if sim is None:continue
        net,reason,xi=sim;busy[si]=t[min(xi,len(t)-1)];ep=o[ei]*(1+entry_slip/100)
        r={'net':float(net),'winner':net>0,'day':datetime.fromtimestamp(et/1000,tz=timezone.utc).strftime('%Y-%m-%d'),'breakout_chg':ch,'breakout_vr':vr}
        for mins in V67_HORIZONS:
            bars=mins//5;end=min(len(o)-1,ei+bars-1);seg_h=h[ei:end+1];seg_l=l[ei:end+1]
            last=c[end];r[f'ret_{mins}m']=((last/ep)-1)*100;r[f'mfe_{mins}m']=((max(seg_h)/ep)-1)*100;r[f'mae_{mins}m']=((min(seg_l)/ep)-1)*100
            r[f'above_breakout_{mins}m']=1.0 if last>=breakout else 0.0;r[f'new_high_{mins}m']=1.0 if max(seg_h)>max(h[max(0,i-5):i+1]) else 0.0
        rows.append(r)
    features=['breakout_chg','breakout_vr']+[f'{x}_{m}m' for m in V67_HORIZONS for x in ('ret','mfe','mae','above_breakout','new_high')]
    groups={}
    for name,pred in [('WINNERS',lambda r:r['winner']),('LOSERS',lambda r:not r['winner'])]:
        rr=[r for r in rows if pred(r)];groups[name]={'n':len(rr),'mean_final_net':round(sum(r['net'] for r in rr)/len(rr),5) if rr else None,'features':{f:v67_summ([r.get(f) for r in rr]) for f in features}}
    dif={f:{'winner_mean':groups['WINNERS']['features'][f]['mean'],'loser_mean':groups['LOSERS']['features'][f]['mean']} for f in features}
    for f,v in dif.items():
        a,b=v['winner_mean'],v['loser_mean'];v['difference']=round(a-b,5) if a is not None and b is not None else None
    days=sorted(set(r['day'] for r in rows));cut=days[len(days)//2] if days else None
    temporal={}
    for lab,rr in [('FIRST_HALF',[r for r in rows if cut and r['day']<cut]),('SECOND_HALF',[r for r in rows if cut and r['day']>=cut])]:
        temporal[lab]={'n':len(rr),'win_rate_pct':round(100*sum(r['winner'] for r in rr)/len(rr),2) if rr else None,
          'mean_net_pct':round(sum(r['net'] for r in rr)/len(rr),5) if rr else None}
    return len(cand),len(rows),groups,dif,temporal

async def v67_run(n_symbols,days,entry_slip,cost):
    V67_STUDY.update(status='RUNNING',progress={'stage':'universe'},params={'symbols':n_symbols,'days':days,'entry_slip_pct':entry_slip,'cost_pct':cost},result=None,error=None,started_utc=utc_now(),finished_utc=None)
    try:
      async with httpx.AsyncClient(timeout=httpx.Timeout(60.0)) as client:
        uni=await build_universe(client);syms=[u['symbol'] for u in uni if u['symbol']!='BTCUSDT'][:n_symbols];sem=asyncio.Semaphore(6)
        async def fetch(sym):
          async with sem:
            try:return sym,await get_5m_candles_days(client,sym,days),None
            except Exception as e:return sym,None,str(e)
        aa,cc,ee,store={},{},{},{};errs=[];done=0
        for st in range(0,len(syms),6):
          for sym,candles,err in await asyncio.gather(*[fetch(x) for x in syms[st:st+6]]):
            done+=1;V67_STUDY['progress']={'stage':'fetch','done':done,'total':len(syms)}
            if err or not candles or len(candles)<400:errs.append({'symbol':sym,'error':err or 'insufficient'});continue
            si=len(store);await asyncio.to_thread(v61_extract_symbol,sym,si,candles,aa,cc,ee,store)
            store[si]['c']=array('d',[x['close'] for x in candles]);store[si]['v']=array('d',[x.get('volume',0.0) for x in candles])
      V67_STUDY['progress']={'stage':'diagnostic'};raw,n,groups,dif,temp=await asyncio.to_thread(v67_compute,store,cost,entry_slip)
      V67_STUDY.update(status='DONE',progress={'stage':'done'},finished_utc=utc_now(),result={'entry':'FROZEN_V66_CONFIRM_10M','exit_label':'TIME120_CONTROL','raw_breakouts':raw,'analyzed_trades':n,'winner_loser':groups,'feature_differences':dif,'temporal_summary':temp,'data':{'symbols_used':len(store),'symbols_failed':errs[:20]},'guardrails':['Diagnostic only.','No exit rule or threshold is selected in V67.','Any apparent early separator must be frozen and validated separately before forward use.','Active V61/V55/FAST_3S unchanged.']})
    except Exception as e:V67_STUDY.update(status='ERROR',error=f'{type(e).__name__}: {e}',finished_utc=utc_now())

@app.get('/v67-start')
async def v67_start(symbols:int=Query(40,ge=20,le=50),days:int=Query(40,ge=20,le=40),entry_slip_pct:float=Query(.10,ge=0,le=1),cost_pct:float=Query(.15,ge=0,le=1)):
    global V67_TASK
    if V67_TASK is not None and not V67_TASK.done():return {'status':'ALREADY_RUNNING','progress':V67_STUDY.get('progress')}
    V67_TASK=asyncio.create_task(v67_run(symbols,days,entry_slip_pct,cost_pct));return {'status':'STARTED','paper_only':True,'entry':'FROZEN_V66_CONFIRM_10M','horizons_minutes':V67_HORIZONS}
@app.get('/v67-status')
async def v67_status():return {**MODE_INFO,'status':'OK','panel':'V67_POST_ENTRY_DIAGNOSTIC','trading':False,'orders':False,'active_strategy_changed':False,'active_risk_changed':False,'study':V67_STUDY,'generated_utc':utc_now()}


# ============================================================
# V68 â€” FROZEN CONFIRM_10M + MOMENTUM FAILURE EXIT VALIDATION
# Entry unchanged from V66/V67.
# Predeclared exit challengers only; no threshold mining.
# ============================================================
V68_STUDY={"status":"IDLE","progress":{},"params":None,"result":None,"error":None,"started_utc":None,"finished_utc":None}
V68_TASK=None

def v68_trade(o,h,l,c,t,ei,breakout,cost,entry_slip,rule):
    ep=o[ei]*(1+entry_slip/100)
    due=min(len(o)-1,ei+24)
    xi=due; reason="TIME_STOP_120M"
    check_bars=None
    if rule=="EXIT_15M_NEGATIVE": check_bars=3
    elif rule in ("EXIT_20M_NEGATIVE","EXIT_20M_BELOW_BREAKOUT","EXIT_20M_NEG_AND_BELOW_BREAKOUT"): check_bars=4
    if check_bars is not None:
        ci=ei+check_bars-1
        if ci < len(c):
            ret=((c[ci]/ep)-1)*100
            below=c[ci] < breakout
            fire=(rule=="EXIT_15M_NEGATIVE" and ret<0) or \
                 (rule=="EXIT_20M_NEGATIVE" and ret<0) or \
                 (rule=="EXIT_20M_BELOW_BREAKOUT" and below) or \
                 (rule=="EXIT_20M_NEG_AND_BELOW_BREAKOUT" and ret<0 and below)
            if fire and ci+1 < len(o):
                xi=ci+1; reason=rule
    xp=o[xi]
    net=((xp/ep)-1)*100-cost
    return net,reason,xi

def v68_stats(rows):
    rng=random.Random(68)
    return v61_stats([(r["net"],r["day"],r["reason"]) for r in rows],rng)

def v68_compute(store,cost,entry_slip):
    cand=v66_candidates(store)
    names=["TIME120_CONTROL","EXIT_15M_NEGATIVE","EXIT_20M_NEGATIVE","EXIT_20M_BELOW_BREAKOUT","EXIT_20M_NEG_AND_BELOW_BREAKOUT"]
    rows={x:[] for x in names}; busy={x:{} for x in names}
    for tm,si,i,ch,vr in cand:
        d=store[si];o,h,l,c,t=d["o"],d["h"],d["l"],d["c"],d["t"]; breakout=c[i]
        if i+3>=len(o) or not (c[i+1]>breakout and c[i+2]>c[i+1]): continue
        ei=i+3; et=t[ei]
        for name in names:
            if busy[name].get(si,0)>et: continue
            if name=="TIME120_CONTROL":
                ep=o[ei]*(1+entry_slip/100); xi=min(len(o)-1,ei+24); xp=o[xi]
                net=((xp/ep)-1)*100-cost; reason="TIME_STOP_120M"
            else:
                net,reason,xi=v68_trade(o,h,l,c,t,ei,breakout,cost,entry_slip,name)
            busy[name][si]=t[min(xi,len(t)-1)]
            day=datetime.fromtimestamp(et/1000,tz=timezone.utc).strftime("%Y-%m-%d")
            rows[name].append({"net":float(net),"day":day,"reason":reason})
    out={}
    for name,rr in rows.items():
        st=v68_stats(rr)
        days=sorted(set(r["day"] for r in rr)); cut=days[len(days)//2] if days else None
        halves={}
        for lab,sub in [("FIRST_HALF",[r for r in rr if cut and r["day"]<cut]),("SECOND_HALF",[r for r in rr if cut and r["day"]>=cut])]:
            halves[lab]=v68_stats(sub) if sub else {"n":0}
        st["temporal_halves"]=halves
        out[name]=st
    return len(cand),out

async def v68_run(n_symbols,days,entry_slip,cost):
    V68_STUDY.update(status="RUNNING",progress={"stage":"universe"},params={"symbols":n_symbols,"days":days,"entry_slip_pct":entry_slip,"cost_pct":cost},result=None,error=None,started_utc=utc_now(),finished_utc=None)
    try:
      async with httpx.AsyncClient(timeout=httpx.Timeout(60.0)) as client:
        uni=await build_universe(client); syms=[u["symbol"] for u in uni if u["symbol"]!="BTCUSDT"][:n_symbols]; sem=asyncio.Semaphore(6)
        async def fetch(sym):
          async with sem:
            try:return sym,await get_5m_candles_days(client,sym,days),None
            except Exception as e:return sym,None,str(e)
        aa,cc,ee,store={},{},{},{}; errs=[]; done=0
        for st in range(0,len(syms),6):
          for sym,candles,err in await asyncio.gather(*[fetch(x) for x in syms[st:st+6]]):
            done+=1; V68_STUDY["progress"]={"stage":"fetch","done":done,"total":len(syms)}
            if err or not candles or len(candles)<400: errs.append({"symbol":sym,"error":err or "insufficient"}); continue
            si=len(store); await asyncio.to_thread(v61_extract_symbol,sym,si,candles,aa,cc,ee,store)
            store[si]["c"]=array("d",[x["close"] for x in candles]); store[si]["v"]=array("d",[x.get("volume",0.0) for x in candles])
      V68_STUDY["progress"]={"stage":"simulate"}
      raw,res=await asyncio.to_thread(v68_compute,store,cost,entry_slip)
      V68_STUDY.update(status="DONE",progress={"stage":"done"},finished_utc=utc_now(),result={
        "entry":"FROZEN_V66_CONFIRM_10M",
        "raw_breakouts":raw,
        "models":res,
        "rules":{
          "control":"hold to 120m",
          "exit_15m_negative":"at 15m checkpoint, if net mark-to-market < 0, exit next 5m open",
          "exit_20m_negative":"at 20m checkpoint, if net mark-to-market < 0, exit next 5m open",
          "exit_20m_below_breakout":"at 20m checkpoint, if close < original breakout close, exit next 5m open",
          "exit_20m_neg_and_below":"at 20m checkpoint, require both negative return and below breakout"
        },
        "data":{"symbols_used":len(store),"symbols_failed":errs[:20]},
        "guardrails":["Research only; no real orders.","Entry frozen.","Exit hypotheses predeclared before this validation.","Active V61/V55/FAST_3S unchanged."]
      })
    except Exception as e:
      V68_STUDY.update(status="ERROR",error=f"{type(e).__name__}: {e}",finished_utc=utc_now())

@app.get("/v68-start")
async def v68_start(symbols:int=Query(40,ge=20,le=50),days:int=Query(40,ge=20,le=40),entry_slip_pct:float=Query(.10,ge=0,le=1),cost_pct:float=Query(.15,ge=0,le=1)):
    global V68_TASK
    if V68_TASK is not None and not V68_TASK.done(): return {"status":"ALREADY_RUNNING","progress":V68_STUDY.get("progress")}
    V68_TASK=asyncio.create_task(v68_run(symbols,days,entry_slip_pct,cost_pct))
    return {"status":"STARTED","paper_only":True,"entry":"FROZEN_V66_CONFIRM_10M","models":["TIME120_CONTROL","EXIT_15M_NEGATIVE","EXIT_20M_NEGATIVE","EXIT_20M_BELOW_BREAKOUT","EXIT_20M_NEG_AND_BELOW_BREAKOUT"]}

@app.get("/v68-status")
async def v68_status():
    return {**MODE_INFO,"status":"OK","panel":"V68_MOMENTUM_FAILURE_EXIT","trading":False,"orders":False,"active_strategy_changed":False,"active_risk_changed":False,"study":V68_STUDY,"generated_utc":utc_now()}


# ============================================================
# V69 â€” DYNAMIC BTC + MARKET REGIME VALIDATION
# Frozen entry: V66 CONFIRM_10M. Frozen exit: TIME120.
# Regime research only; active forward strategy unchanged.
# ============================================================
V69_STUDY={"status":"IDLE","progress":{},"params":None,"result":None,"error":None,"started_utc":None,"finished_utc":None}
V69_TASK=None

def v69_pct(a,b):
    return ((b/a)-1)*100 if a else None

def v69_stats(rr):
    rng=random.Random(69)
    return v61_stats([(x["net"],x["day"],"TIME_STOP_120M") for x in rr],rng) if rr else {"n":0}

def v69_compute(store, btc, cost, entry_slip):
    cand=v66_candidates(store)
    bt={int(x["open_time"]):x for x in btc}
    btc_times=sorted(bt)
    rows=[]; busy={}
    # market breadth: mean prior 30m return across available alt symbols at each entry slot
    for tm,si,i,ch,vr in cand:
        d=store[si];o,h,l,c,t=d["o"],d["h"],d["l"],d["c"],d["t"]; breakout=c[i]
        if i+3>=len(o) or not (c[i+1]>breakout and c[i+2]>c[i+1]): continue
        ei=i+3; et=t[ei]
        if busy.get(si,0)>et: continue
        xi=min(len(o)-1,ei+24); ep=o[ei]*(1+entry_slip/100); net=((o[xi]/ep)-1)*100-cost
        busy[si]=t[xi]; day=datetime.fromtimestamp(et/1000,tz=timezone.utc).strftime("%Y-%m-%d")
        # BTC completed 5m bar at/before entry; returns ending at its open/close neighborhood.
        pos=bisect.bisect_right(btc_times,et)-1
        if pos<48: continue
        def btc_ret(bars):
            p0=float(bt[btc_times[pos-bars]]["close"]); p1=float(bt[btc_times[pos]]["close"])
            return v69_pct(p0,p1)
        b30,b60,b240=btc_ret(6),btc_ret(12),btc_ret(48)
        alt30=[]
        for sj,dd in store.items():
            tt=dd["t"]; k=bisect.bisect_right(tt,et)-1
            if k>=6:
                alt30.append(v69_pct(dd["c"][k-6],dd["c"][k]))
        breadth=sum(alt30)/len(alt30) if alt30 else None
        rows.append({"net":net,"day":day,"btc30":b30,"btc60":b60,"btc240":b240,"breadth30":breadth})
    regimes={
      "ALL":lambda r:True,
      "BTC_30M_UP":lambda r:r["btc30"]>0,
      "BTC_1H_UP":lambda r:r["btc60"]>0,
      "BTC_4H_UP":lambda r:r["btc240"]>0,
      "BTC_ALL_UP":lambda r:r["btc30"]>0 and r["btc60"]>0 and r["btc240"]>0,
      "BTC_ALL_DOWN":lambda r:r["btc30"]<=0 and r["btc60"]<=0 and r["btc240"]<=0,
      "BTC_1H_4H_UP":lambda r:r["btc60"]>0 and r["btc240"]>0,
      "BTC_1H_4H_UP_MARKET_NOT_HOT":lambda r:r["btc60"]>0 and r["btc240"]>0 and r["breadth30"] is not None and r["breadth30"]<0.5,
      "BTC_ALL_UP_MARKET_NOT_HOT":lambda r:r["btc30"]>0 and r["btc60"]>0 and r["btc240"]>0 and r["breadth30"] is not None and r["breadth30"]<0.5,
      "MARKET_BREADTH30_POSITIVE":lambda r:r["breadth30"] is not None and r["breadth30"]>0,
      "MARKET_BREADTH30_NEGATIVE":lambda r:r["breadth30"] is not None and r["breadth30"]<=0,
    }
    out={}
    for name,pred in regimes.items():
        rr=[r for r in rows if pred(r)]
        st=v69_stats(rr)
        days=sorted(set(r["day"] for r in rr)); cut=days[len(days)//2] if days else None
        st["temporal_halves"]={
          "FIRST_HALF":v69_stats([r for r in rr if cut and r["day"]<cut]),
          "SECOND_HALF":v69_stats([r for r in rr if cut and r["day"]>=cut])
        }
        st["mean_context"]={
          "btc30_pct":round(sum(r["btc30"] for r in rr)/len(rr),4) if rr else None,
          "btc1h_pct":round(sum(r["btc60"] for r in rr)/len(rr),4) if rr else None,
          "btc4h_pct":round(sum(r["btc240"] for r in rr)/len(rr),4) if rr else None,
          "alt_breadth30_pct":round(sum(r["breadth30"] for r in rr if r["breadth30"] is not None)/sum(1 for r in rr if r["breadth30"] is not None),4) if any(r["breadth30"] is not None for r in rr) else None
        }
        out[name]=st
    return len(cand),len(rows),out

async def v69_run(n_symbols,days,entry_slip,cost):
    V69_STUDY.update(status="RUNNING",progress={"stage":"universe"},params={"symbols":n_symbols,"days":days,"entry_slip_pct":entry_slip,"cost_pct":cost},result=None,error=None,started_utc=utc_now(),finished_utc=None)
    try:
      async with httpx.AsyncClient(timeout=httpx.Timeout(60.0)) as client:
        uni=await build_universe(client); syms=[u["symbol"] for u in uni if u["symbol"]!="BTCUSDT"][:n_symbols]; sem=asyncio.Semaphore(6)
        async def fetch(sym):
          async with sem:
            try:return sym,await get_5m_candles_days(client,sym,days),None
            except Exception as e:return sym,None,str(e)
        aa,cc,ee,store={},{},{},{}; errs=[]; done=0
        for st in range(0,len(syms),6):
          for sym,candles,err in await asyncio.gather(*[fetch(x) for x in syms[st:st+6]]):
            done+=1; V69_STUDY["progress"]={"stage":"fetch_alts","done":done,"total":len(syms)}
            if err or not candles or len(candles)<400: errs.append({"symbol":sym,"error":err or "insufficient"}); continue
            si=len(store); await asyncio.to_thread(v61_extract_symbol,sym,si,candles,aa,cc,ee,store)
            store[si]["c"]=array("d",[x["close"] for x in candles]); store[si]["v"]=array("d",[x.get("volume",0.0) for x in candles])
        V69_STUDY["progress"]={"stage":"fetch_btc"}
        btc=await get_5m_candles_days(client,"BTCUSDT",days+1)
      V69_STUDY["progress"]={"stage":"regime_analysis"}
      raw,n,res=await asyncio.to_thread(v69_compute,store,btc,cost,entry_slip)
      V69_STUDY.update(status="DONE",progress={"stage":"done"},finished_utc=utc_now(),result={
        "entry":"FROZEN_V66_CONFIRM_10M","exit":"TIME120_CONTROL","raw_breakouts":raw,"analyzed_trades":n,
        "regimes":res,
        "regime_definitions":{
          "BTC_ALL_UP":"BTC 30m >0 AND 1h >0 AND 4h >0",
          "BTC_1H_4H_UP":"BTC 1h >0 AND 4h >0",
          "MARKET_NOT_HOT":"cross-sectional alt mean 30m return < +0.5%",
          "breadth30":"mean 30m return of available selected alt universe at entry time"
        },
        "data":{"symbols_used":len(store),"symbols_failed":errs[:20]},
        "guardrails":["Research only; no real orders.","Entry and TIME120 exit frozen.","Regime buckets predeclared; no threshold optimization in this run.","Active V61/V55/FAST_3S unchanged."]
      })
    except Exception as e:
      V69_STUDY.update(status="ERROR",error=f"{type(e).__name__}: {e}",finished_utc=utc_now())

@app.get("/v69-start")
async def v69_start(symbols:int=Query(40,ge=20,le=50),days:int=Query(40,ge=20,le=40),entry_slip_pct:float=Query(.10,ge=0,le=1),cost_pct:float=Query(.15,ge=0,le=1)):
    global V69_TASK
    if V69_TASK is not None and not V69_TASK.done(): return {"status":"ALREADY_RUNNING","progress":V69_STUDY.get("progress")}
    V69_TASK=asyncio.create_task(v69_run(symbols,days,entry_slip_pct,cost_pct))
    return {"status":"STARTED","paper_only":True,"entry":"FROZEN_V66_CONFIRM_10M","exit":"TIME120_CONTROL","study":"DYNAMIC_BTC_MARKET_REGIME"}

@app.get("/v69-status")
async def v69_status():
    return {**MODE_INFO,"status":"OK","panel":"V69_DYNAMIC_BTC_MARKET_REGIME","trading":False,"orders":False,"active_strategy_changed":False,"active_risk_changed":False,"study":V69_STUDY,"generated_utc":utc_now()}


# ============================================================
# V70 â€” BTC30 x ALT-BREADTH REGIME INTERSECTION VALIDATION
# Frozen entry: V66 CONFIRM_10M. Frozen exit: TIME120.
# No new thresholds; validates the V69 intersection hypothesis.
# ============================================================
V70_STUDY={"status":"IDLE","progress":{},"params":None,"result":None,"error":None,"started_utc":None,"finished_utc":None}
V70_TASK=None

def v70_compute(store, btc, cost, entry_slip):
    cand=v66_candidates(store)
    bt={int(x["open_time"]):x for x in btc}; btc_times=sorted(bt)
    rows=[]; busy={}
    for tm,si,i,ch,vr in cand:
        d=store[si];o,h,l,c,t=d["o"],d["h"],d["l"],d["c"],d["t"]; breakout=c[i]
        if i+3>=len(o) or not (c[i+1]>breakout and c[i+2]>c[i+1]): continue
        ei=i+3; et=t[ei]
        if busy.get(si,0)>et: continue
        xi=min(len(o)-1,ei+24); ep=o[ei]*(1+entry_slip/100); net=((o[xi]/ep)-1)*100-cost
        busy[si]=t[xi]
        pos=bisect.bisect_right(btc_times,et)-1
        if pos<6: continue
        btc30=v69_pct(float(bt[btc_times[pos-6]]["close"]),float(bt[btc_times[pos]]["close"]))
        alt30=[]
        for sj,dd in store.items():
            k=bisect.bisect_right(dd["t"],et)-1
            if k>=6: alt30.append(v69_pct(dd["c"][k-6],dd["c"][k]))
        breadth=sum(alt30)/len(alt30) if alt30 else None
        if breadth is None: continue
        day=datetime.fromtimestamp(et/1000,tz=timezone.utc).strftime("%Y-%m-%d")
        rows.append({"net":net,"day":day,"btc30":btc30,"breadth30":breadth})
    groups={
      "CONTROL_ALL":lambda r:True,
      "ALT_BREADTH30_POSITIVE":lambda r:r["breadth30"]>0,
      "BTC30_POSITIVE":lambda r:r["btc30"]>0,
      "BTC30_POSITIVE_AND_ALT_BREADTH30_POSITIVE":lambda r:r["btc30"]>0 and r["breadth30"]>0,
      "BTC30_NEGATIVE_OR_ALT_BREADTH30_NEGATIVE":lambda r:not (r["btc30"]>0 and r["breadth30"]>0),
    }
    out={}
    for name,pred in groups.items():
        rr=[r for r in rows if pred(r)]
        st=v69_stats(rr)
        days=sorted(set(r["day"] for r in rr)); cut=days[len(days)//2] if days else None
        st["temporal_halves"]={
          "FIRST_HALF":v69_stats([r for r in rr if cut and r["day"]<cut]),
          "SECOND_HALF":v69_stats([r for r in rr if cut and r["day"]>=cut])
        }
        st["mean_context"]={
          "btc30_pct":round(sum(r["btc30"] for r in rr)/len(rr),4) if rr else None,
          "alt_breadth30_pct":round(sum(r["breadth30"] for r in rr)/len(rr),4) if rr else None
        }
        out[name]=st
    return len(cand),len(rows),out

async def v70_run(n_symbols,days,entry_slip,cost):
    V70_STUDY.update(status="RUNNING",progress={"stage":"universe"},params={"symbols":n_symbols,"days":days,"entry_slip_pct":entry_slip,"cost_pct":cost},result=None,error=None,started_utc=utc_now(),finished_utc=None)
    try:
      async with httpx.AsyncClient(timeout=httpx.Timeout(60.0)) as client:
        uni=await build_universe(client); syms=[u["symbol"] for u in uni if u["symbol"]!="BTCUSDT"][:n_symbols]; sem=asyncio.Semaphore(6)
        async def fetch(sym):
          async with sem:
            try:return sym,await get_5m_candles_days(client,sym,days),None
            except Exception as e:return sym,None,str(e)
        aa,cc,ee,store={},{},{},{}; errs=[]; done=0
        for st in range(0,len(syms),6):
          for sym,candles,err in await asyncio.gather(*[fetch(x) for x in syms[st:st+6]]):
            done+=1; V70_STUDY["progress"]={"stage":"fetch_alts","done":done,"total":len(syms)}
            if err or not candles or len(candles)<400: errs.append({"symbol":sym,"error":err or "insufficient"}); continue
            si=len(store); await asyncio.to_thread(v61_extract_symbol,sym,si,candles,aa,cc,ee,store)
            store[si]["c"]=array("d",[x["close"] for x in candles]); store[si]["v"]=array("d",[x.get("volume",0.0) for x in candles])
        V70_STUDY["progress"]={"stage":"fetch_btc"}
        btc=await get_5m_candles_days(client,"BTCUSDT",days+1)
      V70_STUDY["progress"]={"stage":"intersection"}
      raw,n,res=await asyncio.to_thread(v70_compute,store,btc,cost,entry_slip)
      V70_STUDY.update(status="DONE",progress={"stage":"done"},finished_utc=utc_now(),result={
        "entry":"FROZEN_V66_CONFIRM_10M","exit":"TIME120_CONTROL","raw_breakouts":raw,"analyzed_trades":n,
        "groups":res,
        "primary_candidate":"BTC30_POSITIVE_AND_ALT_BREADTH30_POSITIVE",
        "rules":{"btc30_positive":"BTC trailing 30m return > 0","alt_breadth30_positive":"mean trailing 30m return across selected alt universe > 0"},
        "data":{"symbols_used":len(store),"symbols_failed":errs[:20]},
        "guardrails":["Research only; no real orders.","Entry and exit frozen.","No threshold optimization.","Primary intersection hypothesis declared before result.","Active V61/V55/FAST_3S unchanged."]
      })
    except Exception as e:
      V70_STUDY.update(status="ERROR",error=f"{type(e).__name__}: {e}",finished_utc=utc_now())

@app.get("/v70-start")
async def v70_start(symbols:int=Query(40,ge=20,le=50),days:int=Query(40,ge=20,le=40),entry_slip_pct:float=Query(.10,ge=0,le=1),cost_pct:float=Query(.15,ge=0,le=1)):
    global V70_TASK
    if V70_TASK is not None and not V70_TASK.done(): return {"status":"ALREADY_RUNNING","progress":V70_STUDY.get("progress")}
    V70_TASK=asyncio.create_task(v70_run(symbols,days,entry_slip_pct,cost_pct))
    return {"status":"STARTED","paper_only":True,"primary_candidate":"BTC30_POSITIVE_AND_ALT_BREADTH30_POSITIVE"}

@app.get("/v70-status")
async def v70_status():
    return {**MODE_INFO,"status":"OK","panel":"V70_REGIME_INTERSECTION_VALIDATION","trading":False,"orders":False,"active_strategy_changed":False,"active_risk_changed":False,"study":V70_STUDY,"generated_utc":utc_now()}


# ============================================================
# V71 â€” GOOD vs BAD REGIME DIAGNOSTIC
# Frozen cohort: V70 primary candidate
# BTC30 > 0 AND ALT breadth30 > 0
# Frozen entry V66 CONFIRM_10M + TIME120.
# Diagnostic only: NO threshold selection / NO active changes.
# ============================================================
V71_STUDY={"status":"IDLE","progress":{},"params":None,"result":None,"error":None,"started_utc":None,"finished_utc":None}
V71_TASK=None

def v71_mean(vals):
    vals=[x for x in vals if x is not None]
    return round(sum(vals)/len(vals),5) if vals else None

def v71_compute(store, btc, cost, entry_slip):
    cand=v66_candidates(store)
    bt={int(x["open_time"]):x for x in btc}; btimes=sorted(bt)
    rows=[]; busy={}
    for tm,si,i,ch,vr in cand:
        d=store[si];o,h,l,c,t=d["o"],d["h"],d["l"],d["c"],d["t"]; breakout=c[i]
        if i+3>=len(o) or not (c[i+1]>breakout and c[i+2]>c[i+1]): continue
        ei=i+3; et=t[ei]
        if busy.get(si,0)>et: continue
        xi=min(len(o)-1,ei+24); ep=o[ei]*(1+entry_slip/100); net=((o[xi]/ep)-1)*100-cost
        busy[si]=t[xi]
        bp=bisect.bisect_right(btimes,et)-1
        if bp<48: continue
        def br(bars):
            return v69_pct(float(bt[btimes[bp-bars]]["close"]),float(bt[btimes[bp]]["close"]))
        b30,b60,b120,b240=br(6),br(12),br(24),br(48)
        # BTC realized 1h volatility from last 12 completed 5m returns
        rets=[]
        for z in range(bp-11,bp+1):
            if z>0:
                p0=float(bt[btimes[z-1]]["close"]); p1=float(bt[btimes[z]]["close"])
                rets.append(v69_pct(p0,p1))
        btc_vol1h=statistics.pstdev(rets) if len(rets)>=2 else None
        alt30=[];alt60=[];alt240=[]
        for sj,dd in store.items():
            k=bisect.bisect_right(dd["t"],et)-1
            if k>=48:
                alt30.append(v69_pct(dd["c"][k-6],dd["c"][k]))
                alt60.append(v69_pct(dd["c"][k-12],dd["c"][k]))
                alt240.append(v69_pct(dd["c"][k-48],dd["c"][k]))
        breadth30=sum(alt30)/len(alt30) if alt30 else None
        breadth60=sum(alt60)/len(alt60) if alt60 else None
        breadth240=sum(alt240)/len(alt240) if alt240 else None
        # Frozen V70 primary cohort only
        if not (b30>0 and breadth30 is not None and breadth30>0): continue
        day=datetime.fromtimestamp(et/1000,tz=timezone.utc).strftime("%Y-%m-%d")
        rows.append({"net":net,"day":day,"btc30":b30,"btc60":b60,"btc120":b120,"btc240":b240,
                     "btc_accel_30_vs_60":b30-(b60/2.0),"btc_vol1h":btc_vol1h,
                     "breadth30":breadth30,"breadth60":breadth60,"breadth240":breadth240,
                     "breadth_accel_30_vs_60":breadth30-(breadth60/2.0) if breadth60 is not None else None})
    days=sorted(set(r["day"] for r in rows)); cut=days[len(days)//2] if days else None
    halves={"FIRST_HALF":[r for r in rows if cut and r["day"]<cut],
            "SECOND_HALF":[r for r in rows if cut and r["day"]>=cut]}
    def summarize(rr):
        return {
          "performance":v69_stats(rr),
          "context_means":{
            "btc30_pct":v71_mean([r["btc30"] for r in rr]),
            "btc1h_pct":v71_mean([r["btc60"] for r in rr]),
            "btc2h_pct":v71_mean([r["btc120"] for r in rr]),
            "btc4h_pct":v71_mean([r["btc240"] for r in rr]),
            "btc_accel_30_vs_60":v71_mean([r["btc_accel_30_vs_60"] for r in rr]),
            "btc_1h_realized_vol_pct":v71_mean([r["btc_vol1h"] for r in rr]),
            "alt_breadth30_pct":v71_mean([r["breadth30"] for r in rr]),
            "alt_breadth1h_pct":v71_mean([r["breadth60"] for r in rr]),
            "alt_breadth4h_pct":v71_mean([r["breadth240"] for r in rr]),
            "alt_breadth_accel_30_vs_60":v71_mean([r["breadth_accel_30_vs_60"] for r in rr])
          }
        }
    a=summarize(halves["FIRST_HALF"]); b=summarize(halves["SECOND_HALF"])
    dif={}
    for k in a["context_means"]:
        x=a["context_means"][k]; y=b["context_means"][k]
        dif[k]=round(y-x,5) if x is not None and y is not None else None
    return len(cand),len(rows),cut,{"FIRST_HALF":a,"SECOND_HALF":b,"SECOND_MINUS_FIRST":dif}

async def v71_run(n_symbols,days,entry_slip,cost):
    V71_STUDY.update(status="RUNNING",progress={"stage":"universe"},params={"symbols":n_symbols,"days":days,"entry_slip_pct":entry_slip,"cost_pct":cost},result=None,error=None,started_utc=utc_now(),finished_utc=None)
    try:
      async with httpx.AsyncClient(timeout=httpx.Timeout(60.0)) as client:
        uni=await build_universe(client); syms=[u["symbol"] for u in uni if u["symbol"]!="BTCUSDT"][:n_symbols]; sem=asyncio.Semaphore(6)
        async def fetch(sym):
          async with sem:
            try:return sym,await get_5m_candles_days(client,sym,days),None
            except Exception as e:return sym,None,str(e)
        aa,cc,ee,store={},{},{},{}; errs=[];done=0
        for st in range(0,len(syms),6):
          for sym,candles,err in await asyncio.gather(*[fetch(x) for x in syms[st:st+6]]):
            done+=1;V71_STUDY["progress"]={"stage":"fetch_alts","done":done,"total":len(syms)}
            if err or not candles or len(candles)<500: errs.append({"symbol":sym,"error":err or "insufficient"});continue
            si=len(store);await asyncio.to_thread(v61_extract_symbol,sym,si,candles,aa,cc,ee,store)
            store[si]["c"]=array("d",[x["close"] for x in candles]);store[si]["v"]=array("d",[x.get("volume",0.0) for x in candles])
        V71_STUDY["progress"]={"stage":"fetch_btc"};btc=await get_5m_candles_days(client,"BTCUSDT",days+1)
      V71_STUDY["progress"]={"stage":"diagnostic"}
      raw,n,cut,res=await asyncio.to_thread(v71_compute,store,btc,cost,entry_slip)
      V71_STUDY.update(status="DONE",progress={"stage":"done"},finished_utc=utc_now(),result={
        "cohort":"FROZEN_V70_BTC30_POSITIVE_AND_ALT_BREADTH30_POSITIVE",
        "entry":"FROZEN_V66_CONFIRM_10M","exit":"TIME120_CONTROL","raw_breakouts":raw,"cohort_trades":n,
        "half_split_day":cut,"diagnostic":res,
        "features":["BTC 30m/1h/2h/4h returns","BTC 30m-vs-1h acceleration","BTC 1h realized volatility","ALT breadth 30m/1h/4h","ALT breadth acceleration"],
        "data":{"symbols_used":len(store),"symbols_failed":errs[:20]},
        "guardrails":["Diagnostic only; no threshold selected.","Frozen V70 cohort only.","No real orders.","Active V61/V55/FAST_3S unchanged."]
      })
    except Exception as e:
      V71_STUDY.update(status="ERROR",error=f"{type(e).__name__}: {e}",finished_utc=utc_now())

@app.get("/v71-start")
async def v71_start(symbols:int=Query(40,ge=20,le=50),days:int=Query(40,ge=20,le=40),entry_slip_pct:float=Query(.10,ge=0,le=1),cost_pct:float=Query(.15,ge=0,le=1)):
    global V71_TASK
    if V71_TASK is not None and not V71_TASK.done(): return {"status":"ALREADY_RUNNING","progress":V71_STUDY.get("progress")}
    V71_TASK=asyncio.create_task(v71_run(symbols,days,entry_slip_pct,cost_pct))
    return {"status":"STARTED","paper_only":True,"study":"GOOD_VS_BAD_REGIME_DIAGNOSTIC","cohort":"FROZEN_V70_PRIMARY"}

@app.get("/v71-status")
async def v71_status():
    return {**MODE_INFO,"status":"OK","panel":"V71_REGIME_DIAGNOSTIC","trading":False,"orders":False,"active_strategy_changed":False,"active_risk_changed":False,"study":V71_STUDY,"generated_utc":utc_now()}


# ============================================================
# V72 â€” 4H MARKET BREADTH MONOTONICITY / BTC4H CROSS-CHECK
# Frozen cohort: V70 BTC30>0 AND ALT breadth30>0
# Frozen entry V66 CONFIRM_10M + TIME120.
# Quantile buckets are descriptive; no optimized numeric cutoff.
# ============================================================
V72_STUDY={"status":"IDLE","progress":{},"params":None,"result":None,"error":None,"started_utc":None,"finished_utc":None}
V72_TASK=None

def v72_quantile_cuts(vals):
    a=sorted(vals)
    if len(a)<3:return None,None
    return a[len(a)//3],a[(2*len(a))//3]

def v72_bucket(x,q1,q2):
    return "LOW" if x<=q1 else ("MID" if x<=q2 else "HIGH")

def v72_compute(store,btc,cost,entry_slip):
    cand=v66_candidates(store)
    bt={int(x["open_time"]):x for x in btc};btimes=sorted(bt)
    rows=[];busy={}
    for tm,si,i,ch,vr in cand:
        d=store[si];o,h,l,c,t=d["o"],d["h"],d["l"],d["c"],d["t"];breakout=c[i]
        if i+3>=len(o) or not(c[i+1]>breakout and c[i+2]>c[i+1]):continue
        ei=i+3;et=t[ei]
        if busy.get(si,0)>et:continue
        xi=min(len(o)-1,ei+24);ep=o[ei]*(1+entry_slip/100);net=((o[xi]/ep)-1)*100-cost;busy[si]=t[xi]
        bp=bisect.bisect_right(btimes,et)-1
        if bp<48:continue
        b30=v69_pct(float(bt[btimes[bp-6]]["close"]),float(bt[btimes[bp]]["close"]))
        b4=v69_pct(float(bt[btimes[bp-48]]["close"]),float(bt[btimes[bp]]["close"]))
        a30=[];a4=[]
        for sj,dd in store.items():
            k=bisect.bisect_right(dd["t"],et)-1
            if k>=48:
                a30.append(v69_pct(dd["c"][k-6],dd["c"][k]))
                a4.append(v69_pct(dd["c"][k-48],dd["c"][k]))
        if not a30 or not a4:continue
        br30=sum(a30)/len(a30);br4=sum(a4)/len(a4)
        if not(b30>0 and br30>0):continue
        day=datetime.fromtimestamp(et/1000,tz=timezone.utc).strftime("%Y-%m-%d")
        rows.append({"net":net,"day":day,"btc4":b4,"breadth4":br4})
    qA=v72_quantile_cuts([r["breadth4"] for r in rows]);qB=v72_quantile_cuts([r["btc4"] for r in rows])
    for r in rows:
        r["alt_bucket"]=v72_bucket(r["breadth4"],*qA);r["btc_bucket"]=v72_bucket(r["btc4"],*qB)
    def pack(rr):
        st=v69_stats(rr)
        days=sorted(set(r["day"] for r in rr));cut=days[len(days)//2] if days else None
        st["first_half"]=v69_stats([r for r in rr if cut and r["day"]<cut])
        st["second_half"]=v69_stats([r for r in rr if cut and r["day"]>=cut])
        st["mean_alt_breadth4h"]=v71_mean([r["breadth4"] for r in rr])
        st["mean_btc4h"]=v71_mean([r["btc4"] for r in rr])
        return st
    alt={b:pack([r for r in rows if r["alt_bucket"]==b]) for b in ("LOW","MID","HIGH")}
    btcg={b:pack([r for r in rows if r["btc_bucket"]==b]) for b in ("LOW","MID","HIGH")}
    cross={}
    for a in ("LOW","MID","HIGH"):
        for b in ("LOW","MID","HIGH"):
            cross[a+"_ALT__"+b+"_BTC"]=pack([r for r in rows if r["alt_bucket"]==a and r["btc_bucket"]==b])
    return len(cand),len(rows),{"alt_breadth4h_tertiles":alt,"btc4h_tertiles":btcg,"cross_3x3":cross,
      "descriptive_cutpoints":{"alt_breadth4h_q33":round(qA[0],5),"alt_breadth4h_q67":round(qA[1],5),
      "btc4h_q33":round(qB[0],5),"btc4h_q67":round(qB[1],5)}}

async def v72_run(n_symbols,days,entry_slip,cost):
    V72_STUDY.update(status="RUNNING",progress={"stage":"universe"},params={"symbols":n_symbols,"days":days,"entry_slip_pct":entry_slip,"cost_pct":cost},result=None,error=None,started_utc=utc_now(),finished_utc=None)
    try:
      async with httpx.AsyncClient(timeout=httpx.Timeout(60.0)) as client:
        uni=await build_universe(client);syms=[u["symbol"] for u in uni if u["symbol"]!="BTCUSDT"][:n_symbols];sem=asyncio.Semaphore(6)
        async def fetch(sym):
          async with sem:
            try:return sym,await get_5m_candles_days(client,sym,days),None
            except Exception as e:return sym,None,str(e)
        aa,cc,ee,store={},{},{},{};errs=[];done=0
        for st in range(0,len(syms),6):
          for sym,candles,err in await asyncio.gather(*[fetch(x) for x in syms[st:st+6]]):
            done+=1;V72_STUDY["progress"]={"stage":"fetch_alts","done":done,"total":len(syms)}
            if err or not candles or len(candles)<500:errs.append({"symbol":sym,"error":err or "insufficient"});continue
            si=len(store);await asyncio.to_thread(v61_extract_symbol,sym,si,candles,aa,cc,ee,store)
            store[si]["c"]=array("d",[x["close"] for x in candles]);store[si]["v"]=array("d",[x.get("volume",0.0) for x in candles])
        V72_STUDY["progress"]={"stage":"fetch_btc"};btc=await get_5m_candles_days(client,"BTCUSDT",days+1)
      V72_STUDY["progress"]={"stage":"tertiles"}
      raw,n,res=await asyncio.to_thread(v72_compute,store,btc,cost,entry_slip)
      V72_STUDY.update(status="DONE",progress={"stage":"done"},finished_utc=utc_now(),result={
       "cohort":"FROZEN_V70_BTC30_POSITIVE_AND_ALT_BREADTH30_POSITIVE","entry":"FROZEN_V66_CONFIRM_10M","exit":"TIME120_CONTROL",
       "raw_breakouts":raw,"cohort_trades":n,"analysis":res,
       "guardrails":["Descriptive tertiles only; cutpoints are NOT strategy thresholds.","No threshold mining.","Entry/exit/cohort frozen.","Research only; no orders.","Active V61/V55/FAST_3S unchanged."],
       "data":{"symbols_used":len(store),"symbols_failed":errs[:20]}})
    except Exception as e:V72_STUDY.update(status="ERROR",error=f"{type(e).__name__}: {e}",finished_utc=utc_now())

@app.get("/v72-start")
async def v72_start(symbols:int=Query(40,ge=20,le=50),days:int=Query(40,ge=20,le=40),entry_slip_pct:float=Query(.10,ge=0,le=1),cost_pct:float=Query(.15,ge=0,le=1)):
    global V72_TASK
    if V72_TASK is not None and not V72_TASK.done():return {"status":"ALREADY_RUNNING","progress":V72_STUDY.get("progress")}
    V72_TASK=asyncio.create_task(v72_run(symbols,days,entry_slip_pct,cost_pct))
    return {"status":"STARTED","paper_only":True,"study":"ALT_BREADTH4H_MONOTONICITY_AND_BTC4H_CROSSCHECK"}

@app.get("/v72-status")
async def v72_status():
    return {**MODE_INFO,"status":"OK","panel":"V72_BREADTH4H_MONOTONICITY","trading":False,"orders":False,"active_strategy_changed":False,"active_risk_changed":False,"study":V72_STUDY,"generated_utc":utc_now()}



async def v73_fetch_window(client, symbol, start_ms, end_ms):
    """Fetch only the requested completed 5m historical window."""
    out={}
    cursor=end_ms
    max_pages=40
    for _ in range(max_pages):
        raw=await get_json(client,"/api/v3/klines",params={
            "symbol":symbol,"interval":"5m","limit":1000,"endTime":cursor
        })
        if not raw: break
        oldest=int(raw[0][0])
        for k in raw:
            ot=int(k[0]); ct=int(k[6])
            if start_ms <= ot < end_ms and ct < end_ms:
                out[ot]={"open_time":ot,"open":float(k[1]),"high":float(k[2]),"low":float(k[3]),
                         "close":float(k[4]),"volume":float(k[5]),"close_time":ct}
        if oldest <= start_ms: break
        cursor=oldest-1
        await asyncio.sleep(0.01)
    return sorted(out.values(),key=lambda x:x["open_time"])

# ============================================================
# V73.4 â€” TRUE RESUME OOS MID-ZONE VALIDATION
# Operational durability only. Frozen research rules are unchanged.
# Per-symbol candles are persisted to PostgreSQL so a Render restart resumes
# from the first unfinished symbol instead of restarting the study.
# ============================================================
V73_STUDY={"status":"IDLE","progress":{},"params":None,"result":None,"error":None,"started_utc":None,"finished_utc":None}
V73_TASK=None
V73_ALT_LO=0.26294; V73_ALT_HI=1.12931
V73_BTC_LO=0.02413; V73_BTC_HI=0.39814
V73_RUN_KEY="V73_4_FROZEN_OOS_TRUE_RESUME"

def v73_db_init():
    if not V21_DB_URL:return False
    with v21_db_connect() as conn:
      with conn.cursor() as cur:
        cur.execute("""CREATE TABLE IF NOT EXISTS alt_v73_research_state(
          run_key TEXT PRIMARY KEY, payload JSONB NOT NULL, updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW())""")
        cur.execute("""CREATE TABLE IF NOT EXISTS alt_v73_symbol_cache(
          run_key TEXT NOT NULL, symbol TEXT NOT NULL, payload JSONB NOT NULL,
          updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
          PRIMARY KEY(run_key,symbol))""")
      conn.commit()
    return True

def v73_db_save(payload=None):
    if not V21_DB_URL:return False
    p=payload if payload is not None else V73_STUDY
    with v21_db_connect() as conn:
      with conn.cursor() as cur:
        cur.execute("""INSERT INTO alt_v73_research_state(run_key,payload,updated_at)
          VALUES(%s,%s::jsonb,NOW()) ON CONFLICT(run_key) DO UPDATE
          SET payload=EXCLUDED.payload,updated_at=NOW()""",(V73_RUN_KEY,json.dumps(p,default=str)))
      conn.commit()
    return True

def v73_db_load():
    if not V21_DB_URL:return None
    v73_db_init()
    with v21_db_connect() as conn:
      with conn.cursor() as cur:
        cur.execute("SELECT payload FROM alt_v73_research_state WHERE run_key=%s",(V73_RUN_KEY,))
        r=cur.fetchone()
    return r[0] if r else None

def v73_cache_put(symbol,candles):
    if not V21_DB_URL:return False
    with v21_db_connect() as conn:
      with conn.cursor() as cur:
        cur.execute("""INSERT INTO alt_v73_symbol_cache(run_key,symbol,payload,updated_at)
          VALUES(%s,%s,%s::jsonb,NOW()) ON CONFLICT(run_key,symbol) DO UPDATE
          SET payload=EXCLUDED.payload,updated_at=NOW()""",
          (V73_RUN_KEY,symbol,json.dumps(candles,default=str)))
      conn.commit()
    return True

def v73_cache_get(symbol):
    if not V21_DB_URL:return None
    with v21_db_connect() as conn:
      with conn.cursor() as cur:
        cur.execute("SELECT payload FROM alt_v73_symbol_cache WHERE run_key=%s AND symbol=%s",(V73_RUN_KEY,symbol))
        r=cur.fetchone()
    return r[0] if r else None

def v73_cache_symbols():
    if not V21_DB_URL:return set()
    with v21_db_connect() as conn:
      with conn.cursor() as cur:
        cur.execute("SELECT symbol FROM alt_v73_symbol_cache WHERE run_key=%s",(V73_RUN_KEY,))
        return {r[0] for r in cur.fetchall()}

def v73_cache_clear():
    if not V21_DB_URL:return
    with v21_db_connect() as conn:
      with conn.cursor() as cur:
        cur.execute("DELETE FROM alt_v73_symbol_cache WHERE run_key=%s",(V73_RUN_KEY,))
        cur.execute("DELETE FROM alt_v73_research_state WHERE run_key=%s",(V73_RUN_KEY,))
      conn.commit()

def v73_stats_blocks(rows,nblocks=4):
    st=v69_stats(rows);days=sorted(set(r["day"] for r in rows));blocks=[]
    if days:
      for bi in range(nblocks):
        a=(len(days)*bi)//nblocks;b=(len(days)*(bi+1))//nblocks;ds=set(days[a:b]);rr=[r for r in rows if r["day"] in ds]
        z=v69_stats(rr);z["block"]=bi+1;z["start_day"]=days[a] if a<len(days) else None;z["end_day"]=days[b-1] if b>a else None;blocks.append(z)
    st["chronological_blocks"]=blocks;return st

def v73_compute(store,btc,cost,entry_slip,cutoff_ms):
    cand=v66_candidates(store);bt={int(x["open_time"]):x for x in btc};btimes=sorted(bt);rows=[];busy={}
    for tm,si,i,ch,vr in cand:
      d=store[si];o,h,l,c,t=d["o"],d["h"],d["l"],d["c"],d["t"];breakout=c[i]
      if i+3>=len(o) or not(c[i+1]>breakout and c[i+2]>c[i+1]):continue
      ei=i+3;et=t[ei]
      if et>=cutoff_ms or busy.get(si,0)>et:continue
      xi=min(len(o)-1,ei+24);ep=o[ei]*(1+entry_slip/100);net=((o[xi]/ep)-1)*100-cost;busy[si]=t[xi]
      bp=bisect.bisect_right(btimes,et)-1
      if bp<48:continue
      b30=v69_pct(float(bt[btimes[bp-6]]["close"]),float(bt[btimes[bp]]["close"]));b4=v69_pct(float(bt[btimes[bp-48]]["close"]),float(bt[btimes[bp]]["close"]))
      a30=[];a4=[]
      for sj,dd in store.items():
        k=bisect.bisect_right(dd["t"],et)-1
        if k>=48:a30.append(v69_pct(dd["c"][k-6],dd["c"][k]));a4.append(v69_pct(dd["c"][k-48],dd["c"][k]))
      if not a30 or not a4:continue
      br30=sum(a30)/len(a30);br4=sum(a4)/len(a4)
      if not(b30>0 and br30>0):continue
      day=datetime.fromtimestamp(et/1000,tz=timezone.utc).strftime("%Y-%m-%d");rows.append({"net":net,"day":day,"btc4":b4,"breadth4":br4})
    mid=[r for r in rows if V73_ALT_LO<=r["breadth4"]<=V73_ALT_HI and V73_BTC_LO<=r["btc4"]<=V73_BTC_HI]
    altmid=[r for r in rows if V73_ALT_LO<=r["breadth4"]<=V73_ALT_HI]
    return len(cand),rows,altmid,mid


async def v73_run(n_symbols,history_days,oos_days,entry_slip,cost,resume=True):
    global V73_STUDY
    try:v73_db_init()
    except Exception:pass
    old=None
    try:old=v73_db_load()
    except Exception:pass
    started=(old or {}).get("started_utc") or utc_now()
    V73_STUDY={"status":"RUNNING","progress":{"stage":"universe","done":0,"total":n_symbols},
      "params":{"symbols":n_symbols,"history_days":history_days,"oos_days":oos_days,"entry_slip_pct":entry_slip,"cost_pct":cost},
      "result":None,"error":None,"started_utc":started,"finished_utc":None}
    try:v73_db_save()
    except Exception:pass
    try:
      # Fixed historical boundary for this frozen validation. Persist it so restart
      # cannot slide the OOS window forward.
      meta=(old or {}).get("resume_meta") or {}
      cutoff_iso=meta.get("cutoff_utc")
      cutoff=datetime.fromisoformat(cutoff_iso) if cutoff_iso else datetime.now(timezone.utc)-timedelta(days=40)
      cutoff_ms=int(cutoff.timestamp()*1000)
      window_start_ms=cutoff_ms-int((oos_days+3)*24*60*60*1000)
      V73_STUDY["resume_meta"]={"cutoff_utc":cutoff.isoformat()}
      v73_db_save()

      async with httpx.AsyncClient(timeout=httpx.Timeout(60.0)) as client:
        uni=await build_universe(client);syms=[u["symbol"] for u in uni if u["symbol"]!="BTCUSDT"][:n_symbols]
        cached=v73_cache_symbols() if resume else set()
        # Load/fetch each symbol independently. A completed symbol is durable.
        for idx,sym in enumerate(syms,1):
          if sym in cached:
            V73_STUDY["progress"]={"stage":"fetch_alts","done":idx,"total":len(syms),"resumed":True,"symbol":sym}
            continue
          candles=None;last_err=None
          for attempt in range(2):
            try:
              candles=await asyncio.wait_for(v73_fetch_window(client,sym,window_start_ms,cutoff_ms),timeout=75)
              if candles and len(candles)>=500:break
              last_err="insufficient"
            except Exception as e:last_err=f"{type(e).__name__}: {e}"
          # Persist failures too, so one bad symbol cannot block the run forever.
          payload=candles if candles and len(candles)>=500 else {"_failed":True,"error":last_err or "insufficient"}
          v73_cache_put(sym,payload)
          V73_STUDY["progress"]={"stage":"fetch_alts","done":idx,"total":len(syms),"resumed":bool(cached),"symbol":sym}
          v73_db_save()

        # BTC is also durable under a reserved cache key.
        btc=v73_cache_get("__BTCUSDT__")
        if not isinstance(btc,list):
          V73_STUDY["progress"]={"stage":"fetch_btc","done":len(syms),"total":len(syms)};v73_db_save()
          btc=await asyncio.wait_for(v73_fetch_window(client,"BTCUSDT",window_start_ms,cutoff_ms),timeout=90)
          if not btc or len(btc)<500:raise RuntimeError("BTC older OOS data insufficient")
          v73_cache_put("__BTCUSDT__",btc)

      # Rebuild the in-memory arrays from durable symbol cache, then compute.
      V73_STUDY["progress"]={"stage":"rebuild_cache","done":0,"total":len(syms)};v73_db_save()
      aa,cc,ee,store={},{},{},{};errs=[]
      for idx,sym in enumerate(syms,1):
        candles=v73_cache_get(sym)
        if not isinstance(candles,list) or len(candles)<500:
          err=candles.get("error") if isinstance(candles,dict) else "missing cache"
          errs.append({"symbol":sym,"error":err});continue
        si=len(store);await asyncio.to_thread(v61_extract_symbol,sym,si,candles,aa,cc,ee,store)
        store[si]["c"]=array("d",[x["close"] for x in candles]);store[si]["v"]=array("d",[x.get("volume",0.0) for x in candles])
        if idx%5==0:
          V73_STUDY["progress"]={"stage":"rebuild_cache","done":idx,"total":len(syms)};v73_db_save()

      V73_STUDY["progress"]={"stage":"oos_validation","done":len(syms),"total":len(syms)};v73_db_save()
      raw,base,altmid,mid=await asyncio.to_thread(v73_compute,store,btc,cost,entry_slip,cutoff_ms)
      def retention(x):return round(100*len(x)/len(base),2) if base else None
      V73_STUDY.update(status="DONE",progress={"stage":"done","done":len(syms),"total":len(syms)},finished_utc=utc_now(),result={
        "validation_type":"OLDER_NON_OVERLAPPING_OOS","discovery_window_excluded":"most recent 40 days","oos_end_utc":cutoff.isoformat(),
        "frozen_entry":"V66_CONFIRM_10M","frozen_exit":"TIME120","base_cohort":"V70_BTC30_POS_AND_ALT_BREADTH30_POS",
        "frozen_v72_zone":{"alt_breadth4h":[V73_ALT_LO,V73_ALT_HI],"btc4h":[V73_BTC_LO,V73_BTC_HI]},
        "raw_breakouts":raw,"BASE_V70":{**v73_stats_blocks(base),"retention_pct":100.0},
        "ALT4H_MID_ONLY":{**v73_stats_blocks(altmid),"retention_pct":retention(altmid)},
        "ALT4H_MID_AND_BTC4H_MID":{**v73_stats_blocks(mid),"retention_pct":retention(mid)},
        "data":{"symbols_used":len(store),"symbols_failed":errs[:20]},
        "guardrails":["Frozen V72 cutpoints applied unchanged to older OOS.","No threshold search in OOS.","Chronological 4-block stability included.","Current top-volume universe implies survivorship bias.","Research only; no orders.","Active V61/V55/FAST_3S unchanged.","V73.4 persistence changes execution of research only, not strategy rules."]})
      v73_db_save()
    except Exception as e:
      V73_STUDY.update(status="ERROR",error=f"{type(e).__name__}: {e}",finished_utc=utc_now())
      try:v73_db_save()
      except Exception:pass

@app.get("/v73-start")
async def v73_start(symbols:int=Query(40,ge=20,le=50),history_days:int=Query(100,ge=80,le=140),oos_days:int=Query(60,ge=40,le=90),entry_slip_pct:float=Query(.10,ge=0,le=1),cost_pct:float=Query(.15,ge=0,le=1)):
    global V73_TASK,V73_STUDY
    try:
      old=v73_db_load()
      if old and old.get("status")=="DONE":
        V73_STUDY=old;return {"status":"DONE_ALREADY","paper_only":True,"progress":old.get("progress")}
    except Exception:pass
    if V73_TASK is not None and not V73_TASK.done():return {"status":"ALREADY_RUNNING","progress":V73_STUDY.get("progress")}
    V73_TASK=asyncio.create_task(v73_run(symbols,history_days,oos_days,entry_slip_pct,cost_pct,True))
    return {"status":"STARTED_OR_RESUMED","paper_only":True,"study":"OLDER_OOS_MIDZONE_VALIDATION",
      "frozen_zone":{"alt4h":[V73_ALT_LO,V73_ALT_HI],"btc4h":[V73_BTC_LO,V73_BTC_HI]}}

@app.get("/v73-status")
async def v73_status():
    global V73_STUDY
    if V73_STUDY.get("status")=="IDLE":
      try:
        old=v73_db_load()
        if old:V73_STUDY=old
      except Exception:pass
    return {**MODE_INFO,"status":"OK","panel":"V73.4_TRUE_RESUME_OOS_MIDZONE_VALIDATION","trading":False,"orders":False,
      "active_strategy_changed":False,"active_risk_changed":False,"study":V73_STUDY,"generated_utc":utc_now()}


# ============================================================
# V74 â€” PROSPECTIVE PAPER: FROZEN MID-REGIME HYPOTHESIS
# User-requested prospective application of the latest hypothesis.
# PAPER ONLY. No exchange orders. Active V61/V55/FAST_3S remains untouched.
#
# Frozen entry:
#   V66 CONFIRM_10M: breakout 5m >= +0.50%, volume ratio >= 1.20,
#   Top1 per breakout slot, then two completed 5m closes rising above breakout.
# Frozen regime:
#   BTC30 > 0, ALT breadth30 > 0,
#   ALT breadth4h in [0.26294, 1.12931],
#   BTC4h in [0.02413, 0.39814].
# Execution: live ASK paper entry when detected; live BID paper exit at 120m.
# No hard stop/trailing in V74: TIME120 is the frozen validation exit.
# ============================================================
V74_ALT_LO=0.26294; V74_ALT_HI=1.12931
V74_BTC_LO=0.02413; V74_BTC_HI=0.39814
V74_SCAN_SECONDS=60
V74_STATE={"open":[],"closed":[],"seen":[],"last_scan":None,"errors":[]}
V74_TASK=None

def v74_db_init():
    if not V21_DB_URL:return False
    with v21_db_connect() as conn:
      with conn.cursor() as cur:
        cur.execute("""CREATE TABLE IF NOT EXISTS alt_v74_paper_state(
          id INTEGER PRIMARY KEY, payload JSONB NOT NULL, updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW())""")
      conn.commit()
    return True

def v74_save():
    if not V21_DB_URL:return False
    with v21_db_connect() as conn:
      with conn.cursor() as cur:
        cur.execute("""INSERT INTO alt_v74_paper_state(id,payload,updated_at) VALUES(1,%s::jsonb,NOW())
          ON CONFLICT(id) DO UPDATE SET payload=EXCLUDED.payload,updated_at=NOW()""",
          (json.dumps(V74_STATE,default=str),))
      conn.commit()
    return True

def v74_load():
    global V74_STATE
    if not V21_DB_URL:return
    v74_db_init()
    with v21_db_connect() as conn:
      with conn.cursor() as cur:
        cur.execute("SELECT payload FROM alt_v74_paper_state WHERE id=1");r=cur.fetchone()
    if r and isinstance(r[0],dict):V74_STATE=r[0]

async def v74_candles(client,sym,limit=90):
    raw=await get_json(client,"/api/v3/klines",params={"symbol":sym,"interval":"5m","limit":limit})
    now=alt_now_ms();out=[]
    for k in raw or []:
      if int(k[6])>=now:continue
      out.append({"open_time":int(k[0]),"open":float(k[1]),"high":float(k[2]),"low":float(k[3]),
                  "close":float(k[4]),"volume":float(k[5]),"close_time":int(k[6])})
    return out

def v74_pct(a,b):
    return ((b/a)-1)*100 if a else 0.0

async def v74_scan_once():
    scan_utc=utc_now()
    try:
      async with httpx.AsyncClient(timeout=httpx.Timeout(30.0)) as client:
        uni=await build_universe(client)
        syms=[u["symbol"] for u in uni if u["symbol"]!="BTCUSDT"][:40]
        sem=asyncio.Semaphore(12)
        async def one(sym):
          async with sem:
            try:return sym,await asyncio.wait_for(v74_candles(client,sym,90),25),None
            except Exception as e:return sym,None,f"{type(e).__name__}: {e}"
        vals=await asyncio.gather(*[one(x) for x in syms])
        data={sym:c for sym,c,e in vals if c and len(c)>=55}
        errs=[{"symbol":sym,"error":e} for sym,c,e in vals if e]
        btc=await v74_candles(client,"BTCUSDT",90)
        if len(btc)<55:raise RuntimeError("BTC candles insufficient")

        # First close due TIME120 positions using live BID.
        now=alt_now_ms();still=[]
        for p in V74_STATE.get("open",[]):
          if now < int(p["exit_due_ms"]):
            still.append(p);continue
          q=await live_quote(client,p["symbol"])
          xp=float(q["bid"]) if q else None
          if xp is None:
            cc=data.get(p["symbol"],[])
            xp=float(cc[-1]["close"]) if cc else float(p["entry_price"])
          gross=v74_pct(float(p["entry_price"]),xp);net=gross-0.15
          p.update(exit_price=xp,exit_utc=utc_now(),exit_reason="TIME120",
                   gross_pct=round(gross,4),cost_pct=0.15,net_pct=round(net,4),status="CLOSED")
          V74_STATE.setdefault("closed",[]).append(p)
        V74_STATE["open"]=still
        V74_STATE["closed"]=V74_STATE.get("closed",[])[-500:]

        # Current market regime at the causal entry checkpoint.
        b30=v74_pct(btc[-7]["close"],btc[-1]["close"])
        b4=v74_pct(btc[-49]["close"],btc[-1]["close"])
        a30=[v74_pct(c[-7]["close"],c[-1]["close"]) for c in data.values() if len(c)>=49]
        a4=[v74_pct(c[-49]["close"],c[-1]["close"]) for c in data.values() if len(c)>=49]
        br30=sum(a30)/len(a30) if a30 else -999
        br4=sum(a4)/len(a4) if a4 else -999
        regime=(b30>0 and br30>0 and V74_ALT_LO<=br4<=V74_ALT_HI and V74_BTC_LO<=b4<=V74_BTC_HI)

        # V66 CONFIRM10 candidate. Breakout is 3 bars before current next-open checkpoint:
        # breakout i, confirmation i+1 and i+2 are completed; entry is now/live.
        candidates=[]
        for sym,c in data.items():
          if len(c)<25:continue
          i=len(c)-3
          bo=c[i];ch=v74_pct(bo["open"],bo["close"])
          av=sum(x["volume"] for x in c[i-20:i])/20
          vr=(bo["volume"]/av) if av>0 else 0
          if ch<0.50 or vr<1.20:continue
          if not(c[i+1]["close"]>bo["close"] and c[i+2]["close"]>c[i+1]["close"]):continue
          candidates.append({"symbol":sym,"breakout_time":bo["open_time"],"breakout_change_pct":ch,
                             "volume_ratio":vr,"breakout_close":bo["close"],
                             "confirm2_close":c[i+2]["close"]})
        # Frozen Top1 per slot: breakout change, then volume ratio.
        candidates=sorted(candidates,key=lambda x:(x["breakout_change_pct"],x["volume_ratio"]),reverse=True)
        accepted=None
        if candidates and regime:
          e=candidates[0];key=f'{e["symbol"]}:{e["breakout_time"]}'
          seen=set(V74_STATE.get("seen",[]))
          already=any(p["symbol"]==e["symbol"] for p in V74_STATE.get("open",[]))
          if key not in seen and not already:
            q=await live_quote(client,e["symbol"])
            if q and q["spread_pct"]<=SPREAD_MAX_PCT:
              ep=float(q["ask"]);entry_ms=alt_now_ms()
              p={**e,"entry_price":ep,"entry_utc":utc_now(),"entry_ms":entry_ms,
                 "exit_due_ms":entry_ms+120*60*1000,"status":"OPEN",
                 "execution_version":"V74_LIVE_ASK_TIME120",
                 "spread_pct":round(float(q["spread_pct"]),4),
                 "btc30_pct":round(b30,4),"btc4h_pct":round(b4,4),
                 "alt_breadth30_pct":round(br30,4),"alt_breadth4h_pct":round(br4,4)}
              V74_STATE.setdefault("open",[]).append(p);accepted=p
            seen.add(key);V74_STATE["seen"]=list(seen)[-1000:]

        V74_STATE["last_scan"]={"utc":scan_utc,"symbols":len(data),"errors":len(errs),
          "regime_pass":regime,"btc30_pct":round(b30,4),"btc4h_pct":round(b4,4),
          "alt_breadth30_pct":round(br30,4),"alt_breadth4h_pct":round(br4,4),
          "confirm10_candidates":len(candidates),"accepted":accepted["symbol"] if accepted else None}
        V74_STATE["errors"]=errs[-20:]
        v74_save()
        return V74_STATE["last_scan"]
    except Exception as e:
      V74_STATE["last_scan"]={"utc":scan_utc,"error":f"{type(e).__name__}: {e}"}
      try:v74_save()
      except Exception:pass
      return V74_STATE["last_scan"]

async def v74_loop():
    while True:
      try:await v74_scan_once()
      except Exception:pass
      await asyncio.sleep(V74_SCAN_SECONDS)

@app.on_event("startup")
async def v74_startup():
    global V74_TASK
    try:v74_load()
    except Exception:pass
    if V74_TASK is None:V74_TASK=asyncio.create_task(v74_loop())

@app.get("/v74-scan-now")
async def v74_scan_now():
    return {"status":"OK","paper_only":True,"scan":await v74_scan_once()}

@app.get("/v74-status")
async def v74_status():
    cl=V74_STATE.get("closed",[])
    vals=[float(x.get("net_pct",0)) for x in cl]
    wins=[x for x in vals if x>0];loss=[x for x in vals if x<0]
    pf=(sum(wins)/abs(sum(loss))) if loss else (None if not wins else 999.0)
    return {**MODE_INFO,"status":"OK","panel":"V74_PROSPECTIVE_MIDREGIME",
      "trading":False,"orders":False,"active_v61_changed":False,
      "hypothesis":{"entry":"V66_CONFIRM_10M_TOP1","exit":"TIME120",
        "regime":"BTC30>0 & ALT30>0 & frozen ALT4h MID & frozen BTC4h MID",
        "alt4h_zone":[V74_ALT_LO,V74_ALT_HI],"btc4h_zone":[V74_BTC_LO,V74_BTC_HI]},
      "last_scan":V74_STATE.get("last_scan"),"open_count":len(V74_STATE.get("open",[])),
      "closed_count":len(cl),"performance":{"mean_net_pct":round(sum(vals)/len(vals),4) if vals else None,
        "win_rate_pct":round(100*len(wins)/len(vals),2) if vals else None,
        "profit_factor":round(pf,4) if pf is not None else None},
      "open":V74_STATE.get("open",[])[-20:],"recent_closed":cl[-30:],
      "generated_utc":utc_now()}


# ============================================================
# V75 â€” COMBINED VALIDATION PANEL
# Runs alongside V74 prospective paper strategy.
# Combines:
# 1) independent older OOS,
# 2) frozen "medium heat" hypothesis,
# 3) chronological blocks,
# 4) trade count / PF / bootstrap CI,
# without changing V74/V61 execution.
# ============================================================
V75_STATE={"status":"IDLE","progress":{},"params":None,"result":None,"error":None,"started_utc":None,"finished_utc":None}
V75_TASK=None
V75_RUN_KEY="V75_COMBINED_FROZEN_MIDREGIME"

def v75_db_init():
    if not V21_DB_URL:return False
    with v21_db_connect() as conn:
      with conn.cursor() as cur:
        cur.execute("""CREATE TABLE IF NOT EXISTS alt_v75_validation_state(
          run_key TEXT PRIMARY KEY,payload JSONB NOT NULL,updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW())""")
        cur.execute("""CREATE TABLE IF NOT EXISTS alt_v75_symbol_cache(
          run_key TEXT NOT NULL,symbol TEXT NOT NULL,payload JSONB NOT NULL,
          updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),PRIMARY KEY(run_key,symbol))""")
      conn.commit()
    return True

def v75_save():
    if not V21_DB_URL:return
    with v21_db_connect() as conn:
      with conn.cursor() as cur:
        cur.execute("""INSERT INTO alt_v75_validation_state(run_key,payload,updated_at)
          VALUES(%s,%s::jsonb,NOW()) ON CONFLICT(run_key) DO UPDATE
          SET payload=EXCLUDED.payload,updated_at=NOW()""",(V75_RUN_KEY,json.dumps(V75_STATE,default=str)))
      conn.commit()

def v75_load():
    if not V21_DB_URL:return None
    v75_db_init()
    with v21_db_connect() as conn:
      with conn.cursor() as cur:
        cur.execute("SELECT payload FROM alt_v75_validation_state WHERE run_key=%s",(V75_RUN_KEY,));r=cur.fetchone()
    return r[0] if r else None

def v75_cache_put(sym,payload):
    with v21_db_connect() as conn:
      with conn.cursor() as cur:
        cur.execute("""INSERT INTO alt_v75_symbol_cache(run_key,symbol,payload,updated_at)
          VALUES(%s,%s,%s::jsonb,NOW()) ON CONFLICT(run_key,symbol) DO UPDATE
          SET payload=EXCLUDED.payload,updated_at=NOW()""",(V75_RUN_KEY,sym,json.dumps(payload,default=str)))
      conn.commit()

def v75_cache_get(sym):
    with v21_db_connect() as conn:
      with conn.cursor() as cur:
        cur.execute("SELECT payload FROM alt_v75_symbol_cache WHERE run_key=%s AND symbol=%s",(V75_RUN_KEY,sym));r=cur.fetchone()
    return r[0] if r else None

def v75_cached():
    with v21_db_connect() as conn:
      with conn.cursor() as cur:
        cur.execute("SELECT symbol FROM alt_v75_symbol_cache WHERE run_key=%s",(V75_RUN_KEY,));return {x[0] for x in cur.fetchall()}

def v75_ci(vals,seed=7501,nboot=1200):
    if not vals:return [None,None]
    import random
    rr=random.Random(seed);n=len(vals);means=[]
    for _ in range(nboot):
      means.append(sum(vals[rr.randrange(n)] for __ in range(n))/n)
    means.sort()
    return [round(means[int(.025*(nboot-1))],4),round(means[int(.975*(nboot-1))],4)]

def v75_metrics(rows):
    vals=[float(r["net"]) for r in rows];w=[x for x in vals if x>0];l=[x for x in vals if x<0]
    pf=(sum(w)/abs(sum(l))) if l else (999.0 if w else None)
    return {"n":len(vals),"mean_net_pct":round(sum(vals)/len(vals),4) if vals else None,
      "median_net_pct":round(statistics.median(vals),4) if vals else None,
      "win_rate_pct":round(100*len(w)/len(vals),2) if vals else None,
      "profit_factor":round(pf,4) if pf is not None else None,
      "mean_95ci":v75_ci(vals)}

def v75_blocks(rows,n=4):
    days=sorted(set(r["day"] for r in rows));out=[]
    for bi in range(n):
      a=(len(days)*bi)//n;b=(len(days)*(bi+1))//n;ds=set(days[a:b]);rr=[r for r in rows if r["day"] in ds]
      out.append({"block":bi+1,"start_day":days[a] if a<len(days) else None,
        "end_day":days[b-1] if b>a else None,**v75_metrics(rr)})
    return out

def v75_compute(store,btc,cost,entry_slip,cutoff_ms):
    raw,base,altmid,mid=v73_compute(store,btc,cost,entry_slip,cutoff_ms)
    return raw,base,mid

async def v75_run(symbols,history_days,oos_days,entry_slip,cost):
    global V75_STATE
    old=None
    try:old=v75_load()
    except Exception:pass
    started=(old or {}).get("started_utc") or utc_now()
    V75_STATE={"status":"RUNNING","progress":{"stage":"universe","done":0,"total":symbols},
      "params":{"symbols":symbols,"history_days":history_days,"oos_days":oos_days,
        "entry_slip_pct":entry_slip,"cost_pct":cost},
      "result":None,"error":None,"started_utc":started,"finished_utc":None}
    try:v75_save()
    except Exception:pass
    try:
      # Independent older window: exclude latest 40d discovery period.
      # Persist cutoff so restarts cannot move the window.
      meta=(old or {}).get("resume_meta") or {}
      ci=meta.get("cutoff_utc")
      cutoff=datetime.fromisoformat(ci) if ci else datetime.now(timezone.utc)-timedelta(days=40)
      cutoff_ms=int(cutoff.timestamp()*1000)
      start_ms=cutoff_ms-int((oos_days+3)*86400000)
      V75_STATE["resume_meta"]={"cutoff_utc":cutoff.isoformat()};v75_save()

      async with httpx.AsyncClient(timeout=httpx.Timeout(35.0)) as client:
        uni=await build_universe(client);syms=[u["symbol"] for u in uni if u["symbol"]!="BTCUSDT"][:symbols]
        done_set=v75_cached()
        for idx,sym in enumerate(syms,1):
          if sym not in done_set:
            candles=None;err=None
            for attempt in range(2):
              try:
                candles=await asyncio.wait_for(v73_fetch_window(client,sym,start_ms,cutoff_ms),timeout=70)
                if candles and len(candles)>=500:break
                err="insufficient"
              except Exception as e:err=f"{type(e).__name__}: {e}"
            v75_cache_put(sym,candles if candles and len(candles)>=500 else {"_failed":True,"error":err})
          V75_STATE["progress"]={"stage":"fetch_alts","done":idx,"total":len(syms),"symbol":sym};v75_save()
        btc=v75_cache_get("__BTCUSDT__")
        if not isinstance(btc,list):
          V75_STATE["progress"]={"stage":"fetch_btc","done":len(syms),"total":len(syms)};v75_save()
          btc=await asyncio.wait_for(v73_fetch_window(client,"BTCUSDT",start_ms,cutoff_ms),timeout=90)
          if not btc or len(btc)<500:raise RuntimeError("BTC OOS data insufficient")
          v75_cache_put("__BTCUSDT__",btc)

      V75_STATE["progress"]={"stage":"rebuild","done":0,"total":len(syms)};v75_save()
      aa,cc,ee,store={},{},{},{};failed=[]
      for idx,sym in enumerate(syms,1):
        c=v75_cache_get(sym)
        if not isinstance(c,list) or len(c)<500:
          failed.append(sym);continue
        si=len(store);await asyncio.to_thread(v61_extract_symbol,sym,si,c,aa,cc,ee,store)
        store[si]["c"]=array("d",[x["close"] for x in c]);store[si]["v"]=array("d",[x.get("volume",0) for x in c])
        if idx%5==0:
          V75_STATE["progress"]={"stage":"rebuild","done":idx,"total":len(syms)};v75_save()

      V75_STATE["progress"]={"stage":"validate","done":len(syms),"total":len(syms)};v75_save()
      raw,base,mid=await asyncio.to_thread(v75_compute,store,btc,cost,entry_slip,cutoff_ms)
      base_m=v75_metrics(base);mid_m=v75_metrics(mid)
      retention=round(100*len(mid)/len(base),2) if base else None
      V75_STATE.update(status="DONE",progress={"stage":"done","done":len(syms),"total":len(syms)},finished_utc=utc_now(),
        result={"validation":"INDEPENDENT_OLDER_OOS_FROZEN_MIDREGIME",
          "oos_end_utc":cutoff.isoformat(),"discovery_window_excluded_days":40,
          "frozen_hypothesis":{"entry":"V66_CONFIRM_10M_TOP1","base":"BTC30>0 & ALT breadth30>0",
            "alt4h_zone":[V73_ALT_LO,V73_ALT_HI],"btc4h_zone":[V73_BTC_LO,V73_BTC_HI],"exit":"TIME120"},
          "raw_breakouts":raw,
          "BASE_V70":{**base_m,"chronological_blocks":v75_blocks(base)},
          "FROZEN_MIDREGIME":{**mid_m,"retention_pct":retention,"chronological_blocks":v75_blocks(mid)},
          "comparison":{"trade_retention_pct":retention,
            "pf_delta":round((mid_m["profit_factor"] or 0)-(base_m["profit_factor"] or 0),4)},
          "data":{"symbols_used":len(store),"symbols_failed":failed},
          "decision_guardrail":"Do not retune the frozen 4h cutpoints from this OOS result.",
          "prospective_companion":"V74_PROSPECTIVE_MIDREGIME continues simultaneously.",
          "active_strategy_changed":False,"trading":False,"orders":False})
      v75_save()
    except Exception as e:
      V75_STATE.update(status="ERROR",error=f"{type(e).__name__}: {e}",finished_utc=utc_now())
      try:v75_save()
      except Exception:pass

@app.get("/v75-start")
async def v75_start(symbols:int=Query(40,ge=20,le=50),history_days:int=Query(100,ge=80,le=140),
  oos_days:int=Query(60,ge=40,le=90),entry_slip_pct:float=Query(.10,ge=0,le=1),cost_pct:float=Query(.15,ge=0,le=1)):
    global V75_TASK,V75_STATE
    old=None
    try:old=v75_load()
    except Exception:pass
    if old and old.get("status")=="DONE":
      V75_STATE=old;return {"status":"DONE_ALREADY","paper_only":True,"progress":old.get("progress")}
    if V75_TASK is not None and not V75_TASK.done():return {"status":"ALREADY_RUNNING","progress":V75_STATE.get("progress")}
    V75_TASK=asyncio.create_task(v75_run(symbols,history_days,oos_days,entry_slip_pct,cost_pct))
    return {"status":"STARTED_OR_RESUMED","paper_only":True,"study":"COMBINED_OLDER_OOS_MIDREGIME"}

@app.get("/v75-status")
async def v75_status():
    global V75_STATE
    if V75_STATE.get("status")=="IDLE":
      try:
        old=v75_load()
        if old:V75_STATE=old
      except Exception:pass
    return {**MODE_INFO,"status":"OK","panel":"V75_COMBINED_OOS_MIDREGIME_VALIDATION",
      "trading":False,"orders":False,"v74_prospective_continues":True,
      "active_strategy_changed":False,"study":V75_STATE,"generated_utc":utc_now()}


# ============================================================
# V76 â€” LOW-RAM COMBINED OOS VALIDATION
# Reuses V75 durable candle cache, but rebuilds ONLY compact arrays needed by
# frozen V66/V70/V72 validation. It does NOT call v61_extract_symbol.
# V74 prospective paper loop remains unchanged and continues independently.
# ============================================================
V76_STATE={"status":"IDLE","progress":{},"params":None,"result":None,"error":None,"started_utc":None,"finished_utc":None}
V76_TASK=None
V76_RUN_KEY="V76_LOWRAM_FROZEN_MIDREGIME"

def v76_db_init():
    if not V21_DB_URL:return False
    with v21_db_connect() as conn:
      with conn.cursor() as cur:
        cur.execute("""CREATE TABLE IF NOT EXISTS alt_v76_validation_state(
          run_key TEXT PRIMARY KEY,payload JSONB NOT NULL,updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW())""")
      conn.commit()
    return True

def v76_save():
    if not V21_DB_URL:return
    with v21_db_connect() as conn:
      with conn.cursor() as cur:
        cur.execute("""INSERT INTO alt_v76_validation_state(run_key,payload,updated_at)
          VALUES(%s,%s::jsonb,NOW()) ON CONFLICT(run_key) DO UPDATE
          SET payload=EXCLUDED.payload,updated_at=NOW()""",(V76_RUN_KEY,json.dumps(V76_STATE,default=str)))
      conn.commit()

def v76_load():
    if not V21_DB_URL:return None
    v76_db_init()
    with v21_db_connect() as conn:
      with conn.cursor() as cur:
        cur.execute("SELECT payload FROM alt_v76_validation_state WHERE run_key=%s",(V76_RUN_KEY,));r=cur.fetchone()
    return r[0] if r else None

def v76_compact(candles):
    # Only fields used by v66_candidates + v73_compute.
    return {
      "o":array("d",(float(x["open"]) for x in candles)),
      "h":array("d",(float(x["high"]) for x in candles)),
      "l":array("d",(float(x["low"]) for x in candles)),
      "c":array("d",(float(x["close"]) for x in candles)),
      "v":array("d",(float(x.get("volume",0.0)) for x in candles)),
      "t":array("q",(int(x["open_time"]) for x in candles))
    }

async def v76_run(symbols,history_days,oos_days,entry_slip,cost):
    global V76_STATE
    V76_STATE={"status":"RUNNING","progress":{"stage":"load_v75_cache","done":0,"total":symbols},
      "params":{"symbols":symbols,"history_days":history_days,"oos_days":oos_days,
        "entry_slip_pct":entry_slip,"cost_pct":cost},
      "result":None,"error":None,"started_utc":utc_now(),"finished_utc":None}
    try:v76_save()
    except Exception:pass
    try:
      # Use the exact V75 frozen OOS cutoff/cache. No new historical download and
      # no sliding of the validation window.
      v75=v75_load()
      if not v75:raise RuntimeError("V75 state/cache not found")
      cutoff_iso=(v75.get("resume_meta") or {}).get("cutoff_utc")
      if not cutoff_iso:raise RuntimeError("V75 frozen cutoff missing")
      cutoff=datetime.fromisoformat(cutoff_iso);cutoff_ms=int(cutoff.timestamp()*1000)

      # Recover the same symbol universe from V75 cache itself, excluding BTC.
      with v21_db_connect() as conn:
        with conn.cursor() as cur:
          cur.execute("""SELECT symbol FROM alt_v75_symbol_cache
            WHERE run_key=%s AND symbol<>%s ORDER BY updated_at ASC LIMIT %s""",
            (V75_RUN_KEY,"__BTCUSDT__",symbols))
          syms=[r[0] for r in cur.fetchall()]
      if len(syms)<20:raise RuntimeError(f"V75 cache has only {len(syms)} alt symbols")

      store={};failed=[]
      for idx,sym in enumerate(syms,1):
        payload=v75_cache_get(sym)
        if not isinstance(payload,list) or len(payload)<500:
          failed.append(sym)
        else:
          store[len(store)]=v76_compact(payload)
        # Drop JSON payload immediately; only compact C arrays remain.
        payload=None
        V76_STATE["progress"]={"stage":"compact_rebuild","done":idx,"total":len(syms),"symbol":sym,
          "symbols_compact":len(store),"failed":len(failed)}
        v76_save()
        if idx%4==0:
          await asyncio.sleep(0.15)

      btc=v75_cache_get("__BTCUSDT__")
      if not isinstance(btc,list) or len(btc)<500:raise RuntimeError("V75 BTC cache missing")
      # BTC remains a list because v73_compute expects historical candle dicts.
      V76_STATE["progress"]={"stage":"validate","done":len(syms),"total":len(syms)};v76_save()
      raw,base,altmid,mid=await asyncio.to_thread(v73_compute,store,btc,cost,entry_slip,cutoff_ms)

      base_m=v75_metrics(base);mid_m=v75_metrics(mid)
      retention=round(100*len(mid)/len(base),2) if base else None
      V76_STATE.update(status="DONE",progress={"stage":"done","done":len(syms),"total":len(syms)},
        finished_utc=utc_now(),result={
          "validation":"INDEPENDENT_OLDER_OOS_FROZEN_MIDREGIME_LOWRAM",
          "oos_end_utc":cutoff.isoformat(),"discovery_window_excluded_days":40,
          "frozen_hypothesis":{"entry":"V66_CONFIRM_10M_TOP1",
            "base":"BTC30>0 & ALT breadth30>0",
            "alt4h_zone":[V73_ALT_LO,V73_ALT_HI],
            "btc4h_zone":[V73_BTC_LO,V73_BTC_HI],"exit":"TIME120"},
          "raw_breakouts":raw,
          "BASE_V70":{**base_m,"chronological_blocks":v75_blocks(base)},
          "FROZEN_MIDREGIME":{**mid_m,"retention_pct":retention,
            "chronological_blocks":v75_blocks(mid)},
          "comparison":{"trade_retention_pct":retention,
            "pf_delta":round((mid_m["profit_factor"] or 0)-(base_m["profit_factor"] or 0),4)},
          "data":{"symbols_used":len(store),"symbols_failed":failed,
            "source":"V75 durable PostgreSQL candle cache","memory_mode":"compact OHLCVT arrays only"},
          "guardrails":["Frozen V72 cutpoints unchanged.","No OOS threshold search.",
            "Four chronological blocks reported.","Trade count, PF and bootstrap 95% CI reported.",
            "Current top-volume universe retains survivorship-bias limitation.",
            "V74 prospective strategy remains unchanged.","Research/paper only; no orders."]})
      v76_save()
    except Exception as e:
      V76_STATE.update(status="ERROR",error=f"{type(e).__name__}: {e}",finished_utc=utc_now())
      try:v76_save()
      except Exception:pass

@app.get("/v76-start")
async def v76_start(symbols:int=Query(40,ge=20,le=50),history_days:int=Query(100,ge=80,le=140),
  oos_days:int=Query(60,ge=40,le=90),entry_slip_pct:float=Query(.10,ge=0,le=1),
  cost_pct:float=Query(.15,ge=0,le=1)):
    global V76_TASK,V76_STATE
    old=None
    try:old=v76_load()
    except Exception:pass
    if old and old.get("status")=="DONE":
      V76_STATE=old;return {"status":"DONE_ALREADY","paper_only":True,"progress":old.get("progress")}
    if V76_TASK is not None and not V76_TASK.done():
      return {"status":"ALREADY_RUNNING","progress":V76_STATE.get("progress")}
    V76_TASK=asyncio.create_task(v76_run(symbols,history_days,oos_days,entry_slip_pct,cost_pct))
    return {"status":"STARTED","paper_only":True,"study":"LOWRAM_COMBINED_OLDER_OOS_MIDREGIME",
      "uses_existing_v75_cache":True,"v74_prospective_continues":True}

@app.get("/v76-status")
async def v76_status():
    global V76_STATE
    if V76_STATE.get("status")=="IDLE":
      try:
        old=v76_load()
        if old:V76_STATE=old
      except Exception:pass
    return {**MODE_INFO,"status":"OK","panel":"V76_LOWRAM_COMBINED_OOS_VALIDATION",
      "trading":False,"orders":False,"v74_prospective_continues":True,
      "active_strategy_changed":False,"study":V76_STATE,"generated_utc":utc_now()}


# ============================================================
# V77 â€” REGIME SHIFT DIAGNOSTIC
# Diagnostic only. Explains why frozen BASE V70 performs differently across time.
# No threshold selection, no strategy promotion, no real orders.
# Reuses V75 durable OOS cache and V76 compact-memory approach.
# ============================================================
V77_STATE={"status":"IDLE","progress":{},"result":None,"error":None,"started_utc":None,"finished_utc":None}
V77_TASK=None
V77_RUN_KEY="V77_REGIME_SHIFT_DIAGNOSTIC"

def v77_db_init():
    if not V21_DB_URL:return False
    with v21_db_connect() as conn:
      with conn.cursor() as cur:
        cur.execute("""CREATE TABLE IF NOT EXISTS alt_v77_diagnostic_state(
          run_key TEXT PRIMARY KEY,payload JSONB NOT NULL,updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW())""")
      conn.commit()
    return True

def v77_save():
    if not V21_DB_URL:return
    with v21_db_connect() as conn:
      with conn.cursor() as cur:
        cur.execute("""INSERT INTO alt_v77_diagnostic_state(run_key,payload,updated_at)
          VALUES(%s,%s::jsonb,NOW()) ON CONFLICT(run_key) DO UPDATE
          SET payload=EXCLUDED.payload,updated_at=NOW()""",(V77_RUN_KEY,json.dumps(V77_STATE,default=str)))
      conn.commit()

def v77_load():
    if not V21_DB_URL:return None
    v77_db_init()
    with v21_db_connect() as conn:
      with conn.cursor() as cur:
        cur.execute("SELECT payload FROM alt_v77_diagnostic_state WHERE run_key=%s",(V77_RUN_KEY,));r=cur.fetchone()
    return r[0] if r else None

def v77_ret(arr,i,bars):
    if i-bars<0:return None
    a=float(arr[i-bars]);b=float(arr[i])
    return ((b/a)-1)*100 if a else None

def v77_mean(vals):
    x=[v for v in vals if v is not None]
    return round(sum(x)/len(x),5) if x else None

def v77_med(vals):
    x=[v for v in vals if v is not None]
    return round(statistics.median(x),5) if x else None

def v77_summary(rows):
    keys=["btc30","btc1h","btc4h","btc24h","alt30","alt1h","alt4h","alt24h",
          "btc_accel30_1h","alt_accel30_1h","btc_vol1h","alt_dispersion30"]
    return {"n":len(rows),**{k:{"mean":v77_mean([r.get(k) for r in rows]),
      "median":v77_med([r.get(k) for r in rows])} for k in keys}}

def v77_compute(store,btc,cost,entry_slip,cutoff_ms):
    # Recreate frozen V66 CONFIRM_10M + BASE V70 while retaining exact entry timestamp.
    # v66_candidates returns tuples: (breakout_time, symbol_index, breakout_index, change, volume_ratio).
    import bisect
    btc_times=[int(x["open_time"]) for x in btc]
    btc_c=array("d",[float(x["close"]) for x in btc])
    cand=v66_candidates(store)
    out=[];busy={}
    for tm,si,i,ch,vr in cand:
      d=store[si];o,c,t=d["o"],d["c"],d["t"]
      breakout=float(c[i])
      ei=i+3
      if ei>=len(o) or i+2>=len(c):continue
      if not(c[i+1]>breakout and c[i+2]>c[i+1]):continue
      etm=int(t[ei])
      if busy.get(si,0)>etm:continue

      # Frozen TIME120 exit, same-symbol busy window, 0.10% entry slip and 0.15% cost.
      end=min(len(o)-1,ei+24)
      ep=float(o[ei])*(1+entry_slip/100)
      xp=float(o[end])
      net=((xp/ep)-1)*100-cost
      busy[si]=int(t[end])

      bi=bisect.bisect_right(btc_times,etm)-1
      if bi<6:continue
      btc30=v77_ret(btc_c,bi,6)
      a30=[]
      for od in store.values():
        j=bisect.bisect_right(od["t"],etm)-1
        if j>=6:
          x=v77_ret(od["c"],j,6)
          if x is not None:a30.append(x)
      br30=sum(a30)/len(a30) if a30 else None
      # Frozen BASE V70 cohort.
      if btc30 is None or br30 is None or not(btc30>0 and br30>0):continue
      out.append({"day":datetime.fromtimestamp(etm/1000,timezone.utc).strftime("%Y-%m-%d"),
                  "net":float(net),"entry_time_ms":etm,
                  "breakout_change_pct":float(ch),"volume_ratio":float(vr)})
    return out

def v77_enrich(store,btc,base_rows):
    btc_times=[int(x["open_time"]) for x in btc]
    btc_c=array("d",[float(x["close"]) for x in btc])
    import bisect
    out=[]
    for r in base_rows:
      ts=int(r["entry_time_ms"]);bi=bisect.bisect_right(btc_times,ts)-1
      if bi<288:continue
      btc30=v77_ret(btc_c,bi,6);btc1=v77_ret(btc_c,bi,12);btc4=v77_ret(btc_c,bi,48);btc24=v77_ret(btc_c,bi,288)
      alt30=[];alt1=[];alt4=[];alt24=[]
      for d in store.values():
        j=bisect.bisect_right(d["t"],ts)-1
        if j<0:continue
        for bars,bucket in ((6,alt30),(12,alt1),(48,alt4),(288,alt24)):
          x=v77_ret(d["c"],j,bars)
          if x is not None:bucket.append(x)
      if not alt30:continue
      rv=[v77_ret(btc_c,j,1) or 0 for j in range(max(1,bi-11),bi+1)]
      out.append({**r,"btc30":btc30,"btc1h":btc1,"btc4h":btc4,"btc24h":btc24,
        "alt30":sum(alt30)/len(alt30),"alt1h":sum(alt1)/len(alt1) if alt1 else None,
        "alt4h":sum(alt4)/len(alt4) if alt4 else None,"alt24h":sum(alt24)/len(alt24) if alt24 else None,
        "btc_accel30_1h":btc30-(btc1/2) if btc1 is not None else None,
        "alt_accel30_1h":(sum(alt30)/len(alt30))-((sum(alt1)/len(alt1))/2) if alt1 else None,
        "btc_vol1h":statistics.pstdev(rv) if len(rv)>1 else None,
        "alt_dispersion30":statistics.pstdev(alt30) if len(alt30)>1 else None})
    return out

async def v77_run():
    global V77_STATE
    V77_STATE={"status":"RUNNING","progress":{"stage":"load_cache","done":0,"total":40},
      "result":None,"error":None,"started_utc":utc_now(),"finished_utc":None}
    try:
      v77_db_init()
      v77_save()
    except Exception as e:
      V77_STATE.update(status="ERROR",error=f"V77 DB init failed: {type(e).__name__}: {e}",finished_utc=utc_now())
      return
    try:
      v75=v75_load()
      cutoff_iso=(v75.get("resume_meta") or {}).get("cutoff_utc") if v75 else None
      if not cutoff_iso:raise RuntimeError("V75 frozen cutoff missing")
      cutoff=datetime.fromisoformat(cutoff_iso);cutoff_ms=int(cutoff.timestamp()*1000)
      with v21_db_connect() as conn:
        with conn.cursor() as cur:
          cur.execute("""SELECT symbol FROM alt_v75_symbol_cache WHERE run_key=%s AND symbol<>%s
            ORDER BY updated_at ASC LIMIT 40""",(V75_RUN_KEY,"__BTCUSDT__"))
          syms=[x[0] for x in cur.fetchall()]
      store={};failed=[]
      for idx,sym in enumerate(syms,1):
        c=v75_cache_get(sym)
        if isinstance(c,list) and len(c)>=500:store[len(store)]=v76_compact(c)
        else:failed.append(sym)
        c=None
        V77_STATE["progress"]={"stage":"compact_rebuild","done":idx,"total":len(syms)};v77_save()
        if idx%4==0:await asyncio.sleep(.1)
      btc=v75_cache_get("__BTCUSDT__")
      if not isinstance(btc,list):raise RuntimeError("V75 BTC cache missing")
      V77_STATE["progress"]={"stage":"recreate_base","done":len(syms),"total":len(syms)};v77_save()
      base=await asyncio.to_thread(v77_compute,store,btc,.15,.10,cutoff_ms)
      raw=None
      enriched=await asyncio.to_thread(v77_enrich,store,btc,base)
      if not enriched:
        raise RuntimeError("No causal BASE V70 rows could be reconstructed with timestamps")

      days=sorted(set(r["day"] for r in enriched))
      # Same chronological 4-way segmentation as validation; bad=blocks1-2, good=blocks3-4.
      cut=len(days)//2;bad_days=set(days[:cut]);good_days=set(days[cut:])
      bad=[r for r in enriched if r["day"] in bad_days];good=[r for r in enriched if r["day"] in good_days]
      bs=v77_summary(bad);gs=v77_summary(good)
      diffs={}
      for k in ["btc30","btc1h","btc4h","btc24h","alt30","alt1h","alt4h","alt24h",
                "btc_accel30_1h","alt_accel30_1h","btc_vol1h","alt_dispersion30"]:
        a=bs[k]["mean"];b=gs[k]["mean"];diffs[k]=round(b-a,5) if a is not None and b is not None else None
      V77_STATE.update(status="DONE",progress={"stage":"done","done":len(syms),"total":len(syms)},finished_utc=utc_now(),
        result={"diagnostic":"BASE_V70_REGIME_SHIFT","raw_breakouts":raw,"base_trades":len(base),
          "context_trades":len(enriched),"bad_period_blocks":"1-2","good_period_blocks":"3-4",
          "BAD_PERIOD":bs,"GOOD_PERIOD":gs,"GOOD_MINUS_BAD_MEAN":diffs,
          "data":{"symbols_used":len(store),"symbols_failed":failed,"source":"V75 frozen older OOS cache"},
          "guardrails":["Diagnostic only; no threshold selected.","Do not optimize cutpoints from this sample.",
            "BTC horizons: 30m/1h/4h/24h.","Alt breadth horizons: 30m/1h/4h/24h.",
            "Acceleration and volatility/dispersion are descriptive.","V74/V61 execution unchanged.",
            "Research/paper only; no orders."]})
      v77_save()
    except Exception as e:
      V77_STATE.update(status="ERROR",error=f"{type(e).__name__}: {e}",finished_utc=utc_now())
      try:v77_save()
      except Exception:pass

@app.get("/v77-start")
async def v77_start():
    global V77_TASK,V77_STATE
    if V77_TASK is not None and not V77_TASK.done():
      return {"status":"ALREADY_RUNNING","progress":V77_STATE.get("progress")}
    v77_db_init()
    V77_TASK=asyncio.create_task(v77_run())
    return {"status":"STARTED","paper_only":True,"study":"BASE_V70_REGIME_SHIFT_DIAGNOSTIC"}

@app.get("/v77-status")
async def v77_status():
    global V77_STATE
    if V77_STATE.get("status")=="IDLE":
      try:
        old=v77_load()
        if old:V77_STATE=old
      except Exception:pass
    return {**MODE_INFO,"status":"OK","panel":"V77_REGIME_SHIFT_DIAGNOSTIC",
      "trading":False,"orders":False,"active_strategy_changed":False,
      "v74_prospective_unchanged":True,"study":V77_STATE,"generated_utc":utc_now()}


# ============================================================
# V78 â€” REGIME STRUCTURE VALIDATION
# Frozen entry/exit: V66 CONFIRM_10M + TIME120.
# Tests 24h BTC/ALT regime + dispersion structure.
# Research only; no orders; active V61/V74 unchanged.
# ============================================================
V78_STATE={"status":"IDLE","progress":{},"result":None,"error":None,"started_utc":None,"finished_utc":None}
V78_TASK=None
V78_RUN_KEY="V78_REGIME_STRUCTURE_VALIDATION"

def v78_db_init():
    if not V21_DB_URL:return False
    with v21_db_connect() as conn:
      with conn.cursor() as cur:
        cur.execute("""CREATE TABLE IF NOT EXISTS alt_v78_validation_state(
          run_key TEXT PRIMARY KEY,payload JSONB NOT NULL,updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW())""")
      conn.commit()
    return True

def v78_save():
    if not V21_DB_URL:return
    v78_db_init()
    with v21_db_connect() as conn:
      with conn.cursor() as cur:
        cur.execute("""INSERT INTO alt_v78_validation_state(run_key,payload,updated_at)
          VALUES(%s,%s::jsonb,NOW()) ON CONFLICT(run_key) DO UPDATE
          SET payload=EXCLUDED.payload,updated_at=NOW()""",(V78_RUN_KEY,json.dumps(V78_STATE,default=str)))
      conn.commit()

def v78_load():
    if not V21_DB_URL:return None
    v78_db_init()
    with v21_db_connect() as conn:
      with conn.cursor() as cur:
        cur.execute("SELECT payload FROM alt_v78_validation_state WHERE run_key=%s",(V78_RUN_KEY,));r=cur.fetchone()
    return r[0] if r else None

def v78_pf(nets):
    pos=sum(x for x in nets if x>0);neg=-sum(x for x in nets if x<0)
    return round(pos/neg,4) if neg>0 else (999.0 if pos>0 else 0.0)

def v78_ci(nets):
    if not nets:return [None,None]
    # deterministic bootstrap for reproducibility
    import random
    rr=random.Random(7801);n=len(nets);means=[]
    for _ in range(1200):
      means.append(sum(nets[rr.randrange(n)] for __ in range(n))/n)
    means.sort()
    return [round(means[int(.025*len(means))],4),round(means[int(.975*len(means))-1],4)]

def v78_metrics(rows):
    nets=[float(r["net"]) for r in rows]
    if not nets:return {"n":0,"mean_net_pct":None,"median_net_pct":None,"win_rate_pct":None,"profit_factor":None,"mean_95ci":[None,None]}
    return {"n":len(nets),"mean_net_pct":round(sum(nets)/len(nets),4),
      "median_net_pct":round(statistics.median(nets),4),
      "win_rate_pct":round(100*sum(x>0 for x in nets)/len(nets),2),
      "profit_factor":v78_pf(nets),"mean_95ci":v78_ci(nets)}

def v78_blocks(rows):
    if not rows:return []
    z=sorted(rows,key=lambda r:r["entry_time_ms"]);n=len(z);out=[]
    for b in range(4):
      a=(b*n)//4;e=((b+1)*n)//4
      q=z[a:e]
      if not q:continue
      m=v78_metrics(q)
      m.update(block=b+1,start_day=q[0]["day"],end_day=q[-1]["day"])
      out.append(m)
    return out

def v78_recreate(store,btc,cost=.15,entry_slip=.10):
    import bisect
    btc_times=[int(x["open_time"]) for x in btc]
    btc_c=array("d",[float(x["close"]) for x in btc])
    cand=v66_candidates(store)
    out=[];busy={}
    for tm,si,i,ch,vr in cand:
      d=store[si];o,c,t=d["o"],d["c"],d["t"]
      if i+3>=len(o) or i+2>=len(c):continue
      breakout=float(c[i]);ei=i+3
      if not(c[i+1]>breakout and c[i+2]>c[i+1]):continue
      etm=int(t[ei])
      if busy.get(si,0)>etm:continue
      end=min(len(o)-1,ei+24)
      ep=float(o[ei])*(1+entry_slip/100);xp=float(o[end])
      net=((xp/ep)-1)*100-cost
      busy[si]=int(t[end])

      bi=bisect.bisect_right(btc_times,etm)-1
      if bi<288:continue
      btc30=v77_ret(btc_c,bi,6);btc4=v77_ret(btc_c,bi,48);btc24=v77_ret(btc_c,bi,288)
      a30=[];a4=[];a24=[]
      for od in store.values():
        j=bisect.bisect_right(od["t"],etm)-1
        if j<0:continue
        for bars,bucket in ((6,a30),(48,a4),(288,a24)):
          x=v77_ret(od["c"],j,bars)
          if x is not None:bucket.append(x)
      if not a30 or not a24:continue
      alt30=sum(a30)/len(a30);alt4=sum(a4)/len(a4) if a4 else None;alt24=sum(a24)/len(a24)
      # Frozen V70 base cohort remains the parent cohort.
      if not(btc30>0 and alt30>0):continue
      out.append({"day":datetime.fromtimestamp(etm/1000,timezone.utc).strftime("%Y-%m-%d"),
        "entry_time_ms":etm,"net":float(net),"btc30":btc30,"btc4":btc4,"btc24":btc24,
        "alt30":alt30,"alt4":alt4,"alt24":alt24,
        "disp30":statistics.pstdev(a30) if len(a30)>1 else None,
        "breakout_change_pct":float(ch),"volume_ratio":float(vr)})
    return out

def v78_group(rows,pred):
    q=[r for r in rows if pred(r)]
    return {**v78_metrics(q),"chronological_blocks":v78_blocks(q)}

def v78_eval(rows):
    # Predeclared sign-based regime groups; no fitted numeric BTC/ALT thresholds.
    result={}
    result["CONTROL_BASE_V70"]=v78_group(rows,lambda r:True)
    result["BTC24_POSITIVE"]=v78_group(rows,lambda r:r["btc24"]>0)
    result["BTC24_NEGATIVE_OR_ZERO"]=v78_group(rows,lambda r:r["btc24"]<=0)
    result["ALT24_POSITIVE"]=v78_group(rows,lambda r:r["alt24"]>0)
    result["ALT24_NEGATIVE_OR_ZERO"]=v78_group(rows,lambda r:r["alt24"]<=0)
    result["BTC24_AND_ALT24_POSITIVE"]=v78_group(rows,lambda r:r["btc24"]>0 and r["alt24"]>0)
    result["BTC24_OR_ALT24_NONPOSITIVE"]=v78_group(rows,lambda r:not(r["btc24"]>0 and r["alt24"]>0))

    # Continuity: 4h and 24h both positive vs not; no tuned cutpoint.
    result["BTC4H24H_AND_ALT4H24H_POSITIVE"]=v78_group(rows,lambda r:
      r["btc4"] is not None and r["alt4"] is not None and r["btc4"]>0 and r["btc24"]>0 and r["alt4"]>0 and r["alt24"]>0)

    # Dispersion tertiles are descriptive structural ranks, not fixed optimized thresholds.
    valid=sorted(r["disp30"] for r in rows if r["disp30"] is not None)
    if valid:
      q1=valid[int((len(valid)-1)/3)];q2=valid[int(2*(len(valid)-1)/3)]
      result["DISPERSION_TERTILES"]={
        "cutpoints_descriptive":[round(q1,5),round(q2,5)],
        "LOW":v78_group(rows,lambda r:r["disp30"] is not None and r["disp30"]<=q1),
        "MID":v78_group(rows,lambda r:r["disp30"] is not None and q1<r["disp30"]<=q2),
        "HIGH":v78_group(rows,lambda r:r["disp30"] is not None and r["disp30"]>q2)}
      pos=[r for r in rows if r["btc24"]>0 and r["alt24"]>0]
      result["POSITIVE_24H_WITH_DISPERSION_TERTILES"]={
        "LOW":v78_group(pos,lambda r:r["disp30"] is not None and r["disp30"]<=q1),
        "MID":v78_group(pos,lambda r:r["disp30"] is not None and q1<r["disp30"]<=q2),
        "HIGH":v78_group(pos,lambda r:r["disp30"] is not None and r["disp30"]>q2)}
    return result

async def v78_run():
    global V78_STATE
    V78_STATE={"status":"RUNNING","progress":{"stage":"load_frozen_cache","done":0,"total":40},
      "result":None,"error":None,"started_utc":utc_now(),"finished_utc":None}
    try:v78_save()
    except Exception:pass
    try:
      with v21_db_connect() as conn:
        with conn.cursor() as cur:
          cur.execute("""SELECT symbol FROM alt_v75_symbol_cache WHERE run_key=%s AND symbol<>%s
            ORDER BY updated_at ASC LIMIT 40""",(V75_RUN_KEY,"__BTCUSDT__"))
          syms=[x[0] for x in cur.fetchall()]
      store={};failed=[]
      for idx,sym in enumerate(syms,1):
        c=v75_cache_get(sym)
        if isinstance(c,list) and len(c)>=500:store[len(store)]=v76_compact(c)
        else:failed.append(sym)
        c=None
        V78_STATE["progress"]={"stage":"compact_rebuild","done":idx,"total":len(syms)}
        if idx%4==0:
          v78_save();await asyncio.sleep(.05)
      btc=v75_cache_get("__BTCUSDT__")
      if not isinstance(btc,list):raise RuntimeError("V75 BTC cache missing")
      V78_STATE["progress"]={"stage":"evaluate_older_oos","done":len(syms),"total":len(syms)};v78_save()
      older=await asyncio.to_thread(v78_recreate,store,btc,.15,.10)
      older_eval=await asyncio.to_thread(v78_eval,older)

      # Newer comparison uses V77/V76 available cache only if it contains dates beyond older OOS.
      # We do not fabricate a second sample. Report availability explicitly.
      max_day=max((r["day"] for r in older),default=None)
      V78_STATE.update(status="DONE",progress={"stage":"done","done":len(syms),"total":len(syms)},finished_utc=utc_now(),
        result={"validation":"V78_REGIME_STRUCTURE_FROZEN_BASE_V70",
          "frozen_entry":"V66_CONFIRM_10M_TOP1","frozen_exit":"TIME120",
          "parent_cohort":"BTC30>0 AND ALT breadth30>0",
          "OLDER_INDEPENDENT_OOS":older_eval,
          "sample":{"base_trades":len(older),"last_trade_day":max_day,"symbols_used":len(store),"symbols_failed":failed,
            "source":"V75 durable older-OOS cache"},
          "newer_40d_note":"Not synthesized from the older-only cache. A separate newer frozen cache is required for a truly independent second-window comparison.",
          "guardrails":["No fitted BTC24/ALT24 numeric thresholds; sign tests only.",
            "Dispersion tertiles are descriptive rank buckets, not promoted thresholds.",
            "Every main group reports N, mean, median, WR, PF, bootstrap 95% CI and four chronological blocks.",
            "Frozen V66 entry and TIME120 exit unchanged.","No OOS threshold search.",
            "Current top-volume universe has survivorship-bias limitation.",
            "V74/V61 execution unchanged.","Research/paper only; no orders."]})
      v78_save()
    except Exception as e:
      V78_STATE.update(status="ERROR",error=f"{type(e).__name__}: {e}",finished_utc=utc_now())
      try:v78_save()
      except Exception:pass

@app.get("/v78-start")
async def v78_start():
    global V78_TASK
    if V78_TASK is not None and not V78_TASK.done():
      return {"status":"ALREADY_RUNNING","progress":V78_STATE.get("progress")}
    v78_db_init();V78_TASK=asyncio.create_task(v78_run())
    return {"status":"STARTED","paper_only":True,"study":"V78_REGIME_STRUCTURE_VALIDATION"}

@app.get("/v78-status")
async def v78_status():
    global V78_STATE
    if V78_STATE.get("status")=="IDLE":
      try:
        old=v78_load()
        if old:V78_STATE=old
      except Exception:pass
    return {**MODE_INFO,"status":"OK","panel":"V78_REGIME_STRUCTURE_VALIDATION",
      "trading":False,"orders":False,"active_strategy_changed":False,
      "v74_prospective_unchanged":True,"study":V78_STATE,"generated_utc":utc_now()}


# ============================================================
# V79 â€” NEWER 40D DISPERSION VALIDATION
# Independent newer window, frozen V66 CONFIRM10 + TIME120.
# Tests V78 discovery hypothesis without retuning:
# CONTROL vs HIGH dispersion using FROZEN V78 cutoff 0.69157.
# Also reports rank-tertile HIGH as secondary descriptive robustness check.
# Research only. V61/V74 unchanged. No orders.
# ============================================================
V79_STATE={"status":"IDLE","progress":{},"result":None,"error":None,"started_utc":None,"finished_utc":None}
V79_TASK=None
V79_RUN_KEY="V79_NEWER_40D_DISPERSION_VALIDATION"
V79_FROZEN_DISP_CUT=0.69157

def v79_db_init():
    if not V21_DB_URL:return False
    with v21_db_connect() as conn:
      with conn.cursor() as cur:
        cur.execute("""CREATE TABLE IF NOT EXISTS alt_v79_validation_state(
          run_key TEXT PRIMARY KEY,payload JSONB NOT NULL,updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW())""")
        cur.execute("""CREATE TABLE IF NOT EXISTS alt_v79_symbol_cache(
          run_key TEXT NOT NULL,symbol TEXT NOT NULL,payload JSONB NOT NULL,
          updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
          PRIMARY KEY(run_key,symbol))""")
      conn.commit()
    return True

def v79_save():
    if not V21_DB_URL:return
    v79_db_init()
    with v21_db_connect() as conn:
      with conn.cursor() as cur:
        cur.execute("""INSERT INTO alt_v79_validation_state(run_key,payload,updated_at)
          VALUES(%s,%s::jsonb,NOW()) ON CONFLICT(run_key) DO UPDATE
          SET payload=EXCLUDED.payload,updated_at=NOW()""",(V79_RUN_KEY,json.dumps(V79_STATE,default=str)))
      conn.commit()

def v79_load():
    if not V21_DB_URL:return None
    v79_db_init()
    with v21_db_connect() as conn:
      with conn.cursor() as cur:
        cur.execute("SELECT payload FROM alt_v79_validation_state WHERE run_key=%s",(V79_RUN_KEY,));r=cur.fetchone()
    return r[0] if r else None

def v79_cache_get(sym):
    with v21_db_connect() as conn:
      with conn.cursor() as cur:
        cur.execute("SELECT payload FROM alt_v79_symbol_cache WHERE run_key=%s AND symbol=%s",(V79_RUN_KEY,sym));r=cur.fetchone()
    return r[0] if r else None

def v79_cache_put(sym,payload):
    with v21_db_connect() as conn:
      with conn.cursor() as cur:
        cur.execute("""INSERT INTO alt_v79_symbol_cache(run_key,symbol,payload,updated_at)
          VALUES(%s,%s,%s::jsonb,NOW()) ON CONFLICT(run_key,symbol) DO UPDATE
          SET payload=EXCLUDED.payload,updated_at=NOW()""",(V79_RUN_KEY,sym,json.dumps(payload)))
      conn.commit()

async def v79_fetch_klines(sym,start_ms,end_ms,client=None):
    # Durable exact-window fetch using the SAME Binance data endpoint as the working project.
    old=v79_cache_get(sym)
    if isinstance(old,list) and len(old)>=500:
      return old
    rows=[];cur=start_ms
    own_client = client is None
    c = client or httpx.AsyncClient(timeout=30)
    try:
      while cur<end_ms:
        data=await get_json(c,"/api/v3/klines",
          params={"symbol":sym,"interval":"5m","startTime":cur,"endTime":end_ms,"limit":1000})
        if not data:break
        for x in data:
          rows.append({"open_time":int(x[0]),"open":float(x[1]),"high":float(x[2]),
            "low":float(x[3]),"close":float(x[4]),"volume":float(x[5])})
        nxt=int(data[-1][0])+300000
        if nxt<=cur:break
        cur=nxt
        await asyncio.sleep(.04)
    finally:
      if own_client:
        await c.aclose()
    rows=list({int(x["open_time"]):x for x in rows}.values())
    rows.sort(key=lambda x:x["open_time"])
    if rows:v79_cache_put(sym,rows)
    return rows

def v79_eval(rows):
    control={**v78_metrics(rows),"chronological_blocks":v78_blocks(rows)}
    frozen=[r for r in rows if r.get("disp30") is not None and r["disp30"]>V79_FROZEN_DISP_CUT]
    frozen_out={**v78_metrics(frozen),"chronological_blocks":v78_blocks(frozen)}
    vals=sorted(r["disp30"] for r in rows if r.get("disp30") is not None)
    q67=vals[int(2*(len(vals)-1)/3)] if vals else None
    rank=[r for r in rows if q67 is not None and r.get("disp30") is not None and r["disp30"]>q67]
    rank_out={**v78_metrics(rank),"chronological_blocks":v78_blocks(rank)}
    return {"CONTROL_BASE_V70":control,
      "FROZEN_HIGH_DISP_GT_0_69157":frozen_out,
      "SECONDARY_RANK_HIGH_TERTILE":{**rank_out,"new_window_q67_descriptive":round(q67,5) if q67 is not None else None},
      "comparison":{"frozen_retention_pct":round(100*len(frozen)/len(rows),2) if rows else None,
        "frozen_pf_delta":round((frozen_out["profit_factor"] or 0)-(control["profit_factor"] or 0),4) if rows else None}}

async def v79_run(symbols=40,days=40):
    global V79_STATE
    V79_STATE={"status":"RUNNING","progress":{"stage":"prepare","done":0,"total":symbols},
      "result":None,"error":None,"started_utc":utc_now(),"finished_utc":None}
    try:v79_save()
    except Exception:pass
    try:
      # Freeze current completed-candle end at start; 40d window with 2d warmup.
      now_ms=(int(time.time()*1000)//300000)*300000
      start_ms=now_ms-(days+2)*86400000
      # Use the project's actual cleaned universe builder, then freeze top-volume symbols.
      async with httpx.AsyncClient(timeout=30) as universe_client:
        uni=await build_universe(universe_client)
      syms=[x["symbol"] for x in uni[:symbols] if isinstance(x,dict) and x.get("symbol")]
      if not syms:raise RuntimeError("No symbols from current cleaned universe")
      fetch_syms=list(dict.fromkeys(["BTCUSDT"]+syms))
      raw={};failed=[];fetch_errors={}
      # V79.3 bounded downloader: one bad symbol cannot freeze the study.
      sem=asyncio.Semaphore(4)
      async with httpx.AsyncClient(timeout=httpx.Timeout(20.0,connect=10.0)) as data_client:
        async def one(sym):
          async with sem:
            try:
              x=await asyncio.wait_for(v79_fetch_klines(sym,start_ms,now_ms,data_client),timeout=90)
              return sym,x,None
            except asyncio.TimeoutError:
              return sym,[],"TIMEOUT_90S"
            except Exception as e:
              return sym,[],f"{type(e).__name__}: {e}"
        tasks=[asyncio.create_task(one(sym)) for sym in fetch_syms]
        done_count=0
        for fut in asyncio.as_completed(tasks):
          sym,x,err=await fut
          done_count+=1
          if len(x)>=500:
            raw[sym]=x
          else:
            failed.append(sym)
            fetch_errors[sym]=err or f"only_{len(x)}_candles"
          V79_STATE["progress"]={"stage":"fetch_newer_40d","done":done_count,"total":len(fetch_syms),
            "ok":len(raw),"failed":len(failed),"last_symbol":sym,
            "last_error":fetch_errors.get(sym)}
          v79_save()
      if "BTCUSDT" not in raw:
        raise RuntimeError("BTCUSDT newer-window data unavailable; detail="+fetch_errors.get("BTCUSDT","unknown"))
      # compact only selected alts; BTC separate
      store={};used=[]
      for sym in syms:
        if sym in raw:
          store[len(store)]=v76_compact(raw[sym]);used.append(sym)
      btc=raw["BTCUSDT"]
      raw=None
      V79_STATE["progress"]={"stage":"frozen_evaluation","done":len(used),"total":symbols};v79_save()
      rows=await asyncio.to_thread(v78_recreate,store,btc,.15,.10)
      # enforce requested 40d, excluding 2d warmup from reported trades
      report_start=now_ms-days*86400000
      rows=[r for r in rows if int(r["entry_time_ms"])>=report_start and int(r["entry_time_ms"])<now_ms]
      ev=await asyncio.to_thread(v79_eval,rows)
      V79_STATE.update(status="DONE",progress={"stage":"done","done":len(used),"total":symbols},finished_utc=utc_now(),
        result={"validation":"INDEPENDENT_NEWER_40D_FROZEN_DISPERSION",
          "window":{"start_utc":datetime.fromtimestamp(report_start/1000,timezone.utc).isoformat(),
                    "end_utc":datetime.fromtimestamp(now_ms/1000,timezone.utc).isoformat(),"days":days},
          "frozen_hypothesis":{"entry":"V66_CONFIRM_10M_TOP1","parent":"BTC30>0 AND ALT breadth30>0",
            "exit":"TIME120","high_dispersion_rule":"ALT dispersion30 > 0.69157",
            "cutpoint_source":"V78 older-OOS discovery; unchanged in V79"},
          "results":ev,
          "data":{"symbols_requested":symbols,"symbols_used":len(used),"symbols":used,"failed":failed,
                  "fetch_errors":fetch_errors,
                  "source":"fresh Binance data-api 5m historical fetch cached in PostgreSQL"},
          "guardrails":["Primary dispersion threshold 0.69157 frozen before newer-window test.",
            "No V79 threshold search or retuning.","Rank-high tertile is secondary descriptive robustness only.",
            "Frozen V66 entry, BASE V70 parent cohort and TIME120 exit unchanged.",
            "Every primary group reports N, mean, median, WR, PF, bootstrap 95% CI and four chronological blocks.",
            "Current top-volume universe has survivorship-bias limitation.",
            "V61/V74 execution unchanged.","Research/paper only; no orders."]})
      v79_save()
    except Exception as e:
      V79_STATE.update(status="ERROR",error=f"{type(e).__name__}: {e}",finished_utc=utc_now())
      try:v79_save()
      except Exception:pass

@app.get("/v79-start")
async def v79_start(symbols:int=40,days:int=40):
    global V79_TASK
    if V79_TASK is not None and not V79_TASK.done():
      return {"status":"ALREADY_RUNNING","progress":V79_STATE.get("progress")}
    v79_db_init();V79_TASK=asyncio.create_task(v79_run(max(10,min(symbols,40)),max(20,min(days,40))))
    return {"status":"STARTED","paper_only":True,"study":"V79_NEWER_40D_FROZEN_DISPERSION"}

@app.get("/v79-status")
async def v79_status():
    global V79_STATE
    if V79_STATE.get("status")=="IDLE":
      try:
        old=v79_load()
        if old:V79_STATE=old
      except Exception:pass
    return {**MODE_INFO,"status":"OK","panel":"V79.3_NEWER_40D_DISPERSION_VALIDATION_NOHANG",
      "trading":False,"orders":False,"active_strategy_changed":False,
      "v74_prospective_unchanged":True,"study":V79_STATE,"generated_utc":utc_now()}


# ============================================================
# V79.4 â€” RESUMABLE / RESTART-SAFE WORKER
# Scientific rules unchanged. This is operational resilience only.
# - persistent job definition/checkpoint
# - auto-resume after Render restart
# - reuses V79 symbol cache
# - BTC first
# - bounded per-symbol fetch
# ============================================================
V794_RUN_KEY="V79_4_NEWER_40D_RESUMABLE"
V794_TASK=None

def v794_db_init():
    if not V21_DB_URL:return False
    with v21_db_connect() as conn:
      with conn.cursor() as cur:
        cur.execute("""CREATE TABLE IF NOT EXISTS alt_v794_job(
          run_key TEXT PRIMARY KEY,payload JSONB NOT NULL,updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW())""")
      conn.commit()
    return True

def v794_job_get():
    if not V21_DB_URL:return None
    v794_db_init()
    with v21_db_connect() as conn:
      with conn.cursor() as cur:
        cur.execute("SELECT payload FROM alt_v794_job WHERE run_key=%s",(V794_RUN_KEY,))
        r=cur.fetchone()
    return r[0] if r else None

def v794_job_put(p):
    v794_db_init()
    with v21_db_connect() as conn:
      with conn.cursor() as cur:
        cur.execute("""INSERT INTO alt_v794_job(run_key,payload,updated_at)
          VALUES(%s,%s::jsonb,NOW()) ON CONFLICT(run_key) DO UPDATE
          SET payload=EXCLUDED.payload,updated_at=NOW()""",(V794_RUN_KEY,json.dumps(p,default=str)))
      conn.commit()

def v794_cache_ok(sym):
    try:
      x=v79_cache_get(sym)
      return isinstance(x,list) and len(x)>=500
    except Exception:return False

async def v794_worker():
    global V794_TASK
    job=v794_job_get()
    if not job or job.get("status") not in ("RUNNING","RESUMING"):return
    try:
      job["status"]="RUNNING";job["error"]=None;v794_job_put(job)
      syms=job["symbols"]; start_ms=int(job["fetch_start_ms"]); end_ms=int(job["end_ms"])
      fetch_syms=list(dict.fromkeys(["BTCUSDT"]+syms))
      completed=set(job.get("completed",[]))
      # Reconcile checkpoint with durable cache after restart.
      for sym in fetch_syms:
        if v794_cache_ok(sym):completed.add(sym)
      job["completed"]=sorted(completed)
      job["progress"]={"stage":"fetch_newer_40d","done":len(completed),"total":len(fetch_syms),
                       "remaining":len(fetch_syms)-len(completed)}
      v794_job_put(job)

      pending=[x for x in fetch_syms if x not in completed]
      # Sequential + hard timeout = lowest RAM and restart-safe.
      async with httpx.AsyncClient(timeout=httpx.Timeout(20.0,connect=10.0)) as client:
        for sym in pending:
          try:
            x=await asyncio.wait_for(v79_fetch_klines(sym,start_ms,end_ms,client),timeout=90)
            if len(x)>=500:
              completed.add(sym)
              job.setdefault("fetch_errors",{}).pop(sym,None)
            else:
              job.setdefault("fetch_errors",{})[sym]=f"only_{len(x)}_candles"
              completed.add(sym)  # terminal skip; don't hang forever
          except Exception as e:
            job.setdefault("fetch_errors",{})[sym]=f"{type(e).__name__}: {e}"
            completed.add(sym)  # terminal skip for this frozen run
          job["completed"]=sorted(completed)
          job["progress"]={"stage":"fetch_newer_40d","done":len(completed),"total":len(fetch_syms),
                           "remaining":len(fetch_syms)-len(completed),"last_symbol":sym,
                           "last_error":job.get("fetch_errors",{}).get(sym)}
          v794_job_put(job)

      if not v794_cache_ok("BTCUSDT"):
        raise RuntimeError("BTCUSDT unavailable after bounded fetch: "+job.get("fetch_errors",{}).get("BTCUSDT","unknown"))

      # Build only from durable cache. Keep memory bounded to the evaluation phase.
      raw_btc=v79_cache_get("BTCUSDT")
      store={};used=[];failed=[]
      for sym in syms:
        x=v79_cache_get(sym)
        if isinstance(x,list) and len(x)>=500:
          store[len(store)]=v76_compact(x);used.append(sym)
        else: failed.append(sym)
      job["progress"]={"stage":"frozen_evaluation","done":len(used),"total":len(syms)}
      v794_job_put(job)

      rows=await asyncio.to_thread(v78_recreate,store,raw_btc,.15,.10)
      report_start=int(job["report_start_ms"])
      rows=[r for r in rows if int(r["entry_time_ms"])>=report_start and int(r["entry_time_ms"])<end_ms]
      ev=await asyncio.to_thread(v79_eval,rows)
      job.update(status="DONE",finished_utc=utc_now(),error=None,
        progress={"stage":"done","done":len(used),"total":len(syms)},
        result={"validation":"INDEPENDENT_NEWER_40D_FROZEN_DISPERSION_RESUMABLE",
          "window":{"start_utc":datetime.fromtimestamp(report_start/1000,timezone.utc).isoformat(),
                    "end_utc":datetime.fromtimestamp(end_ms/1000,timezone.utc).isoformat(),
                    "days":job["days"]},
          "frozen_hypothesis":{"entry":"V66_CONFIRM_10M_TOP1",
            "parent":"BTC30>0 AND ALT breadth30>0","exit":"TIME120",
            "high_dispersion_rule":"ALT dispersion30 > 0.69157",
            "cutpoint_source":"V78 older-OOS discovery; unchanged in V79.4"},
          "results":ev,
          "data":{"symbols_requested":len(syms),"symbols_used":len(used),
                  "symbols":used,"failed":failed,
                  "fetch_errors":job.get("fetch_errors",{}),
                  "source":"durable V79 PostgreSQL symbol cache"},
          "guardrails":["V79.4 changes execution resilience only; scientific rules unchanged.",
            "Primary dispersion threshold 0.69157 remains frozen.",
            "No threshold search or retuning.","Frozen V66 entry, BASE V70 parent cohort and TIME120 exit unchanged.",
            "Rank-high tertile remains secondary descriptive only.",
            "V61/V74 execution unchanged.","Research/paper only; no orders."]})
      v794_job_put(job)
    except Exception as e:
      job=v794_job_get() or {}
      job.update(status="ERROR",error=f"{type(e).__name__}: {e}",finished_utc=utc_now())
      v794_job_put(job)

async def v794_autoresume():
    global V794_TASK
    await asyncio.sleep(8)
    try:
      job=v794_job_get()
      if job and job.get("status") in ("RUNNING","RESUMING"):
        job["status"]="RESUMING";job["resume_count"]=int(job.get("resume_count",0))+1
        job["last_resume_utc"]=utc_now();v794_job_put(job)
        V794_TASK=asyncio.create_task(v794_worker())
    except Exception:
      pass

@app.on_event("startup")
async def v794_startup_resume():
    asyncio.create_task(v794_autoresume())

@app.get("/v79-4-start")
async def v794_start(symbols:int=40,days:int=40):
    global V794_TASK
    v794_db_init()
    current=v794_job_get()
    if V794_TASK is not None and not V794_TASK.done():
      return {"status":"ALREADY_RUNNING","progress":(current or {}).get("progress")}
    symbols=max(10,min(symbols,40));days=max(20,min(days,40))
    now_ms=(int(time.time()*1000)//300000)*300000
    async with httpx.AsyncClient(timeout=30) as c:
      uni=await build_universe(c)
    syms=[x["symbol"] for x in uni[:symbols] if isinstance(x,dict) and x.get("symbol")]
    job={"status":"RUNNING","created_utc":utc_now(),"started_utc":utc_now(),
      "finished_utc":None,"error":None,"days":days,"symbols":syms,
      "end_ms":now_ms,"report_start_ms":now_ms-days*86400000,
      "fetch_start_ms":now_ms-(days+2)*86400000,
      "completed":[],"fetch_errors":{},"resume_count":0,
      "progress":{"stage":"prepare","done":0,"total":len(syms)+1},
      "scientific_rules":{"dispersion_cut":0.69157,"entry":"V66_CONFIRM_10M_TOP1",
                          "parent":"BTC30>0 AND ALT breadth30>0","exit":"TIME120"}}
    v794_job_put(job)
    V794_TASK=asyncio.create_task(v794_worker())
    return {"status":"STARTED","paper_only":True,"restart_safe":True,
            "study":"V79.4_NEWER_40D_FROZEN_DISPERSION","symbols":len(syms)}

@app.get("/v79-4-status")
async def v794_status():
    job=v794_job_get()
    return {**MODE_INFO,"status":"OK","panel":"V79.4_RESUMABLE_NEWER_40D_DISPERSION",
      "trading":False,"orders":False,"active_strategy_changed":False,
      "v74_prospective_unchanged":True,"restart_safe":True,
      "study":job,"generated_utc":utc_now()}


# ============================================================
# V80 â€” REGIME TRANSITION DIAGNOSTIC
# Diagnostic only: compare V79 newer-window bad half vs good half.
# No new thresholds, no strategy promotion, no execution changes.
# Reuses durable V79 cache and frozen V66/BASE-V70 cohort.
# ============================================================
V80_RUN_KEY="V80_REGIME_TRANSITION_DIAGNOSTIC"
V80_STATE={"status":"IDLE","progress":{},"result":None,"error":None}
V80_TASK=None

def v80_stats(vals):
    vals=[float(x) for x in vals if x is not None and math.isfinite(float(x))]
    if not vals:return {"n":0,"mean":None,"median":None}
    return {"n":len(vals),"mean":round(statistics.mean(vals),5),
            "median":round(statistics.median(vals),5)}

def v80_idx(candles, ts):
    # last candle at or before timestamp
    lo,hi=0,len(candles)-1;ans=None
    while lo<=hi:
      m=(lo+hi)//2
      if int(candles[m]["open_time"])<=ts:ans=m;lo=m+1
      else:hi=m-1
    return ans

def v80_ret(candles,i,bars):
    if i is None or i-bars<0:return None
    a=float(candles[i-bars]["close"]);b=float(candles[i]["close"])
    return (b/a-1)*100 if a else None

def v80_realized_vol(candles,i,bars=12):
    if i is None or i-bars<0:return None
    rr=[]
    for j in range(i-bars+1,i+1):
      a=float(candles[j-1]["close"]);b=float(candles[j]["close"])
      if a>0:rr.append((b/a-1)*100)
    return statistics.pstdev(rr) if len(rr)>=2 else None

def v80_context_for_time(ts, btc, alt_lists):
    bi=v80_idx(btc,ts)
    btc30=v80_ret(btc,bi,6); btc1h=v80_ret(btc,bi,12)
    btc4h=v80_ret(btc,bi,48); btc24h=v80_ret(btc,bi,288)
    btcvol=v80_realized_vol(btc,bi,12)
    r30=[];r1=[];r4=[];r24=[]
    for c in alt_lists:
      i=v80_idx(c,ts)
      for arr,bars in ((r30,6),(r1,12),(r4,48),(r24,288)):
        x=v80_ret(c,i,bars)
        if x is not None:arr.append(x)
    def mean(x): return statistics.mean(x) if x else None
    disp30=statistics.pstdev(r30) if len(r30)>=2 else None
    return {"btc30":btc30,"btc1h":btc1h,"btc4h":btc4h,"btc24h":btc24h,
      "btc_accel_30_vs_1h":None if btc30 is None or btc1h is None else btc30-btc1h/2,
      "btc_vol1h":btcvol,"alt30":mean(r30),"alt1h":mean(r1),"alt4h":mean(r4),"alt24h":mean(r24),
      "alt_accel_30_vs_1h":None if not r30 or not r1 else mean(r30)-mean(r1)/2,
      "alt_dispersion30":disp30}

def v80_summarize(rows, feature_names):
    return {f:v80_stats([r["context"].get(f) for r in rows]) for f in feature_names}

async def v80_run():
    global V80_STATE
    V80_STATE={"status":"RUNNING","progress":{"stage":"load_cache"},"result":None,"error":None,
               "started_utc":utc_now(),"finished_utc":None}
    try:
      job=v794_job_get()
      if not job or job.get("status")!="DONE":
        raise RuntimeError("V79.4 DONE result required")
      syms=job["symbols"]; btc=v79_cache_get("BTCUSDT")
      if not isinstance(btc,list):raise RuntimeError("BTC cache unavailable")
      alt_lists=[];store={};used=[]
      for sym in syms:
        x=v79_cache_get(sym)
        if isinstance(x,list) and len(x)>=500:
          alt_lists.append(x);store[len(store)]=v76_compact(x);used.append(sym)
      V80_STATE["progress"]={"stage":"recreate_frozen_cohort","symbols_used":len(used)}
      rows=await asyncio.to_thread(v78_recreate,store,btc,.15,.10)
      rs=int(job["report_start_ms"]);re=int(job["end_ms"])
      rows=[r for r in rows if int(r["entry_time_ms"])>=rs and int(r["entry_time_ms"])<re]
      rows.sort(key=lambda r:int(r["entry_time_ms"]))
      if len(rows)<20:raise RuntimeError("Too few frozen cohort trades")
      # Exact temporal split: first half vs second half by trade chronology.
      mid=len(rows)//2
      labeled=[]
      for k,r in enumerate(rows):
        ts=int(r["entry_time_ms"])
        ctx=v80_context_for_time(ts,btc,alt_lists)
        labeled.append({"half":"BAD_EARLY" if k<mid else "GOOD_LATE",
                        "net":r.get("net_pct"),"context":ctx,"ts":ts})
        if (k+1)%50==0:
          V80_STATE["progress"]={"stage":"context","done":k+1,"total":len(rows)}
          await asyncio.sleep(0)
      bad=[x for x in labeled if x["half"]=="BAD_EARLY"]
      good=[x for x in labeled if x["half"]=="GOOD_LATE"]
      features=["btc30","btc1h","btc4h","btc24h","btc_accel_30_vs_1h","btc_vol1h",
                "alt30","alt1h","alt4h","alt24h","alt_accel_30_vs_1h","alt_dispersion30"]
      bs=v80_summarize(bad,features);gs=v80_summarize(good,features)
      diffs={}
      for f in features:
        a=bs[f]["mean"];b=gs[f]["mean"]
        diffs[f]=None if a is None or b is None else round(b-a,5)
      # Calendar quartiles as a second descriptive view, no thresholds.
      quart=[]
      qsize=max(1,len(labeled)//4)
      for q in range(4):
        part=labeled[q*qsize:(q+1)*qsize if q<3 else len(labeled)]
        nets=[x["net"] for x in part if x["net"] is not None]
        quart.append({"block":q+1,"n":len(part),
          "start_utc":datetime.fromtimestamp(part[0]["ts"]/1000,timezone.utc).isoformat() if part else None,
          "end_utc":datetime.fromtimestamp(part[-1]["ts"]/1000,timezone.utc).isoformat() if part else None,
          "net":v80_stats(nets),"context":v80_summarize(part,features)})
      V80_STATE={"status":"DONE","progress":{"stage":"done","done":len(labeled),"total":len(labeled)},
        "result":{"validation":"V80_REGIME_TRANSITION_DIAGNOSTIC_NEWER_40D",
          "frozen_parent":{"entry":"V66_CONFIRM_10M_TOP1","parent":"BTC30>0 AND ALT breadth30>0",
                           "exit":"TIME120","dispersion_rule_not_changed":True},
          "sample":{"trades":len(labeled),"symbols_used":len(used),
                    "window_start":datetime.fromtimestamp(rs/1000,timezone.utc).isoformat(),
                    "window_end":datetime.fromtimestamp(re/1000,timezone.utc).isoformat()},
          "BAD_EARLY_HALF":{"n":len(bad),"net":v80_stats([x["net"] for x in bad]),"context":bs},
          "GOOD_LATE_HALF":{"n":len(good),"net":v80_stats([x["net"] for x in good]),"context":gs},
          "GOOD_MINUS_BAD_MEAN":diffs,"CHRONOLOGICAL_QUARTERS":quart,
          "guardrails":["Diagnostic only; no threshold selected.","No strategy/execution changes.",
            "Frozen V66 entry, BASE V70 parent cohort and TIME120 exit preserved.",
            "Context features are descriptive; do not promote observed means to cutpoints.",
            "V61/V74 unchanged.","Research/paper only; no orders."]},
        "error":None,"started_utc":V80_STATE.get("started_utc"),"finished_utc":utc_now()}
    except Exception as e:
      V80_STATE.update(status="ERROR",error=f"{type(e).__name__}: {e}",finished_utc=utc_now())

@app.get("/v80-start")
async def v80_start():
    global V80_TASK
    if V80_TASK is not None and not V80_TASK.done():
      return {"status":"ALREADY_RUNNING","progress":V80_STATE.get("progress")}
    V80_TASK=asyncio.create_task(v80_run())
    return {"status":"STARTED","paper_only":True,"study":"V80_REGIME_TRANSITION_DIAGNOSTIC"}

@app.get("/v80-status")
async def v80_status():
    return {**MODE_INFO,"status":"OK","panel":"V80_REGIME_TRANSITION_DIAGNOSTIC",
      "trading":False,"orders":False,"active_strategy_changed":False,
      "v74_prospective_unchanged":True,"study":V80_STATE,"generated_utc":utc_now()}

# === V80.1 OUTCOME x REGIME DIAGNOSTIC ===
V801_STATE={"status":"IDLE","progress":{},"result":None,"error":None}
V801_TASK=None
def v801_num(v):
    try:
        x=float(v); return x if math.isfinite(x) else None
    except Exception:return None
def v801_net(r):
    for k in ("net_return_pct","net_pct","return_net_pct","ret_net_pct","pnl_pct","return_pct","ret120_net_pct","net"):
        x=v801_num(r.get(k))
        if x is not None:return x,k
    return None,None
def v801_corr(a,b):
    p=[(v801_num(x),v801_num(y)) for x,y in zip(a,b)]
    p=[z for z in p if z[0] is not None and z[1] is not None]
    if len(p)<3:return None
    x=[z[0] for z in p];y=[z[1] for z in p];mx=statistics.mean(x);my=statistics.mean(y)
    n=sum((u-mx)*(v-my) for u,v in p);dx=sum((u-mx)**2 for u in x);dy=sum((v-my)**2 for v in y)
    return round(n/math.sqrt(dx*dy),5) if dx>0 and dy>0 else None
def v801_group(rows,fs):
    ns=[r["net"] for r in rows];w=[x for x in ns if x>0];l=[x for x in ns if x<=0]
    return {"n":len(rows),"mean_net_pct":round(statistics.mean(ns),5) if ns else None,
      "median_net_pct":round(statistics.median(ns),5) if ns else None,
      "win_rate_pct":round(100*len(w)/len(ns),3) if ns else None,
      "profit_factor":round(sum(w)/(-sum(l)),4) if l and -sum(l)>0 else None,
      "context":v80_summarize(rows,fs)}
async def v801_run():
    global V801_STATE
    V801_STATE={"status":"RUNNING","progress":{"stage":"load"},"result":None,"error":None,"started_utc":utc_now()}
    try:
        job=v794_job_get()
        if not job or job.get("status")!="DONE":raise RuntimeError("V79.4 DONE required")
        syms=job["symbols"];btc=v79_cache_get("BTCUSDT");alts=[];store={};used=[]
        for sym in syms:
            x=v79_cache_get(sym)
            if isinstance(x,list) and len(x)>=500:
                alts.append(x);store[len(store)]=v76_compact(x);used.append(sym)
        rows=await asyncio.to_thread(v78_recreate,store,btc,.15,.10)
        rs=int(job["report_start_ms"]);re=int(job["end_ms"])
        rows=sorted([r for r in rows if rs<=int(r["entry_time_ms"])<re],key=lambda r:int(r["entry_time_ms"]))
        en=[];fc={}
        for i,r in enumerate(rows):
            net,f=v801_net(r)
            if net is None:continue
            fc[f]=fc.get(f,0)+1
            en.append({"net":net,"context":v80_context_for_time(int(r["entry_time_ms"]),btc,alts),"ts":int(r["entry_time_ms"])})
            if (i+1)%50==0:
                V801_STATE["progress"]={"stage":"context","done":i+1,"total":len(rows)};await asyncio.sleep(0)
        if not en:raise RuntimeError("No net-return field found; keys="+",".join(sorted(rows[0].keys()) if rows else []))
        fs=["btc30","btc1h","btc4h","btc24h","btc_accel_30_vs_1h","btc_vol1h","alt30","alt1h","alt4h","alt24h","alt_accel_30_vs_1h","alt_dispersion30"]
        w=[r for r in en if r["net"]>0];l=[r for r in en if r["net"]<=0]
        ws=v80_summarize(w,fs);ls=v80_summarize(l,fs)
        diff={f:None if ws[f]["mean"] is None or ls[f]["mean"] is None else round(ws[f]["mean"]-ls[f]["mean"],5) for f in fs}
        corr={f:v801_corr([r["context"].get(f) for r in en],[r["net"] for r in en]) for f in fs}
        qs=[];z=max(1,len(en)//4)
        for q in range(4):
            p=en[q*z:(q+1)*z if q<3 else len(en)]
            qs.append({"block":q+1,"start_utc":datetime.fromtimestamp(p[0]["ts"]/1000,timezone.utc).isoformat() if p else None,
                       "end_utc":datetime.fromtimestamp(p[-1]["ts"]/1000,timezone.utc).isoformat() if p else None,**v801_group(p,fs)})
        V801_STATE={"status":"DONE","progress":{"stage":"done","done":len(en),"total":len(rows)},
          "result":{"validation":"V80.1_OUTCOME_X_REGIME_DIAGNOSTIC",
          "sample":{"recreated":len(rows),"valid_outcomes":len(en),"symbols_used":len(used),"net_field_counts":fc},
          "ALL":v801_group(en,fs),"WINNERS":v801_group(w,fs),"LOSERS":v801_group(l,fs),
          "WINNER_MINUS_LOSER_CONTEXT_MEAN":diff,"PEARSON_CONTEXT_VS_NET":corr,"CHRONOLOGICAL_QUARTERS":qs,
          "guardrails":["Diagnostic only; no threshold selected or optimized.","No strategy/execution changes.",
          "Frozen V66 entry, BASE V70 parent cohort and TIME120 exit preserved.","Correlation is descriptive, not causal.",
          "Do not convert winner/loser means into cutpoints.","V61/V74 unchanged.","Research/paper only; no orders."]},
          "error":None,"finished_utc":utc_now()}
    except Exception as e:V801_STATE.update(status="ERROR",error=f"{type(e).__name__}: {e}",finished_utc=utc_now())
@app.get("/v80-1-start")
async def v801_start():
    global V801_TASK
    if V801_TASK is not None and not V801_TASK.done():return {"status":"ALREADY_RUNNING","progress":V801_STATE.get("progress")}
    V801_TASK=asyncio.create_task(v801_run());return {"status":"STARTED","paper_only":True,"study":"V80.1_OUTCOME_X_REGIME_DIAGNOSTIC"}
@app.get("/v80-1-status")
async def v801_status():
    return {**MODE_INFO,"status":"OK","panel":"V80.1_OUTCOME_X_REGIME_DIAGNOSTIC","trading":False,"orders":False,
      "active_strategy_changed":False,"v74_prospective_unchanged":True,"study":V801_STATE,"generated_utc":utc_now()}


# ============================================================
# V81 â€” COMPREHENSIVE REGIME RESEARCH ENGINE
# One deploy / one start:
# 1) reconstruct frozen V66 + BASE-V70 + TIME120 cohort
# 2) attach regime context
# 3) chronological DEV / VALIDATION / FINAL split
# 4) derive thresholds from DEV only
# 5) test predeclared regime candidates on VALIDATION
# 6) carry qualifying candidates unchanged into FINAL
# 7) N / mean / median / WR / PF / bootstrap CI / time blocks
# 8) automatic REJECT / WATCH / PROSPECTIVE_PAPER_CANDIDATE
#
# IMPORTANT:
# FINAL is untouched *within V81 selection logic*, but the underlying
# 40-day window has been inspected in V79/V80, so it is NOT claimed
# to be a globally pristine holdout.
# No live strategy/execution changes. Research/paper only.
# ============================================================

V81_STATE={"status":"IDLE","progress":{},"result":None,"error":None}
V81_TASK=None

def v81_pct(vals,p):
    a=sorted(float(x) for x in vals if x is not None and math.isfinite(float(x)))
    if not a:return None
    k=(len(a)-1)*p
    lo=int(math.floor(k)); hi=int(math.ceil(k))
    if lo==hi:return a[lo]
    return a[lo]*(hi-k)+a[hi]*(k-lo)

def v81_ci(vals, boots=1200):
    a=[float(x) for x in vals if x is not None and math.isfinite(float(x))]
    if len(a)<8:return [None,None]
    rnd=random.Random(810081)
    means=[]
    n=len(a)
    for _ in range(boots):
        means.append(sum(a[rnd.randrange(n)] for __ in range(n))/n)
    return [round(v81_pct(means,.025),4),round(v81_pct(means,.975),4)]

def v81_metrics(rows):
    ns=[float(r["net"]) for r in rows]
    if not ns:
        return {"n":0,"mean_net_pct":None,"median_net_pct":None,"win_rate_pct":None,
                "profit_factor":None,"mean_95ci":[None,None]}
    w=[x for x in ns if x>0]; l=[x for x in ns if x<=0]
    gl=-sum(l)
    return {"n":len(ns),
      "mean_net_pct":round(statistics.mean(ns),4),
      "median_net_pct":round(statistics.median(ns),4),
      "win_rate_pct":round(100*len(w)/len(ns),2),
      "profit_factor":round(sum(w)/gl,4) if gl>0 else None,
      "mean_95ci":v81_ci(ns)}

def v81_blocks(rows,nblocks=4):
    if not rows:return []
    z=max(1,len(rows)//nblocks); out=[]
    for i in range(nblocks):
        p=rows[i*z:(i+1)*z if i<nblocks-1 else len(rows)]
        if not p:continue
        out.append({"block":i+1,
          "start_utc":datetime.fromtimestamp(p[0]["ts"]/1000,timezone.utc).isoformat(),
          "end_utc":datetime.fromtimestamp(p[-1]["ts"]/1000,timezone.utc).isoformat(),
          **v81_metrics(p)})
    return out

def v81_apply(rows,name,t):
    def ok(r):
        c=r["context"]
        if name=="CONTROL": return True
        if name=="DISP_HIGH": return c["alt_dispersion30"] is not None and c["alt_dispersion30"]>=t["disp"]
        if name=="BTC_VOL_LOW": return c["btc_vol1h"] is not None and c["btc_vol1h"]<=t["btcvol"]
        if name=="BTC4H_LOW": return c["btc4h"] is not None and c["btc4h"]<=t["btc4h"]
        if name=="ALT24_HIGH": return c["alt24h"] is not None and c["alt24h"]>=t["alt24"]
        if name=="DISP_HIGH_BTCVOL_LOW":
            return (c["alt_dispersion30"] is not None and c["btc_vol1h"] is not None and
                    c["alt_dispersion30"]>=t["disp"] and c["btc_vol1h"]<=t["btcvol"])
        if name=="DISP_HIGH_BTC4H_LOW":
            return (c["alt_dispersion30"] is not None and c["btc4h"] is not None and
                    c["alt_dispersion30"]>=t["disp"] and c["btc4h"]<=t["btc4h"])
        if name=="DISP_HIGH_ALT24_HIGH":
            return (c["alt_dispersion30"] is not None and c["alt24h"] is not None and
                    c["alt_dispersion30"]>=t["disp"] and c["alt24h"]>=t["alt24"])
        if name=="CALM_BTC_SELECTIVE_ALTS":
            return (c["alt_dispersion30"] is not None and c["btc_vol1h"] is not None and c["btc4h"] is not None and
                    c["alt_dispersion30"]>=t["disp"] and c["btc_vol1h"]<=t["btcvol"] and c["btc4h"]<=t["btc4h"])
        return False
    return [r for r in rows if ok(r)]

def v81_eval(rows,name,t):
    x=v81_apply(rows,name,t)
    return {**v81_metrics(x),"retention_pct":round(100*len(x)/len(rows),2) if rows else 0,
            "chronological_blocks":v81_blocks(x)}

def v81_label(val,final,control_final):
    # Conservative automatic label; no threshold tuning here.
    if val["n"]<25 or final["n"]<20:
        return "REJECT_LOW_N"
    vp=val.get("profit_factor"); fp=final.get("profit_factor"); cp=control_final.get("profit_factor")
    vlo=val.get("mean_95ci",[None,None])[0]; flo=final.get("mean_95ci",[None,None])[0]
    if vp is None or fp is None:return "REJECT"
    if vp>1.15 and fp>1.15 and fp>(cp or 0) and final["mean_net_pct"]>0:
        if (vlo is not None and vlo>0) and (flo is not None and flo>0):
            return "PROSPECTIVE_PAPER_CANDIDATE"
        return "WATCH"
    return "REJECT"

async def v81_run():
    global V81_STATE
    V81_STATE={"status":"RUNNING","progress":{"stage":"load_cache"},"result":None,"error":None,
               "started_utc":utc_now(),"finished_utc":None}
    try:
        job=v794_job_get()
        if not job or job.get("status")!="DONE":
            raise RuntimeError("V79.4 DONE cache/result required")
        syms=job["symbols"]; btc=v79_cache_get("BTCUSDT")
        if not isinstance(btc,list): raise RuntimeError("BTC cache unavailable")
        alts=[]; store={}; used=[]
        for sym in syms:
            x=v79_cache_get(sym)
            if isinstance(x,list) and len(x)>=500:
                alts.append(x); store[len(store)]=v76_compact(x); used.append(sym)

        V81_STATE["progress"]={"stage":"recreate_frozen_cohort","symbols_used":len(used)}
        rows=await asyncio.to_thread(v78_recreate,store,btc,.15,.10)
        rs=int(job["report_start_ms"]); re=int(job["end_ms"])
        rows=sorted([r for r in rows if rs<=int(r["entry_time_ms"])<re],
                    key=lambda r:int(r["entry_time_ms"]))

        enriched=[]; fields={}
        for i,r in enumerate(rows):
            net,f=v801_net(r)
            if net is None: continue
            fields[f]=fields.get(f,0)+1
            enriched.append({"net":net,"ts":int(r["entry_time_ms"]),
                             "context":v80_context_for_time(int(r["entry_time_ms"]),btc,alts)})
            if (i+1)%50==0:
                V81_STATE["progress"]={"stage":"attach_context","done":i+1,"total":len(rows)}
                await asyncio.sleep(0)
        if len(enriched)<90: raise RuntimeError("Too few valid outcomes")

        # Strict chronological split: 50% DEV, 25% VALIDATION, 25% FINAL.
        n=len(enriched); a=n//2; b=a+(n-a)//2
        dev=enriched[:a]; val=enriched[a:b]; final=enriched[b:]

        # Thresholds are learned ONCE from DEV medians only.
        # No search over threshold grids.
        t={
          "disp":round(v81_pct([r["context"]["alt_dispersion30"] for r in dev],.50),5),
          "btcvol":round(v81_pct([r["context"]["btc_vol1h"] for r in dev],.50),5),
          "btc4h":round(v81_pct([r["context"]["btc4h"] for r in dev],.50),5),
          "alt24":round(v81_pct([r["context"]["alt24h"] for r in dev],.50),5)
        }
        candidates=["CONTROL","DISP_HIGH","BTC_VOL_LOW","BTC4H_LOW","ALT24_HIGH",
          "DISP_HIGH_BTCVOL_LOW","DISP_HIGH_BTC4H_LOW","DISP_HIGH_ALT24_HIGH",
          "CALM_BTC_SELECTIVE_ALTS"]

        dev_res={c:v81_eval(dev,c,t) for c in candidates}
        val_res={c:v81_eval(val,c,t) for c in candidates}

        # Predeclared qualification from validation only.
        qualifiers=[]
        for c in candidates:
            if c=="CONTROL":continue
            z=val_res[c]
            if z["n"]>=25 and z["profit_factor"] is not None and z["profit_factor"]>1.05 and z["mean_net_pct"]>0:
                qualifiers.append(c)

        # FINAL is evaluated for reporting for all candidates, but classification
        # is only meaningful for candidates that qualified before FINAL.
        final_res={c:v81_eval(final,c,t) for c in candidates}
        control_final=final_res["CONTROL"]
        classification={}
        for c in candidates:
            if c=="CONTROL":
                classification[c]="CONTROL"
            elif c not in qualifiers:
                classification[c]="REJECT_AT_VALIDATION"
            else:
                classification[c]=v81_label(val_res[c],final_res[c],control_final)

        # Ranking is descriptive; FINAL is not used to retune thresholds.
        ranking=sorted([c for c in candidates if c!="CONTROL"],
          key=lambda c:(final_res[c]["profit_factor"] or -999, final_res[c]["n"]), reverse=True)

        V81_STATE={"status":"DONE","progress":{"stage":"done","done":len(enriched),"total":len(enriched)},
          "result":{
            "study":"V81_COMPREHENSIVE_REGIME_RESEARCH_ENGINE",
            "sample":{"trades":len(enriched),"symbols_used":len(used),"net_field_counts":fields,
              "window_start":datetime.fromtimestamp(rs/1000,timezone.utc).isoformat(),
              "window_end":datetime.fromtimestamp(re/1000,timezone.utc).isoformat()},
            "frozen_strategy":{"entry":"V66_CONFIRM_10M_TOP1",
              "parent":"BTC30>0 AND ALT breadth30>0","exit":"TIME120","cost_pct":0.15},
            "split":{"method":"chronological_50_25_25","DEV_n":len(dev),"VALIDATION_n":len(val),"FINAL_n":len(final),
              "warning":"FINAL is untouched within V81 selection logic, but this 40-day source window was previously inspected in V79/V80; it is not a globally pristine holdout."},
            "dev_only_thresholds":t,
            "candidate_definitions":{
              "DISP_HIGH":"alt_dispersion30 >= DEV median",
              "BTC_VOL_LOW":"btc_vol1h <= DEV median",
              "BTC4H_LOW":"btc4h <= DEV median",
              "ALT24_HIGH":"alt24h >= DEV median",
              "DISP_HIGH_BTCVOL_LOW":"DISP_HIGH AND BTC_VOL_LOW",
              "DISP_HIGH_BTC4H_LOW":"DISP_HIGH AND BTC4H_LOW",
              "DISP_HIGH_ALT24_HIGH":"DISP_HIGH AND ALT24_HIGH",
              "CALM_BTC_SELECTIVE_ALTS":"DISP_HIGH AND BTC_VOL_LOW AND BTC4H_LOW"},
            "DEV":dev_res,"VALIDATION":val_res,"validation_qualifiers":qualifiers,
            "FINAL":final_res,"classification":classification,
            "descriptive_final_ranking":ranking,
            "decision_rules":{
              "validation_gate":"N>=25, PF>1.05, mean>0",
              "candidate_watch":"qualified; FINAL N>=20, PF>1.15, PF>FINAL control, mean>0",
              "prospective_candidate":"WATCH conditions plus positive lower 95% CI in both validation and FINAL"},
            "guardrails":[
              "No threshold grid search; thresholds come from DEV medians only.",
              "VALIDATION decides which candidates may proceed.",
              "FINAL does not alter thresholds.",
              "Because V79/V80 already inspected this 40-day source window, FINAL is an internal holdout, not a globally untouched OOS.",
              "No live strategy or execution changes.",
              "V61/V74 unchanged.",
              "Research/paper only; no orders."
            ]},
          "error":None,"started_utc":V81_STATE.get("started_utc"),"finished_utc":utc_now()}
    except Exception as e:
        V81_STATE.update(status="ERROR",error=f"{type(e).__name__}: {e}",finished_utc=utc_now())

@app.get("/v81-start")
async def v81_start():
    global V81_TASK
    if V81_TASK is not None and not V81_TASK.done():
        return {"status":"ALREADY_RUNNING","progress":V81_STATE.get("progress")}
    V81_TASK=asyncio.create_task(v81_run())
    return {"status":"STARTED","paper_only":True,"study":"V81_COMPREHENSIVE_REGIME_RESEARCH_ENGINE"}

@app.get("/v81-status")
async def v81_status():
    return {**MODE_INFO,"status":"OK","panel":"V81_COMPREHENSIVE_REGIME_RESEARCH_ENGINE",
      "trading":False,"orders":False,"active_strategy_changed":False,
      "v74_prospective_unchanged":True,"study":V81_STATE,"generated_utc":utc_now()}


# ============================================================
# V82 â€” ONE-SHOT INDEPENDENT OOS + FINALIST REGIME VALIDATION
#
# Purpose:
# - Keep active V61/V74 untouched.
# - Freeze the V81 finalists and thresholds:
#     BTC_VOL_LOW: btc_vol1h <= 0.09330
#     ALT24_HIGH:  alt24h >= 1.54532
# - Add their intersection: ALT24_HIGH_AND_BTC_VOL_LOW.
# - Test on an OLDER, non-overlapping 60-day window ending before
#   the V79/V81 40-day source window began.
# - Report CONTROL + finalists + intersection with N/mean/median/
#   WR/PF/bootstrap CI + 4 chronological blocks.
# - No threshold tuning, no grid search, no active-strategy change.
#
# Operational design:
# - low-RAM sequential fetching
# - persistent PostgreSQL candle cache
# - restart-safe job checkpoint after every symbol
# - startup auto-resume
# ============================================================

V82_TABLE = "alt_v82_job"
V82_CACHE = "alt_v82_cache"
V82_TASK = None

V82_BTCVOL_MAX = 0.09330
V82_ALT24_MIN = 1.54532
V82_DAYS = 60
V82_SYMBOLS = 40

def v82_db_init():
    if not V21_DB_URL:
        return
    with v21_db_connect() as conn:
        with conn.cursor() as cur:
            cur.execute(f"""
                CREATE TABLE IF NOT EXISTS {V82_TABLE} (
                    id INTEGER PRIMARY KEY,
                    payload JSONB NOT NULL,
                    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
                )
            """)
            cur.execute(f"""
                CREATE TABLE IF NOT EXISTS {V82_CACHE} (
                    symbol TEXT PRIMARY KEY,
                    start_ms BIGINT NOT NULL,
                    end_ms BIGINT NOT NULL,
                    candles JSONB NOT NULL,
                    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
                )
            """)
        conn.commit()

def v82_job_get():
    if not V21_DB_URL:
        return None
    try:
        with v21_db_connect() as conn:
            with conn.cursor() as cur:
                cur.execute(f"SELECT payload FROM {V82_TABLE} WHERE id=1")
                row=cur.fetchone()
                return row[0] if row else None
    except Exception:
        return None

def v82_job_save(payload):
    if not V21_DB_URL:
        return
    with v21_db_connect() as conn:
        with conn.cursor() as cur:
            cur.execute(f"""
                INSERT INTO {V82_TABLE}(id,payload,updated_at)
                VALUES(1,%s::jsonb,NOW())
                ON CONFLICT(id) DO UPDATE SET payload=EXCLUDED.payload, updated_at=NOW()
            """,(json.dumps(payload),))
        conn.commit()

def v82_cache_get(symbol,start_ms,end_ms):
    if not V21_DB_URL:
        return None
    try:
        with v21_db_connect() as conn:
            with conn.cursor() as cur:
                cur.execute(f"""
                    SELECT candles FROM {V82_CACHE}
                    WHERE symbol=%s AND start_ms=%s AND end_ms=%s
                """,(symbol,int(start_ms),int(end_ms)))
                row=cur.fetchone()
                return row[0] if row else None
    except Exception:
        return None

def v82_cache_put(symbol,start_ms,end_ms,candles):
    if not V21_DB_URL:
        return
    with v21_db_connect() as conn:
        with conn.cursor() as cur:
            cur.execute(f"""
                INSERT INTO {V82_CACHE}(symbol,start_ms,end_ms,candles,updated_at)
                VALUES(%s,%s,%s,%s::jsonb,NOW())
                ON CONFLICT(symbol) DO UPDATE SET
                    start_ms=EXCLUDED.start_ms,
                    end_ms=EXCLUDED.end_ms,
                    candles=EXCLUDED.candles,
                    updated_at=NOW()
            """,(symbol,int(start_ms),int(end_ms),json.dumps(candles)))
        conn.commit()

def v82_metrics(rows):
    vals=[float(r["net"]) for r in rows if r.get("net") is not None]
    if not vals:
        return {"n":0,"mean_net_pct":None,"median_net_pct":None,
                "win_rate_pct":None,"profit_factor":None,"mean_95ci":[None,None]}
    wins=[x for x in vals if x>0]; losses=[x for x in vals if x<=0]
    gl=-sum(losses)
    return {
      "n":len(vals),
      "mean_net_pct":round(statistics.mean(vals),4),
      "median_net_pct":round(statistics.median(vals),4),
      "win_rate_pct":round(100*len(wins)/len(vals),2),
      "profit_factor":round(sum(wins)/gl,4) if gl>0 else None,
      "mean_95ci":v81_ci(vals)
    }

def v82_blocks(rows,nblocks=4):
    rows=sorted(rows,key=lambda x:x["ts"])
    if not rows:return []
    out=[]
    for i in range(nblocks):
        a=(len(rows)*i)//nblocks
        b=(len(rows)*(i+1))//nblocks
        p=rows[a:b]
        if not p:continue
        out.append({
          "block":i+1,
          "start_utc":datetime.fromtimestamp(p[0]["ts"]/1000,timezone.utc).isoformat(),
          "end_utc":datetime.fromtimestamp(p[-1]["ts"]/1000,timezone.utc).isoformat(),
          **v82_metrics(p)
        })
    return out

def v82_group(rows,name):
    def keep(r):
        c=r["context"]
        if name=="CONTROL": return True
        if name=="BTC_VOL_LOW":
            return c.get("btc_vol1h") is not None and c["btc_vol1h"]<=V82_BTCVOL_MAX
        if name=="ALT24_HIGH":
            return c.get("alt24h") is not None and c["alt24h"]>=V82_ALT24_MIN
        if name=="ALT24_HIGH_AND_BTC_VOL_LOW":
            return (c.get("btc_vol1h") is not None and c.get("alt24h") is not None and
                    c["btc_vol1h"]<=V82_BTCVOL_MAX and c["alt24h"]>=V82_ALT24_MIN)
        return False
    x=[r for r in rows if keep(r)]
    return {
      **v82_metrics(x),
      "retention_pct":round(100*len(x)/len(rows),2) if rows else 0,
      "chronological_blocks":v82_blocks(x)
    }

async def v82_fetch_symbol(symbol,start_ms,end_ms):
    cached=await asyncio.to_thread(v82_cache_get,symbol,start_ms,end_ms)
    if isinstance(cached,list) and len(cached)>=500:
        return cached,"CACHE"

    rows=[]; cur=int(start_ms)
    async with httpx.AsyncClient(timeout=httpx.Timeout(30.0,connect=10.0)) as client:
        while cur<int(end_ms):
            data=await get_json(client,"/api/v3/klines",params={
                "symbol":symbol,"interval":"5m","startTime":cur,
                "endTime":int(end_ms),"limit":1000})
            if not data: break
            for x in data:
                rows.append({
                    "open_time":int(x[0]),"open":float(x[1]),"high":float(x[2]),
                    "low":float(x[3]),"close":float(x[4]),"volume":float(x[5])
                })
            nxt=int(data[-1][0])+300000
            if nxt<=cur: break
            cur=nxt
            await asyncio.sleep(.04)

    rows=list({int(x["open_time"]):x for x in rows}.values())
    rows.sort(key=lambda x:x["open_time"])
    if len(rows)<500:
        raise RuntimeError(f"{symbol}: insufficient candles ({len(rows)})")
    await asyncio.to_thread(v82_cache_put,symbol,start_ms,end_ms,rows)
    return rows,"FETCH"

async def v82_run():
    global V82_TASK
    job=v82_job_get()
    if not job:
        raise RuntimeError("V82 job not initialized")

    try:
        job["status"]="RUNNING"; job["error"]=None
        job["started_utc"]=job.get("started_utc") or utc_now()
        v82_job_save(job)

        start_ms=int(job["fetch_start_ms"]); end_ms=int(job["end_ms"])
        syms=list(job["symbols"])

        # BTC first
        if not job.get("btc_done"):
            btc,src=await v82_fetch_symbol("BTCUSDT",start_ms,end_ms)
            job["btc_done"]=True; job["btc_source"]=src
            job["progress"]={"stage":"btc_done","done":0,"total":len(syms)}
            v82_job_save(job)
        else:
            btc=v82_cache_get("BTCUSDT",start_ms,end_ms)

        done=set(job.get("done_symbols",[]))
        failed=dict(job.get("failed_symbols",{}))

        for idx,sym in enumerate(syms):
            if sym in done:
                continue
            try:
                _,src=await v82_fetch_symbol(sym,start_ms,end_ms)
                done.add(sym)
                failed.pop(sym,None)
                job["last_symbol"]=sym
                job["last_source"]=src
            except Exception as e:
                failed[sym]=f"{type(e).__name__}: {e}"
            job["done_symbols"]=sorted(done)
            job["failed_symbols"]=failed
            job["progress"]={"stage":"fetch_symbols","done":len(done),"total":len(syms),
                             "failed":len(failed),"last_symbol":sym}
            v82_job_save(job)
            await asyncio.sleep(0)

        # Build compact store from successfully cached symbols.
        store={}; alts=[]; used=[]
        for sym in syms:
            x=v82_cache_get(sym,start_ms,end_ms)
            if isinstance(x,list) and len(x)>=500:
                store[len(store)]=v76_compact(x)
                alts.append(x); used.append(sym)

        btc=v82_cache_get("BTCUSDT",start_ms,end_ms)
        if not isinstance(btc,list):
            raise RuntimeError("BTC cache unavailable after fetch")

        job["progress"]={"stage":"recreate_frozen_cohort","done":len(used),"total":len(syms)}
        v82_job_save(job)

        # Same frozen V66 CONFIRM10 + BASE V70 reconstruction.
        rows=await asyncio.to_thread(v78_recreate,store,btc,.15,.10)

        report_start=int(job["report_start_ms"])
        cohort=sorted(
            [r for r in rows if report_start<=int(r["entry_time_ms"])<end_ms],
            key=lambda r:int(r["entry_time_ms"])
        )

        enriched=[]
        for i,r in enumerate(cohort):
            net,_=v801_net(r)
            if net is None: continue
            ts=int(r["entry_time_ms"])
            ctx=v80_context_for_time(ts,btc,alts)
            enriched.append({"ts":ts,"net":float(net),"context":ctx})
            if (i+1)%50==0:
                job["progress"]={"stage":"attach_context","done":i+1,"total":len(cohort)}
                v82_job_save(job)
                await asyncio.sleep(0)

        groups={}
        for name in ["CONTROL","BTC_VOL_LOW","ALT24_HIGH","ALT24_HIGH_AND_BTC_VOL_LOW"]:
            groups[name]=v82_group(enriched,name)

        control=groups["CONTROL"]
        decisions={}
        for name,z in groups.items():
            if name=="CONTROL":
                decisions[name]="CONTROL"; continue
            pf=z.get("profit_factor"); lo=z.get("mean_95ci",[None,None])[0]
            if z["n"]<30:
                decisions[name]="INSUFFICIENT_N"
            elif pf is not None and pf>1.15 and z["mean_net_pct"]>0:
                if lo is not None and lo>0:
                    decisions[name]="INDEPENDENT_OOS_PASS"
                else:
                    decisions[name]="SUPPORTIVE_NOT_CONCLUSIVE"
            else:
                decisions[name]="FAIL"

        result={
          "study":"V82_INDEPENDENT_OOS_FINALIST_VALIDATION",
          "window":{
            "report_start":datetime.fromtimestamp(report_start/1000,timezone.utc).isoformat(),
            "end":datetime.fromtimestamp(end_ms/1000,timezone.utc).isoformat(),
            "days":V82_DAYS,
            "non_overlap_with_v79_v81":True
          },
          "data":{"symbols_requested":len(syms),"symbols_used":len(used),
                  "failed_symbols":failed,"cohort_trades":len(enriched)},
          "frozen_strategy":{"entry":"V66_CONFIRM_10M_TOP1",
             "parent":"BTC30>0 AND ALT breadth30>0","exit":"TIME120","cost_pct":0.15},
          "frozen_v81_finalists":{
             "BTC_VOL_LOW":f"btc_vol1h <= {V82_BTCVOL_MAX}",
             "ALT24_HIGH":f"alt24h >= {V82_ALT24_MIN}",
             "INTERSECTION":"ALT24_HIGH AND BTC_VOL_LOW",
             "threshold_source":"V81 DEV medians; unchanged in V82"
          },
          "groups":groups,
          "decision":decisions,
          "guardrails":[
             "Older non-overlapping window; V82 does not retune thresholds.",
             "No threshold grid search.",
             "CONTROL and both V81 finalists are evaluated exactly as frozen.",
             "Intersection is predeclared before viewing V82 outcomes.",
             "Active V61/V74 execution is unchanged.",
             "Research/paper only; no orders."
          ]
        }

        job["status"]="DONE"; job["result"]=result
        job["progress"]={"stage":"done","done":len(enriched),"total":len(enriched)}
        job["finished_utc"]=utc_now(); job["error"]=None
        v82_job_save(job)

    except Exception as e:
        job=v82_job_get() or {}
        job["status"]="ERROR"; job["error"]=f"{type(e).__name__}: {e}"
        job["finished_utc"]=utc_now()
        v82_job_save(job)

async def v82_make_job(symbols=V82_SYMBOLS,days=V82_DAYS):
    end_dt=datetime(2026,8,28,11,15,tzinfo=timezone.utc)
    report_start_dt=end_dt-timedelta(days=int(days))
    fetch_start_dt=report_start_dt-timedelta(days=3)

    async with httpx.AsyncClient(timeout=30) as client:
        uni=await build_universe(client)
    syms=[x["symbol"] for x in uni[:int(symbols)]
          if isinstance(x,dict) and x.get("symbol")]
    if not syms:
        raise RuntimeError("No symbols from cleaned universe")

    return {
      "status":"READY","study":"V82_INDEPENDENT_OOS_FINALIST_VALIDATION",
      "symbols":syms,
      "report_start_ms":int(report_start_dt.timestamp()*1000),
      "fetch_start_ms":int(fetch_start_dt.timestamp()*1000),
      "end_ms":int(end_dt.timestamp()*1000),
      "done_symbols":[],"failed_symbols":{},"btc_done":False,
      "progress":{"stage":"ready","done":0,"total":len(syms)},
      "result":None,"error":None,"created_utc":utc_now()
    }

@app.get("/v82-start")
async def v82_start(symbols:int=V82_SYMBOLS,days:int=V82_DAYS):
    global V82_TASK
    await asyncio.to_thread(v82_db_init)
    job=await asyncio.to_thread(v82_job_get)

    # Reuse DONE result unless explicit different request requires a new frozen job.
    if job and job.get("status")=="DONE":
        return {"status":"ALREADY_DONE","study":job.get("study"),
                "use":"/v82-status","paper_only":True}

    if not job or int(days)!=V82_DAYS or int(symbols)!=V82_SYMBOLS:
        # Keep protocol frozen to 40x60 for this validation.
        job=await v82_make_job(V82_SYMBOLS,V82_DAYS)
        await asyncio.to_thread(v82_job_save,job)

    if V82_TASK is None or V82_TASK.done():
        V82_TASK=asyncio.create_task(v82_run())
    return {"status":"STARTED_OR_RESUMED",
            "study":"V82_INDEPENDENT_OOS_FINALIST_VALIDATION",
            "window":"older non-overlapping 60d ending 2026-08-28 11:15 UTC",
            "paper_only":True,"trading":False,"orders":False}

@app.get("/v82-status")
async def v82_status():
    await asyncio.to_thread(v82_db_init)
    job=await asyncio.to_thread(v82_job_get)
    return {**MODE_INFO,"status":"OK","panel":"V82_INDEPENDENT_OOS_FINALIST_VALIDATION",
      "trading":False,"orders":False,"active_strategy_changed":False,
      "v61_unchanged":True,"v74_unchanged":True,
      "study":job,"generated_utc":utc_now()}

@app.on_event("startup")
async def v82_autoresume_startup():
    global V82_TASK
    try:
        await asyncio.to_thread(v82_db_init)
        job=await asyncio.to_thread(v82_job_get)
        if job and job.get("status") in ("READY","RUNNING") and (V82_TASK is None or V82_TASK.done()):
            V82_TASK=asyncio.create_task(v82_run())
    except Exception:
        pass


# ============================================================
# V83 â€” CAUSAL EDGE-STATE ENGINE
# ============================================================
# Research only. Active V61/V74 unchanged.
# Uses V82's frozen older 60d cohort/cache.
# For each trade, state is determined ONLY from the previous 30 trades.
# EDGE_ON rule is predeclared:
#   prior30 PF > 1.05 AND prior30 mean net > 0
# No threshold grid, no retuning, no real orders.
# ============================================================

V83_TASK = None
V83_STATE = {
    "status": "IDLE",
    "error": None,
    "result": None,
    "started_utc": None,
    "finished_utc": None,
}
V83_LOOKBACK = 30
V83_PF_GATE = 1.05

def v83_pf(vals):
    wins=[x for x in vals if x>0]
    losses=[x for x in vals if x<=0]
    gl=-sum(losses)
    if gl<=0:
        return None
    return sum(wins)/gl

def v83_summary(rows):
    vals=[float(r["net"]) for r in rows]
    if not vals:
        return {"n":0,"mean_net_pct":None,"median_net_pct":None,
                "win_rate_pct":None,"profit_factor":None,"mean_95ci":[None,None]}
    pf=v83_pf(vals)
    return {
        "n":len(vals),
        "mean_net_pct":round(statistics.mean(vals),4),
        "median_net_pct":round(statistics.median(vals),4),
        "win_rate_pct":round(100*sum(1 for x in vals if x>0)/len(vals),2),
        "profit_factor":round(pf,4) if pf is not None else None,
        "mean_95ci":v81_ci(vals),
    }

def v83_context_summary(rows):
    keys=["btc30","btc1h","btc4h","btc24h","btc_vol1h",
          "alt30","alt1h","alt4h","alt24h","disp30"]
    out={}
    for k in keys:
        vals=[]
        for r in rows:
            v=(r.get("context") or {}).get(k)
            if v is not None:
                vals.append(float(v))
        out[k]=round(statistics.mean(vals),5) if vals else None
    return out

def v83_blocks(rows,n=4):
    rows=sorted(rows,key=lambda r:r["ts"])
    out=[]
    for i in range(n):
        a=len(rows)*i//n
        b=len(rows)*(i+1)//n
        part=rows[a:b]
        if part:
            out.append({"block":i+1,**v83_summary(part)})
    return out

async def v83_rebuild_rows():
    job=await asyncio.to_thread(v82_job_get)
    if not job or job.get("status")!="DONE":
        raise RuntimeError("V82 must be DONE first")

    start_ms=int(job["fetch_start_ms"])
    report_start=int(job["report_start_ms"])
    end_ms=int(job["end_ms"])
    syms=list(job["symbols"])

    btc=await asyncio.to_thread(v82_cache_get,"BTCUSDT",start_ms,end_ms)
    if not isinstance(btc,list):
        raise RuntimeError("V82 BTC cache missing")

    store={}
    alts=[]
    used=[]
    for sym in syms:
        c=await asyncio.to_thread(v82_cache_get,sym,start_ms,end_ms)
        if isinstance(c,list) and len(c)>=500:
            store[len(store)]=v76_compact(c)
            alts.append(c)
            used.append(sym)

    cohort=await asyncio.to_thread(v78_recreate,store,btc,0.15,0.10)
    cohort=sorted(
        [r for r in cohort if report_start<=int(r["entry_time_ms"])<end_ms],
        key=lambda r:int(r["entry_time_ms"])
    )

    enriched=[]
    for r in cohort:
        net,_=v801_net(r)
        if net is None:
            continue
        ts=int(r["entry_time_ms"])
        ctx=v80_context_for_time(ts,btc,alts)
        enriched.append({"ts":ts,"net":float(net),"context":ctx})
    return enriched,used

async def v83_run():
    global V83_STATE
    V83_STATE={
        "status":"RUNNING","error":None,"result":None,
        "started_utc":utc_now(),"finished_utc":None
    }
    try:
        rows,used=await v83_rebuild_rows()
        if len(rows)<=V83_LOOKBACK:
            raise RuntimeError("Not enough cohort trades")

        evaluated=[]
        for i in range(V83_LOOKBACK,len(rows)):
            hist=rows[i-V83_LOOKBACK:i]
            vals=[float(x["net"]) for x in hist]
            prior_pf=v83_pf(vals)
            prior_mean=statistics.mean(vals)
            edge_on=(
                prior_pf is not None
                and prior_pf>V83_PF_GATE
                and prior_mean>0
            )
            evaluated.append({
                **rows[i],
                "edge_on":edge_on,
                "prior30_pf":prior_pf,
                "prior30_mean":prior_mean,
            })

        on=[r for r in evaluated if r["edge_on"]]
        off=[r for r in evaluated if not r["edge_on"]]

        # Chronological thirds are reporting only; rule is identical throughout.
        thirds=[]
        for i in range(3):
            a=len(evaluated)*i//3
            b=len(evaluated)*(i+1)//3
            part=evaluated[a:b]
            po=[r for r in part if r["edge_on"]]
            pf=[r for r in part if not r["edge_on"]]
            thirds.append({
                "segment":i+1,
                "all":v83_summary(part),
                "edge_on":v83_summary(po),
                "edge_off":v83_summary(pf),
            })

        result={
            "study":"V83_CAUSAL_EDGE_STATE_ENGINE",
            "symbols_used":len(used),
            "cohort_trades_total":len(rows),
            "evaluated_after_warmup":len(evaluated),
            "frozen_rule":{
                "lookback_trades":V83_LOOKBACK,
                "EDGE_ON":"prior30 PF > 1.05 AND prior30 mean net > 0",
                "causal":True,
                "uses_future_data":False,
            },
            "results":{
                "ALL":{**v83_summary(evaluated),"chronological_blocks":v83_blocks(evaluated)},
                "EDGE_ON":{**v83_summary(on),"retention_pct":round(100*len(on)/len(evaluated),2),
                           "chronological_blocks":v83_blocks(on),
                           "mean_market_context":v83_context_summary(on)},
                "EDGE_OFF":{**v83_summary(off),"retention_pct":round(100*len(off)/len(evaluated),2),
                            "chronological_blocks":v83_blocks(off),
                            "mean_market_context":v83_context_summary(off)},
            },
            "chronological_thirds":thirds,
            "decision":{
                "edge_on_better_than_all": (
                    v83_summary(on)["profit_factor"] is not None
                    and v83_summary(evaluated)["profit_factor"] is not None
                    and v83_summary(on)["profit_factor"]>v83_summary(evaluated)["profit_factor"]
                    and v83_summary(on)["mean_net_pct"] is not None
                    and v83_summary(on)["mean_net_pct"]>0
                ),
                "note":"Diagnostic gate only. Do not promote without prospective validation."
            },
            "guardrails":[
                "V66 CONFIRM10 + BASE V70 + TIME120 cohort unchanged.",
                "EDGE_ON uses only the previous 30 completed trades.",
                "No threshold grid or post-result retuning.",
                "V82 cache reused; no new Binance download.",
                "This 60d sample has already been inspected in V82, so V83 is diagnostic, not pristine OOS.",
                "V61/V74 unchanged.",
                "Research/paper only; no orders."
            ]
        }
        V83_STATE.update(status="DONE",result=result,finished_utc=utc_now())
    except Exception as e:
        V83_STATE.update(status="ERROR",error=f"{type(e).__name__}: {e}",finished_utc=utc_now())

@app.get("/v83-start")
async def v83_start():
    global V83_TASK
    if V83_TASK is not None and not V83_TASK.done():
        return {"status":"ALREADY_RUNNING","paper_only":True}
    V83_TASK=asyncio.create_task(v83_run())
    return {
        "status":"STARTED",
        "study":"V83_CAUSAL_EDGE_STATE_ENGINE",
        "paper_only":True,
        "trading":False,
        "orders":False,
        "uses_v82_cache":True
    }

@app.get("/v83-status")
async def v83_status():
    return {
        **MODE_INFO,
        "status":"OK",
        "panel":"V83_CAUSAL_EDGE_STATE_ENGINE",
        "trading":False,
        "orders":False,
        "active_strategy_changed":False,
        "v61_unchanged":True,
        "v74_unchanged":True,
        "study":V83_STATE,
        "generated_utc":utc_now()
    }


# ============================================================
# V83.1 â€” LOW-RAM + POSTGRES PERSISTENT EDGE-STATE
# New endpoints: /v831-start and /v831-status
# ============================================================

V831_TABLE = "alt_v831_edge_state"
V831_TASK = None

def v831_db_init():
    if not V21_DB_URL:
        raise RuntimeError("DATABASE_URL missing")
    with v21_db_connect() as conn:
        with conn.cursor() as cur:
            cur.execute(f"""
                CREATE TABLE IF NOT EXISTS {V831_TABLE}(
                    id INTEGER PRIMARY KEY,
                    payload JSONB NOT NULL,
                    updated_at TIMESTAMPTZ DEFAULT NOW()
                )
            """)
        conn.commit()

def v831_save(payload):
    v831_db_init()
    with v21_db_connect() as conn:
        with conn.cursor() as cur:
            cur.execute(f"""
                INSERT INTO {V831_TABLE}(id,payload,updated_at)
                VALUES(1,%s::jsonb,NOW())
                ON CONFLICT(id) DO UPDATE
                SET payload=EXCLUDED.payload,updated_at=NOW()
            """,(json.dumps(payload),))
        conn.commit()

def v831_get():
    try:
        v831_db_init()
        with v21_db_connect() as conn:
            with conn.cursor() as cur:
                cur.execute(f"SELECT payload FROM {V831_TABLE} WHERE id=1")
                r=cur.fetchone()
                return r[0] if r else None
    except Exception:
        return None

def v831_summary(rows):
    vals=[float(r["net"]) for r in rows]
    if not vals:
        return {"n":0,"mean_net_pct":None,"median_net_pct":None,
                "win_rate_pct":None,"profit_factor":None}
    pf=v83_pf(vals)
    return {
        "n":len(vals),
        "mean_net_pct":round(statistics.mean(vals),4),
        "median_net_pct":round(statistics.median(vals),4),
        "win_rate_pct":round(100*sum(x>0 for x in vals)/len(vals),2),
        "profit_factor":round(pf,4) if pf is not None else None,
    }

def v831_blocks(rows,n=4):
    rows=sorted(rows,key=lambda r:r["ts"])
    out=[]
    for i in range(n):
        a=len(rows)*i//n;b=len(rows)*(i+1)//n
        p=rows[a:b]
        if p: out.append({"block":i+1,**v831_summary(p)})
    return out

async def v831_run():
    state={
        "status":"RUNNING","error":None,"result":None,
        "progress":{"stage":"init","done":0,"total":0},
        "started_utc":utc_now(),"finished_utc":None
    }
    try:
        await asyncio.to_thread(v831_save,state)

        job=await asyncio.to_thread(v82_job_get)
        if not job or job.get("status")!="DONE":
            raise RuntimeError("V82 must be DONE first")

        start_ms=int(job["fetch_start_ms"])
        report_start=int(job["report_start_ms"])
        end_ms=int(job["end_ms"])
        syms=list(job["symbols"])

        btc=await asyncio.to_thread(v82_cache_get,"BTCUSDT",start_ms,end_ms)
        if not isinstance(btc,list):
            raise RuntimeError("V82 BTC cache missing")

        # LOW RAM: never retain raw alt candle lists.
        # Each symbol is loaded, compacted, then raw JSON is released.
        store={}
        used=[]
        state["progress"]={"stage":"compact_symbols","done":0,"total":len(syms)}
        await asyncio.to_thread(v831_save,state)

        for sym in syms:
            c=await asyncio.to_thread(v82_cache_get,sym,start_ms,end_ms)
            if isinstance(c,list) and len(c)>=500:
                store[len(store)]=v76_compact(c)
                used.append(sym)
            del c
            state["progress"]["done"]+=1
            if state["progress"]["done"]%5==0:
                await asyncio.to_thread(v831_save,state)
            await asyncio.sleep(0)

        state["progress"]={"stage":"recreate_frozen_cohort","done":0,"total":1}
        await asyncio.to_thread(v831_save,state)

        cohort=await asyncio.to_thread(v78_recreate,store,btc,0.15,0.10)
        del store
        del btc

        rows=[]
        for r in cohort:
            ts=int(r["entry_time_ms"])
            if report_start<=ts<end_ms:
                net,_=v801_net(r)
                if net is not None:
                    rows.append({"ts":ts,"net":float(net)})
        del cohort
        rows.sort(key=lambda x:x["ts"])

        if len(rows)<=V83_LOOKBACK:
            raise RuntimeError("Not enough cohort trades")

        state["progress"]={"stage":"causal_edge_state","done":0,"total":len(rows)-V83_LOOKBACK}
        await asyncio.to_thread(v831_save,state)

        evaluated=[]
        for i in range(V83_LOOKBACK,len(rows)):
            hist=rows[i-V83_LOOKBACK:i]
            vals=[x["net"] for x in hist]
            prior_pf=v83_pf(vals)
            prior_mean=statistics.mean(vals)
            evaluated.append({
                **rows[i],
                "edge_on":bool(prior_pf is not None and prior_pf>V83_PF_GATE and prior_mean>0),
                "prior30_pf":prior_pf,
                "prior30_mean":prior_mean
            })

        on=[r for r in evaluated if r["edge_on"]]
        off=[r for r in evaluated if not r["edge_on"]]

        thirds=[]
        for i in range(3):
            a=len(evaluated)*i//3;b=len(evaluated)*(i+1)//3
            p=evaluated[a:b]
            thirds.append({
                "segment":i+1,
                "ALL":v831_summary(p),
                "EDGE_ON":v831_summary([r for r in p if r["edge_on"]]),
                "EDGE_OFF":v831_summary([r for r in p if not r["edge_on"]])
            })

        all_s=v831_summary(evaluated)
        on_s=v831_summary(on)
        off_s=v831_summary(off)

        result={
            "study":"V83.1_LOW_RAM_CAUSAL_EDGE_STATE",
            "symbols_used":len(used),
            "cohort_trades_total":len(rows),
            "warmup_trades":V83_LOOKBACK,
            "evaluated_trades":len(evaluated),
            "frozen_rule":{
                "EDGE_ON":"previous 30 completed trades PF > 1.05 AND mean net > 0",
                "lookback_trades":30,
                "causal":True,
                "retuned":False
            },
            "results":{
                "ALL":{**all_s,"chronological_blocks":v831_blocks(evaluated)},
                "EDGE_ON":{**on_s,
                    "retention_pct":round(100*len(on)/len(evaluated),2),
                    "chronological_blocks":v831_blocks(on)},
                "EDGE_OFF":{**off_s,
                    "retention_pct":round(100*len(off)/len(evaluated),2),
                    "chronological_blocks":v831_blocks(off)}
            },
            "chronological_thirds":thirds,
            "decision":{
                "EDGE_ON_BETTER":bool(
                    on_s["n"]>=30 and
                    on_s["profit_factor"] is not None and
                    all_s["profit_factor"] is not None and
                    on_s["profit_factor"]>all_s["profit_factor"] and
                    on_s["mean_net_pct"] is not None and
                    on_s["mean_net_pct"]>0
                ),
                "promotion":"DIAGNOSTIC_ONLY"
            },
            "guardrails":[
                "Same frozen V66 CONFIRM10 + BASE V70 + TIME120 cohort.",
                "Only previous completed trades determine EDGE_ON.",
                "No threshold grid search or retuning.",
                "V82 cache reused; no Binance refetch.",
                "PostgreSQL persists progress/result across Render restart.",
                "V83.1 deliberately omits heavy all-symbol market-context reconstruction.",
                "V61/V74 unchanged; research/paper only; no orders."
            ]
        }

        state.update(
            status="DONE",result=result,error=None,
            progress={"stage":"done","done":len(evaluated),"total":len(evaluated)},
            finished_utc=utc_now()
        )
        await asyncio.to_thread(v831_save,state)

    except Exception as e:
        state.update(status="ERROR",error=f"{type(e).__name__}: {e}",finished_utc=utc_now())
        try: await asyncio.to_thread(v831_save,state)
        except Exception: pass

@app.get("/v831-start")
async def v831_start():
    global V831_TASK
    old=await asyncio.to_thread(v831_get)
    if V831_TASK is not None and not V831_TASK.done():
        return {"status":"ALREADY_RUNNING","paper_only":True}
    if old and old.get("status")=="DONE":
        return {"status":"ALREADY_DONE","paper_only":True,"use":"/v831-status"}
    V831_TASK=asyncio.create_task(v831_run())
    return {"status":"STARTED","study":"V83.1_LOW_RAM_CAUSAL_EDGE_STATE",
            "paper_only":True,"trading":False,"orders":False}

@app.get("/v831-status")
async def v831_status():
    state=await asyncio.to_thread(v831_get)
    return {
        **MODE_INFO,
        "status":"OK",
        "panel":"V83.1_LOW_RAM_CAUSAL_EDGE_STATE",
        "trading":False,"orders":False,
        "active_strategy_changed":False,
        "v61_unchanged":True,"v74_unchanged":True,
        "study":state,
        "generated_utc":utc_now()
    }

@app.on_event("startup")
async def v831_resume():
    global V831_TASK
    try:
        state=await asyncio.to_thread(v831_get)
        if state and state.get("status")=="RUNNING":
            V831_TASK=asyncio.create_task(v831_run())
    except Exception:
        pass


# ============================================================
# V84 â€” FROZEN 90D OLDER OOS EDGE-STATE VALIDATION
# Research/paper only. V61/V74 unchanged.
# Frozen V83.1 rule: previous 30 completed trades PF > 1.05 AND mean > 0.
# Window: 90d ending exactly where V82 report window begins.
# Extra 14d pre-window warmup supplies causal prior trades + indicators.
# Separate PostgreSQL cache: DOES NOT overwrite V82 cache.
# ============================================================

V84_JOB_TABLE="alt_v84_job"
V84_CACHE_TABLE="alt_v84_cache"
V84_TASK=None
V84_DAYS=90
V84_WARMUP_DAYS=14
V84_LOOKBACK=30
V84_PF_GATE=1.05

def v84_db_init():
    if not V21_DB_URL: raise RuntimeError("DATABASE_URL missing")
    with v21_db_connect() as conn:
        with conn.cursor() as cur:
            cur.execute(f"""CREATE TABLE IF NOT EXISTS {V84_JOB_TABLE}(
                id INTEGER PRIMARY KEY,payload JSONB NOT NULL,updated_at TIMESTAMPTZ DEFAULT NOW())""")
            cur.execute(f"""CREATE TABLE IF NOT EXISTS {V84_CACHE_TABLE}(
                symbol TEXT PRIMARY KEY,start_ms BIGINT NOT NULL,end_ms BIGINT NOT NULL,
                candles JSONB NOT NULL,updated_at TIMESTAMPTZ DEFAULT NOW())""")
        conn.commit()

def v84_save(p):
    v84_db_init()
    with v21_db_connect() as conn:
        with conn.cursor() as cur:
            cur.execute(f"""INSERT INTO {V84_JOB_TABLE}(id,payload,updated_at)
                VALUES(1,%s::jsonb,NOW()) ON CONFLICT(id) DO UPDATE
                SET payload=EXCLUDED.payload,updated_at=NOW()""",(json.dumps(p),))
        conn.commit()

def v84_get():
    try:
        v84_db_init()
        with v21_db_connect() as conn:
            with conn.cursor() as cur:
                cur.execute(f"SELECT payload FROM {V84_JOB_TABLE} WHERE id=1")
                r=cur.fetchone(); return r[0] if r else None
    except Exception:return None

def v84_cache_get(sym,s,e):
    try:
        v84_db_init()
        with v21_db_connect() as conn:
            with conn.cursor() as cur:
                cur.execute(f"""SELECT candles FROM {V84_CACHE_TABLE}
                    WHERE symbol=%s AND start_ms=%s AND end_ms=%s""",(sym,int(s),int(e)))
                r=cur.fetchone();return r[0] if r else None
    except Exception:return None

def v84_cache_put(sym,s,e,c):
    v84_db_init()
    with v21_db_connect() as conn:
        with conn.cursor() as cur:
            cur.execute(f"""INSERT INTO {V84_CACHE_TABLE}
                (symbol,start_ms,end_ms,candles,updated_at)
                VALUES(%s,%s,%s,%s::jsonb,NOW())
                ON CONFLICT(symbol) DO UPDATE SET start_ms=EXCLUDED.start_ms,
                end_ms=EXCLUDED.end_ms,candles=EXCLUDED.candles,updated_at=NOW()""",
                (sym,int(s),int(e),json.dumps(c)))
        conn.commit()

async def v84_fetch(sym,s,e):
    c=await asyncio.to_thread(v84_cache_get,sym,s,e)
    if isinstance(c,list) and len(c)>=500:return c,"CACHE"
    rows=[];cur=int(s)
    async with httpx.AsyncClient(timeout=httpx.Timeout(30.0,connect=10.0)) as client:
        while cur<int(e):
            d=await get_json(client,"/api/v3/klines",params={
                "symbol":sym,"interval":"5m","startTime":cur,"endTime":int(e),"limit":1000})
            if not d:break
            rows.extend({"open_time":int(x[0]),"open":float(x[1]),"high":float(x[2]),
                         "low":float(x[3]),"close":float(x[4]),"volume":float(x[5])} for x in d)
            nxt=int(d[-1][0])+300000
            if nxt<=cur:break
            cur=nxt;await asyncio.sleep(.04)
    rows=list({x["open_time"]:x for x in rows}.values());rows.sort(key=lambda x:x["open_time"])
    if len(rows)<500:raise RuntimeError(f"{sym}: insufficient candles ({len(rows)})")
    await asyncio.to_thread(v84_cache_put,sym,s,e,rows)
    return rows,"FETCH"

def v84_sum(rows):
    vals=[float(x["net"]) for x in rows]
    if not vals:return {"n":0,"mean_net_pct":None,"median_net_pct":None,
                        "win_rate_pct":None,"profit_factor":None}
    pf=v83_pf(vals)
    return {"n":len(vals),"mean_net_pct":round(statistics.mean(vals),4),
            "median_net_pct":round(statistics.median(vals),4),
            "win_rate_pct":round(100*sum(x>0 for x in vals)/len(vals),2),
            "profit_factor":round(pf,4) if pf is not None else None}

def v84_blocks(rows,n=4):
    rows=sorted(rows,key=lambda x:x["ts"]);out=[]
    for i in range(n):
        p=rows[len(rows)*i//n:len(rows)*(i+1)//n]
        if p:out.append({"block":i+1,**v84_sum(p)})
    return out

async def v84_run():
    state={"status":"RUNNING","error":None,"result":None,
           "progress":{"stage":"init","done":0,"total":0},
           "started_utc":utc_now(),"finished_utc":None}
    try:
        await asyncio.to_thread(v84_save,state)

        old82=await asyncio.to_thread(v82_job_get)
        if not old82 or old82.get("status")!="DONE":
            raise RuntimeError("V82 DONE result required")
        symbols=list(old82["symbols"])

        # Independent older report window.
        report_end=int(old82["report_start_ms"])
        report_start=report_end-V84_DAYS*86400000
        fetch_start=report_start-V84_WARMUP_DAYS*86400000
        fetch_end=report_end

        state.update({"window":{
            "report_start_ms":report_start,"report_end_ms":report_end,
            "fetch_start_ms":fetch_start,"days":V84_DAYS,"warmup_days":V84_WARMUP_DAYS,
            "non_overlap_with_v82":True}})
        state["progress"]={"stage":"btc","done":0,"total":1}
        await asyncio.to_thread(v84_save,state)

        btc,src=await v84_fetch("BTCUSDT",fetch_start,fetch_end)
        state["progress"]={"stage":"symbols","done":0,"total":len(symbols)}
        state["failed_symbols"]={}
        await asyncio.to_thread(v84_save,state)

        store={};used=[]
        for sym in symbols:
            try:
                c,_=await v84_fetch(sym,fetch_start,fetch_end)
                store[len(store)]=v76_compact(c);used.append(sym)
                del c
            except Exception as e:
                state["failed_symbols"][sym]=f"{type(e).__name__}: {e}"
            state["progress"]["done"]+=1
            await asyncio.to_thread(v84_save,state)
            await asyncio.sleep(0)

        state["progress"]={"stage":"recreate","done":0,"total":1}
        await asyncio.to_thread(v84_save,state)
        cohort=await asyncio.to_thread(v78_recreate,store,btc,0.15,0.10)
        del store;del btc
        cohort=sorted(cohort,key=lambda r:int(r["entry_time_ms"]))

        # Build causal stream including pre-window trades.
        stream=[]
        for r in cohort:
            ts=int(r["entry_time_ms"]);net,_=v801_net(r)
            if net is not None and fetch_start<=ts<report_end:
                stream.append({"ts":ts,"net":float(net)})
        del cohort

        evaluated=[]
        for i in range(V84_LOOKBACK,len(stream)):
            r=stream[i]
            if r["ts"]<report_start:continue
            hist=stream[i-V84_LOOKBACK:i]
            vals=[x["net"] for x in hist]
            pf=v83_pf(vals);mn=statistics.mean(vals)
            evaluated.append({**r,"edge_on":bool(pf is not None and pf>V84_PF_GATE and mn>0),
                              "prior30_pf":pf,"prior30_mean":mn})

        if not evaluated:raise RuntimeError("No V84 evaluated trades")
        on=[r for r in evaluated if r["edge_on"]];off=[r for r in evaluated if not r["edge_on"]]
        A=v84_sum(evaluated);O=v84_sum(on);F=v84_sum(off)

        result={
            "study":"V84_FROZEN_90D_OLDER_OOS_EDGE_STATE",
            "window":{"days":90,"warmup_days":14,
                      "report_start_utc":datetime.fromtimestamp(report_start/1000,timezone.utc).isoformat(),
                      "report_end_utc":datetime.fromtimestamp(report_end/1000,timezone.utc).isoformat(),
                      "non_overlap_with_v82":True},
            "symbols_requested":len(symbols),"symbols_used":len(used),
            "failed_symbols":state["failed_symbols"],
            "frozen_rule":{"lookback_trades":30,
                "EDGE_ON":"previous 30 completed trades PF > 1.05 AND mean net > 0",
                "retuned":False,"causal":True},
            "results":{
                "ALL":{**A,"chronological_blocks":v84_blocks(evaluated)},
                "EDGE_ON":{**O,"retention_pct":round(100*len(on)/len(evaluated),2),
                           "chronological_blocks":v84_blocks(on)},
                "EDGE_OFF":{**F,"retention_pct":round(100*len(off)/len(evaluated),2),
                            "chronological_blocks":v84_blocks(off)}},
            "decision":{
                "PASS":bool(O["n"]>=50 and O["profit_factor"] is not None and
                    A["profit_factor"] is not None and O["profit_factor"]>A["profit_factor"] and
                    O["mean_net_pct"] is not None and O["mean_net_pct"]>0 and
                    F["profit_factor"] is not None and F["profit_factor"]<1.0),
                "next_if_pass":"PROSPECTIVE_SHADOW_PAPER",
                "next_if_fail":"DO_NOT_RETUNE_ON_V84"},
            "guardrails":[
                "Frozen V83.1 rule; no threshold changes.",
                "Older 90d report window ends where V82 begins.",
                "14d pre-window data is warmup only, not scored.",
                "Separate V84 PostgreSQL cache; V82 cache untouched.",
                "Current-universe survivorship bias remains.",
                "V61/V74 unchanged; research/paper only; no orders."]}
        state.update(status="DONE",result=result,
            progress={"stage":"done","done":len(evaluated),"total":len(evaluated)},
            finished_utc=utc_now())
        await asyncio.to_thread(v84_save,state)
    except Exception as e:
        state.update(status="ERROR",error=f"{type(e).__name__}: {e}",finished_utc=utc_now())
        try:await asyncio.to_thread(v84_save,state)
        except Exception:pass

@app.get("/v84-start")
async def v84_start():
    global V84_TASK
    old=await asyncio.to_thread(v84_get)
    if V84_TASK is not None and not V84_TASK.done():
        return {"status":"ALREADY_RUNNING","paper_only":True}
    if old and old.get("status")=="DONE":
        return {"status":"ALREADY_DONE","use":"/v84-status","paper_only":True}
    V84_TASK=asyncio.create_task(v84_run())
    return {"status":"STARTED","study":"V84_FROZEN_90D_OLDER_OOS_EDGE_STATE",
            "trading":False,"orders":False,"paper_only":True}

@app.get("/v84-status")
async def v84_status():
    s=await asyncio.to_thread(v84_get)
    return {**MODE_INFO,"status":"OK","panel":"V84_FROZEN_90D_OLDER_OOS_EDGE_STATE",
            "trading":False,"orders":False,"active_strategy_changed":False,
            "v61_unchanged":True,"v74_unchanged":True,"study":s,"generated_utc":utc_now()}

@app.on_event("startup")
async def v84_resume():
    global V84_TASK
    try:
        s=await asyncio.to_thread(v84_get)
        if s and s.get("status")=="RUNNING":
            V84_TASK=asyncio.create_task(v84_run())
    except Exception:pass


# ============================================================
# V85 â€” COMBINED WINNER/LOSER + MARKET-STATE DISCOVERY
# Uses frozen V82 + V84 cohorts. NO threshold fitting.
# Automatically sends an OBSERVATIONAL research summary to Telegram.
# Active V61/V74 signal logic remains unchanged.
# ============================================================

V85_TABLE="alt_v85_discovery_state"
V85_TASK=None
V85_FEATURES=[
    "breakout_change_pct","volume_ratio",
    "btc30","btc4","btc24",
    "alt30","alt4","alt24","disp30"
]

def v85_db_init():
    if not V21_DB_URL: raise RuntimeError("DATABASE_URL missing")
    with v21_db_connect() as conn:
        with conn.cursor() as cur:
            cur.execute(f"""CREATE TABLE IF NOT EXISTS {V85_TABLE}(
                id INTEGER PRIMARY KEY,payload JSONB NOT NULL,
                updated_at TIMESTAMPTZ DEFAULT NOW())""")
        conn.commit()

def v85_save(p):
    v85_db_init()
    with v21_db_connect() as conn:
        with conn.cursor() as cur:
            cur.execute(f"""INSERT INTO {V85_TABLE}(id,payload,updated_at)
                VALUES(1,%s::jsonb,NOW()) ON CONFLICT(id) DO UPDATE
                SET payload=EXCLUDED.payload,updated_at=NOW()""",
                (json.dumps(p,default=str),))
        conn.commit()

def v85_get():
    try:
        v85_db_init()
        with v21_db_connect() as conn:
            with conn.cursor() as cur:
                cur.execute(f"SELECT payload FROM {V85_TABLE} WHERE id=1")
                r=cur.fetchone(); return r[0] if r else None
    except Exception:return None

def v85_pf(rows):
    vals=[float(r["net"]) for r in rows]
    gp=sum(x for x in vals if x>0); gl=-sum(x for x in vals if x<=0)
    return (gp/gl) if gl>0 else None

def v85_perf(rows):
    vals=[float(r["net"]) for r in rows]
    if not vals:return {"n":0,"mean":None,"median":None,"wr":None,"pf":None}
    pf=v85_pf(rows)
    return {"n":len(vals),"mean":round(statistics.mean(vals),4),
            "median":round(statistics.median(vals),4),
            "wr":round(100*sum(x>0 for x in vals)/len(vals),2),
            "pf":round(pf,4) if pf is not None else None}

def v85_fstats(rows,feature):
    x=[float(r[feature]) for r in rows if r.get(feature) is not None]
    if not x:return {"n":0,"mean":None,"median":None}
    return {"n":len(x),"mean":round(statistics.mean(x),5),
            "median":round(statistics.median(x),5)}

def v85_compare(rows):
    win=[r for r in rows if float(r["net"])>0]
    lose=[r for r in rows if float(r["net"])<=0]
    out={}
    for f in V85_FEATURES:
        a=[float(r[f]) for r in win if r.get(f) is not None]
        b=[float(r[f]) for r in lose if r.get(f) is not None]
        allv=a+b
        if not a or not b:
            out[f]={"winner":v85_fstats(win,f),"loser":v85_fstats(lose,f),
                    "diff":None,"effect":None}
            continue
        diff=statistics.mean(a)-statistics.mean(b)
        sd=statistics.pstdev(allv) if len(allv)>1 else 0
        out[f]={
            "winner":v85_fstats(win,f),"loser":v85_fstats(lose,f),
            "diff":round(diff,5),
            "effect":round(diff/sd,4) if sd>0 else None
        }
    return out

def v85_quartiles(rows):
    rows=sorted(rows,key=lambda r:int(r["entry_time_ms"]))
    out=[]
    for i in range(4):
        p=rows[len(rows)*i//4:len(rows)*(i+1)//4]
        if not p:continue
        out.append({
            "block":i+1,
            "performance":v85_perf(p),
            "feature_means":{f:v85_fstats(p,f)["mean"] for f in V85_FEATURES}
        })
    return out

def v85_stability(rows82,rows84):
    c82=v85_compare(rows82); c84=v85_compare(rows84)
    combined=sorted(rows84+rows82,key=lambda r:int(r["entry_time_ms"]))
    blocks=[]
    for i in range(4):
        p=combined[len(combined)*i//4:len(combined)*(i+1)//4]
        blocks.append(v85_compare(p))
    ranked=[]
    for f in V85_FEATURES:
        d82=c82[f]["diff"]; d84=c84[f]["diff"]
        effs=[c82[f]["effect"],c84[f]["effect"]]
        if d82 is None or d84 is None:continue
        direction=1 if d82>0 and d84>0 else (-1 if d82<0 and d84<0 else 0)
        bdiff=[x[f]["diff"] for x in blocks if x[f]["diff"] is not None]
        same=sum(1 for d in bdiff if direction and ((d>0)==(direction>0)))
        avg_eff=statistics.mean([abs(x) for x in effs if x is not None]) if any(x is not None for x in effs) else 0
        ranked.append({
            "feature":f,
            "v82_diff":d82,"v84_diff":d84,
            "same_direction_across_windows":bool(direction),
            "block_direction_agreement":f"{same}/{len(bdiff)}" if direction else "0/4",
            "average_abs_effect":round(avg_eff,4),
            "status":"STABLE_OBSERVATIONAL" if direction and same>=3 else "UNSTABLE"
        })
    ranked.sort(key=lambda x:(x["status"]=="STABLE_OBSERVATIONAL",x["average_abs_effect"]),reverse=True)
    return ranked

def v85_load_window(which):
    if which=="V82":
        job=v82_job_get()
        if not job or job.get("status")!="DONE":raise RuntimeError("V82 DONE required")
        s=int(job["fetch_start_ms"]);e=int(job["end_ms"]);rs=int(job["report_start_ms"])
        btc=v82_cache_get("BTCUSDT",s,e)
        getter=lambda sym:v82_cache_get(sym,s,e)
        syms=list(job["symbols"])
    else:
        job=v84_get()
        if not job or job.get("status")!="DONE":raise RuntimeError("V84 DONE required")
        w=job["window"];s=int(w["fetch_start_ms"]);e=int(w["report_end_ms"]);rs=int(w["report_start_ms"])
        btc=v84_cache_get("BTCUSDT",s,e)
        getter=lambda sym:v84_cache_get(sym,s,e)
        old82=v82_job_get();syms=list(old82["symbols"])
    if not isinstance(btc,list):raise RuntimeError(f"{which} BTC cache missing")
    store={};failed=[]
    for sym in syms:
        c=getter(sym)
        if isinstance(c,list) and len(c)>=500:store[len(store)]=v76_compact(c)
        else:failed.append(sym)
        c=None
    rows=v78_recreate(store,btc,.15,.10)
    rows=[r for r in rows if rs<=int(r["entry_time_ms"])<e]
    rows.sort(key=lambda r:int(r["entry_time_ms"]))
    return rows,failed

def v85_tg_text(result):
    stable=[x for x in result["feature_stability"] if x["status"]=="STABLE_OBSERVATIONAL"]
    lines=[
        "ğŸ”¬ V85 MARKET-STATE / WINNER-LOSER",
        "OBSERVATIONAL ONLY â€” sinyal engellemez",
        "",
        f"V84: n={result['windows']['V84']['performance']['n']} PF={result['windows']['V84']['performance']['pf']}",
        f"V82: n={result['windows']['V82']['performance']['n']} PF={result['windows']['V82']['performance']['pf']}",
        f"BirleÅŸik: n={result['combined']['performance']['n']} PF={result['combined']['performance']['pf']}",
        "",
        "Zamanlar arasÄ± daha istikrarlÄ± Ã¶zellikler:"
    ]
    if stable:
        for x in stable[:5]:
            arrow="â†‘" if x["v82_diff"]>0 else "â†“"
            lines.append(f"â€¢ {x['feature']} {arrow} | blok {x['block_direction_agreement']} | etki {x['average_abs_effect']}")
    else:
        lines.append("â€¢ GÃ¼venilir tekrar eden Ã¶zellik bulunmadÄ±.")
    lines += ["","V61/V74 deÄŸiÅŸmedi. GerÃ§ek emir yok."]
    return "\n".join(lines)

async def v85_run():
    state={"status":"RUNNING","error":None,"result":None,
           "progress":{"stage":"load_v84","done":0,"total":2},
           "telegram":None,"started_utc":utc_now(),"finished_utc":None}
    try:
        await asyncio.to_thread(v85_save,state)
        r84,f84=await asyncio.to_thread(v85_load_window,"V84")
        state["progress"]={"stage":"load_v82","done":1,"total":2};await asyncio.to_thread(v85_save,state)
        r82,f82=await asyncio.to_thread(v85_load_window,"V82")
        state["progress"]={"stage":"analyze","done":2,"total":2};await asyncio.to_thread(v85_save,state)

        combined=sorted(r84+r82,key=lambda r:int(r["entry_time_ms"]))
        result={
            "study":"V85_COMBINED_WINNER_LOSER_MARKET_STATE_DISCOVERY",
            "windows":{
                "V84":{"performance":v85_perf(r84),"winner_loser":v85_compare(r84),
                       "chronological_quartiles":v85_quartiles(r84),"failed_symbols":f84},
                "V82":{"performance":v85_perf(r82),"winner_loser":v85_compare(r82),
                       "chronological_quartiles":v85_quartiles(r82),"failed_symbols":f82}},
            "combined":{"performance":v85_perf(combined),
                        "winner_loser":v85_compare(combined),
                        "chronological_quartiles":v85_quartiles(combined)},
            "feature_stability":v85_stability(r82,r84),
            "telegram_mode":"OBSERVATIONAL_RESEARCH_SUMMARY_ONLY",
            "decision":"DISCOVERY_ONLY_NO_FILTER_PROMOTION",
            "guardrails":[
                "No feature threshold is fitted in V85.",
                "Winner/loser differences are descriptive, not entry rules.",
                "Only features repeating across V82 and V84 and >=3/4 chronological blocks are labelled STABLE_OBSERVATIONAL.",
                "Telegram receives research context only; it does not block or create entries.",
                "V61/V74 execution unchanged.",
                "Research/paper only; no real orders.",
                "Current-universe survivorship bias remains."
            ]}
        state["result"]=result
        try:
            state["telegram"]=await v22_telegram_send(v85_tg_text(result))
        except Exception as te:
            state["telegram"]={"sent":False,"error":f"{type(te).__name__}: {te}"}
        state.update(status="DONE",progress={"stage":"done","done":2,"total":2},
                     finished_utc=utc_now())
        await asyncio.to_thread(v85_save,state)
    except Exception as e:
        state.update(status="ERROR",error=f"{type(e).__name__}: {e}",finished_utc=utc_now())
        try:await asyncio.to_thread(v85_save,state)
        except Exception:pass

@app.get("/v85-start")
async def v85_start():
    global V85_TASK
    old=await asyncio.to_thread(v85_get)
    if V85_TASK is not None and not V85_TASK.done():
        return {"status":"ALREADY_RUNNING","paper_only":True}
    if old and old.get("status")=="DONE":
        return {"status":"ALREADY_DONE","use":"/v85-status","paper_only":True}
    V85_TASK=asyncio.create_task(v85_run())
    return {"status":"STARTED","study":"V85_COMBINED_WINNER_LOSER_MARKET_STATE_DISCOVERY",
            "telegram_summary":True,"trading":False,"orders":False}

@app.get("/v85-status")
async def v85_status():
    s=await asyncio.to_thread(v85_get)
    return {**MODE_INFO,"status":"OK","panel":"V85_COMBINED_WINNER_LOSER_MARKET_STATE_DISCOVERY",
            "trading":False,"orders":False,"active_strategy_changed":False,
            "v61_unchanged":True,"v74_unchanged":True,"study":s,"generated_utc":utc_now()}

@app.get("/v85-telegram")
async def v85_telegram():
    s=await asyncio.to_thread(v85_get)
    if not s or s.get("status")!="DONE" or not s.get("result"):
        return {"status":"NOT_READY"}
    res=await v22_telegram_send(v85_tg_text(s["result"]))
    return {"status":"OK","telegram":res,"trading":False,"orders":False}

@app.on_event("startup")
async def v85_resume():
    global V85_TASK
    try:
        s=await asyncio.to_thread(v85_get)
        if s and s.get("status")=="RUNNING":
            V85_TASK=asyncio.create_task(v85_run())
    except Exception:pass


# ============================================================
# V86 â€” FROZEN 4-FACTOR QUALITY SCORE VALIDATION
# Discovery factors come ONLY from V85:
#   disp30, alt30, btc30, breakout_change_pct
# Thresholds are learned ONCE from older V84 winners (medians),
# then frozen and evaluated on later V82.
# Score 0..4 = number of frozen thresholds passed.
# V82 is validation, not threshold tuning.
# Telegram summary only; active V61/V74 entries remain unchanged.
# ============================================================

V86_TABLE="alt_v86_quality_state"
V86_TASK=None
V86_FACTORS=["disp30","alt30","btc30","breakout_change_pct"]

def v86_db_init():
    if not V21_DB_URL: raise RuntimeError("DATABASE_URL missing")
    with v21_db_connect() as conn:
        with conn.cursor() as cur:
            cur.execute(f"""CREATE TABLE IF NOT EXISTS {V86_TABLE}(
                id INTEGER PRIMARY KEY,payload JSONB NOT NULL,
                updated_at TIMESTAMPTZ DEFAULT NOW())""")
        conn.commit()

def v86_save(p):
    v86_db_init()
    with v21_db_connect() as conn:
        with conn.cursor() as cur:
            cur.execute(f"""INSERT INTO {V86_TABLE}(id,payload,updated_at)
                VALUES(1,%s::jsonb,NOW()) ON CONFLICT(id) DO UPDATE
                SET payload=EXCLUDED.payload,updated_at=NOW()""",
                (json.dumps(p,default=str),))
        conn.commit()

def v86_get():
    try:
        v86_db_init()
        with v21_db_connect() as conn:
            with conn.cursor() as cur:
                cur.execute(f"SELECT payload FROM {V86_TABLE} WHERE id=1")
                r=cur.fetchone(); return r[0] if r else None
    except Exception:return None

def v86_thresholds_from_v84(rows):
    winners=[r for r in rows if float(r["net"])>0]
    if len(winners)<50:raise RuntimeError("Too few V84 winners")
    out={}
    for f in V86_FACTORS:
        vals=sorted(float(r[f]) for r in winners if r.get(f) is not None)
        if not vals:raise RuntimeError(f"Missing factor {f}")
        out[f]=statistics.median(vals)
    return out

def v86_score_row(r,t):
    return sum(1 for f in V86_FACTORS if r.get(f) is not None and float(r[f])>=float(t[f]))

def v86_groups(rows,t):
    scored=[]
    for r in rows:
        q=v86_score_row(r,t)
        scored.append({**r,"v86_score":q})
    exact={str(q):v85_perf([r for r in scored if r["v86_score"]==q]) for q in range(5)}
    cumulative={f"{q}+":v85_perf([r for r in scored if r["v86_score"]>=q]) for q in range(5)}
    return scored,exact,cumulative

def v86_blocks(rows,t,n=4):
    rows=sorted(rows,key=lambda r:int(r["entry_time_ms"]))
    out=[]
    for i in range(n):
        p=rows[len(rows)*i//n:len(rows)*(i+1)//n]
        scored,_,cum=v86_groups(p,t)
        out.append({"block":i+1,"all":v85_perf(scored),
                    "score_3plus":cum["3+"],"score_4":cum["4+"]})
    return out

def v86_label(score):
    return "YUKSEK" if score>=3 else ("ORTA" if score==2 else "DUSUK")

def v86_tg_text(res):
    v=res["validation_v82"]
    lines=[
        "ğŸ§ª V86 4-FAKTOR KALÄ°TE DOÄRULAMA",
        "Paper/research only â€” sinyali engellemez",
        "",
        "FaktÃ¶rler: disp30 + alt30 + btc30 + breakout",
        f"V82 kontrol: n={v['all']['n']} PF={v['all']['pf']} ort={v['all']['mean']}%",
        f"Skor â‰¥3: n={v['cumulative']['3+']['n']} PF={v['cumulative']['3+']['pf']} ort={v['cumulative']['3+']['mean']}%",
        f"Skor 4/4: n={v['cumulative']['4+']['n']} PF={v['cumulative']['4+']['pf']} ort={v['cumulative']['4+']['mean']}%",
        "",
        f"Karar: {res['decision']}",
        "V61/V74 deÄŸiÅŸmedi; gerÃ§ek emir yok."
    ]
    return "\n".join(lines)

async def v86_run():
    state={"status":"RUNNING","error":None,"result":None,"telegram":None,
           "progress":{"stage":"load_v84_training","done":0,"total":3},
           "started_utc":utc_now(),"finished_utc":None}
    try:
        await asyncio.to_thread(v86_save,state)
        r84,_=await asyncio.to_thread(v85_load_window,"V84")
        state["progress"]={"stage":"freeze_thresholds","done":1,"total":3};await asyncio.to_thread(v86_save,state)
        thresholds=await asyncio.to_thread(v86_thresholds_from_v84,r84)

        state["progress"]={"stage":"validate_v82","done":2,"total":3};await asyncio.to_thread(v86_save,state)
        r82,_=await asyncio.to_thread(v85_load_window,"V82")
        s84,e84,c84=v86_groups(r84,thresholds)
        s82,e82,c82=v86_groups(r82,thresholds)

        high=c82["3+"]
        blocks=v86_blocks(r82,thresholds,4)
        positive_blocks=sum(1 for b in blocks if b["score_3plus"]["pf"] is not None and
                            b["score_3plus"]["pf"]>1 and
                            b["score_3plus"]["mean"] is not None and b["score_3plus"]["mean"]>0)

        # Predeclared validation gate. No retuning if it fails.
        passed=bool(high["n"]>=50 and high["pf"] is not None and high["pf"]>1.10 and
                    high["mean"] is not None and high["mean"]>0 and positive_blocks>=3)
        decision="VALIDATED_FOR_TELEGRAM_QUALITY_LABEL" if passed else "FAIL_DO_NOT_RETUNE"

        result={
            "study":"V86_FROZEN_4_FACTOR_QUALITY_VALIDATION",
            "method":{
                "training_window":"V84_OLDER_90D",
                "validation_window":"V82_LATER_60D",
                "factors":V86_FACTORS,
                "threshold_source":"MEDIAN_OF_V84_WINNERS_ONLY",
                "thresholds":{k:round(v,6) for k,v in thresholds.items()},
                "score":"0-4 count of factors >= frozen threshold",
                "high_quality_definition":"score >= 3",
                "validation_gate":"V82 n>=50, PF>1.10, mean>0, and >=3/4 chronological blocks PF>1 & mean>0",
                "retuning_allowed":False},
            "training_v84":{"all":v85_perf(s84),"exact":e84,"cumulative":c84},
            "validation_v82":{"all":v85_perf(s82),"exact":e82,"cumulative":c82,
                              "chronological_blocks":blocks,
                              "positive_score3plus_blocks":positive_blocks},
            "passed":passed,"decision":decision,
            "telegram_behavior":"SUMMARY_NOW; LIVE ENTRY LABEL ONLY AFTER PASS",
            "active_strategy_changed":False,
            "guardrails":["V61/V74 unchanged","research/paper only","no real orders",
                          "V82 validation thresholds are frozen before evaluation",
                          "if FAIL, do not retune on V82"]}
        state["result"]=result
        try:state["telegram"]=await v22_telegram_send(v86_tg_text(result))
        except Exception as te:state["telegram"]={"sent":False,"error":f"{type(te).__name__}: {te}"}
        state.update(status="DONE",progress={"stage":"done","done":3,"total":3},
                     finished_utc=utc_now())
        await asyncio.to_thread(v86_save,state)
    except Exception as e:
        state.update(status="ERROR",error=f"{type(e).__name__}: {e}",finished_utc=utc_now())
        try:await asyncio.to_thread(v86_save,state)
        except Exception:pass

@app.get("/v86-start")
async def v86_start():
    global V86_TASK
    old=await asyncio.to_thread(v86_get)
    if V86_TASK is not None and not V86_TASK.done():
        return {"status":"ALREADY_RUNNING","paper_only":True}
    if old and old.get("status")=="DONE":
        return {"status":"ALREADY_DONE","use":"/v86-status","paper_only":True}
    V86_TASK=asyncio.create_task(v86_run())
    return {"status":"STARTED","study":"V86_FROZEN_4_FACTOR_QUALITY_VALIDATION",
            "telegram_summary":True,"trading":False,"orders":False}

@app.get("/v86-status")
async def v86_status():
    s=await asyncio.to_thread(v86_get)
    return {**MODE_INFO,"status":"OK","panel":"V86_FROZEN_4_FACTOR_QUALITY_VALIDATION",
            "trading":False,"orders":False,"active_strategy_changed":False,
            "v61_unchanged":True,"v74_unchanged":True,"study":s,"generated_utc":utc_now()}

@app.get("/v86-telegram")
async def v86_telegram():
    s=await asyncio.to_thread(v86_get)
    if not s or s.get("status")!="DONE" or not s.get("result"):
        return {"status":"NOT_READY"}
    x=await v22_telegram_send(v86_tg_text(s["result"]))
    return {"status":"OK","telegram":x,"trading":False,"orders":False}

@app.on_event("startup")
async def v86_resume():
    global V86_TASK
    try:
        s=await asyncio.to_thread(v86_get)
        if s and s.get("status")=="RUNNING":
            V86_TASK=asyncio.create_task(v86_run())
    except Exception:pass


@app.get("/v87-status")
async def v87_status():
    tagged_open=[p for p in V32_STATE.get("open",{}).values() if "v86_score" in p]
    tagged_closed=[p for p in V32_STATE.get("closed",[]) if "v86_score" in p]
    return {**MODE_INFO,"status":"OK","panel":"V87_V86_FORWARD_QUALITY_TAG",
            "trading":False,"orders":False,"entry_gate":False,
            "v61_unchanged":True,"v74_unchanged":True,
            "thresholds":V87_V86_THRESHOLDS,
            "high_quality":"score >= 3/4",
            "tagged_open_count":len(tagged_open),
            "tagged_closed_count":len(tagged_closed),
            "recent_tagged_open":tagged_open[-10:],
            "recent_tagged_closed":tagged_closed[-20:][::-1],
            "generated_utc":utc_now()}

# ============================================================
# V88 â€” CAUSAL AUDIT OF V86 QUALITY SCORE
# Fixes market-context alignment: at entry OPEN, only candles with
# open_time < entry_time are allowed. No active strategy/risk changes.
# ============================================================
V88_TABLE="alt_v88_causal_audit_state"
V88_TASK=None

def v88_db_init():
    if not V21_DB_URL: raise RuntimeError("DATABASE_URL missing")
    with v21_db_connect() as conn:
        with conn.cursor() as cur:
            cur.execute(f"""CREATE TABLE IF NOT EXISTS {V88_TABLE}(
                id INTEGER PRIMARY KEY,payload JSONB NOT NULL,
                updated_at TIMESTAMPTZ DEFAULT NOW())""")
        conn.commit()

def v88_save(p):
    v88_db_init()
    with v21_db_connect() as conn:
        with conn.cursor() as cur:
            cur.execute(f"""INSERT INTO {V88_TABLE}(id,payload,updated_at)
                VALUES(1,%s::jsonb,NOW()) ON CONFLICT(id) DO UPDATE
                SET payload=EXCLUDED.payload,updated_at=NOW()""",(json.dumps(p,default=str),))
        conn.commit()

def v88_get():
    try:
        v88_db_init()
        with v21_db_connect() as conn:
            with conn.cursor() as cur:
                cur.execute(f"SELECT payload FROM {V88_TABLE} WHERE id=1")
                r=cur.fetchone(); return r[0] if r else None
    except Exception:return None

def v88_recreate_causal(store,btc,cost=.15,entry_slip=.10):
    import bisect
    btc_times=[int(x["open_time"]) for x in btc]
    btc_c=array("d",[float(x["close"]) for x in btc])
    cand=v66_candidates(store)
    out=[];busy={}
    for tm,si,i,ch,vr in cand:
        d=store[si];o,c,t=d["o"],d["c"],d["t"]
        if i+3>=len(o) or i+2>=len(c):continue
        breakout=float(c[i]);ei=i+3
        if not(c[i+1]>breakout and c[i+2]>c[i+1]):continue
        etm=int(t[ei])
        if busy.get(si,0)>etm:continue
        end=min(len(o)-1,ei+24)
        ep=float(o[ei])*(1+entry_slip/100);xp=float(o[end])
        net=((xp/ep)-1)*100-cost
        busy[si]=int(t[end])

        # CAUSAL FIX: entry happens at candle OPEN etm. The candle whose
        # open_time == etm is not completed yet, therefore it is excluded.
        bi=bisect.bisect_left(btc_times,etm)-1
        if bi<288:continue
        btc30=v77_ret(btc_c,bi,6);btc4=v77_ret(btc_c,bi,48);btc24=v77_ret(btc_c,bi,288)
        a30=[];a4=[];a24=[]
        for od in store.values():
            j=bisect.bisect_left(od["t"],etm)-1
            if j<0:continue
            for bars,bucket in ((6,a30),(48,a4),(288,a24)):
                x=v77_ret(od["c"],j,bars)
                if x is not None:bucket.append(x)
        if not a30 or not a24:continue
        alt30=sum(a30)/len(a30);alt4=sum(a4)/len(a4) if a4 else None;alt24=sum(a24)/len(a24)
        if not(btc30>0 and alt30>0):continue
        out.append({"day":datetime.fromtimestamp(etm/1000,timezone.utc).strftime("%Y-%m-%d"),
          "entry_time_ms":etm,"net":float(net),"btc30":btc30,"btc4":btc4,"btc24":btc24,
          "alt30":alt30,"alt4":alt4,"alt24":alt24,
          "disp30":statistics.pstdev(a30) if len(a30)>1 else None,
          "breakout_change_pct":float(ch),"volume_ratio":float(vr)})
    return out

def v88_load_window(which):
    if which=="V82":
        job=v82_job_get()
        if not job or job.get("status")!="DONE":raise RuntimeError("V82 DONE required")
        s=int(job["fetch_start_ms"]);e=int(job["end_ms"]);rs=int(job["report_start_ms"])
        btc=v82_cache_get("BTCUSDT",s,e);getter=lambda sym:v82_cache_get(sym,s,e);syms=list(job["symbols"])
    else:
        job=v84_get()
        if not job or job.get("status")!="DONE":raise RuntimeError("V84 DONE required")
        w=job["window"];s=int(w["fetch_start_ms"]);e=int(w["report_end_ms"]);rs=int(w["report_start_ms"])
        btc=v84_cache_get("BTCUSDT",s,e);getter=lambda sym:v84_cache_get(sym,s,e)
        old82=v82_job_get();syms=list(old82["symbols"])
    if not isinstance(btc,list):raise RuntimeError(f"{which} BTC cache missing")
    store={};failed=[]
    for sym in syms:
        cc=getter(sym)
        if isinstance(cc,list) and len(cc)>=500:store[len(store)]=v76_compact(cc)
        else:failed.append(sym)
    rows=v88_recreate_causal(store,btc,.15,.10)
    rows=[r for r in rows if rs<=int(r["entry_time_ms"])<e]
    rows.sort(key=lambda r:int(r["entry_time_ms"]))
    return rows,failed

def v88_summary(rows,t):
    scored,exact,cum=v86_groups(rows,t)
    blocks=v86_blocks(rows,t,4)
    pos=sum(1 for b in blocks if b["score_3plus"]["pf"] is not None and b["score_3plus"]["pf"]>1 and b["score_3plus"]["mean"] is not None and b["score_3plus"]["mean"]>0)
    return {"all":v85_perf(scored),"exact":exact,"cumulative":cum,"chronological_blocks":blocks,"positive_score3plus_blocks":pos}

async def v88_run():
    state={"status":"RUNNING","error":None,"result":None,"progress":{"stage":"causal_v84","done":0,"total":3},"started_utc":utc_now(),"finished_utc":None}
    try:
        await asyncio.to_thread(v88_save,state)
        r84,f84=await asyncio.to_thread(v88_load_window,"V84")
        state["progress"]={"stage":"freeze_causal_thresholds","done":1,"total":3};await asyncio.to_thread(v88_save,state)
        thresholds=await asyncio.to_thread(v86_thresholds_from_v84,r84)
        r82,f82=await asyncio.to_thread(v88_load_window,"V82")
        state["progress"]={"stage":"causal_validation_v82","done":2,"total":3};await asyncio.to_thread(v88_save,state)
        s84=v88_summary(r84,thresholds);s82=v88_summary(r82,thresholds)
        high=s82["cumulative"]["3+"]
        passed=bool(high["n"]>=50 and high["pf"] is not None and high["pf"]>1.10 and high["mean"] is not None and high["mean"]>0 and s82["positive_score3plus_blocks"]>=3)
        result={
          "study":"V88_CAUSAL_AUDIT_V86_4_FACTOR",
          "lookahead_fix":"market context uses last COMPLETED 5m candle strictly before entry_open_time",
          "breakout_factor":"V66 breakout candle is completed before two confirmation closes and is therefore known at entry",
          "training":"V84 older 90d; thresholds = medians of causal V84 winners",
          "validation":"V82 later 60d; thresholds frozen before evaluation",
          "thresholds":{k:round(v,6) for k,v in thresholds.items()},
          "training_v84":s84,"validation_v82":s82,
          "passed":passed,
          "decision":"CAUSAL_PASS_FORWARD_V66_SHADOW_ALLOWED" if passed else "CAUSAL_FAIL_RETIRE_V86_QUALITY_LABEL",
          "failed_symbols":{"V84":f84,"V82":f82},
          "active_strategy_changed":False,"entry_gate":False,"trading":False,"orders":False,
          "v61_unchanged":True,"v74_unchanged":True,
          "guardrails":["no V32 gating","no live orders","no threshold retuning on V82","current-universe survivorship bias remains"]}
        state.update(status="DONE",result=result,progress={"stage":"done","done":3,"total":3},finished_utc=utc_now())
        await asyncio.to_thread(v88_save,state)
    except Exception as ex:
        state.update(status="ERROR",error=f"{type(ex).__name__}: {ex}",finished_utc=utc_now())
        try:await asyncio.to_thread(v88_save,state)
        except Exception:pass

@app.get("/v88-start")
async def v88_start():
    global V88_TASK
    if V88_TASK is not None and not V88_TASK.done():return {"status":"ALREADY_RUNNING"}
    V88_TASK=asyncio.create_task(v88_run())
    return {**MODE_INFO,"status":"STARTED","study":"V88_CAUSAL_AUDIT_V86_4_FACTOR","trading":False,"orders":False}

@app.get("/v88-status")
async def v88_status():
    s=await asyncio.to_thread(v88_get)
    return {**MODE_INFO,"status":"OK","panel":"V88_CAUSAL_AUDIT_V86_4_FACTOR","trading":False,"orders":False,"entry_gate":False,"v61_unchanged":True,"v74_unchanged":True,"study":s,"generated_utc":utc_now()}

# ============================================================
# V89 â€” FORWARD SHADOW: V66 CONFIRM10 + V88 CAUSAL QUALITY
# PAPER/RESEARCH ONLY. No entry gate: records ALL frozen V66 CONFIRM10 Top1 entries.
# Quality score is observational only (0-2 vs 3-4). TIME120 frozen exit.
# V32/V61/V74 unchanged.
# ============================================================
V89_THRESH={"alt30":0.412776,"btc30":0.234763,"disp30":0.574416,"breakout_change_pct":0.801574}
V89_SCAN_SECONDS=60
V89_STATE={"open":[],"closed":[],"seen":[],"last_scan":None,"errors":[],"started_utc":None}
V89_TASK=None

def v89_db_init():
    if not V21_DB_URL:return False
    with v21_db_connect() as conn:
      with conn.cursor() as cur:
        cur.execute("""CREATE TABLE IF NOT EXISTS alt_v89_shadow_state(
          id INTEGER PRIMARY KEY,payload JSONB NOT NULL,updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW())""")
      conn.commit()
    return True

def v89_save():
    if not V21_DB_URL:return False
    with v21_db_connect() as conn:
      with conn.cursor() as cur:
        cur.execute("""INSERT INTO alt_v89_shadow_state(id,payload,updated_at) VALUES(1,%s::jsonb,NOW())
          ON CONFLICT(id) DO UPDATE SET payload=EXCLUDED.payload,updated_at=NOW()""",(json.dumps(V89_STATE,default=str),))
      conn.commit()
    return True

def v89_load():
    global V89_STATE
    if not V21_DB_URL:return
    v89_db_init()
    with v21_db_connect() as conn:
      with conn.cursor() as cur:
        cur.execute("SELECT payload FROM alt_v89_shadow_state WHERE id=1");r=cur.fetchone()
    if r and isinstance(r[0],dict):V89_STATE=r[0]

def v89_perf(rows):
    vals=[float(x.get("net_pct",0)) for x in rows if x.get("net_pct") is not None]
    if not vals:return {"n":0,"mean_net_pct":None,"win_rate_pct":None,"profit_factor":None}
    w=[x for x in vals if x>0];l=[x for x in vals if x<0]
    pf=(sum(w)/abs(sum(l))) if l else (999.0 if w else None)
    return {"n":len(vals),"mean_net_pct":round(sum(vals)/len(vals),4),
      "win_rate_pct":round(100*len(w)/len(vals),2),"profit_factor":round(pf,4) if pf is not None else None}

async def v89_scan_once():
    scan_utc=utc_now()
    try:
      async with httpx.AsyncClient(timeout=httpx.Timeout(30.0)) as client:
        uni=await build_universe(client);syms=[u["symbol"] for u in uni if u["symbol"]!="BTCUSDT"][:40]
        sem=asyncio.Semaphore(12)
        async def one(sym):
          async with sem:
            try:return sym,await asyncio.wait_for(v74_candles(client,sym,90),25),None
            except Exception as e:return sym,None,f"{type(e).__name__}: {e}"
        vals=await asyncio.gather(*[one(x) for x in syms])
        data={s:c for s,c,e in vals if c and len(c)>=55};errs=[{"symbol":s,"error":e} for s,c,e in vals if e]
        btc=await v74_candles(client,"BTCUSDT",90)
        if len(btc)<55:raise RuntimeError("BTC candles insufficient")

        # Frozen TIME120 exit using live BID.
        now=alt_now_ms();still=[]
        for p in V89_STATE.get("open",[]):
          if now<int(p["exit_due_ms"]):still.append(p);continue
          q=await live_quote(client,p["symbol"]);xp=float(q["bid"]) if q else None
          if xp is None:
            cc=data.get(p["symbol"],[]);xp=float(cc[-1]["close"]) if cc else float(p["entry_price"])
          gross=v74_pct(float(p["entry_price"]),xp);net=gross-0.15
          p.update(exit_price=xp,exit_utc=utc_now(),exit_reason="TIME120",gross_pct=round(gross,4),cost_pct=0.15,net_pct=round(net,4),status="CLOSED_SHADOW")
          V89_STATE.setdefault("closed",[]).append(p)
          await alt_safe_send(f"ALT V89 SHADOW CIKIS\nCoin: {p['symbol']}\nKalite: {p['v88_score']}/4 - {p['v88_label']}\nNet: {p['net_pct']}%\nCikis: TIME120\nPAPER/GOZLEMSEL - GERCEK EMIR YOK","V89_EXIT",p["symbol"])
        V89_STATE["open"]=still;V89_STATE["closed"]=V89_STATE.get("closed",[])[-1000:]

        # Causal market context: all candles here are completed before live entry.
        b30=v74_pct(btc[-7]["close"],btc[-1]["close"])
        a30=[v74_pct(c[-7]["close"],c[-1]["close"]) for c in data.values() if len(c)>=7]
        alt30=(sum(a30)/len(a30)) if a30 else -999
        disp30=statistics.pstdev(a30) if len(a30)>=2 else -999

        # Frozen V66 CONFIRM10 Top1 candidate: breakout + two completed rising closes.
        candidates=[]
        for sym,c in data.items():
          if len(c)<25:continue
          i=len(c)-3;bo=c[i];ch=v74_pct(bo["open"],bo["close"]);av=sum(x["volume"] for x in c[i-20:i])/20;vr=(bo["volume"]/av) if av>0 else 0
          if ch<0.50 or vr<1.20:continue
          if not(c[i+1]["close"]>bo["close"] and c[i+2]["close"]>c[i+1]["close"]):continue
          candidates.append({"symbol":sym,"breakout_time":bo["open_time"],"breakout_change_pct":ch,"volume_ratio":vr,"breakout_close":bo["close"],"confirm2_close":c[i+2]["close"]})
        candidates.sort(key=lambda x:(x["breakout_change_pct"],x["volume_ratio"]),reverse=True)
        accepted=None
        if candidates:
          e=candidates[0];key=f'{e["symbol"]}:{e["breakout_time"]}';seen=set(V89_STATE.get("seen",[]));already=any(p["symbol"]==e["symbol"] for p in V89_STATE.get("open",[]))
          if key not in seen and not already:
            q=await live_quote(client,e["symbol"])
            if q and q["spread_pct"]<=SPREAD_MAX_PCT:
              factors={"alt30":alt30,"btc30":b30,"disp30":disp30,"breakout_change_pct":float(e["breakout_change_pct"])}
              passed={k:(factors[k]>=V89_THRESH[k]) for k in V89_THRESH};score=sum(1 for x in passed.values() if x);label="YUKSEK" if score>=3 else ("ORTA" if score==2 else "DUSUK")
              ep=float(q["ask"]);entry_ms=alt_now_ms();p={**e,"entry_price":ep,"entry_utc":utc_now(),"entry_ms":entry_ms,"exit_due_ms":entry_ms+120*60*1000,"status":"OPEN_SHADOW","execution_version":"V89_V66_CONFIRM10_CAUSAL_QUALITY_TIME120","spread_pct":round(float(q["spread_pct"]),4),"alt30_pct":round(alt30,6),"btc30_pct":round(b30,6),"disp30_pct":round(disp30,6),"v88_score":score,"v88_label":label,"v88_factor_pass":passed,"entry_gate":False}
              V89_STATE.setdefault("open",[]).append(p);accepted=p
              await alt_safe_send(f"ALT V89 SHADOW GIRIS\nCoin: {e['symbol']}\nV88 Kalite: {score}/4 - {label} (gozlemsel)\nBreakout: {e['breakout_change_pct']:.3f}% | Vol: {e['volume_ratio']:.2f}x\nALT30: {alt30:.3f}% | BTC30: {b30:.3f}% | Disp30: {disp30:.3f}\nGiris ask: {ep}\nTIME120 | PAPER - GERCEK EMIR YOK","V89_ENTRY",e["symbol"])
            seen.add(key);V89_STATE["seen"]=list(seen)[-2000:]
        V89_STATE["last_scan"]={"utc":scan_utc,"symbols":len(data),"errors":len(errs),"confirm10_candidates":len(candidates),"accepted":accepted["symbol"] if accepted else None,"alt30_pct":round(alt30,4),"btc30_pct":round(b30,4),"disp30_pct":round(disp30,4)}
        V89_STATE["errors"]=errs[-20:];v89_save();return V89_STATE["last_scan"]
    except Exception as e:
      V89_STATE["last_scan"]={"utc":scan_utc,"error":f"{type(e).__name__}: {e}"}
      try:v89_save()
      except Exception:pass
      return V89_STATE["last_scan"]

async def v89_loop():
    while True:
      try:await v89_scan_once()
      except Exception:pass
      await asyncio.sleep(V89_SCAN_SECONDS)

@app.on_event("startup")
async def v89_startup():
    global V89_TASK
    try:v89_load()
    except Exception:pass
    if not V89_STATE.get("started_utc"):V89_STATE["started_utc"]=utc_now()
    if V89_TASK is None:V89_TASK=asyncio.create_task(v89_loop())

@app.get("/v89-scan-now")
async def v89_scan_now():return {"status":"OK","paper_only":True,"scan":await v89_scan_once()}

@app.get("/v89-status")
async def v89_status():
    cl=V89_STATE.get("closed",[]);low=[x for x in cl if int(x.get("v88_score",-1))<=2];high=[x for x in cl if int(x.get("v88_score",-1))>=3]
    return {**MODE_INFO,"status":"OK","panel":"V89_FORWARD_V66_CAUSAL_QUALITY_SHADOW","trading":False,"orders":False,"entry_gate":False,"active_strategy_changed":False,"v32_unchanged":True,"v61_unchanged":True,"v74_unchanged":True,"frozen_entry":"V66_CONFIRM_10M_TOP1","frozen_exit":"TIME120","quality_thresholds":V89_THRESH,"quality_use":"OBSERVATIONAL_ONLY_NOT_ENTRY_GATE","last_scan":V89_STATE.get("last_scan"),"open_count":len(V89_STATE.get("open",[])),"closed_count":len(cl),"performance":{"all":v89_perf(cl),"score_0_2":v89_perf(low),"score_3_4":v89_perf(high)},"open":V89_STATE.get("open",[])[-20:],"recent_closed":cl[-30:],"errors":V89_STATE.get("errors",[])[-10:],"started_utc":V89_STATE.get("started_utc"),"generated_utc":utc_now()}

# V90 - READ-ONLY COUNTERFACTUAL EXIT RESEARCH FOR V89 CLOSED SHADOWS
# Does not mutate V89 state or any paper strategy.
V90_HORIZONS = (30, 60, 90, 120)


def v90_stats(values):
    if not values:
        return {"n": 0, "mean_net_pct": None, "win_rate_pct": None, "profit_factor": None}
    w = [v for v in values if v > 0]
    l = [v for v in values if v < 0]
    pf = sum(w) / abs(sum(l)) if l else (None if w else 0.0)
    return {"n": len(values), "mean_net_pct": round(sum(values)/len(values), 4),
            "win_rate_pct": round(100*len(w)/len(values), 2),
            "profit_factor": round(pf, 4) if pf is not None else None}


def v90_replay(trade, raw):
    entry_ms = int(trade["entry_ms"])
    entry = float(trade["entry_price"])
    # First fully observed minute strictly after live entry. The partial entry
    # minute is intentionally excluded (unknown pre/post entry price ordering).
    first_open = ((entry_ms // 60000) + 1) * 60000
    candles = [k for k in raw if first_open <= int(k[0]) and int(k[6]) <= entry_ms + 120*60000]
    candles.sort(key=lambda k: int(k[0]))
    if not candles:
        return None, "NO_COMPLETE_POST_ENTRY_CANDLES"
    expected = first_open
    for k in candles:
        if int(k[0]) != expected:
            return None, "MISSING_1M_CANDLE"
        expected += 60000
    if len(candles) < 118:
        return None, "INSUFFICIENT_1M_CANDLES"
    cost = 0.15
    results = {}
    for mins in V90_HORIZONS:
        target = entry_ms + mins*60000
        # Candle open at/after requested duration; up to 1m late.
        after = next((k for k in raw if int(k[0]) >= target), None)
        if after is None or int(after[0]) > target + 60000:
            return None, "MISSING_TIME_EXIT_OPEN"
        results[f"TIME{mins}"] = (float(after[1])/entry - 1)*100 - cost
    stop_level = entry*0.97
    trailing_active = False
    peak = entry
    stop_out = None
    trailing_out = None
    for k in candles:
        op, hi, lo = float(k[1]), float(k[2]), float(k[3])
        if stop_out is None and lo <= stop_level:
            stop_out = (min(op, stop_level)/entry - 1)*100 - cost
        if trailing_out is None:
            # Conservative intrabar ordering: check previously established
            # trailing stop before crediting this minute's new high.
            if trailing_active:
                level = peak*0.985
                if lo <= level:
                    trailing_out = (min(op, level)/entry - 1)*100 - cost
            if trailing_out is None:
                peak = max(peak, hi)
                if peak >= entry*1.02:
                    trailing_active = True
    results["STOP3_OR_TIME120"] = stop_out if stop_out is not None else results["TIME120"]
    results["TRAIL_2_1_5_OR_TIME120"] = trailing_out if trailing_out is not None else results["TIME120"]

    # V90 research-only addition: identical trailing rules, but TIME90 fallback.
    # Only fully completed 1m candles before the 90m deadline are eligible.
    trailing90_active = False
    peak90 = entry
    trailing90_out = None
    deadline90 = entry_ms + 90*60000
    for k in candles:
        if int(k[6]) >= deadline90:
            break
        op, hi, lo = float(k[1]), float(k[2]), float(k[3])
        if trailing90_active:
            level = peak90*0.985
            if lo <= level:
                trailing90_out = (min(op, level)/entry - 1)*100 - cost
                break
        peak90 = max(peak90, hi)
        if peak90 >= entry*1.02:
            trailing90_active = True
    results["TRAIL_2_1_5_OR_TIME90"] = (
        trailing90_out if trailing90_out is not None else results["TIME90"]
    )
    return results, None


@app.get("/v90-exit-study")
async def v90_exit_study(limit: int = Query(default=100, ge=1, le=200)):
    # Snapshot, read-only: do not touch any live/paper state.
    trades = [dict(x) for x in V89_STATE.get("closed", []) if x.get("entry_ms") and x.get("entry_price")]
    trades = trades[-limit:]
    semaphore = asyncio.Semaphore(4)
    async with httpx.AsyncClient(timeout=httpx.Timeout(35.0)) as client:
        async def one(t):
            async with semaphore:
                try:
                    start = int(t["entry_ms"])
                    raw = await get_json(client, "/api/v3/klines", {
                        "symbol": t["symbol"], "interval": "1m",
                        "startTime": start - 60000,
                        "endTime": start + 122*60000,
                        "limit": 150})
                    r, err = v90_replay(t, raw)
                    return t, r, err
                except Exception as exc:
                    return t, None, f"{type(exc).__name__}: {str(exc)[:160]}"
        rows = await asyncio.gather(*(one(t) for t in trades))
    good = [(t, r) for t, r, err in rows if r is not None]
    failed = [{"symbol": t["symbol"], "entry_utc": t.get("entry_utc"), "error": err}
              for t, r, err in rows if r is None]
    keys = [f"TIME{m}" for m in V90_HORIZONS] + ["STOP3_OR_TIME120", "TRAIL_2_1_5_OR_TIME120", "TRAIL_2_1_5_OR_TIME90"]
    def group(items):
        return {key: v90_stats([r[key] for _, r in items]) for key in keys}
    return {**MODE_INFO, "status": "OK" if not failed else "PARTIAL",
            "panel": "V90_READ_ONLY_V89_EXIT_COUNTERFACTUAL",
            "no_state_mutations": True, "real_orders": False,
            "source_closed_count": len(V89_STATE.get("closed", [])),
            "requested": len(trades), "analyzed": len(good), "failed_count": len(failed),
            "cost_pct": 0.15,
            "all": group(good),
            "score_0_2": group([(t,r) for t,r in good if int(t.get("v88_score", -1)) <= 2]),
            "score_3_4": group([(t,r) for t,r in good if int(t.get("v88_score", -1)) >= 3]),
            "per_trade": [{"symbol": t["symbol"], "entry_utc": t.get("entry_utc"),
                           "score": t.get("v88_score"), "actual_v89_net_pct": t.get("net_pct"),
                           "counterfactual": {k: round(v,4) for k,v in r.items()}}
                          for t,r in good],
            "failures": failed,
            "limitations": [
                "Historical counterfactuals, not forward-executed exits.",
                "Entry minute excluded because the pre/post-entry sequence is unknown; early stops may be missed.",
                "1m OHLC has no intraminute event ordering; trailing stop assumes prior peak and conservative gap handling.",
                "Stops may fill worse than modeled in real markets; fixed 0.15% costs exclude variable slippage.",
                "TIME120 replay uses 1m candle OPEN, while V89 uses live BID around 120m; differences are expected.",
                "Only already-closed V89 positions are studied; selection and market-regime biases remain."
            ], "generated_utc": utc_now()}


# ============================================================
# V91: FORWARD, PAPER-ONLY PAIRED 60m vs FROZEN V89 120m
# New V89 entries only, no edits to V89 / V32 / V61 / V74.
# Persisted independently in Postgres when configured.
# ============================================================
V91_STATE = {"started_ms": None, "trades": {}, "errors": [], "last_check_utc": None}
V91_TASK = None
V91_LOCK = None
V91_POLL_SECONDS = 15
V91_MAX_EXIT_DELAY_MS = 90_000


def v91_key(p):
    return f'{p["symbol"]}:{int(p["entry_ms"])}'


def v91_db_init():
    if not V21_DB_URL:
        return False
    with v21_db_connect() as conn:
        with conn.cursor() as cur:
            cur.execute("""CREATE TABLE IF NOT EXISTS alt_v91_forward_state (
                id INTEGER PRIMARY KEY, payload JSONB NOT NULL,
                updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW())""")
        conn.commit()
    return True


def v91_save():
    if not V21_DB_URL:
        return False
    with v21_db_connect() as conn:
        with conn.cursor() as cur:
            cur.execute("""INSERT INTO alt_v91_forward_state(id,payload,updated_at)
                VALUES(1,%s::jsonb,NOW()) ON CONFLICT(id)
                DO UPDATE SET payload=EXCLUDED.payload,updated_at=NOW()""",
                (json.dumps(V91_STATE,default=str),))
        conn.commit()
    return True


def v91_load():
    global V91_STATE
    if not V21_DB_URL:
        return
    v91_db_init()
    with v21_db_connect() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT payload FROM alt_v91_forward_state WHERE id=1")
            r=cur.fetchone()
    if r and isinstance(r[0],dict):
        V91_STATE=r[0]


async def v91_check_once():
    global V91_LOCK
    if V91_LOCK is None:
        V91_LOCK=asyncio.Lock()
    async with V91_LOCK:
        now=alt_now_ms()
        V91_STATE["last_check_utc"]=utc_now()
        trades=V91_STATE.setdefault("trades",{})
        errors=V91_STATE.setdefault("errors",[])
        started=int(V91_STATE["started_ms"])
        # Snapshot only. V89 is never mutated.
        v89_open=list(V89_STATE.get("open",[]))
        v89_closed=list(V89_STATE.get("closed",[]))
        for p in v89_open+v89_closed:
            try:
                entry_ms=int(p["entry_ms"])
                if entry_ms < started:
                    continue
                key=v91_key(p)
                if key not in trades:
                    trades[key]={"symbol":p["symbol"],"entry_ms":entry_ms,
                        "entry_utc":p.get("entry_utc"),"entry_price":float(p["entry_price"]),
                        "score":p.get("v88_score"),"due60_ms":entry_ms+60*60*1000,
                        "status60":"WAITING", "exit60":None,"exit120":None}
            except Exception as exc:
                errors.append(f"register: {type(exc).__name__}: {str(exc)[:120]}")
        closed_map={v91_key(p):p for p in v89_closed if p.get("entry_ms")}
        due=[]
        for key,t in trades.items():
            if t["status60"]=="WAITING" and now>=t["due60_ms"]:
                if now-t["due60_ms"]>V91_MAX_EXIT_DELAY_MS:
                    t["status60"]="MISSED_WINDOW"
                    t["missed_by_seconds"]=round((now-t["due60_ms"])/1000,1)
                else:
                    due.append((key,t))
        if due:
            async with httpx.AsyncClient(timeout=httpx.Timeout(15.0)) as client:
                for key,t in due:
                    try:
                        q=await live_quote(client,t["symbol"])
                        if not q or not q.get("bid"):
                            raise ValueError("LIVE_BID_UNAVAILABLE")
                        bid=float(q["bid"])
                        sampled_ms=alt_now_ms()
                        if sampled_ms-t["due60_ms"]>V91_MAX_EXIT_DELAY_MS:
                            t["status60"]="MISSED_WINDOW"
                            continue
                        t["exit60"]={"bid":bid,"sampled_ms":sampled_ms,
                            "sampled_utc":utc_now(),"delay_seconds":round((sampled_ms-t["due60_ms"])/1000,2),
                            "net_pct":round((bid/t["entry_price"]-1)*100-0.15,4)}
                        t["status60"]="CAPTURED"
                    except Exception as exc:
                        errors.append(f'{t["symbol"]} 60m: {type(exc).__name__}: {str(exc)[:120]}')
        for key,t in trades.items():
            p=closed_map.get(key)
            if p and t["exit120"] is None and p.get("net_pct") is not None:
                t["exit120"]={"net_pct":float(p["net_pct"]),
                    "exit_utc":p.get("exit_utc"),"exit_price":p.get("exit_price"),
                    "source":"V89_ACTUAL_PAPER_TIME120"}
        V91_STATE["errors"]=errors[-30:]
        # Bound storage while keeping paired results long enough for evaluation.
        if len(trades)>3000:
            old=sorted(trades, key=lambda k:trades[k]["entry_ms"])[:-3000]
            for k in old:del trades[k]
        try:v91_save()
        except Exception as exc:
            V91_STATE["errors"].append(f"DB_SAVE: {type(exc).__name__}: {str(exc)[:120]}")
        return {"tracked":len(trades),"captured60":sum(t["status60"]=="CAPTURED" for t in trades.values())}


async def v91_loop():
    while True:
        try:await v91_check_once()
        except Exception as exc:
            V91_STATE.setdefault("errors",[]).append(f"LOOP: {type(exc).__name__}: {str(exc)[:120]}")
        await asyncio.sleep(V91_POLL_SECONDS)


@app.on_event("startup")
async def v91_startup():
    global V91_TASK
    try:v91_load()
    except Exception as exc:
        V91_STATE.setdefault("errors",[]).append(f"LOAD: {type(exc).__name__}: {str(exc)[:120]}")
    if V91_STATE.get("started_ms") is None:
        V91_STATE["started_ms"]=alt_now_ms()
        try:v91_save()
        except Exception:pass
    if V91_TASK is None:
        V91_TASK=asyncio.create_task(v91_loop())


@app.get("/v91-status")
async def v91_status():
    trades=sorted(V91_STATE.get("trades",{}).values(),key=lambda x:x["entry_ms"])
    paired=[t for t in trades if t.get("exit60") and t.get("exit120")]
    def group(rows):
        a=[float(t["exit60"]["net_pct"]) for t in rows]
        b=[float(t["exit120"]["net_pct"]) for t in rows]
        return {"n":len(rows),"TIME60":v90_stats(a),"TIME120_V89":v90_stats(b),
            "mean_60_minus_120_pct":round(sum(x-y for x,y in zip(a,b))/len(rows),4) if rows else None,
            "time60_better_count":sum(x>y for x,y in zip(a,b))}
    return {**MODE_INFO,"status":"OK","panel":"V91_FORWARD_PAIRED_TIME60_VS_V89_TIME120",
        "paper_only":True,"real_orders":False,"v89_unchanged":True,
        "new_entries_only":True,"started_ms":V91_STATE.get("started_ms"),
        "tracked_count":len(trades),"waiting_60_count":sum(t["status60"]=="WAITING" for t in trades),
        "missed_60_count":sum(t["status60"]=="MISSED_WINDOW" for t in trades),
        "paired_count":len(paired),"all":group(paired),
        "score_0_2":group([t for t in paired if t.get("score") is not None and int(t["score"])<=2]),
        "score_3_4":group([t for t in paired if t.get("score") is not None and int(t["score"])>=3]),
        "recent":trades[-30:],"errors":V91_STATE.get("errors",[])[-10:],
        "last_check_utc":V91_STATE.get("last_check_utc"),"generated_utc":utc_now(),
        "limitations":["Only entries created after V91 activation are included.",
          "60m uses observed live BID within 90 seconds of target; missed windows are excluded.",
          "120m uses V89 actual paper exit BID, not an independent execution.",
          "Poll/deployment outages may cause missing observations.",
          "0.15 percent fixed roundtrip cost; no variable slippage or real orders."]}


# V92 - READ-ONLY FAILED-RALLY DIAGNOSTICS (V89 CLOSED PAPER TRADES)
# Descriptive analysis only; no new filters, signals, orders or strategy mutations.
V92_FEATURES = (
    ("breakout_change_pct", "breakout_change_pct"),
    ("alt30_pct", "alt30_pct"),
    ("btc30_pct", "btc30_pct"),
    ("disp30_pct", "disp30_pct"),
    ("spread_pct", "spread_pct"),
    ("v88_score", "v88_score"),
    ("volume_ratio", "volume_ratio"),
)


def v92_numeric(value):
    try:
        n = float(value)
        return n if math.isfinite(n) else None
    except (ValueError, TypeError, OverflowError):
        return None


def v92_summary(trades):
    vals = [v92_numeric(t.get("net_pct")) for t in trades]
    vals = [x for x in vals if x is not None]
    out = v90_stats(vals)
    out["losing_count"] = sum(x < 0 for x in vals)
    out["flat_count"] = sum(x == 0 for x in vals)
    out["loss_le_minus3_count"] = sum(x <= -3 for x in vals)
    return out


@app.get("/v92-failed-rally-study")
async def v92_failed_rally_study(limit: int = Query(default=1000, ge=1, le=1000)):
    # Snapshot existing V89 closed trades only. No calls to Binance, no writes.
    rows = [dict(t) for t in V89_STATE.get("closed", [])
            if v92_numeric(t.get("net_pct")) is not None][-limit:]
    winners = [t for t in rows if float(t["net_pct"]) > 0]
    losers = [t for t in rows if float(t["net_pct"]) < 0]
    feature_results = {}
    for label, key in V92_FEATURES:
        valid = [(v92_numeric(t.get(key)), t) for t in rows]
        valid = [(v, t) for v, t in valid if v is not None]
        valid.sort(key=lambda item: item[0])
        if not valid:
            feature_results[label] = {"available_n": 0, "missing_n": len(rows)}
            continue
        mid = len(valid) // 2
        low = [t for _, t in valid[:mid]]
        high = [t for _, t in valid[mid:]]
        # Median split is exploratory, including ties; NOT an entry rule.
        win_vals = sorted(v for v,t in valid if float(t["net_pct"]) > 0)
        lose_vals = sorted(v for v,t in valid if float(t["net_pct"]) < 0)
        def median(vals):
            n=len(vals)
            return round((vals[(n-1)//2]+vals[n//2])/2,6) if n else None
        feature_results[label] = {
            "available_n": len(valid), "missing_n": len(rows)-len(valid),
            "median_all": median([v for v,_ in valid]),
            "median_winners": median(win_vals),
            "median_losers": median(lose_vals),
            "lower_half": v92_summary(low), "upper_half": v92_summary(high),
            "split_method": "ranked_halves_exploratory_ties_possible"
        }
    return {**MODE_INFO, "status": "OK", "panel": "V92_FAILED_RALLY_DIAGNOSTICS",
        "paper_only": True, "real_orders": False, "read_only": True,
        "v89_unchanged": True, "v91_unchanged": True,
        "source": "V89_CLOSED_SHADOWS", "closed_n": len(rows),
        "all": v92_summary(rows), "winners": v92_summary(winners),
        "losers": v92_summary(losers),
        "features": feature_results,
        "worst_10": [{"symbol":t.get("symbol"),"entry_utc":t.get("entry_utc"),
            "net_pct":t.get("net_pct"),"score":t.get("v88_score")}
            for t in sorted(rows,key=lambda t:float(t["net_pct"]))[:10]],
        "limitations": [
            "Descriptive in-sample comparisons; no causal inference or validated predictive power.",
            "Features reflect the existing V89 selected-entry population, not all altcoin candidates.",
            "Only features recorded at entry can be studied; missing volume ratio is reported explicitly.",
            "Small groups, multiple comparisons and outliers can create misleading apparent effects.",
            "No entry filters are proposed or applied; independent out-of-sample validation is required."
        ], "generated_utc": utc_now()}


# V93 â€” independent prospective observational cohort; NO V89/V91 mutation.
V93_THRESHOLDS = {"alt30_pct": 0.175735, "disp30_pct": 1.118592}
V93_ACTIVATION_MS = None
V93_ACTIVATION_ERROR = None

def v93_init():
    global V93_ACTIVATION_MS, V93_ACTIVATION_ERROR
    try:
        if not V21_DB_URL:
            raise RuntimeError("Persistent database is not configured")
        with v21_db_connect() as conn:
            with conn.cursor() as cur:
                cur.execute("""CREATE TABLE IF NOT EXISTS alt_v93_cohort_state (
                    id INTEGER PRIMARY KEY, started_ms BIGINT NOT NULL)""")
                cur.execute("SELECT started_ms FROM alt_v93_cohort_state WHERE id=1")
                row = cur.fetchone()
                if row:
                    V93_ACTIVATION_MS = int(row[0])
                else:
                    now = alt_now_ms()
                    cur.execute("""INSERT INTO alt_v93_cohort_state(id,started_ms)
                        VALUES(1,%s) ON CONFLICT(id) DO NOTHING""", (now,))
                    cur.execute("SELECT started_ms FROM alt_v93_cohort_state WHERE id=1")
                    V93_ACTIVATION_MS = int(cur.fetchone()[0])
            conn.commit()
        V93_ACTIVATION_ERROR = None
    except Exception as exc:
        V93_ACTIVATION_ERROR = f"{type(exc).__name__}: {str(exc)[:200]}"

@app.on_event("startup")
async def v93_startup():
    v93_init()

@app.get("/v93-market-euphoria-study")
async def v93_market_euphoria_study():
    if V93_ACTIVATION_MS is None:
        return {**MODE_INFO, "status": "NOT_READY",
            "panel": "V93_PROSPECTIVE_MARKET_EUPHORIA",
            "error": V93_ACTIVATION_ERROR or "V93 activation timestamp not persisted",
            "paper_only": True, "real_orders": False}
    rows = [dict(t) for t in V89_STATE.get("closed", [])
        if v92_numeric(t.get("net_pct")) is not None
        and int(t.get("entry_ms") or 0) >= V93_ACTIVATION_MS]
    def stats(rr):
        return v92_summary(rr)
    def feature_split(field):
        threshold = V93_THRESHOLDS[field]
        valid = [(v92_numeric(t.get(field)), t) for t in rows]
        valid = [(v,t) for v,t in valid if v is not None]
        low = [t for v,t in valid if v < threshold]
        high = [t for v,t in valid if v >= threshold]
        return {"fixed_threshold": threshold, "available_n": len(valid),
            "missing_n": len(rows)-len(valid),
            "below": stats(low), "at_or_above": stats(high)}
    both = {"low_alt_low_disp": [], "low_alt_high_disp": [],
        "high_alt_low_disp": [], "high_alt_high_disp": []}
    missing = 0
    for t in rows:
        a = v92_numeric(t.get("alt30_pct"))
        d = v92_numeric(t.get("disp30_pct"))
        if a is None or d is None:
            missing += 1
            continue
        key = ("high_alt" if a >= V93_THRESHOLDS["alt30_pct"] else "low_alt")
        key += ("_high_disp" if d >= V93_THRESHOLDS["disp30_pct"] else "_low_disp")
        both[key].append(t)
    return {**MODE_INFO, "status": "OK",
        "panel": "V93_PROSPECTIVE_MARKET_EUPHORIA",
        "paper_only": True, "real_orders": False, "read_only": True,
        "v89_unchanged": True, "v91_unchanged": True,
        "new_entries_only": True, "activation_ms": V93_ACTIVATION_MS,
        "source": "V89_CLOSED_AFTER_V93_ACTIVATION",
        "fixed_thresholds_from_v92_25_trade_exploration": V93_THRESHOLDS,
        "closed_n": len(rows), "all": stats(rows),
        "alt30": feature_split("alt30_pct"),
        "disp30": feature_split("disp30_pct"),
        "joint_groups": {k: stats(v) for k,v in both.items()},
        "joint_missing_n": missing,
        "recent": [{"symbol": t.get("symbol"), "entry_utc": t.get("entry_utc"),
            "net_pct": t.get("net_pct"), "alt30_pct": t.get("alt30_pct"),
            "disp30_pct": t.get("disp30_pct")} for t in rows[-20:]],
        "limitations": [
            "Prospective observational cohort, not randomized; association is not causation.",
            "Fixed thresholds were selected after viewing V92 data and require independent validation.",
            "Only V89-selected entries are included, not all candidates.",
            "Correlated alt30/disp30 and small subgroup sizes can mislead.",
            "Closed trades only; open trades are excluded until V89 exit.",
            "No trading rules, entry gates or live orders are changed."
        ], "generated_utc": utc_now()}


# V32 GECMIS ISLEMLER - SADECE OKUMA

from fastapi import Header, HTTPException
import os

@app.get("/alt-v32-trades-export")
async def alt_v32_trades_export(
    x_api_key: str = Header(default="")
):
    expected_key = os.environ.get("V32_EXPORT_API_KEY")

    if not expected_key or x_api_key != expected_key:
        raise HTTPException(
            status_code=401,
            detail="Unauthorized"
        )

    trades = V32_STATE.get("closed", [])

    return {
        **MODE_INFO,
        "status": "OK",
        "strategy": "V32",
        "trade_count": len(trades),
        "trades": trades,
        "generated_utc": utc_now(),
    }



# ============================================================
# V94 â€” V62 SPREAD/VOLUME SHADOW COHORT (V32 PAPER ENTRIES)
# Observational only. Does not gate V32, V55, V87, V89 or V91.
# Frozen thresholds, prospective new entries only; no real orders.
# ============================================================
V94_SPREAD_MAX_PCT = 0.10
V94_VOL_MIN = 1.0
V94_VOL_MAX_EXCLUSIVE = 5.0

def v94_candidate_tag(candles, entry_open_ms, spread_pct, entry_live_ms):
    """Tag a V32 paper entry with an independently auditable, causal filter.

    Use only fully closed candles at the intended entry-open timestamp.
    Missing spread or absent live fill reference fails closed for the shadow
    cohort; this never blocks the baseline V32 paper position.
    """
    tag = {
        "v94_version": "V94_V62_SPREAD_VOLUME_OBSERVATIONAL",
        "v94_observational_only": True,
        "v94_spread_max_pct": V94_SPREAD_MAX_PCT,
        "v94_vol_min": V94_VOL_MIN,
        "v94_vol_max_exclusive": V94_VOL_MAX_EXCLUSIVE,
        "v94_shadow_pass": False,
        "v94_reason": "MISSING_DATA",
        "v94_vol_ratio": None,
    }
    try:
        entry_ms = int(entry_open_ms)
        # Reproduce V61 volume windows using only fully completed bars.
        prior = sorted(
            (c for c in (candles or [])
             if int(c.get("open_time", entry_ms)) < entry_ms
             and int(c.get("close_time", entry_ms)) < entry_ms),
            key=lambda c: int(c["open_time"]),
        )
        if len(prior) < 60:
            tag["v94_reason"] = "INSUFFICIENT_CLOSED_CANDLES"
            return tag
        recent = prior[-3:]
        baseline = prior[-51:-3]
        v_base = sum(float(c["volume"]) for c in baseline) / len(baseline)
        if v_base <= 0:
            tag["v94_reason"] = "INVALID_BASE_VOLUME"
            return tag
        ratio = (sum(float(c["volume"]) for c in recent) / 3.0) / v_base
        tag["v94_vol_ratio"] = round(ratio, 6)
        if spread_pct is None or entry_live_ms is None:
            tag["v94_reason"] = "MISSING_LIVE_SPREAD_OR_ENTRY"
            return tag
        spread = float(spread_pct)
        if not math.isfinite(spread) or not math.isfinite(ratio):
            tag["v94_reason"] = "NONFINITE_VALUE"
            return tag
        if spread > V94_SPREAD_MAX_PCT:
            tag["v94_reason"] = "SPREAD_GT_0_10"
        elif ratio < V94_VOL_MIN:
            tag["v94_reason"] = "VOLUME_LT_1"
        elif ratio >= V94_VOL_MAX_EXCLUSIVE:
            tag["v94_reason"] = "VOLUME_GTE_5"
        else:
            tag["v94_shadow_pass"] = True
            tag["v94_reason"] = "PASS"
        return tag
    except (TypeError, ValueError, KeyError, ZeroDivisionError) as exc:
        tag["v94_reason"] = "TAG_ERROR_" + type(exc).__name__
        return tag


@app.get("/v94-spread-volume-shadow")
async def v94_spread_volume_shadow():
    """Read-only matched V32 paper outcomes; no independent shadow fills."""
    opened = [p for p in V32_STATE.get("open", {}).values()
              if p.get("v94_version") == "V94_V62_SPREAD_VOLUME_OBSERVATIONAL"]
    closed = [p for p in V32_STATE.get("closed", [])
              if p.get("v94_version") == "V94_V62_SPREAD_VOLUME_OBSERVATIONAL"]
    def stats(rows):
        values = [float(p["net_pct"]) for p in rows
                  if p.get("net_pct") is not None]
        if not values:
            return {"n": 0, "mean_net_pct": None,
                    "win_rate_pct": None, "profit_factor": None}
        wins = [v for v in values if v > 0]
        losses = [v for v in values if v < 0]
        pf = sum(wins) / abs(sum(losses)) if losses else None
        return {"n": len(values), "mean_net_pct": round(sum(values)/len(values),4),
                "win_rate_pct": round(100*len(wins)/len(values),2),
                "profit_factor": round(pf,4) if pf is not None else None}
    passed = [p for p in closed if p.get("v94_shadow_pass") is True]
    rejected = [p for p in closed if p.get("v94_shadow_pass") is not True]
    reasons = {}
    for p in opened + closed:
        reason = p.get("v94_reason", "UNKNOWN")
        reasons[reason] = reasons.get(reason, 0) + 1
    return {**MODE_INFO, "status": "OK",
            "panel": "V94_V62_SPREAD_VOLUME_SHADOW",
            "research_only": True, "read_only": True,
            "entry_gate": False, "real_orders": False,
            "v32_entry_exit_unchanged": True, "v55_risk_unchanged": True,
            "v87_v89_v91_unchanged": True,
            "new_entries_only": True,
            "thresholds": {"spread_max_pct": V94_SPREAD_MAX_PCT,
                           "vol_ratio_min": V94_VOL_MIN,
                           "vol_ratio_max_exclusive": V94_VOL_MAX_EXCLUSIVE},
            "tagged_open_n": len(opened), "tagged_closed_n": len(closed),
            "closed_all_baseline": stats(closed),
            "closed_shadow_pass": stats(passed),
            "closed_shadow_reject": stats(rejected),
            "reason_counts_open_and_closed": reasons,
            "recent_closed": [{"key": p.get("key"), "symbol": p.get("symbol"),
                               "net_pct": p.get("net_pct"),
                               "shadow_pass": p.get("v94_shadow_pass"),
                               "reason": p.get("v94_reason"),
                               "spread_pct": p.get("spread_pct"),
                               "vol_ratio": p.get("v94_vol_ratio")}
                              for p in closed[-20:]],
            "limitations": [
                "Matched subset of V32 paper entries, not an independently executed portfolio.",
                "V32 cooldown and capacity rules remain unchanged; rejected shadow candidates may affect later opportunities.",
                "Original 25-trade filter result was in-sample and needs independent forward validation.",
                "No entry is blocked; baseline V32 paper positions and stops are unchanged.",
                "No live quote or live entry reference means shadow filter fails closed."
            ], "generated_utc": utc_now()}


# ============================================================
# V95 â€” MOMENTUM / VOLUME OBSERVATIONAL SHADOW COHORT
# Prospective tags on new V32 paper entries only.
# No entry gate, no exit/risk changes, no real orders.
# ============================================================
V95_VOL_MAX_EXCLUSIVE = 5.0


def v95_momentum_volume_tag(pos):
    """Tag an entry using only V61 causal entry-context fields."""
    tag = {
        "v95_version": "V95_MOMENTUM_VOLUME_OBSERVATIONAL",
        "v95_observational_only": True,
        "v95_volume_max_exclusive": V95_VOL_MAX_EXCLUSIVE,
        "v95_shadow_pass": False,
        "v95_reason": "MISSING_CONTEXT",
        "v95_momentum_30m_pct": None,
        "v95_momentum_60m_pct": None,
        "v95_vol_ratio_15m_vs_4h": None,
    }
    try:
        raw30 = pos.get("ctx_ret_30m_pct")
        raw60 = pos.get("ctx_ret_60m_pct")
        rawvol = pos.get("ctx_vol_ratio_15m_vs_4h")
        if raw30 is None or raw60 is None or rawvol is None:
            return tag

        mom30 = float(raw30)
        mom60 = float(raw60)
        vol = float(rawvol)
        if not all(math.isfinite(x) for x in (mom30, mom60, vol)) or vol < 0:
            tag["v95_reason"] = "INVALID_CONTEXT"
            return tag

        tag["v95_momentum_30m_pct"] = round(mom30, 4)
        tag["v95_momentum_60m_pct"] = round(mom60, 4)
        tag["v95_vol_ratio_15m_vs_4h"] = round(vol, 4)

        reasons = []
        if mom30 < 0:
            reasons.append("MOMENTUM_30M_NEGATIVE")
        if mom60 < 0:
            reasons.append("MOMENTUM_60M_NEGATIVE")
        if vol >= V95_VOL_MAX_EXCLUSIVE:
            reasons.append("VOLUME_GTE_5")

        if reasons:
            tag["v95_reason"] = "|".join(reasons)
        else:
            tag["v95_shadow_pass"] = True
            tag["v95_reason"] = "PASS"
        return tag
    except (TypeError, ValueError, OverflowError):
        tag["v95_reason"] = "INVALID_CONTEXT"
        return tag


@app.get("/v95-momentum-volume-shadow")
async def v95_momentum_volume_shadow():
    """Read-only outcomes for V95-tagged V32 paper trades."""
    version = "V95_MOMENTUM_VOLUME_OBSERVATIONAL"
    opened = [
        p for p in V32_STATE.get("open", {}).values()
        if p.get("v95_version") == version
    ]
    closed = [
        p for p in V32_STATE.get("closed", [])
        if p.get("v95_version") == version
    ]

    def stats(rows):
        values = []
        for p in rows:
            try:
                value = p.get("net_pct")
                if value is not None and math.isfinite(float(value)):
                    values.append(float(value))
            except (TypeError, ValueError):
                continue
        if not values:
            return {
                "n": 0, "mean_net_pct": None,
                "win_rate_pct": None, "profit_factor": None,
            }
        wins = [v for v in values if v > 0]
        losses = [v for v in values if v < 0]
        pf = sum(wins) / abs(sum(losses)) if losses else None
        return {
            "n": len(values),
            "mean_net_pct": round(sum(values) / len(values), 4),
            "win_rate_pct": round(100 * len(wins) / len(values), 2),
            "profit_factor": round(pf, 4) if pf is not None else None,
        }

    passed = [p for p in closed if p.get("v95_shadow_pass") is True]
    flagged = [p for p in closed if p.get("v95_shadow_pass") is not True]
    reasons = {}
    for p in opened + closed:
        reason = p.get("v95_reason", "UNKNOWN")
        reasons[reason] = reasons.get(reason, 0) + 1

    return {
        **MODE_INFO,
        "status": "OK",
        "panel": "V95_MOMENTUM_VOLUME_SHADOW",
        "research_only": True,
        "read_only": True,
        "entry_gate": False,
        "real_orders": False,
        "v32_entry_exit_unchanged": True,
        "v55_risk_unchanged": True,
        "v94_unchanged": True,
        "new_entries_only": True,
        "thresholds": {
            "momentum_30m_min_pct": 0.0,
            "momentum_60m_min_pct": 0.0,
            "vol_ratio_max_exclusive": V95_VOL_MAX_EXCLUSIVE,
        },
        "tagged_open_n": len(opened),
        "tagged_closed_n": len(closed),
        "closed_all_tagged": stats(closed),
        "closed_shadow_pass": stats(passed),
        "closed_shadow_flagged": stats(flagged),
        "reason_counts_open_and_closed": reasons,
        "recent_closed": [
            {
                "key": p.get("key"),
                "symbol": p.get("symbol"),
                "net_pct": p.get("net_pct"),
                "shadow_pass": p.get("v95_shadow_pass"),
                "reason": p.get("v95_reason"),
                "momentum_30m_pct": p.get("v95_momentum_30m_pct"),
                "momentum_60m_pct": p.get("v95_momentum_60m_pct"),
                "vol_ratio_15m_vs_4h": p.get("v95_vol_ratio_15m_vs_4h"),
            }
            for p in closed[-20:]
        ],
        "limitations": [
            "Prospective observational tags on matched V32 paper trades; not an independently executed portfolio.",
            "Thresholds were chosen after inspecting prior sample results and need independent forward validation.",
            "V32 cooldown/capacity and V55 risk rules remain unchanged.",
            "Missing entry-context fields are flagged and do not block entries.",
            "No live orders are sent.",
        ],
        "generated_utc": utc_now(),
    }


# ============================================================
# V97 MEAN REVERSION SHORT DIAGNOSTIC — RESEARCH ONLY
# Does not open paper positions, send orders, or alter V96.
# ============================================================

def _v97_candidates(candles, symbol):
    ret30, mus, sigmas = precompute_rolling_volatility_v12(candles, 288)
    rows = []
    wait = 12  # 60m, twelve completed 5m bars
    for i in range(294, len(candles) - wait - 25):
        mom = ret30[i]
        mu, sigma = mus[i], sigmas[i]
        if mom is None or mom <= 0 or mu is None or sigma is None or sigma <= 0:
            continue
        z = (mom - mu) / sigma
        if z < 0:
            continue
        # Signal candle i is completed. Observe exactly the NEXT 12 completed
        # candles i+1 ... i+12; enter at open of i+13.
        obs = candles[i + 1:i + 13]
        if len(obs) != wait:
            continue
        reference = candles[i]['close']
        end_price = obs[-1]['close']
        end_change = pct_change(reference, end_price)
        max_down = pct_change(reference, min(x['low'] for x in obs))
        if end_change >= 0.75:
            behavior = 'CONTINUED_UP'
        elif max_down > -0.75 and -0.50 <= end_change < 0.75:
            behavior = 'SHALLOW_CONSOLIDATION'
        elif max_down <= -0.75 and end_change >= -0.25:
            behavior = 'PULLBACK_RECOVERY'
        else:
            behavior = 'PULLBACK_UNRECOVERED'
        entry_i = i + 13
        entry = candles[entry_i]['open']
        if entry <= 0:
            continue
        outcomes = {}
        for horizon in (30, 60, 120):
            bars = horizon // 5
            exit_i = entry_i + bars
            exit_price = candles[exit_i]['open']
            gross = (entry - exit_price) / entry * 100.0
            net = gross - ROUND_TRIP_COST_PCT
            # Include only price excursions BEFORE the exit open.
            during = candles[entry_i:exit_i]
            mae = max(0.0, (max(c['high'] for c in during) / entry - 1) * 100)
            mfe = max(0.0, (1 - min(c['low'] for c in during) / entry) * 100)
            outcomes[horizon] = {'net': net, 'mae': mae, 'mfe': mfe}
        rows.append({
            'symbol': symbol, 'signal_time_ms': candles[i]['close_time'],
            'entry_open_time': candles[entry_i]['open_time'],
            'relative_momentum_z': z, 'behavior': behavior,
            'outcomes': outcomes,
        })
    return rows


def _v97_stats(rows, horizon):
    values = [e['outcomes'][horizon]['net'] for e in rows]
    if not values:
        return {'n': 0, 'mean_net_pct': None, 'median_net_pct': None,
                'win_rate_pct': None, 'profit_factor': None,
                'mean_mae_pct': None, 'p95_mae_pct': None, 'max_mae_pct': None}
    wins = sum(v for v in values if v > 0)
    losses = -sum(v for v in values if v < 0)
    maes = sorted(e['outcomes'][horizon]['mae'] for e in rows)
    return {
        'n': len(values), 'mean_net_pct': round(mean(values), 4),
        'median_net_pct': round(median(values), 4),
        'win_rate_pct': round(100 * sum(v > 0 for v in values) / len(values), 2),
        'profit_factor': round(wins / losses, 4) if losses > 0 else None,
        'mean_mae_pct': round(mean(maes), 4),
        'p95_mae_pct': round(maes[max(0, math.ceil(len(maes) * .95) - 1)], 4),
        'max_mae_pct': round(max(maes), 4),
    }


@app.get('/mean-reversion90')
async def mean_reversion90_v97(
    count: int = Query(default=10, ge=5, le=20),
    days: int = Query(default=90, ge=30, le=90),
):
    try:
        async with httpx.AsyncClient(timeout=httpx.Timeout(240.0)) as client:
            universe = await build_universe(client)
            selected = universe[:count]
            semaphore = asyncio.Semaphore(2)

            async def worker(item):
                async with semaphore:
                    try:
                        candles = await get_5m_candles_days(client, item['symbol'], days)
                        return {'symbol': item['symbol'], 'ok': True,
                                'rows': _v97_candidates(candles, item['symbol'])}
                    except Exception as exc:
                        return {'symbol': item['symbol'], 'ok': False, 'error': str(exc)}

            fetched = await asyncio.gather(*(worker(x) for x in selected))
        good = [x for x in fetched if x['ok']]
        failed = [{'symbol': x['symbol'], 'error': x['error']}
                  for x in fetched if not x['ok']]
        raw = [r for x in good for r in x['rows']]
        by_time = {}
        for r in raw:
            by_time.setdefault(r['signal_time_ms'], []).append(r)
        ranked = []
        for group in by_time.values():
            if len(group) < 5:
                continue
            ordered = sorted(group, key=lambda x: (x['relative_momentum_z'], x['symbol']))
            for j, r in enumerate(ordered):
                copy = dict(r)
                copy['rank_percentile'] = j / (len(ordered) - 1)
                ranked.append(copy)
        ranked.sort(key=lambda x: (x['symbol'], x['signal_time_ms']))
        cooled = []
        last = {}
        for r in ranked:
            symbol = r['symbol']
            if symbol in last and r['signal_time_ms'] - last[symbol] < 60 * 60 * 1000:
                continue
            cooled.append(r)
            last[symbol] = r['signal_time_ms']
        # Single global calendar split shared by all hypotheses and coins.
        times = sorted(r['entry_open_time'] for r in cooled)
        cutoff = times[int(len(times) * 2 / 3)] if times else None
        hypotheses = {
            'Z_GE_1': lambda r: r['relative_momentum_z'] >= 1,
            'Z_GE_2': lambda r: r['relative_momentum_z'] >= 2,
            'Z_GE_1_TOP_20': lambda r: r['relative_momentum_z'] >= 1 and r['rank_percentile'] >= .8,
            'Z_GE_1_TOP_20_PULLBACK_RECOVERY': lambda r: r['relative_momentum_z'] >= 1 and r['rank_percentile'] >= .8 and r['behavior'] == 'PULLBACK_RECOVERY',
            'Z_GE_1_TOP_20_PULLBACK_UNRECOVERED': lambda r: r['relative_momentum_z'] >= 1 and r['rank_percentile'] >= .8 and r['behavior'] == 'PULLBACK_UNRECOVERED',
        }
        report = {}
        for name, predicate in hypotheses.items():
            subset = [r for r in cooled if predicate(r)]
            dev = [r for r in subset if r['entry_open_time'] < cutoff] if cutoff else []
            oos = [r for r in subset if r['entry_open_time'] >= cutoff] if cutoff else []
            report[name] = {
                str(h): {
                    'all': _v97_stats(subset, h),
                    'discovery_first_2_3': _v97_stats(dev, h),
                    'reference_last_1_3': _v97_stats(oos, h),
                } for h in (30, 60, 120)
            }
        return {
            **MODE_INFO, 'status': 'OK', 'signal': False,
            'study': 'V97_MEAN_REVERSION_SHORT_DIAGNOSTIC',
            'days': days, 'selected_coin_count': len(selected),
            'successful_coin_count': len(good), 'failed_coin_count': len(failed),
            'entry': 'OPEN_AFTER_12_COMPLETED_5M_OBSERVATION_CANDLES',
            'hold_minutes': [30, 60, 120],
            'round_trip_cost_pct': ROUND_TRIP_COST_PCT,
            'direction': 'HYPOTHETICAL_SHORT_ONLY',
            'cooldown_minutes': 60,
            'calendar_split_utc_ms': cutoff,
            'ranked_and_cooled_observations': len(cooled),
            'results': report, 'failed': failed, 'generated_utc': utc_now(),
            'limitations': [
                'No actual orders or paper positions are created.',
                'Hypothetical spot-price short: borrow availability, borrow fees, funding and liquidation not modeled.',
                'Fixed 0.15% cost; variable slippage/spread not modeled.',
                'MAE is adverse intratrade excursion, not a tested stop-loss execution.',
                'Current liquid universe introduces survivorship and selection bias.',
                'Global calendar split is descriptive; hypotheses were motivated by earlier research, so reference is not fully independent.',
                'Signals may overlap across coins; no executable portfolio simulation.',
            ],
        }
    except Exception as exc:
        return {**MODE_INFO, 'status': 'ERROR', 'signal': False,
                'study': 'V97_MEAN_REVERSION_SHORT_DIAGNOSTIC',
                'error': str(exc), 'generated_utc': utc_now()}


# ============================================================
# V98 VOLATILITY SQUEEZE BREAKOUT — DIAGNOSTIC, RESEARCH ONLY
# Independent frozen hypothesis; no V96/paper/order modifications.
# ============================================================

def _v98_events(candles, symbol):
    from collections import deque
    n = len(candles)
    if n < 150:
        return []
    o = [float(c['open']) for c in candles]
    h = [float(c['high']) for c in candles]
    l = [float(c['low']) for c in candles]
    cl = [float(c['close']) for c in candles]
    vol = [float(c['volume']) for c in candles]
    # Each rolling 12-bar range uses only fully completed historical bars.
    widths = [None] * n
    for j in range(11, n):
        low = min(l[j-11:j+1]); high = max(h[j-11:j+1])
        widths[j] = (high-low)/low*100 if low > 0 else None
    events=[]
    # i is the fully closed breakout bar. Entry is NEXT bar open i+1.
    for i in range(110, n-26):
        # Squeeze ends BEFORE breakout bar; no future candles used.
        prior_width = widths[i-1]
        baseline = [w for w in widths[i-73:i-1] if w is not None]
        if prior_width is None or len(baseline) != 72:
            continue
        baseline_median = median(baseline)
        if baseline_median <= 0 or prior_width > 0.65 * baseline_median:
            continue
        ceiling = max(h[i-24:i]); floor = min(l[i-24:i])
        if cl[i] > ceiling:
            direction='LONG'
        elif cl[i] < floor:
            direction='SHORT'
        else:
            continue
        average_volume = sum(vol[i-20:i])/20
        if average_volume <= 0:
            continue
        volume_ratio = vol[i]/average_volume
        entry = o[i+1]
        if entry <= 0:
            continue
        outcomes={}
        for hold in (30,60,120):
            bars=hold//5
            exit_index=i+1+bars
            exit_price=o[exit_index]
            gross=(exit_price/entry-1)*100 if direction=='LONG' else (entry-exit_price)/entry*100
            inside=range(i+1,exit_index)
            if direction=='LONG':
                mae=max(0.0,(1-min(l[k] for k in inside)/entry)*100)
            else:
                mae=max(0.0,(max(h[k] for k in inside)/entry-1)*100)
            outcomes[hold]={'net':gross-ROUND_TRIP_COST_PCT,'mae':mae}
        events.append({'symbol':symbol,'signal_time_ms':int(candles[i]['close_time']),
                       'entry_open_time':int(candles[i+1]['open_time']),
                       'direction':direction,'volume_ratio':volume_ratio,
                       'squeeze_ratio':prior_width/baseline_median,'outcomes':outcomes})
    return events


def _v98_stats(rows, hold):
    if not rows:
        return {'n':0,'mean_net_pct':None,'median_net_pct':None,'win_rate_pct':None,
                'profit_factor':None,'mean_mae_pct':None,'p95_mae_pct':None}
    values=[r['outcomes'][hold]['net'] for r in rows]
    adverse=sorted(r['outcomes'][hold]['mae'] for r in rows)
    wins=sum(x for x in values if x>0)
    losses=-sum(x for x in values if x<0)
    return {'n':len(values),'mean_net_pct':round(mean(values),4),
            'median_net_pct':round(median(values),4),
            'win_rate_pct':round(100*sum(x>0 for x in values)/len(values),2),
            'profit_factor':round(wins/losses,4) if losses>0 else None,
            'mean_mae_pct':round(mean(adverse),4),
            'p95_mae_pct':round(adverse[math.ceil(.95*len(adverse))-1],4)}


@app.get('/squeeze-breakout90')
async def squeeze_breakout90_v98(
    count: int = Query(default=10, ge=5, le=20),
    days: int = Query(default=90, ge=30, le=90),
):
    try:
        async with httpx.AsyncClient(timeout=httpx.Timeout(240.0)) as client:
            universe=await build_universe(client)
            selected=universe[:count]
            sem=asyncio.Semaphore(2)
            async def worker(item):
                async with sem:
                    try:
                        candles=await get_5m_candles_days(client,item['symbol'],days)
                        return {'symbol':item['symbol'],'ok':True,
                                'events':_v98_events(candles,item['symbol'])}
                    except Exception as exc:
                        return {'symbol':item['symbol'],'ok':False,'error':str(exc)}
            fetched=await asyncio.gather(*(worker(x) for x in selected))
        successful=[r for r in fetched if r['ok']]
        failed=[{'symbol':r['symbol'],'error':r['error']} for r in fetched if not r['ok']]
        all_events=sorted((e for r in successful for e in r['events']),
                          key=lambda x:(x['symbol'],x['signal_time_ms']))
        # Non-overlapping 120-minute observation windows within each symbol.
        cooled=[];last={}
        for e in all_events:
            sym=e['symbol'];t=e['signal_time_ms']
            if sym in last and t-last[sym]<120*60*1000:
                continue
            cooled.append(e);last[sym]=t
        # Fixed global chronological split, independent of chosen variants.
        times=sorted(e['entry_open_time'] for e in cooled)
        cutoff=times[int(len(times)*2/3)] if times else None
        variants={
            'BREAKOUT_VOLUME_GE_1_0':lambda e:e['volume_ratio']>=1.0,
            'BREAKOUT_VOLUME_GE_1_5':lambda e:e['volume_ratio']>=1.5,
        }
        results={}
        for name,pred in variants.items():
            results[name]={}
            for direction in ('LONG','SHORT'):
                subset=[e for e in cooled if pred(e) and e['direction']==direction]
                discovery=[e for e in subset if cutoff is not None and e['entry_open_time']<cutoff]
                reference=[e for e in subset if cutoff is not None and e['entry_open_time']>=cutoff]
                results[name][direction]={str(hold):{
                    'all':_v98_stats(subset,hold),
                    'discovery_first_2_3':_v98_stats(discovery,hold),
                    'reference_last_1_3':_v98_stats(reference,hold),
                } for hold in (30,60,120)}
        return {**MODE_INFO,'status':'OK','signal':False,
                'study':'V98_VOLATILITY_SQUEEZE_BREAKOUT_DIAGNOSTIC',
                'days':days,'selected_coin_count':len(selected),
                'successful_coin_count':len(successful),'failed_coin_count':len(failed),
                'squeeze_definition':'Prior 12 completed 5m bars range <= 65% of median rolling 12-bar range over preceding 72 completed bars',
                'breakout_definition':'Completed signal candle CLOSE outside prior 24-bar high/low',
                'volume_filters':[1.0,1.5],
                'entry':'NEXT_5M_OPEN_AFTER_COMPLETED_BREAKOUT_CANDLE',
                'hold_minutes':[30,60,120],
                'round_trip_cost_pct':ROUND_TRIP_COST_PCT,
                'cooldown_minutes':120,
                'calendar_split_utc_ms':cutoff,
                'cooled_event_count':len(cooled),
                'results':results,'failed':failed,'generated_utc':utc_now(),
                'limitations':['Research only: no real or paper orders are placed.',
                    'Thresholds are exploratory, not validated on independent future data.',
                    'Current-universe survivorship bias; overlapping cross-coin signals possible.',
                    'Fixed trading cost does not model variable slippage/spread.',
                    'Hypothetical short ignores borrow/funding/liquidation.',
                    'MAE is descriptive; no executable stop loss or portfolio simulation.']}
    except Exception as exc:
        return {**MODE_INFO,'status':'ERROR','signal':False,
                'study':'V98_VOLATILITY_SQUEEZE_BREAKOUT_DIAGNOSTIC',
                'error':str(exc),'generated_utc':utc_now()}

# ============================================================
# V99 FALSE BREAKOUT REVERSAL — RESEARCH ONLY
# Breakout close -> first confirmed re-entry close within 30m -> next open.
# No V96/paper/order code is modified.
# ============================================================

def _v99_events(candles, symbol):
    n = len(candles)
    if n < 150:
        return []
    o = [float(c['open']) for c in candles]
    h = [float(c['high']) for c in candles]
    l = [float(c['low']) for c in candles]
    close = [float(c['close']) for c in candles]
    vol = [float(c['volume']) for c in candles]
    widths = [None] * n
    for j in range(11, n):
        low12 = min(l[j-11:j+1])
        widths[j] = (max(h[j-11:j+1])-low12)/low12*100 if low12 > 0 else None
    events = []
    for i in range(110, n-33):
        baseline = [w for w in widths[i-73:i-1] if w is not None]
        if len(baseline) != 72 or widths[i-1] is None:
            continue
        base_med = median(baseline)
        if base_med <= 0 or widths[i-1] > .65*base_med:
            continue
        ceiling = max(h[i-24:i]); floor = min(l[i-24:i])
        if close[i] > ceiling:
            breakout = 'UP'; reversal = 'SHORT'
        elif close[i] < floor:
            breakout = 'DOWN'; reversal = 'LONG'
        else:
            continue
        avg_volume = sum(vol[i-20:i])/20
        if avg_volume <= 0:
            continue
        ratio = vol[i]/avg_volume
        # Confirm reversal only after a fully completed subsequent candle.
        # Require CLOSE back inside the ORIGINAL pre-breakout 24-bar range.
        confirm_idx = None
        for k in range(i+1, min(i+7, n-26)):
            if floor <= close[k] <= ceiling:
                confirm_idx = k
                break
        if confirm_idx is None:
            continue
        entry_idx = confirm_idx+1
        entry = o[entry_idx]
        if entry <= 0:
            continue
        outcomes = {}
        for hold in (30, 60, 120):
            exit_idx = entry_idx + hold//5
            exit_price = o[exit_idx]
            gross = (exit_price/entry-1)*100 if reversal == 'LONG' else (entry-exit_price)/entry*100
            active = range(entry_idx, exit_idx)
            if reversal == 'LONG':
                mae = max(0., (1-min(l[k] for k in active)/entry)*100)
            else:
                mae = max(0., (max(h[k] for k in active)/entry-1)*100)
            outcomes[hold] = {'net':gross-ROUND_TRIP_COST_PCT,'mae':mae}
        events.append({'symbol':symbol,'signal_time_ms':int(candles[i]['close_time']),
                       'confirm_time_ms':int(candles[confirm_idx]['close_time']),
                       'entry_open_time':int(candles[entry_idx]['open_time']),
                       'breakout_direction':breakout,'reversal_direction':reversal,
                       'confirmation_delay_minutes':(confirm_idx-i)*5,
                       'volume_ratio':ratio,'outcomes':outcomes})
    return events


@app.get('/false-breakout90')
async def false_breakout90_v99(
    count: int = Query(default=10, ge=5, le=20),
    days: int = Query(default=90, ge=30, le=90),
):
    try:
        async with httpx.AsyncClient(timeout=httpx.Timeout(240.0)) as client:
            universe = await build_universe(client)
            selected = universe[:count]
            sem = asyncio.Semaphore(2)
            async def worker(item):
                async with sem:
                    try:
                        candles = await get_5m_candles_days(client,item['symbol'],days)
                        return {'symbol':item['symbol'],'ok':True,
                                'events':_v99_events(candles,item['symbol'])}
                    except Exception as exc:
                        return {'symbol':item['symbol'],'ok':False,'error':str(exc)}
            fetched = await asyncio.gather(*(worker(x) for x in selected))
        good = [x for x in fetched if x['ok']]
        failed = [{'symbol':x['symbol'],'error':x['error']} for x in fetched if not x['ok']]
        all_events = sorted((e for x in good for e in x['events']),
                            key=lambda e:(e['symbol'],e['entry_open_time']))
        cooled=[]; last_entry={}
        for e in all_events:
            sym=e['symbol']; t=e['entry_open_time']
            if sym in last_entry and t-last_entry[sym]<120*60*1000:
                continue
            cooled.append(e);last_entry[sym]=t
        times=sorted(e['entry_open_time'] for e in cooled)
        cutoff=times[int(len(times)*2/3)] if times else None
        variants={'VOLUME_GE_1_0':1.0,'VOLUME_GE_1_5':1.5}
        results={}
        for name, minimum in variants.items():
            results[name]={}
            for direction in ('LONG','SHORT'):
                subset=[e for e in cooled if e['volume_ratio']>=minimum and e['reversal_direction']==direction]
                dev=[e for e in subset if cutoff is not None and e['entry_open_time']<cutoff]
                ref=[e for e in subset if cutoff is not None and e['entry_open_time']>=cutoff]
                results[name][direction]={str(hold):{
                    'all':_v98_stats(subset,hold),
                    'discovery_first_2_3':_v98_stats(dev,hold),
                    'reference_last_1_3':_v98_stats(ref,hold),
                } for hold in (30,60,120)}
        return {**MODE_INFO,'status':'OK','signal':False,
                'study':'V99_FALSE_BREAKOUT_REVERSAL_DIAGNOSTIC',
                'days':days,'selected_coin_count':len(selected),
                'successful_coin_count':len(good),'failed_coin_count':len(failed),
                'squeeze_definition':'Prior 12 completed 5m bars range <= 65% of median previous 72 rolling 12-bar ranges',
                'breakout_definition':'Completed candle close outside previous 24-bar high/low',
                'confirmation':'First subsequent completed 5m candle CLOSE back inside ORIGINAL prior 24-bar range, within 6 candles (30m)',
                'entry':'NEXT_5M_OPEN_AFTER_CONFIRMED_RETURN_INSIDE_RANGE',
                'direction':'OPPOSITE_TO_ORIGINAL_BREAKOUT',
                'volume_filters':[1.0,1.5], 'hold_minutes':[30,60,120],
                'round_trip_cost_pct':ROUND_TRIP_COST_PCT,
                'cooldown_minutes':120,'calendar_split_utc_ms':cutoff,
                'raw_confirmed_event_count':len(all_events),
                'cooled_event_count':len(cooled),
                'results':results,'failed':failed,'generated_utc':utc_now(),
                'limitations':['Research only: no real or paper orders.',
                    'V99 hypothesis motivated by V98: reference period is not a truly untouched out-of-sample test.',
                    'Current liquid universe creates survivorship bias; cross-coin signals can overlap.',
                    'Fixed cost excludes variable spread and slippage.',
                    'Hypothetical short excludes borrow/funding/liquidation.',
                    'MAE is descriptive; no tested stop execution or portfolio sizing.',
                    'First confirmation is chosen only from completed candles; entry follows confirmation.']}
    except Exception as exc:
        return {**MODE_INFO,'status':'ERROR','signal':False,
                'study':'V99_FALSE_BREAKOUT_REVERSAL_DIAGNOSTIC',
                'error':str(exc),'generated_utc':utc_now()}


# ============================================================
# V100 TRADE PATH / EXIT DIAGNOSTIC — READ-ONLY RESEARCH
# Reuses the frozen V99 event definitions; no trading state changes.
# ============================================================
def _v100_stats(rows):
    if not rows:
        return {'n': 0, 'mean_net_pct': None, 'median_net_pct': None,
                'win_rate_pct': None, 'profit_factor': None,
                'mean_mfe_pct': None, 'mean_mae_pct': None,
                'mfe_before_mae_pct': None, 'mean_first_15m_pct': None,
                'mean_first_30m_pct': None, 'gross_positive_but_net_negative_pct': None}
    vals = [r['net_120'] for r in rows]
    pos = sum(x for x in vals if x > 0)
    neg = -sum(x for x in vals if x < 0)
    def avg(field):
        return round(mean(r[field] for r in rows), 4)
    return {'n':len(rows), 'mean_net_pct':round(mean(vals),4),
            'median_net_pct':round(median(vals),4),
            'win_rate_pct':round(100*sum(v>0 for v in vals)/len(vals),2),
            'profit_factor':round(pos/neg,4) if neg else None,
            'mean_mfe_pct':avg('mfe'), 'mean_mae_pct':avg('mae'),
            'mfe_before_mae_pct':round(100*sum(r['first_favorable'] for r in rows)/len(rows),2),
            'mean_first_15m_pct':avg('first15'),
            'mean_first_30m_pct':avg('first30'),
            'gross_positive_but_net_negative_pct':round(100*sum(0<r['gross_120']<=ROUND_TRIP_COST_PCT for r in rows)/len(rows),2)}


def _v100_paths(candles, symbol):
    events = _v99_events(candles, symbol)
    index_by_open = {int(c['open_time']):i for i,c in enumerate(candles)}
    rows=[]
    for e in events:
        idx = index_by_open.get(e['entry_open_time'])
        if idx is None or idx+24 >= len(candles):
            continue
        entry = float(candles[idx]['open'])
        if entry<=0: continue
        side = e['reversal_direction']
        direction = 1 if side=='LONG' else -1
        def pnl(price): return direction*(float(price)/entry-1)*100
        first15=pnl(candles[idx+3]['open'])
        first30=pnl(candles[idx+6]['open'])
        gross120=pnl(candles[idx+24]['open'])
        # High/low order within a single candle is unknown. A tie is indeterminate.
        best=(-float('inf'),None)
        worst=(float('inf'),None)
        for j in range(idx,idx+24):
            c=candles[j]
            favorable=pnl(c['high'] if side=='LONG' else c['low'])
            adverse=pnl(c['low'] if side=='LONG' else c['high'])
            if favorable>best[0]: best=(favorable,j)
            if adverse<worst[0]: worst=(adverse,j)
        mfe=max(0.,best[0]); mae=max(0.,-worst[0])
        rows.append({'symbol':symbol,'entry_open_time':e['entry_open_time'],
                     'volume_ratio':e['volume_ratio'],'direction':side,
                     'net_120':gross120-ROUND_TRIP_COST_PCT,'gross_120':gross120,
                     'mfe':mfe,'mae':mae,
                     'first_favorable':best[1]<worst[1] if best[1]!=worst[1] else False,
                     'first15':first15,'first30':first30,
                     'net_30':first30-ROUND_TRIP_COST_PCT,
                     'net_60':pnl(candles[idx+12]['open'])-ROUND_TRIP_COST_PCT})
    return rows


@app.get('/trade-path90')
async def trade_path90_v100(
    count: int = Query(default=10,ge=5,le=20),
    days: int = Query(default=90,ge=30,le=90),
):
    try:
        async with httpx.AsyncClient(timeout=httpx.Timeout(240.0)) as client:
            universe=await build_universe(client)
            selected=universe[:count]
            sem=asyncio.Semaphore(2)
            async def worker(item):
                async with sem:
                    try:
                        candles=await get_5m_candles_days(client,item['symbol'],days)
                        return {'ok':True,'symbol':item['symbol'],
                                'rows':_v100_paths(candles,item['symbol'])}
                    except Exception as exc:
                        return {'ok':False,'symbol':item['symbol'],'error':str(exc)}
            fetched=await asyncio.gather(*(worker(item) for item in selected))
        good=[x for x in fetched if x['ok']]
        failed=[{'symbol':x['symbol'],'error':x['error']} for x in fetched if not x['ok']]
        all_rows=sorted((r for x in good for r in x['rows']),
                        key=lambda r:(r['symbol'],r['entry_open_time']))
        cooled=[]; last={}
        for r in all_rows:
            sym=r['symbol']; t=r['entry_open_time']
            if sym in last and t-last[sym]<120*60*1000: continue
            cooled.append(r); last[sym]=t
        times=sorted(r['entry_open_time'] for r in cooled)
        cutoff=times[int(len(times)*2/3)] if times else None
        results={}
        for volname,threshold in [('VOLUME_GE_1_0',1.),('VOLUME_GE_1_5',1.5)]:
            results[volname]={}
            for direction in ('LONG','SHORT'):
                subset=[r for r in cooled if r['volume_ratio']>=threshold and r['direction']==direction]
                dev=[r for r in subset if cutoff is not None and r['entry_open_time']<cutoff]
                ref=[r for r in subset if cutoff is not None and r['entry_open_time']>=cutoff]
                results[volname][direction]={
                    'all':_v100_stats(subset),
                    'discovery_first_2_3':_v100_stats(dev),
                    'reference_last_1_3':_v100_stats(ref),
                    'mean_net_by_hold_pct':{
                        str(h):round(mean(r['net_'+str(h)] for r in subset),4) if subset else None
                        for h in (30,60,120)},
                    'first_15m_positive':_v100_stats([r for r in subset if r['first15']>0]),
                    'first_15m_nonpositive':_v100_stats([r for r in subset if r['first15']<=0]),
                }
        return {**MODE_INFO,'status':'OK','signal':False,
                'study':'V100_TRADE_PATH_EXIT_DIAGNOSTIC',
                'base_events':'V99_CONFIRMED_FALSE_BREAKOUT',
                'days':days,'selected_coin_count':len(selected),
                'successful_coin_count':len(good),'failed_coin_count':len(failed),
                'raw_event_count':len(all_rows),'cooled_event_count':len(cooled),
                'cooldown_minutes':120,'round_trip_cost_pct':ROUND_TRIP_COST_PCT,
                'calendar_split_utc_ms':cutoff,'results':results,
                'failed':failed,'generated_utc':utc_now(),
                'limitations':[
                    'Descriptive diagnostics, not a new optimized or independently validated strategy.',
                    'First 15m positive subgroup uses future price information and is NOT an entry-time signal.',
                    'MFE and MAE are intrabar extremes; exact high/low ordering inside one 5m candle is unknown.',
                    'MFE-before-MAE uses distinct bar indices; same-bar ties are counted as not confirmed.',
                    'No executable stop, target, or exit fill is simulated from intrabar extremes.',
                    'Current-universe survivorship bias and cross-coin overlap remain.',
                    'Fixed cost excludes spread/slippage; hypothetical shorts exclude borrow/funding.',
                    'V96 and all paper/order mechanisms remain unchanged.'
                ]}
    except Exception as exc:
        return {**MODE_INFO,'status':'ERROR','signal':False,
                'study':'V100_TRADE_PATH_EXIT_DIAGNOSTIC',
                'error':str(exc),'generated_utc':utc_now()}


# ============================================================
# V101 15-MIN EARLY EXIT — RESEARCH ONLY, NO ORDER PLACEMENT
# V99 frozen entries, observe 15m (open idx+3), act at next
# 5m candle open (idx+4). Positive holds until idx+24 open.
# ============================================================
def _v101_rows(candles, symbol):
    events = _v99_events(candles, symbol)
    indices = {int(c['open_time']): i for i,c in enumerate(candles)}
    rows = []
    for event in events:
        idx = indices.get(event['entry_open_time'])
        if idx is None or idx+24 >= len(candles):
            continue
        entry = float(candles[idx]['open'])
        if entry <= 0:
            continue
        direction = 1 if event['reversal_direction'] == 'LONG' else -1
        def gross(j):
            return direction * (float(candles[j]['open']) / entry - 1) * 100
        decision_15m = gross(idx+3)
        early_exit = decision_15m <= 0
        # Deliberate one-bar execution delay, not exit at observed decision price.
        executed_exit_idx = idx+4 if early_exit else idx+24
        rows.append({
            'symbol': symbol,
            'entry_open_time': event['entry_open_time'],
            'direction': event['reversal_direction'],
            'volume_ratio': event['volume_ratio'],
            'first_15m_gross_pct': decision_15m,
            'early_exit': early_exit,
            'baseline_net_pct': gross(idx+24)-ROUND_TRIP_COST_PCT,
            'policy_net_pct': gross(executed_exit_idx)-ROUND_TRIP_COST_PCT,
            'exit_delay_minutes': 20 if early_exit else 120,
        })
    return rows


def _v101_summary(rows):
    if not rows:
        return {'n':0, 'early_exit_count':0, 'early_exit_rate_pct':None,
                'baseline_mean_net_pct':None, 'policy_mean_net_pct':None,
                'improvement_pp':None, 'baseline_profit_factor':None,
                'policy_profit_factor':None, 'baseline_win_rate_pct':None,
                'policy_win_rate_pct':None, 'early_exit_mean_net_pct':None,
                'held_mean_net_pct':None}
    base=[r['baseline_net_pct'] for r in rows]
    policy=[r['policy_net_pct'] for r in rows]
    early=[r['policy_net_pct'] for r in rows if r['early_exit']]
    held=[r['policy_net_pct'] for r in rows if not r['early_exit']]
    def pf(vals):
        loss=-sum(v for v in vals if v<0)
        return round(sum(v for v in vals if v>0)/loss,4) if loss else None
    return {
        'n':len(rows), 'early_exit_count':len(early),
        'early_exit_rate_pct':round(100*len(early)/len(rows),2),
        'baseline_mean_net_pct':round(mean(base),4),
        'policy_mean_net_pct':round(mean(policy),4),
        'improvement_pp':round(mean(policy)-mean(base),4),
        'baseline_profit_factor':pf(base), 'policy_profit_factor':pf(policy),
        'baseline_win_rate_pct':round(100*sum(v>0 for v in base)/len(base),2),
        'policy_win_rate_pct':round(100*sum(v>0 for v in policy)/len(policy),2),
        'early_exit_mean_net_pct':round(mean(early),4) if early else None,
        'held_mean_net_pct':round(mean(held),4) if held else None,
    }


@app.get('/early-exit90')
async def early_exit90_v101(
    count: int = Query(default=10, ge=5, le=20),
    days: int = Query(default=90, ge=30, le=90),
):
    try:
        async with httpx.AsyncClient(timeout=httpx.Timeout(240.0)) as client:
            universe=await build_universe(client)
            selected=universe[:count]
            sem=asyncio.Semaphore(2)
            async def worker(item):
                async with sem:
                    try:
                        candles=await get_5m_candles_days(client,item['symbol'],days)
                        return {'ok':True,'symbol':item['symbol'],
                                'rows':_v101_rows(candles,item['symbol'])}
                    except Exception as exc:
                        return {'ok':False,'symbol':item['symbol'],'error':str(exc)}
            fetched=await asyncio.gather(*(worker(item) for item in selected))
        good=[x for x in fetched if x['ok']]
        failed=[{'symbol':x['symbol'],'error':x['error']} for x in fetched if not x['ok']]
        all_rows=sorted((r for x in good for r in x['rows']),
                        key=lambda r:(r['symbol'],r['entry_open_time']))
        cooled=[]; last={}
        for r in all_rows:
            sym=r['symbol']; t=r['entry_open_time']
            if sym in last and t-last[sym]<120*60*1000:
                continue
            cooled.append(r); last[sym]=t
        times=sorted(r['entry_open_time'] for r in cooled)
        cutoff=times[int(len(times)*2/3)] if times else None
        results={}
        for name,threshold in [('VOLUME_GE_1_0',1.),('VOLUME_GE_1_5',1.5)]:
            results[name]={}
            for direction in ('LONG','SHORT'):
                group=[r for r in cooled if r['volume_ratio']>=threshold and r['direction']==direction]
                results[name][direction]={
                    'all':_v101_summary(group),
                    'discovery_first_2_3':_v101_summary([r for r in group if cutoff is not None and r['entry_open_time']<cutoff]),
                    'reference_last_1_3':_v101_summary([r for r in group if cutoff is not None and r['entry_open_time']>=cutoff]),
                }
        return {**MODE_INFO,'status':'OK','signal':False,
                'study':'V101_15M_EARLY_EXIT_DIAGNOSTIC',
                'base_events':'V99_CONFIRMED_FALSE_BREAKOUT',
                'days':days,'selected_coin_count':len(selected),
                'successful_coin_count':len(good),'failed_coin_count':len(failed),
                'raw_event_count':len(all_rows),'cooled_event_count':len(cooled),
                'round_trip_cost_pct':ROUND_TRIP_COST_PCT,
                'decision':'15m directional return at entry+3 open; <=0 means exit',
                'execution':'early exit at entry+4 open (20m); otherwise entry+24 open (120m)',
                'calendar_split_utc_ms':cutoff,'results':results,'failed':failed,
                'generated_utc':utc_now(),
                'limitations':[
                    'Exploratory in-sample policy motivated by V100, not independent validation.',
                    'First 15m is observed before decision; execution deliberately delayed one 5m candle.',
                    'Fixed 0.15% cost; real spread/slippage and short borrow costs omitted.',
                    'Current-universe survivorship bias and overlapping signals remain.',
                    'No executable portfolio simulation, orders, or paper positions.',
                    'All V96-V100 existing code remains unchanged.',
                ]}
    except Exception as exc:
        return {**MODE_INFO,'status':'ERROR','signal':False,
                'study':'V101_15M_EARLY_EXIT_DIAGNOSTIC',
                'error':str(exc),'generated_utc':utc_now()}


# ============================================================
# V102 EXIT TIMING GRID — DIAGNOSTIC ONLY
# V99 entries and 120-minute per-symbol cooldown preserved.
# All decisions use already observed 5m OPEN prices.
# 15m exit requires a 10m decision; 20/30/60m exits use
# the 15m decision. 30/60/120m positive-path holds.
# ============================================================
def _v102_rows(candles, symbol):
    events = _v99_events(candles, symbol)
    indices = {int(c['open_time']): i for i, c in enumerate(candles)}
    rows = []
    for event in events:
        idx = indices.get(event['entry_open_time'])
        if idx is None or idx + 24 >= len(candles):
            continue
        entry = float(candles[idx]['open'])
        if entry <= 0:
            continue
        direction = 1 if event['reversal_direction'] == 'LONG' else -1
        def net(minutes):
            return direction * (float(candles[idx + minutes//5]['open']) / entry - 1) * 100 - ROUND_TRIP_COST_PCT
        ret10 = net(10) + ROUND_TRIP_COST_PCT
        ret15 = net(15) + ROUND_TRIP_COST_PCT
        policies = {}
        for bad_exit in (15, 20, 30, 60):
            for good_hold in (30, 60, 120):
                # To execute at 15m, decision must be made at 10m.
                observed = ret10 if bad_exit == 15 else ret15
                exit_min = bad_exit if observed <= 0 else good_hold
                policies[f'BAD_{bad_exit}_GOOD_{good_hold}'] = net(exit_min)
        rows.append({
            'symbol': symbol,
            'entry_open_time': event['entry_open_time'],
            'direction': event['reversal_direction'],
            'volume_ratio': event['volume_ratio'],
            'baseline_net_pct': net(120),
            'policies': policies,
        })
    return rows


def _v102_metrics(vals):
    if not vals:
        return {'n':0,'mean_net_pct':None,'profit_factor':None,'win_rate_pct':None}
    wins=sum(v for v in vals if v>0)
    gain=sum(v for v in vals if v>0)
    loss=-sum(v for v in vals if v<0)
    return {
        'n':len(vals),
        'mean_net_pct':round(mean(vals),4),
        'profit_factor':round(gain/loss,4) if loss else None,
        'win_rate_pct':round(100*wins/len(vals),2),
    }


@app.get('/exit-timing90')
async def exit_timing90_v102(
    count: int = Query(default=10,ge=5,le=20),
    days: int = Query(default=90,ge=30,le=90),
):
    try:
        async with httpx.AsyncClient(timeout=httpx.Timeout(240.0)) as client:
            universe=await build_universe(client)
            selected=universe[:count]
            sem=asyncio.Semaphore(2)
            async def worker(item):
                async with sem:
                    try:
                        candles=await get_5m_candles_days(client,item['symbol'],days)
                        return {'ok':True,'symbol':item['symbol'],
                                'rows':_v102_rows(candles,item['symbol'])}
                    except Exception as exc:
                        return {'ok':False,'symbol':item['symbol'],'error':str(exc)}
            fetched=await asyncio.gather(*(worker(item) for item in selected))
        good=[x for x in fetched if x['ok']]
        failed=[{'symbol':x['symbol'],'error':x['error']} for x in fetched if not x['ok']]
        all_rows=sorted((r for x in good for r in x['rows']),
                        key=lambda r:(r['symbol'],r['entry_open_time']))
        cooled=[]; last={}
        for r in all_rows:
            sym=r['symbol']; t=r['entry_open_time']
            if sym in last and t-last[sym]<120*60*1000:
                continue
            cooled.append(r); last[sym]=t
        times=sorted(r['entry_open_time'] for r in cooled)
        cutoff=times[int(len(times)*2/3)] if times else None
        names=[f'BAD_{b}_GOOD_{g}' for b in (15,20,30,60) for g in (30,60,120)]
        results={}
        for label,threshold in [('VOLUME_GE_1_0',1.0),('VOLUME_GE_1_5',1.5)]:
            results[label]={}
            for direction in ('LONG','SHORT'):
                group=[r for r in cooled if r['volume_ratio']>=threshold and r['direction']==direction]
                discovery=[r for r in group if cutoff is not None and r['entry_open_time']<cutoff]
                reference=[r for r in group if cutoff is not None and r['entry_open_time']>=cutoff]
                candidates=[]
                for name in names:
                    candidates.append({
                        'policy':name,
                        'all':_v102_metrics([r['policies'][name] for r in group]),
                        'discovery_first_2_3':_v102_metrics([r['policies'][name] for r in discovery]),
                        'reference_last_1_3':_v102_metrics([r['policies'][name] for r in reference]),
                    })
                # Choose ONLY on discovery; report reference without re-selection.
                eligible=[c for c in candidates if c['discovery_first_2_3']['n']>=30]
                chosen=max(eligible,key=lambda c:c['discovery_first_2_3']['mean_net_pct']) if eligible else None
                results[label][direction]={
                    'baseline_120m':{
                        'all':_v102_metrics([r['baseline_net_pct'] for r in group]),
                        'discovery_first_2_3':_v102_metrics([r['baseline_net_pct'] for r in discovery]),
                        'reference_last_1_3':_v102_metrics([r['baseline_net_pct'] for r in reference]),
                    },
                    'selected_on_discovery':chosen['policy'] if chosen else None,
                    'selected_policy_metrics':chosen,
                    'all_candidate_policies':candidates,
                }
        return {**MODE_INFO,'status':'OK','signal':False,
                'study':'V102_EXIT_TIMING_GRID_DIAGNOSTIC',
                'base_events':'V99_CONFIRMED_FALSE_BREAKOUT',
                'days':days,'selected_coin_count':len(selected),
                'successful_coin_count':len(good),'failed_coin_count':len(failed),
                'raw_event_count':len(all_rows),'cooled_event_count':len(cooled),
                'round_trip_cost_pct':ROUND_TRIP_COST_PCT,
                'decision':'BAD_15 uses 10m observation; BAD_20/30/60 use 15m observation; <=0 triggers early exit',
                'execution':'All exits use subsequent 5m OPEN at named minute; otherwise GOOD_30/60/120 exit',
                'calendar_split_utc_ms':cutoff,
                'selection':'Max mean net on discovery only; reference reported without reselection',
                'results':results,'failed':failed,'generated_utc':utc_now(),
                'limitations':[
                    'Exploratory and not independent of V99-V101 research; multiple policies tested.',
                    'BAD_15 necessarily uses 10m rather than 15m observation to avoid same-instant execution.',
                    'Fixed 0.15% cost; variable slippage/spread and short financing excluded.',
                    'Current-universe selection bias and overlapping signals across symbols.',
                    'No executable portfolio, stop orders, paper orders, or real trading.',
                    'V96-V101 code remains unchanged.',
                ]}
    except Exception as exc:
        return {**MODE_INFO,'status':'ERROR','signal':False,
                'study':'V102_EXIT_TIMING_GRID_DIAGNOSTIC',
                'error':str(exc),'generated_utc':utc_now()}


# ============================================================
# V103 BACKTEST INTEGRITY AUDIT — RESEARCH ONLY
# Does not change any V96–V102 route, signal, order or policy.
# ============================================================
def _v103_stats(vals):
    if not vals:
        return {'n':0,'mean_net_pct':None,'win_rate_pct':None,
                'profit_factor':None,'wins':0,'losses':0,'breakeven':0}
    winners=[v for v in vals if v>0]
    losers=[v for v in vals if v<0]
    return {'n':len(vals),'mean_net_pct':round(mean(vals),4),
            'win_rate_pct':round(100*len(winners)/len(vals),2),
            'profit_factor':round(sum(winners)/(-sum(losers)),4) if losers else None,
            'wins':len(winners),'losses':len(losers),
            'breakeven':len(vals)-len(winners)-len(losers)}


@app.get('/integrity-audit90')
async def integrity_audit90_v103(
    count: int = Query(default=10,ge=5,le=20),
    days: int = Query(default=90,ge=30,le=90),
):
    try:
        async with httpx.AsyncClient(timeout=httpx.Timeout(240.0)) as client:
            universe=await build_universe(client)
            selected=universe[:count]
            sem=asyncio.Semaphore(2)
            async def worker(item):
                async with sem:
                    try:
                        candles=await get_5m_candles_days(client,item['symbol'],days)
                        events=_v99_events(candles,item['symbol'])
                        rows=_v102_rows(candles,item['symbol'])
                        return {'ok':True,'symbol':item['symbol'],'events':events,'rows':rows}
                    except Exception as exc:
                        return {'ok':False,'symbol':item['symbol'],'error':str(exc)}
            fetched=await asyncio.gather(*(worker(item) for item in selected))
        good=[x for x in fetched if x['ok']]
        failed=[{'symbol':x['symbol'],'error':x['error']} for x in fetched if not x['ok']]
        events=sorted((e for x in good for e in x['events']),
                      key=lambda e:(e['symbol'],e['entry_open_time']))
        rows=sorted((r for x in good for r in x['rows']),
                    key=lambda r:(r['symbol'],r['entry_open_time']))
        last={}; cooled=[]
        for r in rows:
            t=r['entry_open_time']; sym=r['symbol']
            if sym in last and t-last[sym]<120*60*1000:
                continue
            cooled.append(r); last[sym]=t
        times=sorted(r['entry_open_time'] for r in cooled)
        cutoff=times[int(len(times)*2/3)] if times else None
        # Cross-check V99 net outcomes with independently reconstructed V102 baseline.
        event_lookup={(e['symbol'],e['entry_open_time']):e for e in events}
        discrepancies=[]
        for r in cooled:
            e=event_lookup.get((r['symbol'],r['entry_open_time']))
            if e is None:
                discrepancies.append({'symbol':r['symbol'],'time':r['entry_open_time'],'issue':'event_missing'})
            elif abs(e['outcomes'][120]['net']-r['baseline_net_pct'])>1e-9:
                discrepancies.append({'symbol':r['symbol'],'time':r['entry_open_time'],'issue':'baseline_mismatch'})
        names=[f'BAD_{b}_GOOD_{g}' for b in (15,20,30,60) for g in (30,60,120)]
        results={}
        for variant,threshold in [('VOLUME_GE_1_0',1.0),('VOLUME_GE_1_5',1.5)]:
            results[variant]={}
            for direction in ('LONG','SHORT'):
                group=[r for r in cooled if r['volume_ratio']>=threshold and r['direction']==direction]
                discovery=[r for r in group if cutoff is not None and r['entry_open_time']<cutoff]
                reference=[r for r in group if cutoff is not None and r['entry_open_time']>=cutoff]
                candidates=[{
                    'policy':name,
                    'discovery':_v103_stats([r['policies'][name] for r in discovery]),
                    'reference':_v103_stats([r['policies'][name] for r in reference])
                } for name in names]
                eligible=[x for x in candidates if x['discovery']['n']>=30]
                chosen=max(eligible,key=lambda x:x['discovery']['mean_net_pct']) if eligible else None
                results[variant][direction]={
                    'baseline_120m':{
                        'all':_v103_stats([r['baseline_net_pct'] for r in group]),
                        'discovery':_v103_stats([r['baseline_net_pct'] for r in discovery]),
                        'reference':_v103_stats([r['baseline_net_pct'] for r in reference]),
                    },
                    'selected_on_discovery':chosen,
                    'policy_count':len(candidates),
                    'all_discovery_means_negative':all(x['discovery']['mean_net_pct'] is None or x['discovery']['mean_net_pct']<0 for x in candidates),
                    'any_reference_positive':any(x['reference']['mean_net_pct'] is not None and x['reference']['mean_net_pct']>0 for x in candidates),
                }
        # Synthetic tests for metric invariants, independent of live data.
        synthetic={
            'two_wins_one_loss':_v103_stats([1.0,2.0,-1.0]),
            'all_losses':_v103_stats([-0.1,-0.2]),
            'zero_returns':_v103_stats([0.0,0.0]),
        }
        assert synthetic['two_wins_one_loss']['win_rate_pct']==66.67
        assert synthetic['two_wins_one_loss']['profit_factor']==3.0
        assert synthetic['all_losses']['win_rate_pct']==0.0
        assert synthetic['zero_returns']['win_rate_pct']==0.0
        return {**MODE_INFO,'status':'OK','signal':False,
                'study':'V103_BACKTEST_INTEGRITY_AUDIT',
                'days':days,'selected_coin_count':len(selected),
                'successful_coin_count':len(good),'failed_coin_count':len(failed),
                'raw_event_count':len(events),'cooled_event_count':len(cooled),
                'round_trip_cost_pct':ROUND_TRIP_COST_PCT,
                'calendar_split_utc_ms':cutoff,
                'checks':{
                    'synthetic_metric_tests_passed':True,
                    'baseline_reconstruction_match':len(discrepancies)==0,
                    'baseline_discrepancy_count':len(discrepancies),
                    'baseline_discrepancy_examples':discrepancies[:5],
                    'correct_win_rate_formula':'100 * count(net_return > 0) / n',
                    'v102_win_rate_bug':'V102 used sum(positive_return_values)/n rather than count(positive_returns)/n',
                    'selection_uses_discovery_only':True,
                    'reference_is_not_independent_of_prior_hypothesis_development':True,
                    'execution_timing':'BAD_15: observe 10m, exit 15m; other BAD: observe 15m, exit 20/30/60m',
                },
                'results':results,'failed':failed,'generated_utc':utc_now(),
                'limitations':[
                    'Recomputes baseline and policies from the same historical candles, not external independent verification.',
                    'Current coin universe, 90d window and research hypotheses were repeatedly inspected.',
                    'Only the V103 report corrects win-rate; legacy V102 endpoint is intentionally unchanged.',
                    'Fixed round-trip cost omits variable spread/slippage and short funding.',
                    'No real trading, paper order, or V96 policy changes.',
                ]}
    except Exception as exc:
        return {**MODE_INFO,'status':'ERROR','signal':False,
                'study':'V103_BACKTEST_INTEGRITY_AUDIT',
                'error':str(exc),'generated_utc':utc_now()}


# ==============================================================
# V104 ATR + PARTIAL TP RESEARCH — NO ORDERS / NO V96 MODIFICATION
# Frozen test specification:
# ATR14 on completed 5m candles, SL 2 ATR, TP1 1.5 ATR (50%),
# TP2 3 ATR (25%), last 25% trails by 2 ATR after TP1.
# Break-even stop after TP1; 120-minute time exit.
# Conservative OHLC: opening gaps then stop BEFORE any TP if both
# levels appear in same bar. No fill improvement beyond stop/target.
# ==============================================================

def _v104_atr14(candles):
    n=len(candles)
    result=[None]*n
    if n<16:
        return result
    tr=[0.0]*n
    for i in range(1,n):
        high=float(candles[i]['high']); low=float(candles[i]['low'])
        prev=float(candles[i-1]['close'])
        tr[i]=max(high-low,abs(high-prev),abs(low-prev))
    value=sum(tr[1:15])/14
    result[14]=value
    for i in range(15,n):
        value=(value*13+tr[i])/14
        result[i]=value
    return result


def _v104_simulate(candles, idx, direction, atr, cost_pct):
    entry=float(candles[idx]['open'])
    if entry<=0 or atr is None or atr<=0 or idx+24>=len(candles):
        return None
    sign=1 if direction=='LONG' else -1
    stop=entry-sign*2*atr
    tp1=entry+sign*1.5*atr
    tp2=entry+sign*3*atr
    remaining=1.0
    tp1_done=False; tp2_done=False
    trail_active=False
    extreme=entry
    weighted_gross=0.0
    fills=[]
    def fill(fraction, price, why, minute):
        nonlocal remaining,weighted_gross
        fraction=min(fraction,remaining)
        if fraction<=1e-10:
            return
        weighted_gross+=fraction*sign*(price/entry-1)*100
        remaining=max(0.0,remaining-fraction)
        fills.append({'fraction':round(fraction,4),'minute':minute,'reason':why})
    for step in range(24):
        bar=candles[idx+step]
        op=float(bar['open']); hi=float(bar['high']); lo=float(bar['low'])
        minute=step*5
        # Opening gaps: fill at open, not at optimistic target/stop.
        stop_gap=(op<=stop) if sign==1 else (op>=stop)
        if stop_gap:
            fill(remaining,op,'STOP_GAP',minute)
            break
        if not tp1_done and ((op>=tp1) if sign==1 else (op<=tp1)):
            fill(.50,op,'TP1_GAP',minute); tp1_done=True
            stop=max(stop,entry) if sign==1 else min(stop,entry)
            trail_active=True
        if tp1_done and not tp2_done and ((op>=tp2) if sign==1 else (op<=tp2)):
            fill(.25,op,'TP2_GAP',minute); tp2_done=True
        if remaining<=1e-10:
            break
        # Conservative same-bar ordering: check stop before targets.
        stop_hit=(lo<=stop) if sign==1 else (hi>=stop)
        if stop_hit:
            fill(remaining,stop,'STOP',minute)
            break
        if not tp1_done and ((hi>=tp1) if sign==1 else (lo<=tp1)):
            fill(.50,tp1,'TP1',minute); tp1_done=True
            stop=max(stop,entry) if sign==1 else min(stop,entry)
            trail_active=True
        if tp1_done and not tp2_done and ((hi>=tp2) if sign==1 else (lo<=tp2)):
            fill(.25,tp2,'TP2',minute); tp2_done=True
        # Trail moves AFTER the bar closes: no intra-bar lookahead.
        if trail_active:
            extreme=max(extreme,hi) if sign==1 else min(extreme,lo)
            proposed=extreme-sign*2*atr
            stop=max(stop,proposed) if sign==1 else min(stop,proposed)
    if remaining>1e-10:
        fill(remaining,float(candles[idx+24]['open']),'TIME_120',120)
    return {
        'net_pct':weighted_gross-cost_pct,
        'tp1_hit':tp1_done,'tp2_hit':tp2_done,
        'fills':fills,
    }


def _v104_build_rows(candles,symbol):
    events=_v99_events(candles,symbol)
    indices={int(c['open_time']):i for i,c in enumerate(candles)}
    atrs=_v104_atr14(candles)
    rows=[]
    for e in events:
        idx=indices.get(e['entry_open_time'])
        if idx is None or idx+24>=len(candles) or idx<15:
            continue
        # ATR from last COMPLETED bar only, never entry bar.
        atr=atrs[idx-1]
        sim=_v104_simulate(candles,idx,e['reversal_direction'],atr,ROUND_TRIP_COST_PCT)
        if sim is None:
            continue
        rows.append({'symbol':symbol,'entry_open_time':e['entry_open_time'],
                     'direction':e['reversal_direction'],
                     'volume_ratio':e['volume_ratio'],
                     'baseline_net_pct':e['outcomes'][120]['net'],
                     'atr_net_pct':sim['net_pct'],
                     'tp1_hit':sim['tp1_hit'],
                     'tp2_hit':sim['tp2_hit'],
                     'fills':sim['fills']})
    return rows


@app.get('/atr-partial-tp90')
async def atr_partial_tp90_v104(
    count: int = Query(default=10,ge=5,le=20),
    days: int = Query(default=90,ge=30,le=90),
):
    try:
        async with httpx.AsyncClient(timeout=httpx.Timeout(240.0)) as client:
            universe=await build_universe(client)
            selected=universe[:count]
            sem=asyncio.Semaphore(2)
            async def worker(item):
                async with sem:
                    try:
                        candles=await get_5m_candles_days(client,item['symbol'],days)
                        return {'ok':True,'symbol':item['symbol'],
                                'rows':_v104_build_rows(candles,item['symbol'])}
                    except Exception as exc:
                        return {'ok':False,'symbol':item['symbol'],'error':str(exc)}
            fetched=await asyncio.gather(*(worker(item) for item in selected))
        good=[x for x in fetched if x['ok']]
        failed=[{'symbol':x['symbol'],'error':x['error']} for x in fetched if not x['ok']]
        all_rows=sorted((r for x in good for r in x['rows']),
                        key=lambda r:(r['symbol'],r['entry_open_time']))
        cooled=[];last={}
        for r in all_rows:
            sym=r['symbol'];t=r['entry_open_time']
            if sym in last and t-last[sym]<120*60*1000:
                continue
            cooled.append(r);last[sym]=t
        times=sorted(r['entry_open_time'] for r in cooled)
        cutoff=times[int(len(times)*2/3)] if times else None
        results={}
        for variant,threshold in [('VOLUME_GE_1_0',1.0),('VOLUME_GE_1_5',1.5)]:
            results[variant]={}
            for direction in ('LONG','SHORT'):
                group=[r for r in cooled if r['volume_ratio']>=threshold and r['direction']==direction]
                result={}
                for split,subset in [
                    ('all',group),
                    ('discovery_first_2_3',[r for r in group if cutoff is not None and r['entry_open_time']<cutoff]),
                    ('reference_last_1_3',[r for r in group if cutoff is not None and r['entry_open_time']>=cutoff]),
                ]:
                    base=_v103_stats([r['baseline_net_pct'] for r in subset])
                    atr=_v103_stats([r['atr_net_pct'] for r in subset])
                    result[split]={
                        'baseline_120m':base,'atr_partial_tp':atr,
                        'mean_delta_pp':round(atr['mean_net_pct']-base['mean_net_pct'],4) if subset else None,
                        'tp1_hit_count':sum(r['tp1_hit'] for r in subset),
                        'tp2_hit_count':sum(r['tp2_hit'] for r in subset),
                    }
                results[variant][direction]=result
        return {**MODE_INFO,'status':'OK','signal':False,
                'study':'V104_ATR_PARTIAL_TP_DIAGNOSTIC',
                'days':days,'selected_coin_count':len(selected),
                'successful_coin_count':len(good),'failed_coin_count':len(failed),
                'raw_event_count':len(all_rows),'cooled_event_count':len(cooled),
                'round_trip_cost_pct':ROUND_TRIP_COST_PCT,
                'calendar_split_utc_ms':cutoff,
                'settings':{
                    'atr':'Wilder ATR14, last fully completed 5m bar before entry',
                    'stop_loss':'2 ATR from entry',
                    'tp1':'1.5 ATR favorable; close 50%',
                    'tp2':'3 ATR favorable; close 25% of original',
                    'trailing':'Remaining 25% trails 2 ATR after TP1; trail updated only after completed bar',
                    'break_even':'After TP1, stop for remainder moves to entry, not fee-adjusted',
                    'max_holding_minutes':120,
                    'intrabar_priority':'open gap first; then stop before TP; trail updates after bar',
                    'cost':'0.15% applied once to full weighted round trip',
                    'cooldown_minutes':120,
                },
                'results':results,'failed':failed,'generated_utc':utc_now(),
                'limitations':[
                    'Historical exploratory diagnostic, NOT an independent forward test.',
                    'Same-bar stop-first ordering is conservative but OHLC cannot reveal actual tick sequence.',
                    '5m gap and liquidity execution modeled approximately; spread/slippage may exceed fixed cost.',
                    'Fixed ATR based on pre-entry 5m bars; no dynamic ATR recalculation.',
                    'No leverage, funding, liquidation, exchange filters or portfolio capital simulation.',
                    'No live trades, paper orders, V96 modifications or strategy deployment.',
                ]}
    except Exception as exc:
        return {**MODE_INFO,'status':'ERROR','signal':False,
                'study':'V104_ATR_PARTIAL_TP_DIAGNOSTIC',
                'error':str(exc),'generated_utc':utc_now()}


# ============================================================
# V105 EXECUTION AUDIT — diagnostic only; V96–V104 unchanged
# Conservative strict alternative: no TP1+TP2 within same 5m bar;
# TP1 break-even/trail starts NEXT bar; stop checked before target.
# ============================================================
def _v105_strict(candles,idx,direction,atr,cost):
    entry=float(candles[idx]['open'])
    if entry<=0 or atr is None or atr<=0 or idx+24>=len(candles):
        return None
    sign=1 if direction=='LONG' else -1
    stop=entry-sign*2*atr
    tp1=entry+sign*1.5*atr
    tp2=entry+sign*3*atr
    remain=1.0; stage=0; gross=0.0; fills=[]; extreme=entry
    ambiguous=0
    def record(qty,price,reason,minute):
        nonlocal remain,gross
        qty=min(qty,remain)
        if qty<=1e-10:return
        part=qty*sign*(price/entry-1)*100
        gross+=part; remain-=qty
        fills.append({'reason':reason,'minute':minute,
                      'fraction':round(qty,4),
                      'weighted_gross_pp':round(part,6),
                      'allocated_cost_pp':round(cost*qty,6),
                      'weighted_net_pp':round(part-cost*qty,6)})
    for step in range(24):
        bar=candles[idx+step]; op=float(bar['open'])
        hi=float(bar['high']);lo=float(bar['low']);minute=step*5
        stop_gap=(op<=stop) if sign==1 else (op>=stop)
        stop_hit=(lo<=stop) if sign==1 else (hi>=stop)
        target=tp1 if stage==0 else tp2
        target_hit=(hi>=target) if sign==1 else (lo<=target)
        if stop_hit and target_hit:ambiguous+=1
        if stop_gap:
            record(remain,op,'STOP_GAP',minute);break
        # Existing stop wins if same candle contains stop and target.
        if stop_hit:
            record(remain,stop,'STOP',minute);break
        target_gap=(op>=target) if sign==1 else (op<=target)
        if target_hit:
            if stage==0:
                record(.50,op if target_gap else tp1,'TP1_GAP' if target_gap else 'TP1',minute)
                stage=1
                stop=max(stop,entry) if sign==1 else min(stop,entry)
                # Do not use this candle's extremes for trailing.
                extreme=entry
                continue
            if stage==1:
                record(.25,op if target_gap else tp2,'TP2_GAP' if target_gap else 'TP2',minute)
                stage=2
        # Trail after TP1, using only bars strictly after TP1 bar.
        if stage>=1:
            extreme=max(extreme,hi) if sign==1 else min(extreme,lo)
            proposed=extreme-sign*2*atr
            stop=max(stop,proposed) if sign==1 else min(stop,proposed)
    if remain>1e-10:
        record(remain,float(candles[idx+24]['open']),'TIME_120',120)
    return {'net_pct':gross-cost,'gross_pct':gross,'fills':fills,
            'tp1_hit':stage>=1,'tp2_hit':stage>=2,
            'ambiguous_bar_count':ambiguous}


def _v105_rows(candles,symbol):
    events=_v99_events(candles,symbol)
    indices={int(c['open_time']):i for i,c in enumerate(candles)}
    atrs=_v104_atr14(candles)
    rows=[]
    for e in events:
        idx=indices.get(e['entry_open_time'])
        if idx is None or idx<15 or idx+24>=len(candles):continue
        atr=atrs[idx-1]
        old=_v104_simulate(candles,idx,e['reversal_direction'],atr,ROUND_TRIP_COST_PCT)
        new=_v105_strict(candles,idx,e['reversal_direction'],atr,ROUND_TRIP_COST_PCT)
        if old is None or new is None:continue
        rows.append({'symbol':symbol,'entry_open_time':e['entry_open_time'],
                     'direction':e['reversal_direction'],'volume_ratio':e['volume_ratio'],
                     'baseline':e['outcomes'][120]['net'],
                     'v104':old['net_pct'],'strict':new['net_pct'],
                     'old_fills':old['fills'],'strict_fills':new['fills'],
                     'tp1_hit':new['tp1_hit'],'tp2_hit':new['tp2_hit'],
                     'ambiguous_bar_count':new['ambiguous_bar_count']})
    return rows


@app.get('/atr-execution-audit90')
async def atr_execution_audit90_v105(
    count: int = Query(default=10,ge=5,le=20),
    days: int = Query(default=90,ge=30,le=90),
):
    try:
        async with httpx.AsyncClient(timeout=httpx.Timeout(240.0)) as client:
            universe=await build_universe(client)
            selected=universe[:count]
            sem=asyncio.Semaphore(2)
            async def worker(item):
                async with sem:
                    try:
                        candles=await get_5m_candles_days(client,item['symbol'],days)
                        return {'ok':True,'symbol':item['symbol'],
                                'rows':_v105_rows(candles,item['symbol'])}
                    except Exception as exc:
                        return {'ok':False,'symbol':item['symbol'],'error':str(exc)}
            fetched=await asyncio.gather(*(worker(item) for item in selected))
        good=[x for x in fetched if x['ok']]
        failed=[{'symbol':x['symbol'],'error':x['error']} for x in fetched if not x['ok']]
        all_rows=sorted((r for x in good for r in x['rows']),
                        key=lambda r:(r['symbol'],r['entry_open_time']))
        cooled=[];last={}
        for r in all_rows:
            sym=r['symbol'];t=r['entry_open_time']
            if sym in last and t-last[sym]<120*60*1000:continue
            cooled.append(r);last[sym]=t
        times=sorted(r['entry_open_time'] for r in cooled)
        cutoff=times[int(len(times)*2/3)] if times else None
        results={}
        for label,threshold in [('VOLUME_GE_1_0',1.0),('VOLUME_GE_1_5',1.5)]:
            results[label]={}
            for direction in ('LONG','SHORT'):
                group=[r for r in cooled if r['direction']==direction and r['volume_ratio']>=threshold]
                output={}
                for split,subset in [
                    ('all',group),
                    ('discovery_first_2_3',[r for r in group if cutoff is not None and r['entry_open_time']<cutoff]),
                    ('reference_last_1_3',[r for r in group if cutoff is not None and r['entry_open_time']>=cutoff]),
                ]:
                    old=_v103_stats([r['v104'] for r in subset])
                    strict=_v103_stats([r['strict'] for r in subset])
                    base=_v103_stats([r['baseline'] for r in subset])
                    reasons={}
                    for r in subset:
                        for f in r['strict_fills']:
                            reason=f['reason']
                            bucket=reasons.setdefault(reason,{'fill_count':0,'total_closed_fraction':0.0,
                                                               'total_weighted_gross_pp':0.0,'total_allocated_cost_pp':0.0})
                            bucket['fill_count']+=1
                            bucket['total_closed_fraction']+=f['fraction']
                            bucket['total_weighted_gross_pp']+=f['weighted_gross_pp']
                            bucket['total_allocated_cost_pp']+=f['allocated_cost_pp']
                    for v in reasons.values():
                        for k in ('total_closed_fraction','total_weighted_gross_pp','total_allocated_cost_pp'):
                            v[k]=round(v[k],5)
                    output[split]={
                        'baseline_120m':base,'v104_original':old,'v105_strict':strict,
                        'strict_minus_v104_mean_pp':round(strict['mean_net_pct']-old['mean_net_pct'],4) if subset else None,
                        'strict_minus_baseline_mean_pp':round(strict['mean_net_pct']-base['mean_net_pct'],4) if subset else None,
                        'changed_trade_count':sum(abs(r['strict']-r['v104'])>1e-9 for r in subset),
                        'ambiguous_bar_count':sum(r['ambiguous_bar_count'] for r in subset),
                        'tp1_hit_count':sum(r['tp1_hit'] for r in subset),
                        'tp2_hit_count':sum(r['tp2_hit'] for r in subset),
                        'exit_fill_breakdown':reasons,
                        'fill_fraction_integrity_failures':sum(abs(sum(f['fraction'] for f in r['strict_fills'])-1)>1e-6 for r in subset),
                        'weighted_net_integrity_failures':sum(abs(sum(f['weighted_net_pp'] for f in r['strict_fills'])-r['strict'])>1e-4 for r in subset),
                    }
                results[label][direction]=output
        return {**MODE_INFO,'status':'OK','signal':False,
                'study':'V105_ATR_EXECUTION_AUDIT',
                'days':days,'selected_coin_count':len(selected),
                'successful_coin_count':len(good),'failed_coin_count':len(failed),
                'raw_event_count':len(all_rows),'cooled_event_count':len(cooled),
                'round_trip_cost_pct':ROUND_TRIP_COST_PCT,
                'calendar_split_utc_ms':cutoff,
                'strict_assumptions':[
                    'Uses completed-bar ATR14 at entry and same frozen 2 ATR stop, 1.5 ATR TP1, 3 ATR TP2.',
                    'If stop and TP target touched in one 5m bar, assume stop executed first.',
                    'TP1 and TP2 cannot both fill in the same 5m candle.',
                    'Break-even and trailing after TP1 activate only from next 5m candle.',
                    'Trailing stop updated after each completed eligible candle, not within it.',
                    'Each partial fill pays pro-rata share of fixed 0.15% round-trip cost.',
                    'Historical diagnostic only; no real or paper orders.',
                ],
                'results':results,'failed':failed,'generated_utc':utc_now(),
                'limitations':[
                    'Strict engine is an alternate OHLC assumption, not a reconstruction of true intrabar order.',
                    'Current coin universe and reused 90-day sample are not independent forward data.',
                    'TP2 may not be reachable if remaining shares stop out earlier; no exchange fill simulation.',
                    'Short financing, spreads, slippage and real-time data delays not fully modeled.',
                    'Existing V96–V104 endpoints and paper signals remain unchanged.',
                ]}
    except Exception as exc:
        return {**MODE_INFO,'status':'ERROR','signal':False,
                'study':'V105_ATR_EXECUTION_AUDIT',
                'error':str(exc),'generated_utc':utc_now()}


# V106 FROZEN FORWARD PAPER VALIDATION (stateless, read-only)
# Only entries from 2026-10-10 15:30 UTC onward count.
# Uses a fixed ten-symbol basket; no optimization or order placement.
_V106_START_MS = 1791646200000  # 2026-10-10 15:30:00 UTC
_V106_SYMBOLS = ('BTCUSDT','ETHUSDT','SOLUSDT','XRPUSDT','BNBUSDT',
                 'DOGEUSDT','ADAUSDT','LINKUSDT','AVAXUSDT','LTCUSDT')

@app.get('/forward-paper-v106')
async def forward_paper_v106(days: int = Query(default=30,ge=7,le=90)):
    try:
        now_ms=int(datetime.now(timezone.utc).timestamp()*1000)
        async with httpx.AsyncClient(timeout=httpx.Timeout(240.0)) as client:
            sem=asyncio.Semaphore(2)
            async def worker(symbol):
                async with sem:
                    try:
                        candles=await get_5m_candles_days(client,symbol,days)
                        # Existing V99 event detector only yields fully matured trades.
                        events=_v99_events(candles,symbol)
                        idx_by_time={int(c['open_time']):i for i,c in enumerate(candles)}
                        atrs=_v104_atr14(candles)
                        rows=[]
                        for e in events:
                            t=int(e['entry_open_time'])
                            if t < _V106_START_MS or t+120*60*1000 > now_ms:continue
                            idx=idx_by_time.get(t)
                            if idx is None or idx<15 or idx+24>=len(candles):continue
                            sim=_v105_strict(candles,idx,e['reversal_direction'],atrs[idx-1],ROUND_TRIP_COST_PCT)
                            if sim is None:continue
                            rows.append({'symbol':symbol,'entry_open_time':t,
                                         'direction':e['reversal_direction'],
                                         'volume_ratio':e['volume_ratio'],
                                         'baseline_net_pct':e['outcomes'][120]['net'],
                                         'atr_net_pct':sim['net_pct'],
                                         'tp1_hit':sim['tp1_hit'],'tp2_hit':sim['tp2_hit'],
                                         'ambiguous_bar_count':sim['ambiguous_bar_count']})
                        return {'symbol':symbol,'rows':rows,'error':None}
                    except Exception as exc:
                        return {'symbol':symbol,'rows':[],'error':str(exc)}
            fetched=await asyncio.gather(*(worker(s) for s in _V106_SYMBOLS))
        failed=[{'symbol':x['symbol'],'error':x['error']} for x in fetched if x['error']]
        # Important: per-symbol chronological cooldown, identical to V105.
        all_rows=sorted((r for x in fetched for r in x['rows']),
                        key=lambda r:(r['symbol'],r['entry_open_time']))
        cooled=[];last={}
        for r in all_rows:
            sym=r['symbol'];t=r['entry_open_time']
            if sym in last and t-last[sym]<120*60*1000:continue
            cooled.append(r);last[sym]=t
        results={}
        for threshold_name,threshold in [('VOLUME_GE_1_0',1.0),('VOLUME_GE_1_5',1.5)]:
            results[threshold_name]={}
            for direction in ('LONG','SHORT'):
                subset=[r for r in cooled if r['direction']==direction and r['volume_ratio']>=threshold]
                base=_v103_stats([r['baseline_net_pct'] for r in subset])
                atr=_v103_stats([r['atr_net_pct'] for r in subset])
                results[threshold_name][direction]={
                    'baseline_120m':base,'atr_partial_tp':atr,
                    'mean_delta_pp':round(atr['mean_net_pct']-base['mean_net_pct'],4) if subset else None,
                    'tp1_hit_count':sum(r['tp1_hit'] for r in subset),
                    'tp2_hit_count':sum(r['tp2_hit'] for r in subset),
                    'ambiguous_bar_count':sum(r['ambiguous_bar_count'] for r in subset),
                }
        return {**MODE_INFO,'status':'OK' if not failed else 'PARTIAL',
                'signal':False,'study':'V106_FROZEN_FORWARD_PAPER',
                'forward_start_utc':'2026-10-10T15:30:00+00:00',
                'forward_start_ms':_V106_START_MS,
                'generated_utc':utc_now(),'days_rolling_lookback':days,
                'symbols_fixed':list(_V106_SYMBOLS),
                'successful_coin_count':len(fetched)-len(failed),
                'failed':failed,'raw_matured_event_count':len(all_rows),
                'cooled_matured_event_count':len(cooled),
                'settings_frozen':{'atr_period':14,'stop_atr':2.0,'tp1_atr':1.5,
                                   'tp1_fraction':0.5,'tp2_atr':3.0,'tp2_fraction':0.25,
                                   'trailing_atr':2.0,'max_holding_minutes':120,
                                   'round_trip_cost_pct':ROUND_TRIP_COST_PCT,
                                   'cooldown_minutes':120},
                'results':results,
                'limitations':[
                    'Only matured entries at or after frozen forward start are included.',
                    'This is a stateless rolling lookback, NOT a durable signal ledger.',
                    'Signals older than rolling days fall out of view; export periodic snapshots.',
                    'First 110+ candles are detector warmup; earliest forward entries may be missed if days window begins near start.',
                    'Frozen basket differs from dynamically selected V105 universe; cross-version raw comparisons are not paired.',
                    'V99 historical detector needs future bars to establish mature 120-minute outcomes; no live signals or orders.',
                    'Historical OHLC execution ambiguity, fees, slippage and short funding remain limitations.',
                    'Forward start date alone does not guarantee a fully prospective test if code/rules were previously tuned on similar data.'
                ]}
    except Exception as exc:
        return {**MODE_INFO,'status':'ERROR','signal':False,
                'study':'V106_FROZEN_FORWARD_PAPER','error':str(exc),
                'generated_utc':utc_now()}

# V107 READ-ONLY FORWARD SIGNAL VISIBILITY DIAGNOSTIC
# Does not change V96, V99, V106, any order routes, or frozen settings.
@app.get('/forward-signal-v107')
async def forward_signal_v107(days: int = Query(default=7, ge=2, le=30)):
    try:
        now_ms=int(datetime.now(timezone.utc).timestamp()*1000)
        async with httpx.AsyncClient(timeout=httpx.Timeout(240.0)) as client:
            sem=asyncio.Semaphore(2)
            async def worker(symbol):
                async with sem:
                    try:
                        cs=await get_5m_candles_days(client,symbol,days)
                        # Exclude the still-forming 5-minute candle.
                        cs=[c for c in cs if int(c['close_time'])<=now_ms]
                        n=len(cs)
                        o=[float(c['open']) for c in cs]
                        h=[float(c['high']) for c in cs]
                        l=[float(c['low']) for c in cs]
                        cl=[float(c['close']) for c in cs]
                        v=[float(c['volume']) for c in cs]
                        widths=[None]*n
                        for j in range(11,n):
                            lo=min(l[j-11:j+1]);widths[j]=(max(h[j-11:j+1])-lo)/lo*100 if lo>0 else None
                        stats={'completed_candles':n,'detector_windows':0,'compression_pass':0,
                               'breakout_pass':0,'confirmation_pass':0,'volume_ge_1_0':0,
                               'volume_ge_1_5':0,'entries_since_start':0,
                               'matured_since_start':0,'open_120m_since_start':0,
                               'unconfirmed_breakouts_since_start':0}
                        candidates=[]
                        for i in range(110,n):
                            base=[w for w in widths[i-73:i-1] if w is not None]
                            if len(base)!=72 or widths[i-1] is None:continue
                            stats['detector_windows']+=1
                            bm=median(base)
                            if bm<=0 or widths[i-1]>.65*bm:continue
                            stats['compression_pass']+=1
                            ceiling=max(h[i-24:i]);floor=min(l[i-24:i])
                            direction='SHORT' if cl[i]>ceiling else ('LONG' if cl[i]<floor else None)
                            if not direction:continue
                            stats['breakout_pass']+=1
                            avg=sum(v[i-20:i])/20
                            ratio=v[i]/avg if avg>0 else 0
                            # Confirm only on completed subsequent candles, within 30 minutes.
                            k_confirm=next((k for k in range(i+1,min(i+7,n))
                                            if floor<=cl[k]<=ceiling),None)
                            t=int(cs[i]['close_time'])
                            if k_confirm is None:
                                if t>=_V106_START_MS:
                                    stats['unconfirmed_breakouts_since_start']+=1
                                    candidates.append({'symbol':symbol,'stage':'BREAKOUT_UNCONFIRMED',
                                                       'direction':direction,'breakout_time_ms':t,
                                                       'volume_ratio':round(ratio,3)})
                                continue
                            stats['confirmation_pass']+=1
                            if ratio>=1:stats['volume_ge_1_0']+=1
                            if ratio>=1.5:stats['volume_ge_1_5']+=1
                            entry_idx=k_confirm+1
                            if entry_idx>=n:continue
                            entry_ms=int(cs[entry_idx]['open_time'])
                            if entry_ms<_V106_START_MS:continue
                            stats['entries_since_start']+=1
                            matured=entry_idx+24<n and entry_ms+120*60000<=now_ms
                            stats['matured_since_start' if matured else 'open_120m_since_start']+=1
                            candidates.append({'symbol':symbol,'stage':'MATURED_120M' if matured else 'OPEN_PAPER_120M',
                                               'direction':direction,'entry_time_ms':entry_ms,
                                               'entry_price':o[entry_idx],
                                               'volume_ratio':round(ratio,3),
                                               'eligible_ge_1_0':ratio>=1,
                                               'eligible_ge_1_5':ratio>=1.5,
                                               'minutes_since_entry':round((now_ms-entry_ms)/60000,1)})
                        return {'symbol':symbol,'stats':stats,'recent':candidates[-10:],'error':None}
                    except Exception as exc:
                        return {'symbol':symbol,'stats':None,'recent':[],'error':str(exc)}
            fetched=await asyncio.gather(*(worker(s) for s in _V106_SYMBOLS))
        failed=[{'symbol':x['symbol'],'error':x['error']} for x in fetched if x['error']]
        keys=('detector_windows','compression_pass','breakout_pass','confirmation_pass',
              'volume_ge_1_0','volume_ge_1_5','entries_since_start',
              'matured_since_start','open_120m_since_start','unconfirmed_breakouts_since_start')
        totals={k:sum(x['stats'][k] for x in fetched if x['stats']) for k in keys}
        latest=sorted((r for x in fetched for r in x['recent']),
                      key=lambda r:r.get('entry_time_ms',r.get('breakout_time_ms',0)),reverse=True)[:30]
        return {**MODE_INFO,'status':'OK' if not failed else 'PARTIAL',
                'signal':False,'study':'V107_FORWARD_SIGNAL_VISIBILITY',
                'generated_utc':utc_now(),'forward_start_ms':_V106_START_MS,
                'days_rolling_lookback':days,'symbols_fixed':list(_V106_SYMBOLS),
                'successful_coin_count':len(fetched)-len(failed),'failed':failed,
                'stage_totals':totals,'by_symbol':fetched,'latest_candidates':latest,
                'note':'Read-only diagnostic. OPEN_PAPER_120M is an inferred historical candidate, not a persisted live position or order. No execution or Telegram delivery.',
                'limitations':['No database ledger or live execution.','Same detector thresholds as V99; no parameter optimization.',
                               'Unconfirmed breakouts may still confirm in later candles.',
                               'Entry count is before 120-minute per-symbol cooldown.']}
    except Exception as exc:
        return {**MODE_INFO,'status':'ERROR','signal':False,'study':'V107_FORWARD_SIGNAL_VISIBILITY',
                'error':str(exc),'generated_utc':utc_now()}


# V108: all-volume forward paper event visibility; read-only, stateless.
# Frozen V106 benchmark is not modified. Historical reconstruction is NOT live execution.
@app.get('/forward-ledger-v108')
async def forward_ledger_v108(days: int = Query(default=7, ge=2, le=30)):
    try:
        now_ms=int(datetime.now(timezone.utc).timestamp()*1000)
        async with httpx.AsyncClient(timeout=httpx.Timeout(240.0)) as client:
            sem=asyncio.Semaphore(2)
            async def worker(symbol):
                async with sem:
                    try:
                        cs=await get_5m_candles_days(client,symbol,days)
                        cs=[c for c in cs if int(c['close_time'])<=now_ms]
                        n=len(cs)
                        op=[float(c['open']) for c in cs]
                        hi=[float(c['high']) for c in cs]
                        lo=[float(c['low']) for c in cs]
                        cl=[float(c['close']) for c in cs]
                        vol=[float(c['volume']) for c in cs]
                        widths=[None]*n
                        for j in range(11,n):
                            low12=min(lo[j-11:j+1])
                            widths[j]=(max(hi[j-11:j+1])-low12)/low12*100 if low12>0 else None
                        atrs=_v104_atr14(cs)
                        records=[]
                        for i in range(110,n):
                            baseline=[w for w in widths[i-73:i-1] if w is not None]
                            if len(baseline)!=72 or widths[i-1] is None:continue
                            base_med=median(baseline)
                            if base_med<=0 or widths[i-1]>.65*base_med:continue
                            ceiling=max(hi[i-24:i]);floor=min(lo[i-24:i])
                            direction='SHORT' if cl[i]>ceiling else ('LONG' if cl[i]<floor else None)
                            if direction is None:continue
                            avg=sum(vol[i-20:i])/20
                            if avg<=0:continue
                            ratio=vol[i]/avg
                            k_confirm=next((k for k in range(i+1,min(i+7,n))
                                            if floor<=cl[k]<=ceiling),None)
                            if k_confirm is None:continue
                            idx=k_confirm+1
                            if idx>=n or idx<15:continue
                            entry_ms=int(cs[idx]['open_time'])
                            if entry_ms<_V106_START_MS:continue
                            entry=op[idx]
                            if entry<=0:continue
                            matured=(idx+24<n and entry_ms+120*60000<=now_ms)
                            row={'symbol':symbol,'direction':direction,
                                 'entry_time_ms':entry_ms,'entry_price':round(entry,9),
                                 'volume_ratio':round(ratio,4),
                                 'volume_group':'GE_1_5' if ratio>=1.5 else ('GE_1_0' if ratio>=1 else 'LT_1_0'),
                                 'eligible_v106_ge_1_0':ratio>=1,
                                 'eligible_v106_ge_1_5':ratio>=1.5,
                                 'elapsed_minutes':round((now_ms-entry_ms)/60000,1),
                                 'stage':'CLOSED_120M' if matured else 'OPEN_PAPER_CANDIDATE',
                                 'paper_only':True}
                            if matured:
                                exit_price=op[idx+24]
                                gross=((exit_price/entry-1)*100 if direction=='LONG'
                                       else (entry-exit_price)/entry*100)
                                row['baseline_120m_net_pct']=round(gross-ROUND_TRIP_COST_PCT,5)
                                sim=_v105_strict(cs,idx,direction,atrs[idx-1],ROUND_TRIP_COST_PCT)
                                if sim is not None:
                                    row['atr_partial_net_pct']=round(sim['net_pct'],5)
                                    row['tp1_hit']=sim['tp1_hit']
                                    row['tp2_hit']=sim['tp2_hit']
                                    row['fills']=sim['fills']
                                    row['ambiguous_bar_count']=sim['ambiguous_bar_count']
                                else:
                                    row['atr_partial_net_pct']=None
                                    row['calculation_note']='ATR unavailable'
                            else:
                                row['last_completed_close']=round(cl[-1],9)
                                row['unrealized_gross_pct']=round(((cl[-1]/entry-1)*100 if direction=='LONG'
                                             else (entry-cl[-1])/entry*100),5)
                                atr=atrs[idx-1]
                                if atr is not None and atr>0:
                                    sign=1 if direction=='LONG' else -1
                                    row['initial_stop']=round(entry-sign*2*atr,9)
                                    row['tp1_price']=round(entry+sign*1.5*atr,9)
                                    row['tp2_price']=round(entry+sign*3*atr,9)
                                row['status_note']='Inferred from completed candles; not a stored live position. Exit path pending.'
                            records.append(row)
                        records.sort(key=lambda r:r['entry_time_ms'])
                        cooled=[];last=None
                        for r in records:
                            if last is not None and r['entry_time_ms']-last<120*60000:continue
                            cooled.append(r);last=r['entry_time_ms']
                        return {'symbol':symbol,'rows':cooled,'raw_count':len(records),'error':None}
                    except Exception as exc:
                        return {'symbol':symbol,'rows':[],'raw_count':0,'error':str(exc)}
            fetched=await asyncio.gather(*(worker(s) for s in _V106_SYMBOLS))
        failed=[{'symbol':x['symbol'],'error':x['error']} for x in fetched if x['error']]
        rows=sorted((r for x in fetched for r in x['rows']),key=lambda r:r['entry_time_ms'],reverse=True)
        closed=[r for r in rows if r['stage']=='CLOSED_120M']
        open_rows=[r for r in rows if r['stage']=='OPEN_PAPER_CANDIDATE']
        def summary(group):
            done=[r for r in group if r['stage']=='CLOSED_120M']
            base=[r['baseline_120m_net_pct'] for r in done]
            atr=[r['atr_partial_net_pct'] for r in done if r.get('atr_partial_net_pct') is not None]
            return {'all_entries':len(group),'closed_120m':len(done),
                    'open_candidates':len(group)-len(done),
                    'baseline':_v103_stats(base),'atr_partial':_v103_stats(atr)}
        groups={key:summary([r for r in rows if r['volume_group']==key])
                for key in ('LT_1_0','GE_1_0','GE_1_5')}
        return {**MODE_INFO,'status':'PARTIAL' if failed else 'OK','signal':False,
                'study':'V108_ALL_VOLUME_FORWARD_VISIBILITY',
                'generated_utc':utc_now(),'forward_start_ms':_V106_START_MS,
                'days_rolling_lookback':days,'symbols_fixed':list(_V106_SYMBOLS),
                'successful_coin_count':len(fetched)-len(failed),'failed':failed,
                'raw_entry_count':sum(x['raw_count'] for x in fetched),
                'cooled_entry_count':len(rows),'open_candidate_count':len(open_rows),
                'closed_120m_count':len(closed),
                'groups_exclusive':groups,
                'recent_open_candidates':open_rows[:20],
                'recent_closed_trades':closed[:30],
                'notes':['Read-only historical reconstruction, not live orders or durable positions.',
                         'Groups LT_1_0, GE_1_0 and GE_1_5 are mutually exclusive.',
                         'V106 volume thresholds and all original endpoints remain unchanged.',
                         'Only completed 5-minute bars are used; an open candidate may not be executable.',
                         'Trailing, stop and targets are simulated only for matured 120-minute trades.',
                         'Results are rolling and can change as new candles arrive; export snapshots.',
                         'Past-sample signals are not independently forward-logged.']}
    except Exception as exc:
        return {**MODE_INFO,'status':'ERROR','signal':False,
                'study':'V108_ALL_VOLUME_FORWARD_VISIBILITY','error':str(exc),
                'generated_utc':utc_now()}


# V109: BTC regime attribution, research only. Existing routes untouched.
# EMA(144) and EMA(576) on *completed* BTC 5m closes (~12h and 48h).
def _v109_ema(values, period):
    result=[None]*len(values)
    if len(values)<period:return result
    v=sum(values[:period])/period
    result[period-1]=v
    alpha=2/(period+1)
    for i in range(period,len(values)):
        v=alpha*values[i]+(1-alpha)*v
        result[i]=v
    return result

@app.get('/btc-regime-v109')
async def btc_regime_v109(days: int = Query(default=90,ge=30,le=90)):
    try:
        from bisect import bisect_right
        async with httpx.AsyncClient(timeout=httpx.Timeout(240.0)) as client:
            btc=await get_5m_candles_days(client,'BTCUSDT',days)
            btc_close=[float(c['close']) for c in btc]
            btc_times=[int(c['close_time']) for c in btc]
            fast=_v109_ema(btc_close,144)
            slow=_v109_ema(btc_close,576)
            sem=asyncio.Semaphore(2)
            async def worker(symbol):
                async with sem:
                    try:
                        candles=btc if symbol=='BTCUSDT' else await get_5m_candles_days(client,symbol,days)
                        return {'symbol':symbol,'rows':_v105_rows(candles,symbol),'error':None}
                    except Exception as exc:
                        return {'symbol':symbol,'rows':[],'error':str(exc)}
            fetched=await asyncio.gather(*(worker(s) for s in _V106_SYMBOLS))
        failures=[{'symbol':x['symbol'],'error':x['error']} for x in fetched if x['error']]
        raw=sorted((r for x in fetched for r in x['rows']),key=lambda r:(r['symbol'],r['entry_open_time']))
        cooled=[];last={}
        for r in raw:
            sym=r['symbol'];t=r['entry_open_time']
            if sym in last and t-last[sym]<120*60000:continue
            last[sym]=t
            # Use last BTC candle fully closed strictly before the entry open.
            idx=bisect_right(btc_times,t-1)-1
            if idx<0 or fast[idx] is None or slow[idx] is None:continue
            if btc_close[idx]>fast[idx]>slow[idx]:regime='UPTREND'
            elif btc_close[idx]<fast[idx]<slow[idx]:regime='DOWNTREND'
            else:regime='MIXED'
            group='LT_1_0' if r['volume_ratio']<1 else ('GE_1_0_TO_1_5' if r['volume_ratio']<1.5 else 'GE_1_5')
            cooled.append({**r,'btc_regime':regime,'volume_bucket':group})
        timestamps=sorted(r['entry_open_time'] for r in cooled)
        cutoff=timestamps[int(len(timestamps)*2/3)] if timestamps else None
        def summarize(rows):
            return {'baseline_120m':_v103_stats([r['baseline'] for r in rows]),
                    'atr_strict':_v103_stats([r['strict'] for r in rows]),
                    'tp1_hit_count':sum(bool(r['tp1_hit']) for r in rows),
                    'tp2_hit_count':sum(bool(r['tp2_hit']) for r in rows)}
        result={}
        for regime in ('UPTREND','DOWNTREND','MIXED'):
            result[regime]={}
            for direction in ('LONG','SHORT'):
                result[regime][direction]={}
                for bucket in ('LT_1_0','GE_1_0_TO_1_5','GE_1_5'):
                    subset=[r for r in cooled if r['btc_regime']==regime and r['direction']==direction and r['volume_bucket']==bucket]
                    result[regime][direction][bucket]={
                        'all':summarize(subset),
                        'discovery_first_2_3':summarize([r for r in subset if cutoff is not None and r['entry_open_time']<cutoff]),
                        'reference_last_1_3':summarize([r for r in subset if cutoff is not None and r['entry_open_time']>=cutoff])}
        return {**MODE_INFO,'status':'PARTIAL' if failures else 'OK','signal':False,
                'study':'V109_BTC_TREND_REGIME_RESEARCH','generated_utc':utc_now(),
                'days':days,'symbols_fixed':list(_V106_SYMBOLS),'successful_coin_count':len(fetched)-len(failures),
                'failed':failures,'raw_event_count':len(raw),'cooled_classified_count':len(cooled),
                'calendar_split_utc_ms':cutoff,'btc_ema_fast_5m_bars':144,'btc_ema_slow_5m_bars':576,
                'btc_regime_definition':{'UPTREND':'BTC close > EMA144 > EMA576',
                                         'DOWNTREND':'BTC close < EMA144 < EMA576',
                                         'MIXED':'All other conditions'},
                'volume_buckets_exclusive':True,'round_trip_cost_pct':ROUND_TRIP_COST_PCT,
                'results':result,
                'limitations':['Retrospective reused 90-day sample, NOT independent validation.',
                               'Fixed 10-symbol basket differs from dynamic V105 universe; results not paired.',
                               'BTC regime uses the last fully closed BTC candle before entry; no future candle used.',
                               'Regime is a descriptive segmentation, not a validated trading filter.',
                               'Small subgroup counts are unstable; multiple comparisons can overfit.',
                               'OHLC execution assumptions and fixed cost omit slippage, spreads and funding.',
                               'All existing routes and paper-only execution settings unchanged.']}
    except Exception as exc:
        return {**MODE_INFO,'status':'ERROR','signal':False,
                'study':'V109_BTC_TREND_REGIME_RESEARCH','error':str(exc),'generated_utc':utc_now()}


# V110 — robustness of the V109 selected cohort; retrospective only.
# Does not change detector, original routes, trade execution, or frozen forward rules.
@app.get('/btc-regime-robustness-v110')
async def btc_regime_robustness_v110(days: int = Query(default=90, ge=30, le=90)):
    try:
        from bisect import bisect_right
        from collections import defaultdict
        from datetime import datetime as _dt, timezone as _tz
        async with httpx.AsyncClient(timeout=httpx.Timeout(240.0)) as client:
            btc=await get_5m_candles_days(client,'BTCUSDT',days)
            bt=[int(c['close_time']) for c in btc]
            bc=[float(c['close']) for c in btc]
            fast=_v109_ema(bc,144)
            slow=_v109_ema(bc,576)
            sem=asyncio.Semaphore(2)
            async def worker(symbol):
                async with sem:
                    try:
                        cs=btc if symbol=='BTCUSDT' else await get_5m_candles_days(client,symbol,days)
                        return {'symbol':symbol,'rows':_v105_rows(cs,symbol),'error':None}
                    except Exception as exc:
                        return {'symbol':symbol,'rows':[],'error':str(exc)}
            fetched=await asyncio.gather(*(worker(sym) for sym in _V106_SYMBOLS))
        failed=[{'symbol':x['symbol'],'error':x['error']} for x in fetched if x['error']]
        raw=sorted((r for x in fetched for r in x['rows']),key=lambda r:(r['symbol'],r['entry_open_time']))
        cooled=[];last={}
        for r in raw:
            sym=r['symbol'];t=r['entry_open_time']
            if sym in last and t-last[sym]<120*60000:continue
            last[sym]=t
            idx=bisect_right(bt,t-1)-1
            if idx<0 or fast[idx] is None or slow[idx] is None:continue
            regime=('UPTREND' if bc[idx]>fast[idx]>slow[idx] else
                    'DOWNTREND' if bc[idx]<fast[idx]<slow[idx] else 'MIXED')
            cooled.append({**r,'btc_regime':regime})
        timestamps=sorted(r['entry_open_time'] for r in cooled)
        cutoff=timestamps[int(len(timestamps)*2/3)] if timestamps else None
        selected=sorted((r for r in cooled if r['btc_regime']=='DOWNTREND' and
                         r['direction']=='LONG' and r['volume_ratio']>=1.5),
                        key=lambda r:r['entry_open_time'])
        base_cost=float(ROUND_TRIP_COST_PCT)
        def metrics(rs,extra_cost=0.0):
            return _v103_stats([r['baseline']-extra_cost for r in rs])
        cost_scenarios={}
        for cost in (0.15,0.20,0.25,0.35,0.50):
            # Original baseline already includes the configured round-trip cost.
            extra=cost-base_cost
            cost_scenarios[str(cost)]={'total_round_trip_cost_pct':cost,
                                      'baseline_120m':metrics(selected,extra)}
        by_symbol={}
        for sym in _V106_SYMBOLS:
            ss=[r for r in selected if r['symbol']==sym]
            by_symbol[sym]={'baseline_120m':metrics(ss),
                            'atr_strict':_v103_stats([r['strict'] for r in ss]),
                            'sum_net_percentage_points':round(sum(r['baseline'] for r in ss),5)}
        by_day=defaultdict(list)
        for r in selected:
            day=_dt.fromtimestamp(r['entry_open_time']/1000,_tz.utc).strftime('%Y-%m-%d')
            by_day[day].append(r)
        days_sorted=sorted(by_day)
        daily_sums=[sum(r['baseline'] for r in by_day[d]) for d in days_sorted]
        positives=sorted((r for r in selected if r['baseline']>0),key=lambda r:r['baseline'],reverse=True)
        remove_top=max(1,int(len(selected)*0.05)) if selected else 0
        excluded_ids={id(r) for r in positives[:remove_top]}
        without_top=[r for r in selected if id(r) not in excluded_ids]
        total=sum(r['baseline'] for r in selected)
        top5=sum(r['baseline'] for r in positives[:remove_top])
        # Concentration: daily sums are not compounded portfolio returns; overlapping positions possible.
        # One-day-at-a-time leave-out diagnostic tests sensitivity to single clustered days.
        leave_one_day_out=[]
        for day in days_sorted:
            rest=[r for r in selected if r not in by_day[day]]
            if rest:
                leave_one_day_out.append({'excluded_day':day,'n':len(rest),
                                          'mean_net_pct':round(sum(r['baseline'] for r in rest)/len(rest),5)})
        leave_one_day_out.sort(key=lambda x:x['mean_net_pct'])
        return {**MODE_INFO,'status':'PARTIAL' if failed else 'OK','signal':False,
                'study':'V110_BTC_DOWNTREND_LONG_HIGH_VOLUME_ROBUSTNESS',
                'generated_utc':utc_now(),'days':days,'symbols_fixed':list(_V106_SYMBOLS),
                'successful_coin_count':len(fetched)-len(failed),'failed':failed,
                'raw_event_count':len(raw),'cooled_classified_count':len(cooled),
                'calendar_split_utc_ms':cutoff,
                'selected_cohort':{'btc_regime':'DOWNTREND','direction':'LONG',
                                   'volume_ratio_min':1.5,'exit':'fixed_120m',
                                   'selection_origin':'V109 retrospective discovery'},
                'all':{'baseline_120m':metrics(selected),
                       'atr_strict':_v103_stats([r['strict'] for r in selected])},
                'discovery_first_2_3':metrics([r for r in selected if cutoff is not None and r['entry_open_time']<cutoff]),
                'reference_last_1_3':metrics([r for r in selected if cutoff is not None and r['entry_open_time']>=cutoff]),
                'cost_scenarios':cost_scenarios,'by_symbol':by_symbol,
                'concentration':{'active_utc_days':len(days_sorted),
                                 'positive_day_count':sum(v>0 for v in daily_sums),
                                 'negative_day_count':sum(v<0 for v in daily_sums),
                                 'zero_day_count':sum(v==0 for v in daily_sums),
                                 'top_5pct_trade_count':remove_top,
                                 'top_5pct_positive_sum_pp':round(top5,5),
                                 'all_trades_sum_pp':round(total,5),
                                 'excluding_top_5pct_positive_trades':metrics(without_top),
                                 'best_5_utc_days_by_sum_pp':sorted(
                                     ({'utc_day':d,'n':len(by_day[d]),
                                       'sum_net_pp':round(sum(r['baseline'] for r in by_day[d]),5)}
                                      for d in days_sorted),key=lambda x:x['sum_net_pp'],reverse=True)[:5],
                                 'worst_5_utc_days_by_sum_pp':sorted(
                                     ({'utc_day':d,'n':len(by_day[d]),
                                       'sum_net_pp':round(sum(r['baseline'] for r in by_day[d]),5)}
                                      for d in days_sorted),key=lambda x:x['sum_net_pp'])[:5],
                                 'leave_one_day_out_worst_5':leave_one_day_out[:5],
                                 'leave_one_day_out_best_5':leave_one_day_out[-5:]},
                'limitations':['Same reused 90-day data; not independent forward validation.',
                               'V109 cohort was selected after examining many groups: selection bias.',
                               'Cost scenarios add incremental percentage-point costs to net returns; no market impact model.',
                               'No funding, true spread, order-book liquidity, or intrabar path reconstruction.',
                               'Same 10-symbol basket and 120m per-symbol cooldown as V109.',
                               'Overlapping trades may share market shocks; day sums are not portfolio returns.',
                               'This is research-only; no orders, execution, or trading signals.']}
    except Exception as exc:
        return {**MODE_INFO,'status':'ERROR','signal':False,
                'study':'V110_BTC_DOWNTREND_LONG_HIGH_VOLUME_ROBUSTNESS',
                'error':str(exc),'generated_utc':utc_now()}


# V111: prospective observation audit, read-only, with local SQLite journal.
# IMPORTANT: Render ephemeral filesystem is NOT durable across redeploys/restarts.
# An external persistent database and an independent scheduler are required for
# truly durable unattended prospective collection. No retrospective backfill.
@app.get('/prospective-audit-v111')
async def prospective_audit_v111():
    import os, sqlite3
    from bisect import bisect_right
    now_ms=int(datetime.now(timezone.utc).timestamp()*1000)
    path=os.getenv('V111_SQLITE_PATH','/tmp/alt_momentum_v111.sqlite3')
    try:
        con=sqlite3.connect(path,timeout=15)
        con.execute('CREATE TABLE IF NOT EXISTS observations (id INTEGER PRIMARY KEY AUTOINCREMENT, observed_ms INTEGER NOT NULL, symbol TEXT NOT NULL, entry_ms INTEGER NOT NULL, direction TEXT NOT NULL, entry_price REAL NOT NULL, volume_ratio REAL NOT NULL, btc_regime TEXT NOT NULL, UNIQUE(symbol,entry_ms,direction))')
        con.execute('CREATE TABLE IF NOT EXISTS outcomes (observation_id INTEGER PRIMARY KEY, exit_price REAL NOT NULL, net_pct REAL NOT NULL, settled_ms INTEGER NOT NULL)')
        con.commit()
        async with httpx.AsyncClient(timeout=httpx.Timeout(240.0)) as client:
            sem=asyncio.Semaphore(2)
            async def worker(sym):
                async with sem:
                    try:
                        cs=await get_5m_candles_days(client,sym,3)
                        cs=[c for c in cs if int(c['close_time'])<=now_ms]
                        return sym,cs,None
                    except Exception as e:return sym,[],str(e)
            fetched=await asyncio.gather(*(worker(sym) for sym in _V106_SYMBOLS))
        data={sym:cs for sym,cs,err in fetched if err is None}
        failed=[{'symbol':sym,'error':err} for sym,cs,err in fetched if err]
        btc=data.get('BTCUSDT',[])
        btc_close=[float(c['close']) for c in btc]
        btc_times=[int(c['close_time']) for c in btc]
        fast=_v109_ema(btc_close,144);slow=_v109_ema(btc_close,576)
        added=0;settled=0;skipped_stale=0
        for sym,cs in data.items():
            n=len(cs)
            if n<580:continue
            o=[float(c['open']) for c in cs];h=[float(c['high']) for c in cs]
            l=[float(c['low']) for c in cs];cl=[float(c['close']) for c in cs]
            v=[float(c['volume']) for c in cs]
            widths=[None]*n
            for j in range(11,n):
                low=min(l[j-11:j+1]);widths[j]=(max(h[j-11:j+1])-low)/low*100 if low>0 else None
            # Only recently completed entry candles (up to 15 minutes old) may be recorded.
            # This prevents old historical signals from being called prospective.
            for i in range(max(110,n-12),n-1):
                base=[w for w in widths[i-73:i-1] if w is not None]
                if len(base)!=72 or widths[i-1] is None:continue
                bm=median(base)
                if bm<=0 or widths[i-1]>.65*bm:continue
                ceiling=max(h[i-24:i]);floor=min(l[i-24:i])
                direction='SHORT' if cl[i]>ceiling else ('LONG' if cl[i]<floor else None)
                if not direction:continue
                avg=sum(v[i-20:i])/20
                if avg<=0:continue
                ratio=v[i]/avg
                confirm=next((k for k in range(i+1,min(i+7,n)) if floor<=cl[k]<=ceiling),None)
                if confirm is None:continue
                entry_idx=confirm+1
                if entry_idx>=n:continue
                entry_ms=int(cs[entry_idx]['open_time'])
                if entry_ms<_V106_START_MS:continue
                if not (0<=now_ms-entry_ms<900000):
                    skipped_stale+=1;continue
                ix=bisect_right(btc_times,entry_ms-1)-1
                if ix<0 or fast[ix] is None or slow[ix] is None:continue
                regime='UPTREND' if btc_close[ix]>fast[ix]>slow[ix] else ('DOWNTREND' if btc_close[ix]<fast[ix]<slow[ix] else 'MIXED')
                if regime!='DOWNTREND' or direction!='LONG' or ratio<1.5:continue
                # Do not claim an entry at the historical open is executable;
                # this is a delayed near-real-time paper observation only.
                before=con.total_changes
                con.execute('INSERT OR IGNORE INTO observations(observed_ms,symbol,entry_ms,direction,entry_price,volume_ratio,btc_regime) VALUES (?,?,?,?,?,?,?)',(now_ms,sym,entry_ms,direction,o[entry_idx],ratio,regime))
                if con.total_changes>before:added+=1
        con.commit()
        rows=con.execute('SELECT id,symbol,entry_ms,direction,entry_price FROM observations WHERE id NOT IN (SELECT observation_id FROM outcomes)').fetchall()
        for oid,sym,entry_ms,direction,price in rows:
            cs=data.get(sym,[])
            lookup={int(c['open_time']):float(c['open']) for c in cs}
            exit_ms=entry_ms+120*60000
            if exit_ms not in lookup:continue
            exit_price=lookup[exit_ms]
            gross=(exit_price/price-1)*100 if direction=='LONG' else (price-exit_price)/price*100
            con.execute('INSERT OR IGNORE INTO outcomes VALUES (?,?,?,?)',(oid,exit_price,gross-ROUND_TRIP_COST_PCT,now_ms))
            settled+=1
        con.commit()
        complete=con.execute('SELECT o.net_pct FROM outcomes o JOIN observations s ON s.id=o.observation_id ORDER BY s.entry_ms').fetchall()
        count=con.execute('SELECT COUNT(*) FROM observations').fetchone()[0]
        recent=con.execute('SELECT s.symbol,s.entry_ms,s.observed_ms,s.volume_ratio,s.btc_regime,o.net_pct FROM observations s LEFT JOIN outcomes o ON o.observation_id=s.id ORDER BY s.entry_ms DESC LIMIT 15').fetchall()
        con.close()
        return {**MODE_INFO,'status':'PARTIAL' if failed else 'OK','signal':False,
                'study':'V111_LOCAL_PROSPECTIVE_OBSERVATION_AUDIT','generated_utc':utc_now(),
                'observation_count':count,'new_observations_this_call':added,
                'settled_count':len(complete),'new_settlements_this_call':settled,
                'results_120m':_v103_stats([r[0] for r in complete]),
                'recent':[{'symbol':r[0],'entry_ms':r[1],'observed_ms':r[2],
                           'volume_ratio':round(r[3],4),'btc_regime':r[4],
                           'net_pct':round(r[5],5) if r[5] is not None else None} for r in recent],
                'failed':failed,'storage':'LOCAL_SQLITE_EPHEMERAL',
                'limitations':['Not durable on Render free ephemeral filesystem: restart/redeploy can erase observations.',
                               'Requires calls at least once per 5-minute candle; no scheduler is installed.',
                               'No bulk backfill; a recently completed entry may be observed up to 15 minutes late.',
                               'Entry price is observed candle open, not a verified executable fill.',
                               'Candle close availability, API delays and polling gaps can miss observations.',
                               '120m outcomes require a subsequent call while exit candle remains in fetched window.',
                               'Historical selection bias persists; costs exclude spread/slippage/funding.',
                               'Research only; no orders, trading or execution.']}
    except Exception as exc:
        return {**MODE_INFO,'status':'ERROR','signal':False,
                'study':'V111_LOCAL_PROSPECTIVE_OBSERVATION_AUDIT',
                'error':str(exc),'generated_utc':utc_now()}


# V112: durable prospective PostgreSQL observations, isolated schema.
# Requires DATABASE_URL and external 5-minute scheduler. Read-only market access.
@app.get('/prospective-audit-v112')
async def prospective_audit_v112():
    from bisect import bisect_right
    now_ms=int(datetime.now(timezone.utc).timestamp()*1000)
    url=os.getenv('DATABASE_URL','').strip()
    if not url:
        return {**MODE_INFO,'status':'ERROR','study':'V112_POSTGRES_PROSPECTIVE_AUDIT','error':'DATABASE_URL missing','signal':False}
    try:
        # V112 writes only to its own qualified schema and tables; V7 tables are untouched.
        with psycopg.connect(url,connect_timeout=15) as con:
            with con.cursor() as cur:
                cur.execute('CREATE SCHEMA IF NOT EXISTS alt_momentum_v112')
                cur.execute('CREATE TABLE IF NOT EXISTS alt_momentum_v112.observations (id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY, observed_ms BIGINT NOT NULL, symbol TEXT NOT NULL, entry_ms BIGINT NOT NULL, direction TEXT NOT NULL, entry_price DOUBLE PRECISION NOT NULL, volume_ratio DOUBLE PRECISION NOT NULL, btc_regime TEXT NOT NULL, UNIQUE(symbol,entry_ms,direction))')
                cur.execute('CREATE TABLE IF NOT EXISTS alt_momentum_v112.outcomes (observation_id BIGINT PRIMARY KEY REFERENCES alt_momentum_v112.observations(id), exit_price DOUBLE PRECISION NOT NULL, gross_pct DOUBLE PRECISION NOT NULL, settled_ms BIGINT NOT NULL)')
                cur.execute('CREATE TABLE IF NOT EXISTS alt_momentum_v112.polls (polled_ms BIGINT PRIMARY KEY, observed_added INTEGER NOT NULL, settled_added INTEGER NOT NULL, failures INTEGER NOT NULL)')
                con.commit()
                added=0;settled=0;skipped_stale=0
                async with httpx.AsyncClient(timeout=httpx.Timeout(240.0)) as client:
                    sem=asyncio.Semaphore(2)
                    async def worker(sym):
                        async with sem:
                            try:
                                cs=await get_5m_candles_days(client,sym,3)
                                cs=[c for c in cs if int(c['close_time'])<=now_ms]
                                return sym,cs,None
                            except Exception as e:return sym,[],str(e)
                    fetched=await asyncio.gather(*(worker(sym) for sym in _V106_SYMBOLS))
                data={sym:cs for sym,cs,err in fetched if err is None}
                failed=[{'symbol':sym,'error':err} for sym,cs,err in fetched if err]
                btc=data.get('BTCUSDT',[])
                btc_close=[float(c['close']) for c in btc]
                btc_times=[int(c['close_time']) for c in btc]
                fast=_v109_ema(btc_close,144);slow=_v109_ema(btc_close,576)
                added=0;settled=0;skipped_stale=0
                for sym,cs in data.items():
                    n=len(cs)
                    if n<580:continue
                    o=[float(c['open']) for c in cs];h=[float(c['high']) for c in cs]
                    l=[float(c['low']) for c in cs];cl=[float(c['close']) for c in cs]
                    v=[float(c['volume']) for c in cs]
                    widths=[None]*n
                    for j in range(11,n):
                        low=min(l[j-11:j+1]);widths[j]=(max(h[j-11:j+1])-low)/low*100 if low>0 else None
                    # Only recently completed entry candles (up to 15 minutes old) may be recorded.
                    # This prevents old historical signals from being called prospective.
                    for i in range(max(110,n-12),n-1):
                        base=[w for w in widths[i-73:i-1] if w is not None]
                        if len(base)!=72 or widths[i-1] is None:continue
                        bm=median(base)
                        if bm<=0 or widths[i-1]>.65*bm:continue
                        ceiling=max(h[i-24:i]);floor=min(l[i-24:i])
                        direction='SHORT' if cl[i]>ceiling else ('LONG' if cl[i]<floor else None)
                        if not direction:continue
                        avg=sum(v[i-20:i])/20
                        if avg<=0:continue
                        ratio=v[i]/avg
                        confirm=next((k for k in range(i+1,min(i+7,n)) if floor<=cl[k]<=ceiling),None)
                        if confirm is None:continue
                        entry_idx=confirm+1
                        if entry_idx>=n:continue
                        entry_ms=int(cs[entry_idx]['open_time'])
                        if entry_ms<_V106_START_MS:continue
                        if not (0<=now_ms-entry_ms<900000):
                            skipped_stale+=1;continue
                        ix=bisect_right(btc_times,entry_ms-1)-1
                        if ix<0 or fast[ix] is None or slow[ix] is None:continue
                        regime='UPTREND' if btc_close[ix]>fast[ix]>slow[ix] else ('DOWNTREND' if btc_close[ix]<fast[ix]<slow[ix] else 'MIXED')
                        if regime!='DOWNTREND' or direction!='LONG' or ratio<1.5:continue
                        # Do not claim an entry at the historical open is executable;
                        # this is a delayed near-real-time paper observation only.
                        cur.execute('INSERT INTO alt_momentum_v112.observations(observed_ms,symbol,entry_ms,direction,entry_price,volume_ratio,btc_regime) VALUES (%s,%s,%s,%s,%s,%s,%s) ON CONFLICT(symbol,entry_ms,direction) DO NOTHING RETURNING id',(now_ms,sym,entry_ms,direction,o[entry_idx],ratio,regime))
                        if cur.fetchone() is not None:added+=1

                con.commit()
                cur.execute('SELECT id,symbol,entry_ms,direction,entry_price FROM alt_momentum_v112.observations WHERE id NOT IN (SELECT observation_id FROM alt_momentum_v112.outcomes)')
                pending=cur.fetchall()
                for oid,sym,entry_ms,direction,price in pending:
                    cs=data.get(sym,[])
                    lookup={int(c['open_time']):float(c['open']) for c in cs}
                    exit_ms=entry_ms+120*60000
                    if exit_ms not in lookup:continue
                    exit_price=lookup[exit_ms]
                    gross=(exit_price/price-1)*100 if direction=='LONG' else (price-exit_price)/price*100
                    cur.execute('INSERT INTO alt_momentum_v112.outcomes(observation_id,exit_price,gross_pct,settled_ms) VALUES (%s,%s,%s,%s) ON CONFLICT(observation_id) DO NOTHING RETURNING observation_id',(oid,exit_price,gross,now_ms))
                    if cur.fetchone() is not None:settled+=1
                cur.execute('INSERT INTO alt_momentum_v112.polls VALUES (%s,%s,%s,%s) ON CONFLICT(polled_ms) DO NOTHING',(now_ms,added,settled,len(failed)))
                con.commit()
                cur.execute('SELECT o.gross_pct FROM alt_momentum_v112.outcomes o JOIN alt_momentum_v112.observations s ON s.id=o.observation_id ORDER BY s.entry_ms')
                gross_returns=[float(r[0]) for r in cur.fetchall()]
                cur.execute('SELECT COUNT(*) FROM alt_momentum_v112.observations')
                count=cur.fetchone()[0]
                cur.execute('SELECT s.symbol,s.entry_ms,s.observed_ms,s.volume_ratio,s.btc_regime,o.gross_pct FROM alt_momentum_v112.observations s LEFT JOIN alt_momentum_v112.outcomes o ON o.observation_id=s.id ORDER BY s.entry_ms DESC LIMIT 15')
                recent=cur.fetchall()
                cur.execute('SELECT COUNT(*),MAX(polled_ms) FROM alt_momentum_v112.polls')
                poll_count,last_poll=cur.fetchone()
                scenarios={str(cost):_v103_stats([g-cost for g in gross_returns]) for cost in (0.15,0.20,0.25,0.35)}
                return {**MODE_INFO,'status':'PARTIAL' if failed else 'OK','signal':False,
                        'study':'V112_POSTGRES_PROSPECTIVE_AUDIT','generated_utc':utc_now(),
                        'observation_count':count,'new_observations_this_call':added,
                        'settled_count':len(gross_returns),'new_settlements_this_call':settled,
                        'cost_scenarios':scenarios,
                        'recent':[{'symbol':r[0],'entry_ms':r[1],'observed_ms':r[2],
                                   'volume_ratio':round(r[3],4),'btc_regime':r[4],
                                   'gross_pct':round(r[5],5) if r[5] is not None else None} for r in recent],
                        'poll_count':poll_count,'last_poll_ms':last_poll,
                        'skipped_stale_candidates':skipped_stale,'failed':failed,
                        'storage':'POSTGRES_SCHEMA_alt_momentum_v112',
                        'limitations':['Endpoint is pull-based; an external scheduler must invoke every 5 minutes.',
                                       'No retrospective backfill; only observations near time of first detection.',
                                       'Entry is observed candle open, not executable fill; latency and gaps may miss signals.',
                                       'Costs are scenarios, not observed spread/slippage/funding.',
                                       'Past strategy selection bias remains; no orders or trading.']}
    except Exception as exc:
        return {**MODE_INFO,'status':'ERROR','signal':False,'study':'V112_POSTGRES_PROSPECTIVE_AUDIT',
                'generated_utc':utc_now(),'error_type':type(exc).__name__,
                'error':'PostgreSQL connection or audit failed; check Render logs (secrets omitted).'}

# V113: read-only PostgreSQL research quality dashboard. Does not trigger market polling.
@app.get('/research-quality-v113')
async def research_quality_v113():
    from collections import defaultdict
    from statistics import mean, median
    now_ms = int(datetime.now(timezone.utc).timestamp() * 1000)
    url = os.getenv('DATABASE_URL', '').strip()
    if not url:
        return {**MODE_INFO, 'status':'ERROR', 'study':'V113_RESEARCH_QUALITY', 'signal':False, 'error':'DATABASE_URL missing'}
    try:
        with psycopg.connect(url, connect_timeout=15) as con:
            with con.cursor() as cur:
                # All reads are restricted to the V112 schema; no CREATE/UPDATE/DELETE.
                cur.execute('SELECT polled_ms, observed_added, settled_added, failures FROM alt_momentum_v112.polls ORDER BY polled_ms')
                polls = cur.fetchall()
                cur.execute('SELECT s.id,s.symbol,s.entry_ms,s.observed_ms,s.direction,s.entry_price,s.volume_ratio,s.btc_regime,o.gross_pct,o.settled_ms FROM alt_momentum_v112.observations s LEFT JOIN alt_momentum_v112.outcomes o ON o.observation_id=s.id ORDER BY s.entry_ms')
                rows = cur.fetchall()
        times = [int(p[0]) for p in polls]
        gaps = [(times[i]-times[i-1])/1000 for i in range(1,len(times))]
        long_gaps = [{'from_ms':times[i-1], 'to_ms':times[i], 'gap_minutes':round((times[i]-times[i-1])/60000,2)} for i in range(1,len(times)) if times[i]-times[i-1]>450000]
        # Number of absent 5-minute intervals is an estimate; manual requests and jitter affect it.
        estimated_missing = sum(max(0, round(g/300)-1) for g in gaps)
        settled = [r for r in rows if r[8] is not None]
        pending = [r for r in rows if r[8] is None]
        delays = [(int(r[3])-int(r[2]))/1000 for r in rows]
        by_symbol = defaultdict(list)
        for r in settled: by_symbol[r[1]].append(float(r[8]))
        costs = (0.15,0.20,0.25,0.35)
        gross = [float(r[8]) for r in settled]
        cost_results = {str(c):_v103_stats([g-c for g in gross]) for c in costs}
        concentration = None
        if gross:
            net = sorted([g-0.15 for g in gross], reverse=True)
            k=min(5,len(net))
            concentration = {'sample_n':len(net),'top_5_or_fewer_sum_net_pct_points':round(sum(net[:k]),5),
                             'all_sum_net_pct_points':round(sum(net),5),
                             'excluding_top_5_or_fewer':_v103_stats(net[k:]) if len(net)>k else None}
        return {**MODE_INFO,'status':'OK','signal':False,'study':'V113_RESEARCH_QUALITY',
                'generated_utc':utc_now(),'storage':'POSTGRES_SCHEMA_alt_momentum_v112_READ_ONLY',
                'polls':{'count':len(polls),'first_ms':times[0] if times else None,'last_ms':times[-1] if times else None,
                         'age_of_last_poll_minutes':round((now_ms-times[-1])/60000,2) if times else None,
                         'average_interval_seconds':round(mean(gaps),2) if gaps else None,
                         'median_interval_seconds':round(median(gaps),2) if gaps else None,
                         'estimated_missing_5m_slots':estimated_missing,
                         'gaps_over_7_5_minutes':long_gaps[-20:],
                         'polls_with_fetch_failures':sum(1 for p in polls if p[3]>0),
                         'sum_symbol_fetch_failures':sum(int(p[3]) for p in polls)},
                'observations':{'count':len(rows),'settled_count':len(settled),'pending_count':len(pending),
                                'median_observation_delay_seconds':round(median(delays),2) if delays else None,
                                'max_observation_delay_seconds':round(max(delays),2) if delays else None,
                                'pending_over_150_minutes':sum(1 for r in pending if now_ms-int(r[2])>150*60000)},
                'net_120m_by_cost_pct':cost_results,
                'by_symbol_at_cost_0_15_pct':{s:_v103_stats([g-0.15 for g in values]) for s,values in sorted(by_symbol.items())},
                'top_winner_concentration_at_cost_0_15_pct':concentration,
                'limitations':['Read-only dashboard: calling this endpoint does not create a poll.',
                               'Missing slots are inferred from poll timestamps, not verified cron-job.org executions.',
                               'Manual polling and scheduler jitter distort gap estimates.',
                               'Small prospective samples cannot establish profitability.',
                               'Entry and exit use candle opens; no executable fills or observed slippage.',
                               'Research only; no orders, signals, or live trading.']}
    except Exception as exc:
        return {**MODE_INFO,'status':'ERROR','signal':False,'study':'V113_RESEARCH_QUALITY',
                'generated_utc':utc_now(),'error_type':type(exc).__name__,
                'error':'Research quality query failed; inspect Render logs (secrets omitted).'}


# V114: non-mutating funnel diagnostic. This endpoint is exploratory and
# does not insert observations, trigger a V112 poll, or change entry rules.
@app.get('/candidate-funnel-v114')
async def candidate_funnel_v114():
    from bisect import bisect_right
    from statistics import median
    now_ms=int(datetime.now(timezone.utc).timestamp()*1000)
    try:
        async with httpx.AsyncClient(timeout=httpx.Timeout(240.0)) as client:
            sem=asyncio.Semaphore(2)
            async def worker(sym):
                async with sem:
                    try:
                        cs=await get_5m_candles_days(client,sym,3)
                        return sym,[c for c in cs if int(c['close_time'])<=now_ms],None
                    except Exception as exc:
                        return sym,[],type(exc).__name__
            fetched=await asyncio.gather(*(worker(sym) for sym in _V106_SYMBOLS))
        data={sym:cs for sym,cs,err in fetched if err is None}
        failed=[{'symbol':sym,'error_type':err} for sym,cs,err in fetched if err]
        btc=data.get('BTCUSDT',[])
        btc_close=[float(c['close']) for c in btc]
        btc_times=[int(c['close_time']) for c in btc]
        fast=_v109_ema(btc_close,144)
        slow=_v109_ema(btc_close,576)
        keys=('evaluated_candles','width_valid','compression_pass','breakout_pass','volume_valid','confirmation_pass','entry_candle_available','fresh_entry','btc_regime_available','btc_downtrend','long_direction','volume_ge_1_5','final_candidates')
        counts={k:0 for k in keys}
        by_symbol={}
        # Match V112's trailing candidate window exactly. Counts are sequential
        # funnel stages, not independent mutually exclusive rejection reasons.
        for sym,cs in data.items():
            local={k:0 for k in keys}
            n=len(cs)
            if n<580:
                by_symbol[sym]={'status':'INSUFFICIENT_CANDLES','candles':n,'funnel':local}
                continue
            o=[float(c['open']) for c in cs]
            h=[float(c['high']) for c in cs]
            l=[float(c['low']) for c in cs]
            cl=[float(c['close']) for c in cs]
            v=[float(c['volume']) for c in cs]
            widths=[None]*n
            for j in range(11,n):
                low=min(l[j-11:j+1])
                widths[j]=(max(h[j-11:j+1])-low)/low*100 if low>0 else None
            for i in range(max(110,n-12),n-1):
                local['evaluated_candles']+=1
                base=[w for w in widths[i-73:i-1] if w is not None]
                if len(base)!=72 or widths[i-1] is None:continue
                local['width_valid']+=1
                bm=median(base)
                if bm<=0 or widths[i-1]>.65*bm:continue
                local['compression_pass']+=1
                ceiling=max(h[i-24:i]);floor=min(l[i-24:i])
                direction='SHORT' if cl[i]>ceiling else ('LONG' if cl[i]<floor else None)
                if not direction:continue
                local['breakout_pass']+=1
                avg=sum(v[i-20:i])/20
                if avg<=0:continue
                local['volume_valid']+=1
                ratio=v[i]/avg
                confirm=next((k for k in range(i+1,min(i+7,n)) if floor<=cl[k]<=ceiling),None)
                if confirm is None:continue
                local['confirmation_pass']+=1
                entry_idx=confirm+1
                if entry_idx>=n:continue
                local['entry_candle_available']+=1
                entry_ms=int(cs[entry_idx]['open_time'])
                if entry_ms<_V106_START_MS or not (0<=now_ms-entry_ms<900000):continue
                local['fresh_entry']+=1
                ix=bisect_right(btc_times,entry_ms-1)-1
                if ix<0 or fast[ix] is None or slow[ix] is None:continue
                local['btc_regime_available']+=1
                regime='UPTREND' if btc_close[ix]>fast[ix]>slow[ix] else ('DOWNTREND' if btc_close[ix]<fast[ix]<slow[ix] else 'MIXED')
                if regime!='DOWNTREND':continue
                local['btc_downtrend']+=1
                if direction!='LONG':continue
                local['long_direction']+=1
                if ratio<1.5:continue
                local['volume_ge_1_5']+=1
                local['final_candidates']+=1
            for key in keys:counts[key]+=local[key]
            by_symbol[sym]={'status':'OK','candles':n,'funnel':local}
        return {**MODE_INFO,'status':'PARTIAL' if failed else 'OK','signal':False,
                'study':'V114_CANDIDATE_FUNNEL_DIAGNOSTIC','generated_utc':utc_now(),
                'lookback':'V112 most recent 11 candidate candle indices per symbol, as available at call time',
                'funnel_order':list(keys),'funnel':counts,'by_symbol':by_symbol,
                'fetch_failures':failed,'writes_to_database':False,
                'limitations':['Snapshot diagnostic only: not a persisted audit of previous scheduler calls.',
                               'Stages are sequential; differences indicate first stage failed, not independent causes.',
                               'Counts may overlap across successive manual calls; do not add snapshots.',
                               'Freshness is evaluated at diagnostic call time, not past scheduler time.',
                               'No trading, orders, signals, or changes to V112 entry criteria.']}
    except Exception as exc:
        return {**MODE_INFO,'status':'ERROR','signal':False,
                'study':'V114_CANDIDATE_FUNNEL_DIAGNOSTIC',
                'generated_utc':utc_now(),'error_type':type(exc).__name__}


# V115: Historical funnel attribution; read-only, no database writes.
# Historical candidates are NOT prospective observations.
@app.get('/historical-funnel-v115')
async def historical_funnel_v115(days: int = Query(default=90, ge=7, le=90)):
    from bisect import bisect_right
    from collections import Counter
    from statistics import median
    now_ms = int(datetime.now(timezone.utc).timestamp() * 1000)
    keys = ('evaluated_candles','width_valid','compression_pass','breakout_pass',
            'volume_valid','confirmation_pass','entry_candle_available',
            'btc_regime_available','btc_downtrend','long_direction',
            'volume_ge_1_5','final_candidates')
    total = Counter({k:0 for k in keys})
    by_symbol = {}
    failed = []
    try:
        # 90 days = ~260k candles across 10 symbols; requests can take time.
        async with httpx.AsyncClient(timeout=httpx.Timeout(240.0)) as client:
            sem = asyncio.Semaphore(3)
            async def fetch(sym):
                async with sem:
                    try:
                        rows = await get_5m_candles_days(client, sym, days)
                        return sym, [c for c in rows if int(c['close_time']) <= now_ms], None
                    except Exception as exc:
                        return sym, [], type(exc).__name__
            fetched = await asyncio.gather(*(fetch(s) for s in _V106_SYMBOLS))
        data = {s:cs for s,cs,e in fetched if e is None}
        failed = [{'symbol':s,'error_type':e} for s,cs,e in fetched if e]
        btc = data.get('BTCUSDT', [])
        btc_cl = [float(c['close']) for c in btc]
        btc_times = [int(c['close_time']) for c in btc]
        ema144 = _v109_ema(btc_cl, 144)
        ema576 = _v109_ema(btc_cl, 576)
        for sym in _V106_SYMBOLS:
            if sym not in data: continue
            cs = data[sym]; n=len(cs)
            local = Counter({k:0 for k in keys})
            if n < 580:
                by_symbol[sym]={'status':'INSUFFICIENT_CANDLES','candles':n,'funnel':dict(local)}
                continue
            h=[float(c['high']) for c in cs]
            l=[float(c['low']) for c in cs]
            cl=[float(c['close']) for c in cs]
            vol=[float(c['volume']) for c in cs]
            widths=[None]*n
            for j in range(11,n):
                low=min(l[j-11:j+1])
                widths[j]=(max(h[j-11:j+1])-low)/low*100 if low>0 else None
            # Candidate logic follows V112 exactly, except that historical
            # freshness is intentionally omitted and separately disclosed.
            for i in range(110,n-1):
                local['evaluated_candles']+=1
                base=[w for w in widths[i-73:i-1] if w is not None]
                if len(base)!=72 or widths[i-1] is None:continue
                local['width_valid']+=1
                bm=median(base)
                if bm<=0 or widths[i-1]>.65*bm:continue
                local['compression_pass']+=1
                ceiling=max(h[i-24:i]); floor=min(l[i-24:i])
                direction='SHORT' if cl[i]>ceiling else ('LONG' if cl[i]<floor else None)
                if direction is None:continue
                local['breakout_pass']+=1
                avg=sum(vol[i-20:i])/20
                if avg<=0:continue
                local['volume_valid']+=1
                ratio=vol[i]/avg
                confirm=next((k for k in range(i+1,min(i+7,n)) if floor<=cl[k]<=ceiling),None)
                if confirm is None:continue
                local['confirmation_pass']+=1
                entry_idx=confirm+1
                if entry_idx>=n:continue
                local['entry_candle_available']+=1
                entry_ms=int(cs[entry_idx]['open_time'])
                ix=bisect_right(btc_times,entry_ms-1)-1
                if ix<0 or ema144[ix] is None or ema576[ix] is None:continue
                local['btc_regime_available']+=1
                if not (btc_cl[ix]<ema144[ix]<ema576[ix]):continue
                local['btc_downtrend']+=1
                if direction!='LONG':continue
                local['long_direction']+=1
                if ratio<1.5:continue
                local['volume_ge_1_5']+=1
                local['final_candidates']+=1
            total.update(local)
            by_symbol[sym]={'status':'OK','candles':n,'funnel':dict(local)}
        counts=dict(total)
        return {**MODE_INFO,'status':'PARTIAL' if failed else 'OK','signal':False,
                'study':'V115_HISTORICAL_FUNNEL','generated_utc':utc_now(),
                'requested_days':days,'funnel_order':list(keys),
                'funnel':counts,'by_symbol':by_symbol,'fetch_failures':failed,
                'writes_to_database':False,'prospective_observations_added':0,
                'limitations':[
                    'Historical descriptive diagnostic only; not independent validation or prospective evidence.',
                    'Freshness filter is intentionally omitted: historical candidates cannot be fresh at call time.',
                    'Overlapping candidate windows can count related setups multiple times; no cooldown is applied.',
                    'The final_candidates count is not a trade count or backtest performance result.',
                    'All funnel stages are sequential; later-stage zero counts do not identify independent blockers.',
                    'BTC EMA uses fetched historical warmup only; earliest observations may lack regime context.',
                    'No trading, orders, signals, or changes to V112 entry rules.'
                ]}
    except Exception as exc:
        return {**MODE_INFO,'status':'ERROR','signal':False,
                'study':'V115_HISTORICAL_FUNNEL','generated_utc':utc_now(),
                'error_type':type(exc).__name__}


# V116: descriptive historical candidate timing, isolated from V112 storage.
@app.get('/candidate-timing-v116')
async def candidate_timing_v116(days: int = Query(default=90, ge=7, le=90)):
    from bisect import bisect_right
    from collections import Counter, defaultdict
    from statistics import median
    now_ms = int(datetime.now(timezone.utc).timestamp() * 1000)
    failures=[]
    try:
        async with httpx.AsyncClient(timeout=httpx.Timeout(240.0)) as client:
            sem=asyncio.Semaphore(3)
            async def fetch(sym):
                async with sem:
                    try:
                        rows=await get_5m_candles_days(client,sym,days)
                        return sym,[c for c in rows if int(c['close_time'])<=now_ms],None
                    except Exception as exc:
                        return sym,[],type(exc).__name__
            fetched=await asyncio.gather(*(fetch(sym) for sym in _V106_SYMBOLS))
        data={sym:rows for sym,rows,err in fetched if err is None}
        failures=[{'symbol':sym,'error_type':err} for sym,rows,err in fetched if err]
        btc=data.get('BTCUSDT',[])
        btc_close=[float(c['close']) for c in btc]
        btc_times=[int(c['close_time']) for c in btc]
        ema144=_v109_ema(btc_close,144)
        ema576=_v109_ema(btc_close,576)
        candidates=[]
        per_symbol=Counter()
        for sym in _V106_SYMBOLS:
            cs=data.get(sym,[]);n=len(cs)
            if n<580:continue
            highs=[float(c['high']) for c in cs]
            lows=[float(c['low']) for c in cs]
            closes=[float(c['close']) for c in cs]
            volumes=[float(c['volume']) for c in cs]
            widths=[None]*n
            for j in range(11,n):
                low=min(lows[j-11:j+1])
                widths[j]=(max(highs[j-11:j+1])-low)/low*100 if low>0 else None
            for i in range(110,n-1):
                base=[w for w in widths[i-73:i-1] if w is not None]
                if len(base)!=72 or widths[i-1] is None:continue
                bm=median(base)
                if bm<=0 or widths[i-1]>.65*bm:continue
                ceiling=max(highs[i-24:i]);floor=min(lows[i-24:i])
                direction='SHORT' if closes[i]>ceiling else ('LONG' if closes[i]<floor else None)
                if direction is None:continue
                avg=sum(volumes[i-20:i])/20
                if avg<=0:continue
                ratio=volumes[i]/avg
                confirm=next((k for k in range(i+1,min(i+7,n)) if floor<=closes[k]<=ceiling),None)
                if confirm is None or confirm+1>=n:continue
                entry_ms=int(cs[confirm+1]['open_time'])
                ix=bisect_right(btc_times,entry_ms-1)-1
                if ix<0 or ema144[ix] is None or ema576[ix] is None:continue
                if not btc_close[ix]<ema144[ix]<ema576[ix]:continue
                if direction!='LONG' or ratio<1.5:continue
                candidates.append({'symbol':sym,'entry_ms':entry_ms,'ratio':round(ratio,4)})
                per_symbol[sym]+=1
        candidates.sort(key=lambda x:(x['entry_ms'],x['symbol']))
        # Independent 120-minute cooldown per symbol; no global suppression.
        last_by_symbol={}
        cooldown=[]
        for c in candidates:
            previous=last_by_symbol.get(c['symbol'])
            if previous is None or c['entry_ms']-previous>=120*60*1000:
                cooldown.append(c)
                last_by_symbol[c['symbol']]=c['entry_ms']
        def date_key(ms):
            return datetime.fromtimestamp(ms/1000,timezone.utc).strftime('%Y-%m-%d')
        def daily_stats(rows):
            count=Counter(date_key(c['entry_ms']) for c in rows)
            return [{'date_utc':d,'candidates':count[d]} for d in sorted(count)]
        days_utc=sorted({date_key(int(c['open_time'])) for cs in data.values() for c in cs})
        # Only days fully covered in the returned BTC candle series.
        btc_days=Counter(date_key(int(c['open_time'])) for c in btc)
        full_days=sorted(d for d,n in btc_days.items() if n>=288)
        raw_daily=Counter(date_key(c['entry_ms']) for c in candidates)
        cd_daily=Counter(date_key(c['entry_ms']) for c in cooldown)
        daily=[{'date_utc':d,'raw_candidates':raw_daily[d],
                'cooldown_candidates':cd_daily[d]} for d in full_days]
        gaps=[]
        for a,b in zip(cooldown,cooldown[1:]):
            gaps.append(round((b['entry_ms']-a['entry_ms'])/60000,2))
        return {**MODE_INFO,'status':'PARTIAL' if failures else 'OK','signal':False,
            'study':'V116_HISTORICAL_CANDIDATE_TIMING','generated_utc':utc_now(),
            'requested_days':days,'historical_raw_candidates':len(candidates),
            'historical_after_per_symbol_120m_cooldown':len(cooldown),
            'cooldown_removed':len(candidates)-len(cooldown),
            'raw_by_symbol':dict(per_symbol),
            'cooldown_by_symbol':dict(Counter(c['symbol'] for c in cooldown)),
            'fully_covered_utc_days':len(full_days),
            'full_days_without_raw_candidates':sum(raw_daily[d]==0 for d in full_days),
            'full_days_without_cooldown_candidates':sum(cd_daily[d]==0 for d in full_days),
            'daily_full_utc_days':daily,
            'raw_daily_nonzero':daily_stats(candidates),
            'cooldown_daily_nonzero':daily_stats(cooldown),
            'max_daily_raw_candidates':max(raw_daily.values(),default=0),
            'max_daily_cooldown_candidates':max(cd_daily.values(),default=0),
            'fetch_failures':failures,'writes_to_database':False,
            'prospective_observations_added':0,
            'limitations':[
                'Historical diagnostic only; not prospective validation or a performance backtest.',
                'Candidate logic matches V115; V112 live freshness and actual scheduler timing are not simulated.',
                'Cooldown is per symbol and 120 minutes from accepted entry; verify exact live V112 cooldown implementation separately.',
                'Only UTC days with >=288 fetched BTC candles are used for zero-day statistics.',
                'Overlapping setups before cooldown are not independent trades.',
                'No reliable capture-rate estimate without persisted actual polling and candidate timestamps.',
                'No trading, orders, signals, or modification of V112 collection.'
            ]}
    except Exception as exc:
        return {**MODE_INFO,'status':'ERROR','signal':False,
                'study':'V116_HISTORICAL_CANDIDATE_TIMING',
                'generated_utc':utc_now(),'error_type':type(exc).__name__}


# V117: read-only prospective polling coverage audit.
# Does not retroactively label any historical candle as a prospective observation.
@app.get('/poll-coverage-v117')
async def poll_coverage_v117(hours: int = Query(default=24, ge=1, le=168)):
    from statistics import median
    now_ms = int(datetime.now(timezone.utc).timestamp() * 1000)
    url = os.getenv('DATABASE_URL', '').strip()
    if not url:
        return {**MODE_INFO, 'status':'ERROR', 'signal':False,
                'study':'V117_POLL_COVERAGE', 'error':'DATABASE_URL missing'}
    try:
        with psycopg.connect(url, connect_timeout=15) as con:
            with con.cursor() as cur:
                cur.execute('SELECT polled_ms,observed_added,settled_added,failures FROM alt_momentum_v112.polls WHERE polled_ms >= %s ORDER BY polled_ms', (now_ms-hours*3600000,))
                rows=cur.fetchall()
                cur.execute('SELECT COUNT(*), MIN(polled_ms), MAX(polled_ms) FROM alt_momentum_v112.polls')
                all_count,first_ms,last_ms=cur.fetchone()
                cur.execute('SELECT COUNT(*), COUNT(*) FILTER (WHERE o.observation_id IS NOT NULL) FROM alt_momentum_v112.observations s LEFT JOIN alt_momentum_v112.outcomes o ON o.observation_id=s.id')
                obs_count,settled_count=cur.fetchone()
                cur.execute('SELECT s.symbol,s.entry_ms,s.observed_ms FROM alt_momentum_v112.observations s WHERE s.observed_ms >= %s ORDER BY s.observed_ms DESC LIMIT 100', (now_ms-hours*3600000,))
                obs=cur.fetchall()
        stamps=[int(r[0]) for r in rows]
        intervals=[round((b-a)/1000,2) for a,b in zip(stamps,stamps[1:])]
        # A scheduled 5-minute tick may start late. Only report gap estimates,
        # not exact scheduler misses, because manual requests are mixed in.
        gaps=[{'start_ms':a,'end_ms':b,'gap_minutes':round((b-a)/60000,2),
               'minimum_possible_unobserved_5m_slots':max(0,int((b-a)//300000)-1)}
              for a,b in zip(stamps,stamps[1:]) if b-a>450000]
        return {**MODE_INFO,'status':'OK','signal':False,
            'study':'V117_POLL_COVERAGE','generated_utc':utc_now(),
            'storage':'POSTGRES_SCHEMA_alt_momentum_v112_READ_ONLY',
            'window_hours':hours,'polls_in_window':len(rows),
            'polls_total':all_count,'first_poll_ms':first_ms,'last_poll_ms':last_ms,
            'age_last_poll_minutes':round((now_ms-last_ms)/60000,2) if last_ms else None,
            'interval_median_seconds':round(median(intervals),2) if intervals else None,
            'interval_max_seconds':max(intervals) if intervals else None,
            'gaps_over_7_5m':gaps[-30:],
            'polls_with_fetch_failures':sum(int(r[3])>0 for r in rows),
            'sum_symbol_fetch_failures':sum(int(r[3]) for r in rows),
            'observations_total':obs_count,'settled_total':settled_count,
            'recent_observation_delays_seconds':[{'symbol':sym,'entry_ms':int(entry),
                'observed_ms':int(observed),'delay_seconds':round((observed-entry)/1000,2)}
                for sym,entry,observed in obs[:30]],
            'writes_to_database':False,'prospective_observations_added':0,
            'limitations':[
                'Polls record completed API audit calls, including manual calls, not cron scheduling intentions.',
                'Missing scheduled executions cannot be proven from the database; use cron-job.org history.',
                'A poll without a candidate does not prove every candidate was evaluated or captured.',
                'No historical candidate is reclassified as prospectively observed.',
                'No trading, orders, signals, or modification of V112 collection.'
            ]}
    except Exception as exc:
        return {**MODE_INFO,'status':'ERROR','signal':False,
                'study':'V117_POLL_COVERAGE','generated_utc':utc_now(),
                'error_type':type(exc).__name__,
                'error':'Read-only PostgreSQL audit failed; check Render logs.'}


# V118: Three predefined research strategies, fixed 120m exit, chronological holdout.
# Historical simulation only. No orders, no prospective DB writes.
@app.get('/strategy-comparison-v118')
async def strategy_comparison_v118(days: int = Query(default=90, ge=30, le=90)):
    from collections import defaultdict
    from statistics import mean
    import math
    now_ms=int(datetime.now(timezone.utc).timestamp()*1000)
    cutoff_ms=now_ms-int(days*86400000/3)  # Last third of calendar period is untouched holdout.
    strategies=('TREND_PULLBACK','SQUEEZE_BREAKOUT','MEAN_REVERSION')
    costs=(0.15,0.20,0.25,0.30)
    all_trades=defaultdict(list)
    errors=[]
    async with httpx.AsyncClient(timeout=httpx.Timeout(240.0)) as client:
        sem=asyncio.Semaphore(3)
        async def fetch(sym):
            async with sem:
                try:
                    cs=await get_5m_candles_days(client,sym,days)
                    return sym,[c for c in cs if int(c['close_time'])<=now_ms],None
                except Exception as exc:
                    return sym,[],type(exc).__name__
        fetched=await asyncio.gather(*(fetch(s) for s in _V106_SYMBOLS))
    for sym,cs,err in fetched:
        if err:
            errors.append({'symbol':sym,'error_type':err})
            continue
        n=len(cs)
        if n<700:
            errors.append({'symbol':sym,'error_type':'INSUFFICIENT_CANDLES'})
            continue
        close=[float(c['close']) for c in cs]
        high=[float(c['high']) for c in cs]
        low=[float(c['low']) for c in cs]
        volume=[float(c['volume']) for c in cs]
        times=[int(c['open_time']) for c in cs]
        ema20=_v109_ema(close,20)
        ema50=_v109_ema(close,50)
        ema144=_v109_ema(close,144)
        # Wilder RSI and ATR: all indicators use only the closed signal candle.
        rsi=[None]*n
        ag=sum(max(close[k]-close[k-1],0) for k in range(1,15))/14
        al=sum(max(close[k-1]-close[k],0) for k in range(1,15))/14
        for i in range(14,n):
            if i>14:
                d=close[i]-close[i-1]
                ag=(ag*13+max(d,0))/14
                al=(al*13+max(-d,0))/14
            rsi[i]=100 if al==0 else 100-100/(1+ag/al)
        atr=[None]*n
        tr=[high[0]-low[0]]+[max(high[i]-low[i],abs(high[i]-close[i-1]),abs(low[i]-close[i-1])) for i in range(1,n)]
        av=sum(tr[1:15])/14
        for i in range(14,n):
            if i>14: av=(av*13+tr[i])/14
            atr[i]=av
        last_entry={k:-10**18 for k in strategies}
        for i in range(200,n-25):
            if any(v is None for v in (ema20[i],ema50[i],ema144[i],rsi[i],atr[i])):continue
            if times[i+1]-times[i]!=300000:continue
            if times[i+24]-times[i+1]!=23*300000:continue
            if close[i]<=0 or atr[i]<=0:continue
            vbase=sum(volume[i-20:i])/20
            vr=volume[i]/vbase if vbase>0 else 0
            # A: rising trend, pullback to EMA20 and bullish recovery; long only.
            a=(ema20[i]>ema50[i]>ema144[i] and close[i]>ema20[i]
               and low[i-1]<=ema20[i-1]*1.003 and close[i]>close[i-1]
               and 45<=rsi[i]<=70 and vr>=0.8)
            # B: narrow 20-bar range relative to ATR, then volume-confirmed upside break.
            prior_hi=max(high[i-20:i]);prior_lo=min(low[i-20:i])
            b=((prior_hi-prior_lo)/close[i-1]<0.025 and close[i]>prior_hi
               and vr>=1.5 and close[i]>ema50[i])
            # C: oversold below medium trend, bullish reversal; long only.
            c=(rsi[i-1]<30 and rsi[i]>rsi[i-1] and close[i]<ema50[i]
               and (ema20[i]-close[i])/close[i]>=0.01 and close[i]>close[i-1])
            entry=float(cs[i+1]['open'])
            # Exit at next candle open exactly 24 5m bars after entry (120m).
            exit_price=float(cs[i+25]['open'])
            if times[i+25]-times[i+1]!=24*300000 or entry<=0:continue
            gross=(exit_price/entry-1)*100
            for key,passed in zip(strategies,(a,b,c)):
                if not passed or times[i+1]-last_entry[key]<120*60000:continue
                last_entry[key]=times[i+1]
                all_trades[key].append({'symbol':sym,'entry_ms':times[i+1],
                                        'gross_pct':gross})
    def metrics(trades,cost):
        net=[t['gross_pct']-cost for t in trades]
        pos=sum(x for x in net if x>0)
        neg=-sum(x for x in net if x<0)
        return {'n':len(net),'mean_net_pct':round(mean(net),5) if net else None,
                'win_rate_pct':round(100*sum(x>0 for x in net)/len(net),2) if net else None,
                'profit_factor':round(pos/neg,4) if neg>0 else (None if not net else 'NO_LOSSES'),
                'sum_net_pct_points':round(sum(net),4)}
    report={}
    for key in strategies:
        rows=sorted(all_trades[key],key=lambda t:t['entry_ms'])
        dev=[t for t in rows if t['entry_ms']<cutoff_ms]
        holdout=[t for t in rows if t['entry_ms']>=cutoff_ms]
        report[key]={
            'all':{str(cost):metrics(rows,cost) for cost in costs},
            'development':{str(cost):metrics(dev,cost) for cost in costs},
            'holdout_last_third':{str(cost):metrics(holdout,cost) for cost in costs},
            'by_symbol':{sym:sum(t['symbol']==sym for t in rows) for sym in _V106_SYMBOLS}}
    return {**MODE_INFO,'status':'PARTIAL' if errors else 'OK','signal':False,
        'study':'V118_THREE_STRATEGY_COMPARISON','generated_utc':utc_now(),
        'requested_days':days,'timeframe':'5m','horizon_minutes':120,
        'cost_pct_round_trip':[0.15,0.20,0.25,0.30],
        'holdout_start_utc':datetime.fromtimestamp(cutoff_ms/1000,timezone.utc).isoformat(),
        'strategies':report,'fetch_failures':errors,
        'writes_to_database':False,'prospective_observations_added':0,
        'limitations':[
            'Exploratory historical simulation, not independently verified and not live execution.',
            'Three strategy definitions are exploratory and must be frozen before future independent validation.',
            'Indicators use completed signal candles; entry and exit are next-bar opens, not executable fills.',
            'Cost scenarios subtract a fixed round-trip percentage; slippage and market impact may exceed these assumptions.',
            '120-minute holding period, per-symbol 120-minute cooldown, long-only; overlapping symbols can be correlated.',
            'Chronological holdout is still exposed to researcher selection if used to choose or tune the winner.',
            'No orders, trading, signals, or changes to V112 collection.'
        ]}


# V119: Regime-stratified read-only extension of frozen V118 strategy definitions.
# Historical simulation only. No orders, no prospective DB writes.
@app.get('/regime-analysis-v119')
async def regime_analysis_v119(days: int = Query(default=90, ge=30, le=90)):
    from collections import defaultdict
    from statistics import mean
    import math
    now_ms=int(datetime.now(timezone.utc).timestamp()*1000)
    cutoff_ms=now_ms-int(days*86400000/3)  # Last third of calendar period is untouched holdout.
    strategies=('TREND_PULLBACK','SQUEEZE_BREAKOUT','MEAN_REVERSION')
    costs=(0.15,0.20,0.25,0.30)
    all_trades=defaultdict(list)
    errors=[]
    async with httpx.AsyncClient(timeout=httpx.Timeout(240.0)) as client:
        sem=asyncio.Semaphore(3)
        async def fetch(sym):
            async with sem:
                try:
                    cs=await get_5m_candles_days(client,sym,days)
                    return sym,[c for c in cs if int(c['close_time'])<=now_ms],None
                except Exception as exc:
                    return sym,[],type(exc).__name__
        fetched=await asyncio.gather(*(fetch(s) for s in _V106_SYMBOLS))
    # BTC regime and volatility computed strictly from completed BTC 5m candles.
    # Regime at signal candle timestamp; no future information used.
    btc_rows=next((cs for sym,cs,err in fetched if sym=='BTCUSDT' and not err),[])
    btc_close=[float(c['close']) for c in btc_rows]
    btc_high=[float(c['high']) for c in btc_rows]
    btc_low=[float(c['low']) for c in btc_rows]
    btc_e144=_v109_ema(btc_close,144) if btc_rows else []
    btc_e576=_v109_ema(btc_close,576) if btc_rows else []
    btc_regimes={}
    if btc_rows:
        tr_b=[btc_high[0]-btc_low[0]]+[max(btc_high[j]-btc_low[j],abs(btc_high[j]-btc_close[j-1]),abs(btc_low[j]-btc_close[j-1])) for j in range(1,len(btc_rows))]
        for j in range(576,len(btc_rows)):
            if btc_close[j]<=0 or btc_e144[j] is None or btc_e576[j] is None:continue
            # Mean true range of preceding 14 bars including completed signal bar.
            atr_pct=100*sum(tr_b[j-13:j+1])/14/btc_close[j]
            trend=('UP' if btc_close[j]>btc_e144[j]>btc_e576[j] else
                   'DOWN' if btc_close[j]<btc_e144[j]<btc_e576[j] else 'MIXED')
            vol=('LOW' if atr_pct<0.25 else 'HIGH' if atr_pct>=0.60 else 'MEDIUM')
            btc_regimes[int(btc_rows[j]['open_time'])]=(trend,vol,round(atr_pct,5))
    for sym,cs,err in fetched:
        if err:
            errors.append({'symbol':sym,'error_type':err})
            continue
        n=len(cs)
        if n<700:
            errors.append({'symbol':sym,'error_type':'INSUFFICIENT_CANDLES'})
            continue
        close=[float(c['close']) for c in cs]
        high=[float(c['high']) for c in cs]
        low=[float(c['low']) for c in cs]
        volume=[float(c['volume']) for c in cs]
        times=[int(c['open_time']) for c in cs]
        ema20=_v109_ema(close,20)
        ema50=_v109_ema(close,50)
        ema144=_v109_ema(close,144)
        # Wilder RSI and ATR: all indicators use only the closed signal candle.
        rsi=[None]*n
        ag=sum(max(close[k]-close[k-1],0) for k in range(1,15))/14
        al=sum(max(close[k-1]-close[k],0) for k in range(1,15))/14
        for i in range(14,n):
            if i>14:
                d=close[i]-close[i-1]
                ag=(ag*13+max(d,0))/14
                al=(al*13+max(-d,0))/14
            rsi[i]=100 if al==0 else 100-100/(1+ag/al)
        atr=[None]*n
        tr=[high[0]-low[0]]+[max(high[i]-low[i],abs(high[i]-close[i-1]),abs(low[i]-close[i-1])) for i in range(1,n)]
        av=sum(tr[1:15])/14
        for i in range(14,n):
            if i>14: av=(av*13+tr[i])/14
            atr[i]=av
        last_entry={k:-10**18 for k in strategies}
        for i in range(200,n-25):
            if any(v is None for v in (ema20[i],ema50[i],ema144[i],rsi[i],atr[i])):continue
            if times[i+1]-times[i]!=300000:continue
            if times[i+24]-times[i+1]!=23*300000:continue
            if close[i]<=0 or atr[i]<=0:continue
            vbase=sum(volume[i-20:i])/20
            vr=volume[i]/vbase if vbase>0 else 0
            # A: rising trend, pullback to EMA20 and bullish recovery; long only.
            a=(ema20[i]>ema50[i]>ema144[i] and close[i]>ema20[i]
               and low[i-1]<=ema20[i-1]*1.003 and close[i]>close[i-1]
               and 45<=rsi[i]<=70 and vr>=0.8)
            # B: narrow 20-bar range relative to ATR, then volume-confirmed upside break.
            prior_hi=max(high[i-20:i]);prior_lo=min(low[i-20:i])
            b=((prior_hi-prior_lo)/close[i-1]<0.025 and close[i]>prior_hi
               and vr>=1.5 and close[i]>ema50[i])
            # C: oversold below medium trend, bullish reversal; long only.
            c=(rsi[i-1]<30 and rsi[i]>rsi[i-1] and close[i]<ema50[i]
               and (ema20[i]-close[i])/close[i]>=0.01 and close[i]>close[i-1])
            entry=float(cs[i+1]['open'])
            # Exit at next candle open exactly 24 5m bars after entry (120m).
            exit_price=float(cs[i+25]['open'])
            if times[i+25]-times[i+1]!=24*300000 or entry<=0:continue
            gross=(exit_price/entry-1)*100
            for key,passed in zip(strategies,(a,b,c)):
                if not passed or times[i+1]-last_entry[key]<120*60000:continue
                last_entry[key]=times[i+1]
                regime=btc_regimes.get(times[i])
                all_trades[key].append({'symbol':sym,'entry_ms':times[i+1],
                                        'gross_pct':gross,
                                        'btc_trend':regime[0] if regime else 'UNKNOWN',
                                        'btc_volatility':regime[1] if regime else 'UNKNOWN'})
    def metrics(trades,cost):
        net=[t['gross_pct']-cost for t in trades]
        pos=sum(x for x in net if x>0)
        neg=-sum(x for x in net if x<0)
        return {'n':len(net),'mean_net_pct':round(mean(net),5) if net else None,
                'win_rate_pct':round(100*sum(x>0 for x in net)/len(net),2) if net else None,
                'profit_factor':round(pos/neg,4) if neg>0 else (None if not net else 'NO_LOSSES'),
                'sum_net_pct_points':round(sum(net),4)}
    report={}
    for key in strategies:
        rows=sorted(all_trades[key],key=lambda t:t['entry_ms'])
        dev=[t for t in rows if t['entry_ms']<cutoff_ms]
        holdout=[t for t in rows if t['entry_ms']>=cutoff_ms]
        report[key]={
            'all':{str(cost):metrics(rows,cost) for cost in costs},
            'development':{str(cost):metrics(dev,cost) for cost in costs},
            'holdout_last_third':{str(cost):metrics(holdout,cost) for cost in costs},
            'by_symbol':{sym:sum(t['symbol']==sym for t in rows) for sym in _V106_SYMBOLS},
            'by_btc_trend':{reg:{'development':{str(cost):metrics([t for t in dev if t['btc_trend']==reg],cost) for cost in costs},
                                 'last_third':{str(cost):metrics([t for t in holdout if t['btc_trend']==reg],cost) for cost in costs}}
                            for reg in ('UP','DOWN','MIXED','UNKNOWN')},
            'by_btc_volatility':{reg:{'development':{str(cost):metrics([t for t in dev if t['btc_volatility']==reg],cost) for cost in costs},
                                      'last_third':{str(cost):metrics([t for t in holdout if t['btc_volatility']==reg],cost) for cost in costs}}
                                 for reg in ('LOW','MEDIUM','HIGH','UNKNOWN')}}
    return {**MODE_INFO,'status':'PARTIAL' if errors else 'OK','signal':False,
        'study':'V119_MARKET_REGIME_ANALYSIS','generated_utc':utc_now(),
        'requested_days':days,'timeframe':'5m','horizon_minutes':120,
        'cost_pct_round_trip':[0.15,0.20,0.25,0.30],
        'holdout_start_utc':datetime.fromtimestamp(cutoff_ms/1000,timezone.utc).isoformat(),
        'strategies':report,'fetch_failures':errors,
        'regime_definitions':{'btc_trend':'UP: close > EMA144 > EMA576; DOWN: close < EMA144 < EMA576; otherwise MIXED',
          'btc_volatility':'BTC 5m ATR14 / close: LOW <0.25%, MEDIUM 0.25%-<0.60%, HIGH >=0.60%',
          'timestamp':'BTC last completed signal candle; UNKNOWN if BTC unavailable'},
        'writes_to_database':False,'prospective_observations_added':0,
        'limitations':[
            'Exploratory historical simulation, not independently verified and not live execution.',
            'Regime thresholds are exploratory, not tuned; small subgroup counts and multiple comparisons can mislead.',
            'The last third was already examined in V118; it is NOT an untouched independent holdout.',
            'Regime is assigned from BTC at the signal candle; no post-entry regime information used.',
            'Three strategy definitions are exploratory and must be frozen before future independent validation.',
            'Indicators use completed signal candles; entry and exit are next-bar opens, not executable fills.',
            'Cost scenarios subtract a fixed round-trip percentage; slippage and market impact may exceed these assumptions.',
            '120-minute holding period, per-symbol 120-minute cooldown, long-only; overlapping symbols can be correlated.',
            'Chronological holdout is still exposed to researcher selection if used to choose or tune the winner.',
            'No orders, trading, signals, or changes to V112 collection.'
        ]}
