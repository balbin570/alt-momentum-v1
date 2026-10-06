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


V26_NON_ALT_BASE_EXCLUSIONS = {'AVGO', 'SKHYB', 'MSTRB', 'WDC', 'SOXL', 'AVGOB', 'SOXLB', 'SKHY', 'GOOGL', 'AAPL', 'TSLA', 'GOOGLB', 'NVDA', 'QQQB', 'USTC', 'EURI', 'AAPLB', 'MSTR', 'QQQ', 'USDE', 'SOXSB', 'WDCB', 'INTC', 'NVDAB', 'XUSD', 'TSLAB', 'INTCB', 'SOXS'}

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
V43_STARTED_UTC = utc_now()

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
    pos["entry_delay_seconds"] = round((now_ms - int(pos["entry_open_time"])) / 1000.0)
    pos["entry_slippage_vs_candle_pct"] = round(pct_change(candle_open_price, q["ask"]), 4)
    pos["execution_version"] = V47_EXECUTION_VERSION

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
V44_SNAPSHOT_TTL_SECONDS = 45
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
}
V32_LAST_SCAN = {
    "status": "NOT_RUN",
    "started_utc": None,
    "finished_utc": None,
    "error": None,
}
V32_AUTO_TASK = None
V32_COST_PCT = 0.15
V32_HOLD_MS = 120 * 60 * 1000
V32_SCAN_INTERVAL_SECONDS = 60

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
        conn.commit()

def v32_serializable_state():
    return {
        "open": V32_STATE["open"],
        "closed": V32_STATE["closed"],
        "seen_signal_keys": sorted(prune_seen_keys(V32_STATE["seen_signal_keys"])),
        "started_utc": V32_STATE["started_utc"],
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

async def v32_scan_once():
    import time as _t
    scan_started = _t.perf_counter()
    async with httpx.AsyncClient(timeout=httpx.Timeout(120.0)) as client:
        good, errors, universe_count, snapshot_cache_hit = await v44_market_snapshot(client)

        raw = []
        for sym, candles in good.items():
            if sym == "BTCUSDT":
                continue
            rows = relative_candidates_forward_live(candles, sym)
            if rows:
                raw.extend(rows)

        # Cross-sectional percentile at the original signal timestamp.
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

        # True selected-alt 30m market mean at signal timestamp.
        snapshots = {}
        for sym, candles in good.items():
            if sym == "BTCUSDT":
                continue
            for i in range(6, len(candles)):
                t = candles[i]["close_time"]
                snapshots.setdefault(t, []).append(
                    pct_change(candles[i - 6]["close"], candles[i]["close"])
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

        # TOP1 per decision cohort using only already-observed 60m continuation.
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

        newest_open_ms = max(
            (c[-1]["open_time"] for c in good.values() if c), default=0
        )
        freshness_ms = 10 * 60 * 1000

        entry_funnel = {
            "top1_total": len(top1_rows),
            "fresh_last_10m": 0,
            "rejected_not_fresh": 0,
            "rejected_seen_key": 0,
            "rejected_symbol_already_open": 0,
            "rejected_cooldown": 0,
            "rejected_missing_entry_candle": 0,
            "quote_or_entry_attempted": 0,
            "rejected_delay_gt_120s": 0,
            "rejected_spread": 0,
            "rejected_other_entry_reason": 0,
            "accepted_entries": 0,
            "rejection_samples": [],
        }

        def _v46_reject(kind, e, reason=None, pos=None):
            entry_funnel[kind] += 1
            if len(entry_funnel["rejection_samples"]) < 25:
                row = {
                    "symbol": e.get("symbol"),
                    "entry_open_time": e.get("entry_open_time"),
                    "z": round(float(e.get("relative_momentum_z", 0.0)), 4),
                    "percentile": round(float(e.get("cross_section_percentile", 0.0)), 4),
                    "continuation_60m_pct": round(float(e.get("wait_end_change_pct", 0.0)), 4),
                    "stage": kind,
                }
                if reason:
                    row["reason"] = reason
                if pos and pos.get("entry_delay_seconds") is not None:
                    row["entry_delay_seconds"] = pos.get("entry_delay_seconds")
                if pos and pos.get("spread_pct") is not None:
                    row["spread_pct"] = pos.get("spread_pct")
                entry_funnel["rejection_samples"].append(row)

        new_entries = []
        for e in top1_rows:
            if newest_open_ms - e["entry_open_time"] > freshness_ms:
                _v46_reject("rejected_not_fresh", e, "older than 10m freshness window")
                continue
            entry_funnel["fresh_last_10m"] += 1

            key = f'{e["symbol"]}:{e["entry_open_time"]}'
            if key in V47_SEEN_KEYS["V32"]:
                _v46_reject("rejected_seen_key", e, "key already processed in V47")
                continue
            if e["symbol"] in V32_STATE["open"]:
                _v46_reject("rejected_symbol_already_open", e, "symbol already open")
                continue

            # Same-symbol 60m cooldown from the last V32 entry.
            prior_times = []
            for x in V32_STATE["closed"]:
                if x.get("symbol") == e["symbol"]:
                    prior_times.append(int(x.get("entry_open_time", 0)))
            if e["symbol"] in V32_STATE["open"]:
                prior_times.append(int(V32_STATE["open"][e["symbol"]].get("entry_open_time", 0)))
            if prior_times and e["entry_open_time"] - max(prior_times) < 60 * 60 * 1000:
                _v46_reject("rejected_cooldown", e, "same-symbol cooldown <60m")
                continue

            candles = good.get(e["symbol"], [])
            price = next(
                (c["open"] for c in candles if c["open_time"] == e["entry_open_time"]),
                None
            )
            if price is None:
                _v46_reject("rejected_missing_entry_candle", e, "entry candle open not found")
                continue

            pos = {
                "key": key,
                "strategy": "V32_TOP1_60M_NO_BTC_FORWARD_CHALLENGER",
                "symbol": e["symbol"],
                "signal_time_ms": e["signal_time_ms"],
                "entry_open_time": e["entry_open_time"],
                "entry_price": price,
                "exit_due_time": e["entry_open_time"] + V32_HOLD_MS,
                "relative_momentum_z": round(e["relative_momentum_z"], 4),
                "cross_section_percentile": round(e["cross_section_percentile"], 4),
                "cohort_size": e.get("cohort_size"),
                "continuation_60m_pct": round(e["wait_end_change_pct"], 4),
                "alt_market_mean_30m_pct": round(e["alt_market_mean_30m_pct"], 4),
                "status": "OPEN_PAPER",
            }
            entry_funnel["quote_or_entry_attempted"] += 1
            ok_entry, skip_reason = await apply_live_entry(client, pos, price, V32_HOLD_MS)
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
            net = gross - V32_COST_PCT
            closed = {
                **pos,
                "status": "CLOSED_PAPER",
                "exit_open_time": exit_candle["open_time"],
                "exit_price": exit_price,
                "gross_pct": round(gross, 4),
                "cost_pct": V32_COST_PCT,
                "net_pct": round(net, 4),
            }
            closed.update(shadow_stop_results(candles, closed, exit_candle, V32_COST_PCT))
            V32_STATE["closed"].append(closed)
            del V32_STATE["open"][sym]
            newly_closed.append(closed)

        V44_METRICS["v32_last_scan_seconds"] = round(_t.perf_counter() - scan_started, 3)
        V44_METRICS["v32_last_scan_utc"] = utc_now()

        return {
            "status": "OK",
            "universe_size": universe_count,
            "symbols_fetched": len(good),
            "eligible_rows_seen": len(eligible),
            "top1_rows_seen": len(top1_rows),
            "entry_funnel_v46": entry_funnel,
            "new_entries": new_entries,
            "newly_closed": newly_closed,
            "fetch_errors": errors,
        }

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


async def alt_daily_summary_loop():
    await asyncio.sleep(30)
    while True:
        try:
            now = datetime.now(timezone.utc)
            today = now.date().isoformat()
            if now.hour >= ALT_DAILY_HOUR_UTC and ALT_DAILY_LAST_SENT["date"] != today:
                await alt_safe_send(alt_daily_summary_text(), "DAILY", "-")
                ALT_DAILY_LAST_SENT["date"] = today
        except Exception:
            pass
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
            if e.get("relative_momentum_z", -999) < V32_RELATIVE_Z_MIN:
                continue
            if e.get("cross_section_percentile", 0) < V32_CROSS_SECTION_MIN:
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
