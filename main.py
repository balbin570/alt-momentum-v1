import asyncio
from datetime import datetime, timezone
from statistics import mean

import httpx
from fastapi import FastAPI, Query

app = FastAPI(title="ALT-MOMENTUM-V1")

BINANCE = "https://data-api.binance.vision"
MODEL = "ALT-MOMENTUM-V1"

MIN_QUOTE_VOLUME_USDT = 5_000_000
ROUND_TRIP_COST_PCT = 0.15

# Altcoin araştırması için istemediğimiz baz varlıklar
EXCLUDED_BASES = {
    "BTC",
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


async def build_universe(client):
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
    Burada AL sinyali üretmiyoruz.

    Amaç:
    Geçmişte belirli momentum + hacim koşulları oluştuğunda
    fiyatın 15/30/60 dakika sonra ne yaptığını ölçmek.
    """

    events = []

    # İleri ölçüm için 12 mum = 60 dakika gerekir.
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

        # İlk araştırma koşulu.
        # Bunlar nihai parametre değildir.
        if mom5 < 0.30:
            continue

        if mom15 < 0.50:
            continue

        if mom30 < 0.50:
            continue

        if volume_ratio < 1.20:
            continue

        # Çoktan aşırı koşmuş hareketleri ilk aşamada ayır.
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
    Emir üretmez.
    Paper trade açmaz.
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
    Aynı coin için birbirine çok yakın eventleri bağımsız işlem gibi
    saymamak amacıyla cooldown uygular.
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
    15/30/60 dk brüt sonuçları ve sabit round-trip maliyet sonrası
    net sonuçları birlikte özetler.
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
    Çoklu altcoin research/event-study endpointi.

    - Emir üretmez.
    - Paper trade açmaz.
    - Mevcut 24h likidite evreninden en yüksek hacimli coinleri seçer.
    - Ham eventleri, 30 dk cooldown ve 60 dk cooldown sonuçlarını karşılaştırır.
    - %0.15 round-trip araştırma maliyetini net sonuçlardan düşer.

    Not:
    Bu ilk geniş test current-universe yaklaşımı kullanır; dolayısıyla
    survivorship / current-liquidity bias içerebilir.
    """
    try:
        async with httpx.AsyncClient() as client:
            universe_data = await build_universe(client)
            selected = universe_data[:count]

            semaphore = asyncio.Semaphore(8)

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

        # Coin listesini hacme göre okunabilir sırada tut.
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

