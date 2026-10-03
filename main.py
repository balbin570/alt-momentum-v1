import asyncio
from datetime import datetime, timezone
from statistics import mean, median

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

async def get_5m_candles_days(client, symbol, days=30):
    """
    Binance 1000-kline limitini geriye doğru sayfalayarak tamamlanmış
    5m mumları toplar. Varsayılan 30 gün ~= 8640 mum.
    """
    now_ms = int(datetime.now(timezone.utc).timestamp() * 1000)
    start_ms = now_ms - (days * 24 * 60 * 60 * 1000)
    end_time = now_ms
    by_open_time = {}

    # 30 gün için yaklaşık 9 istek gerekir. Güvenli üst sınır bırakıyoruz.
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

        # Bir önceki sayfanın sonundan daha eski veriye git.
        end_time = oldest_open - 1

        # Binance'a gereksiz burst yapmamak için küçük bir ara.
        await asyncio.sleep(0.03)

    candles = sorted(
        by_open_time.values(),
        key=lambda x: x["open_time"],
    )

    return candles


def historical_trades_v2(candles, symbol):
    """
    V2 research trade modeli:
    - Sinyal tamamlanmış 5m mum kapanışında hesaplanır.
    - Giriş bir sonraki 5m mumun OPEN fiyatıdır.
    - Çıkış girişten tam 60 dakika sonraki mumun OPEN fiyatıdır.
    - Aynı coin için girişler arasında en az 60 dakika cooldown.
    - Sabit %0.15 round-trip araştırma maliyeti.
    """
    trades = []
    last_entry_open_time = None
    cooldown_ms = 60 * 60 * 1000

    # i sinyal mumu; i+1 giriş; i+13 = girişten 60 dk sonraki open.
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

    # Çok küçük örneklerde yine kronolojik bir ayrım yap.
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
    30 günlük çoklu-altcoin V2 araştırma backtesti.

    Bu endpoint:
    - gerçek emir üretmez,
    - paper trade açmaz,
    - next-candle-open giriş kullanır,
    - 60 dk hold + 60 dk same-symbol cooldown kullanır,
    - %0.15 maliyet düşer,
    - kronolojik DEV/OOS raporu üretir.
    """
    try:
        async with httpx.AsyncClient(
            timeout=httpx.Timeout(90.0)
        ) as client:
            universe_data = await build_universe(client)
            selected = universe_data[:count]

            # 30 coin x ~9 Binance sayfası. Render/Binance için kontrollü eşzamanlılık.
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

        # Birleşik DEV/OOS coin başına değil, tüm işlemlerin kronolojik
        # ilk 2/3 ve son 1/3'ü olarak da raporlanır.
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
    BTC 5m verisini 1 saatlik kapanışlara indirger.
    Rejim:
      BULL    = close > EMA50 > EMA200
      BEAR    = close < EMA50 < EMA200
      NEUTRAL = diğer durumlar
    Her 5m zaman damgası için yalnızca o ana kadar tamamlanmış 1H bilgi kullanılır.
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
    Daha gerçekçi ortak-portföy araştırma simülasyonu.

    - Başlangıç equity = 100.
    - Aynı anda en fazla max_positions.
    - Her yeni pozisyon başlangıçtaki değil, o andaki equity'nin sabit yüzdesi
      kadar nominal sermaye kullanır.
    - Pozisyonlar 60 dk sonra kapanır.
    - Aynı timestamp'teki sinyaller deterministik olarak symbol sırasına göre işlenir.
    - Kaldıraç yok; toplam hedef tahsis <= %100.
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

    # Kalan pozisyonları son exit zamanına kadar kapat.
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
    - 30-90 gün sayfalı 5m veri
    - next-candle-open giriş
    - 60 dk hold / 60 dk same-symbol cooldown
    - %0.15 round-trip cost
    - BTC 1H EMA50/EMA200 piyasa rejimi
    - kronolojik DEV/OOS
    - max 5 eşzamanlı pozisyonlu ortak portföy simülasyonu
    """
    try:
        async with httpx.AsyncClient(
            timeout=httpx.Timeout(120.0)
        ) as client:
            universe_data = await build_universe(client)
            selected = universe_data[:count]

            # Önce BTC rejim verisi.
            btc_candles = await get_5m_candles_days(
                client,
                "BTCUSDT",
                days,
            )
            regime_map = btc_regime_map_from_5m(
                btc_candles
            )

            # 90 gün x 30 coin ağır bir iş; kontrollü concurrency.
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

        # Portföy simülasyonu aynı birleşik kronolojik trade akışında.
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
    Amaç tahmin etmek değil:
    Coin ZATEN yükselmişken ve hacim artmışken, sonraki 60 dakikada
    hareket devam ediyor mu sorusunu ölçmek.

    Sinyal tamamlanmış 5m mum kapanışında görülür.
    Giriş bir sonraki 5m mum OPEN.
    Çıkış 60 dakika sonraki OPEN.
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

        # Sadece halihazırda yükselmiş hareketleri inceliyoruz.
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
    Önceden belirlenmiş geniş kovalar.
    Bunlar optimize edilmiş giriş eşikleri değildir; davranışı görmek içindir.
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
    'Hangisi yükselecek?' değil,
    'Zaten yükselmiş coin ne zaman yükselmeye devam ediyor?' testi.
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
    V5: Coin zaten yükselmişken hareketin KALİTESİNİ ölçer.
    Geleceği tahmin eden özellik kullanılmaz.

    Sinyal anında bilinenler:
    - son 30m yükseliş
    - son altı 5m getirinin yapısı
    - pozitif 5m mum sayısı
    - son 15m / ilk 15m momentum karşılaştırması
    - 30m içi peak'ten mevcut close'a geri çekilme
    - son 3 mum hacminin önceki 3 muma göre devamlılığı

    Giriş: sonraki 5m OPEN
    Çıkış: girişten 60m sonraki OPEN
    """
    events = []

    for i in range(7, len(candles) - 13):
        signal_close = candles[i]["close"]

        mom5 = pct_change(candles[i - 1]["close"], signal_close)
        mom15 = pct_change(candles[i - 3]["close"], signal_close)
        mom30 = pct_change(candles[i - 6]["close"], signal_close)

        # Biz sadece zaten yükselmiş hareketleri inceliyoruz.
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
    Tek değişkenli davranış raporu.
    Amaç en iyi hücreyi seçmek değil, hangi hareket özelliklerinin
    OOS'ta continuation ile ilişkili olduğunu görmek.
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
    V4'te keşfedilen genel bölgeyi ayrı raporlar:
    30m momentum 1.0%-1.99%.
    Burada yeni kalite özelliklerini inceliyoruz; yeni eşik optimize etmiyoruz.
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
    Önceden tanımlı continuation hipotezleri.
    Amaç OOS sonucuna göre eşik uydurmak değil; V4/V5'te gözlenen
    yapıları ayrı, anlaşılır hipotezler olarak test etmektir.
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

        # Ayrı coin genişliği: sonuç tek/az sayıda coin tarafından mı taşınıyor?
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
            semaphore = asyncio.Semaphore(2)

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
    V8: H6 eşiklerine DOKUNMADAN, sinyal anındaki rejimi ekler.
    Gelecek veri kullanılmaz.

    Rejim değişkenleri:
      BTC 1h / 4h / 24h return
      ALT 1h / 4h / 24h return
    5m tamamlanmış mumlardan hesaplanır.
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
    Önceden tanımlı, kaba rejim ayrımları.
    Amaç 'en iyi eşik' aramak değil; H6'nın hangi piyasa yönünde
    bozulduğunu veya iyileştiğini görmek.
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

