from fastapi import FastAPI, Query
import httpx
import asyncio
from datetime import datetime, timezone

app = FastAPI(
    title="ALT-MOMENTUM-V1",
    version="1.0"
)

BINANCE = "https://data-api.binance.vision"

MODEL = "ALT-MOMENTUM-V1"

FLAGS = {
    "mode": "RESEARCH_PAPER_ONLY",
    "trading": False,
    "orders": False,
}

# Stablecoin / fiat / leveraged vb. tarama dışında tut
EXCLUDED_BASES = {
    "USDC", "FDUSD", "TUSD", "USDP", "DAI",
    "EUR", "TRY", "GBP", "BRL", "AUD",
    "BIDR", "IDRT", "UAH", "RUB",
}

MIN_QUOTE_VOLUME_USDT = 5_000_000


async def get_json(client, path, params=None):
    r = await client.get(
        BINANCE + path,
        params=params,
        timeout=15,
    )
    r.raise_for_status()
    return r.json()


@app.get("/")
async def root():
    return {
        "model": MODEL,
        **FLAGS,
        "status": "ONLINE",
        "purpose": "BINANCE_USDT_ALTCOIN_MOMENTUM_RESEARCH",
        "endpoints": [
            "/health",
            "/universe",
            "/scan",
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
            "model": MODEL,
            **FLAGS,
            "status": "OK",
            "binance": "CONNECTED",
            "server_time": data.get("serverTime"),
            "checked_utc": datetime.now(
                timezone.utc
            ).isoformat(),
        }

    except Exception as exc:
        return {
            "model": MODEL,
            **FLAGS,
            "status": "ERROR",
            "binance": "NOT_CONNECTED",
            "error": type(exc).__name__,
            "detail": str(exc),
        }


async def build_universe():
    async with httpx.AsyncClient() as client:

        exchange_info, tickers = await asyncio.gather(
            get_json(client, "/api/v3/exchangeInfo"),
            get_json(client, "/api/v3/ticker/24hr"),
        )

    ticker_map = {
        x["symbol"]: x
        for x in tickers
    }

    universe = []

    for s in exchange_info["symbols"]:

        if s.get("quoteAsset") != "USDT":
            continue

        if s.get("status") != "TRADING":
            continue

        if not s.get("isSpotTradingAllowed", False):
            continue

        base = s.get("baseAsset", "")

        if base in EXCLUDED_BASES:
            continue

        # Leveraged tokenları ele
        if (
            base.endswith("UP")
            or base.endswith("DOWN")
            or base.endswith("BULL")
            or base.endswith("BEAR")
        ):
            continue

        ticker = ticker_map.get(s["symbol"])

        if not ticker:
            continue

        try:
            quote_volume = float(
                ticker.get("quoteVolume", 0)
            )
        except Exception:
            continue

        if quote_volume < MIN_QUOTE_VOLUME_USDT:
            continue

        universe.append({
            "symbol": s["symbol"],
            "base_asset": base,
            "quote_volume_24h": round(
                quote_volume, 2
            ),
        })

    universe.sort(
        key=lambda x: x["quote_volume_24h"],
        reverse=True,
    )

    return universe


@app.get("/universe")
async def universe():
    try:
        data = await build_universe()

        return {
            "model": MODEL,
            **FLAGS,
            "status": "OK",
            "min_quote_volume_usdt":
                MIN_QUOTE_VOLUME_USDT,
            "symbol_count": len(data),
            "symbols": data,
        }

    except Exception as exc:
        return {
            "model": MODEL,
            **FLAGS,
            "status": "ERROR",
            "error": type(exc).__name__,
            "detail": str(exc),
        }


async def analyze_symbol(
    client,
    symbol,
    volume24h,
    semaphore,
):
    async with semaphore:

        try:
            klines = await get_json(
                client,
                "/api/v3/klines",
                {
                    "symbol": symbol,
                    "interval": "5m",
                    "limit": 20,
                },
            )

            if len(klines) < 7:
                return None

            now_ms = int(
                datetime.now(
                    timezone.utc
                ).timestamp() * 1000
            )

            # Sadece tamamlanmış mumlar
            closed = [
                k for k in klines
                if int(k[6]) < now_ms
            ]

            if len(closed) < 7:
                return None

            closes = [
                float(k[4])
                for k in closed
            ]

            volumes = [
                float(k[5])
                for k in closed
            ]

            current = closes[-1]

            def momentum(n):
                old = closes[-1 - n]
                return (
                    current / old - 1
                ) * 100

            mom_5m = momentum(1)
            mom_15m = momentum(3)
            mom_30m = momentum(6)

            avg_volume = (
                sum(volumes[-7:-1]) / 6
            )

            volume_ratio = (
                volumes[-1] / avg_volume
                if avg_volume > 0
                else 0
            )

            return {
                "symbol": symbol,
                "price": current,
                "momentum_5m_pct":
                    round(mom_5m, 4),
                "momentum_15m_pct":
                    round(mom_15m, 4),
                "momentum_30m_pct":
                    round(mom_30m, 4),
                "volume_ratio":
                    round(volume_ratio, 3),
                "quote_volume_24h":
                    round(volume24h, 2),
            }

        except Exception:
            return None


@app.get("/scan")
async def scan(
    count: int = Query(
        default=20,
        ge=5,
        le=50,
    )
):
    try:
        universe_data = await build_universe()

        semaphore = asyncio.Semaphore(12)

        async with httpx.AsyncClient() as client:

            tasks = [
                analyze_symbol(
                    client,
                    x["symbol"],
                    x["quote_volume_24h"],
                    semaphore,
                )
                for x in universe_data
            ]

            results = await asyncio.gather(
                *tasks
            )

        results = [
            x for x in results
            if x is not None
        ]

        # İlk araştırma skoru.
        # Henüz al/sat sinyali DEĞİLDİR.
        for x in results:

            score = (
                x["momentum_5m_pct"] * 3
                + x["momentum_15m_pct"] * 2
                + x["momentum_30m_pct"]
                + min(
                    x["volume_ratio"], 5
                )
            )

            x["research_score"] = round(
                score, 4
            )

        results.sort(
            key=lambda x: x["research_score"],
            reverse=True,
        )

        return {
            "model": MODEL,
            **FLAGS,
            "status": "OK",
            "signal": False,
            "note":
                "RESEARCH RANKING ONLY - NOT A BUY SIGNAL",
            "universe_size":
                len(universe_data),
            "analyzed":
                len(results),
            "returned":
                min(count, len(results)),
            "generated_utc":
                datetime.now(
                    timezone.utc
                ).isoformat(),
            "leaders":
                results[:count],
        }

    except Exception as exc:
        return {
            "model": MODEL,
            **FLAGS,
            "status": "ERROR",
            "error": type(exc).__name__,
            "detail": str(exc),
        }
