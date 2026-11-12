import asyncio
import time
import unittest

from top20_scalp import (MODEL, Top20Scalper, change_pct, select_leaders,
                         confirm_pullback, MAX_OPEN, LEGACY_MODEL, MONITOR_SECONDS)


def bars_for_rebound():
    now = int(time.time() * 1000)
    rows = []
    for i in range(15):
        close_ms = now - (15 - i) * 60_000 - 500
        p = 100.6
        if i == 13:
            p = 100.5
        if i == 14:
            p = 100.65
        rows.append([close_ms - 59999, "100", "101", "100", str(p),
                     "1000", close_ms, "100000"])
    return rows


class FakeCore:
    MODE_INFO = {"mode": "RESEARCH_PAPER_ONLY", "trading": False, "orders": False}

    def __init__(self):
        self.V90_STATE = {"trades": [], "seen": {}, "signals": 0}
        self.V915_STORAGE = {"last_restore_status": "RESTORED", "last_save_ok": True}
        self.V90_LOCK = None
        self.saved = 0
        self.notifications = []
        self.bid = 100.68
        self.ask = 100.70
        self.fail_save = False

    async def build_universe(self, client):
        return [{"symbol": "AAAUSDT", "quote_volume_24h": 2_500_000},
                {"symbol": "ILLUSDT", "quote_volume_24h": 300_000}]

    async def get_json(self, client, path, params):
        if path == "/api/v3/ticker":
            symbols = __import__("json").loads(params["symbols"])
            if params["windowSize"] == "5m":
                return [{"symbol": x, "openPrice": "100", "lastPrice": "100.8"}
                        for x in symbols]
            if params["windowSize"] == "10m":
                return [{"symbol": x, "openPrice": "99.5", "lastPrice": "100.8"}
                        for x in symbols]
        if path == "/api/v3/klines":
            return bars_for_rebound()
        if path == "/api/v3/ticker/bookTicker":
            return {"askPrice": str(self.ask), "bidPrice": str(self.bid)}
        raise AssertionError(f"unexpected request {path}")

    def utc_now(self):
        from datetime import datetime, timezone
        return datetime.now(timezone.utc).isoformat()

    def v903_save_state(self):
        self.saved += 1
        self.V915_STORAGE["last_save_ok"] = not self.fail_save

    def v90_close(self, trade, price, reason, now_ms):
        trade["exit_price"] = price
        trade["exit_utc"] = self.utc_now()
        trade["exit_reason"] = reason
        trade["status"] = "CLOSED_SHADOW"
        trade["net_pct"] = round(change_pct(trade["entry_price"], price) - 0.15, 5)
        trade["hold_minutes"] = (now_ms - trade["entry_ms"]) / 60000

    async def v902_notify(self, msg, event, symbol):
        self.notifications.append((event, symbol, msg))

    def v90_stats(self, rows):
        closed = [t for t in rows if t["status"] == "CLOSED_SHADOW"]
        return {"open": len([t for t in rows if t["status"] == "OPEN_SHADOW"]),
                "closed": len(closed)}


class Top20Tests(unittest.IsolatedAsyncioTestCase):
    def test_selection_does_not_use_volume_to_rank(self):
        vol = {"AAAUSDT": 2_000_000, "BBBUSDT": 100_000_000}
        five = [{"symbol": "AAAUSDT", "openPrice": "100", "lastPrice": "102"},
                {"symbol": "BBBUSDT", "openPrice": "100", "lastPrice": "100.5"}]
        ten = [{"symbol": "AAAUSDT", "openPrice": "100", "lastPrice": "103"},
               {"symbol": "BBBUSDT", "openPrice": "100", "lastPrice": "101"}]
        self.assertEqual(["AAAUSDT", "BBBUSDT"],
                         [x["symbol"] for x in select_leaders(five, ten, vol)])

    def test_rebound_needs_completed_candles(self):
        candles = bars_for_rebound()
        self.assertIsNotNone(confirm_pullback(candles, int(time.time() * 1000)))
        self.assertIsNone(confirm_pullback(candles[:-6],
                           int(time.time() * 1000)))

    async def test_opens_one_paper_entry_then_exits_at_observed_take_profit(self):
        core = FakeCore()
        strategy = Top20Scalper()
        await strategy.cycle(core)
        self.assertEqual(1, len(core.V90_STATE["trades"]))
        trade = core.V90_STATE["trades"][0]
        self.assertEqual(MODEL, trade["variant"])
        self.assertEqual("OPEN_SHADOW", trade["status"])
        self.assertFalse(trade["orders"])
        self.assertEqual(100, trade["paper_notional_usdt"])
        self.assertEqual(10, trade["exit_monitor_seconds"])
        self.assertEqual("TOP20_SCALP_V2", MODEL)
        self.assertEqual(1, strategy.status(core, True)["stats"]["open"])
        self.assertEqual(1, len(core.notifications))
        await strategy.cycle(core)
        self.assertEqual(1, len(core.V90_STATE["trades"]))
        core.bid = 101.4
        await strategy.cycle(core)
        self.assertEqual("CLOSED_SHADOW", trade["status"])
        self.assertEqual("TAKE_PROFIT_MONITORED", trade["exit_reason"])
        self.assertEqual(2, len(core.notifications))
        self.assertGreater(trade["net_pct"], 0)
        self.assertLess(trade["net_pct"], 1)
        self.assertEqual(1, len(core.V90_STATE["trades"]), "cooldown prevents rapid reentry")

    async def test_stop_is_loss_and_not_order(self):
        core = FakeCore()
        strategy = Top20Scalper()
        await strategy.cycle(core)
        core.bid = 100.1
        await strategy.cycle(core)
        trade = core.V90_STATE["trades"][0]
        self.assertEqual("STOP_MONITORED", trade["exit_reason"])
        self.assertLess(trade["net_pct"], -0.4)
        self.assertFalse(trade["orders"])

    async def test_unavailable_checkpoint_blocks_entries(self):
        core = FakeCore()
        core.V915_STORAGE["last_restore_status"] = "MISSING_CHECKPOINT_STOPPED"
        strategy = Top20Scalper()
        await strategy.cycle(core)
        self.assertEqual([], core.V90_STATE["trades"])
        self.assertEqual("CHECKPOINT_NOT_READY", strategy.report["last_error"])

    async def test_failed_entry_checkpoint_rolls_back_without_notification(self):
        core = FakeCore()
        core.fail_save = True
        strategy = Top20Scalper()
        await strategy.cycle(core)
        self.assertEqual([], core.V90_STATE["trades"])
        self.assertEqual({}, core.V90_STATE["seen"])
        self.assertEqual([], core.notifications)

    def test_weak_rebound_and_weak_close_rejected(self):
        candles = bars_for_rebound()
        candles[-1][4] = "100.56"
        self.assertIsNone(confirm_pullback(candles, int(time.time() * 1000)))
        candles = bars_for_rebound()
        candles[-1][1] = "100.60"
        self.assertIsNone(confirm_pullback(candles, int(time.time() * 1000)))

    async def test_v1_drains_without_new_v1_entries_and_keeps_history(self):
        core = FakeCore()
        strategy = Top20Scalper()
        await strategy.cycle(core)
        old = core.V90_STATE["trades"][0]
        old["variant"] = LEGACY_MODEL
        core.bid = 101.4
        await strategy.monitor_cycle(core)
        self.assertEqual("CLOSED_SHADOW", old["status"])
        self.assertEqual(LEGACY_MODEL, old["variant"])
        self.assertEqual("EXIT_" + LEGACY_MODEL, core.notifications[-1][0])
        await strategy.cycle(core, include_monitor=False)
        self.assertEqual(1, len(core.V90_STATE["trades"]))
        self.assertFalse(strategy.status(core, True)["legacy_v1_new_entries"])
        self.assertEqual(0, strategy.status(core, True)["stats"]["closed"])
        self.assertEqual(1, strategy.status(core, True)["legacy_v1_stats"]["closed"])

    async def test_monitor_does_not_wait_for_c_scan_lock(self):
        core = FakeCore()
        strategy = Top20Scalper()
        await strategy.cycle(core)
        core.V90_LOCK = asyncio.Lock()
        await core.V90_LOCK.acquire()
        core.bid = 100.1
        try:
            await asyncio.wait_for(strategy.monitor_cycle(core), timeout=2)
        finally:
            core.V90_LOCK.release()
        self.assertEqual("CLOSED_SHADOW", core.V90_STATE["trades"][0]["status"])
        self.assertEqual(10, MONITOR_SECONDS)

    async def test_existing_history_and_other_models_untouched(self):
        core = FakeCore()
        old = {"variant": "C", "status": "CLOSED_SHADOW",
               "symbol": "OLDUSDT", "entry_ms": 1000, "net_pct": 1.3}
        core.V90_STATE["trades"].append(old.copy())
        await Top20Scalper().cycle(core)
        self.assertEqual(old, core.V90_STATE["trades"][0])
        self.assertEqual(2, len(core.V90_STATE["trades"]))
        self.assertEqual(1, sum(t["variant"] == "C" for t in core.V90_STATE["trades"]))


if __name__ == "__main__":
    unittest.main()
