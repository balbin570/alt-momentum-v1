import asyncio
import time
import unittest

import main as app


class CTrail3ExitTests(unittest.TestCase):
    """Yeni C: yalniz %3 takip eden stop + 120 dk; baska cikis yok."""

    def setUp(self):
        self.saved = list(app.V90_STATE.get("trades", []))
        self.prices = {}
        self.orig_get_json = app.get_json
        self.orig_save = app.v903_save_state
        self.orig_notify = app.v902_notify

        async def fake_get_json(client, path, params):
            return {"bidPrice": str(self.prices[params["symbol"]])}

        async def fake_notify(*a, **k):
            return None

        app.get_json = fake_get_json
        app.v903_save_state = lambda *a, **k: None
        app.v902_notify = fake_notify

    def tearDown(self):
        app.V90_STATE["trades"] = self.saved
        app.get_json = self.orig_get_json
        app.v903_save_state = self.orig_save
        app.v902_notify = self.orig_notify

    def trade(self, minutes_ago=1, entry=100.0):
        now = int(time.time() * 1000)
        return {"variant": "C", "symbol": "XUSDT", "status": "OPEN_SHADOW",
                "entry_ms": now - minutes_ago * 60000, "entry_price": entry,
                "stop_price": entry * 0.97, "max_bid": entry, "min_bid": entry,
                "tp1_done": False, "trail_active": False, "be_armed": False,
                "c_exit_version": app.C_EXIT_VERSION}

    def tick(self, t, bid):
        self.prices["XUSDT"] = bid
        app.V90_STATE["trades"] = [t]
        asyncio.run(app.v90_monitor(None))

    def test_trailing_stop_follows_high(self):
        t = self.trade()
        self.tick(t, 110.0)
        self.assertEqual(t["status"], "OPEN_SHADOW")
        self.assertAlmostEqual(t["stop_price"], 110.0 * 0.97)
        self.tick(t, 106.6)  # 110*0.97 = 106.7 -> stop
        self.assertEqual(t["status"], "CLOSED_SHADOW")
        self.assertEqual(t["exit_reason"], "C_TRAIL3")
        self.assertAlmostEqual(t["net_pct"], 6.6 - app.V90_COST_PCT, places=4)

    def test_no_partial_take_profit_at_plus3(self):
        t = self.trade()
        self.tick(t, 103.5)
        self.assertEqual(t["status"], "OPEN_SHADOW")
        self.assertFalse(t.get("tp1_done"))
        self.tick(t, 100.3)  # 103.5*0.97 = 100.395 -> stop, tam pozisyon bu fiyattan
        self.assertEqual(t["exit_reason"], "C_TRAIL3")
        self.assertAlmostEqual(t["net_pct"], 0.3 - app.V90_COST_PCT, places=4)

    def test_initial_stop_minus3(self):
        t = self.trade()
        self.tick(t, 97.5)
        self.assertEqual(t["status"], "OPEN_SHADOW")
        self.tick(t, 96.9)
        self.assertEqual(t["exit_reason"], "C_TRAIL3")

    def test_time_exit_120(self):
        t = self.trade(minutes_ago=121)
        self.tick(t, 101.0)
        self.assertEqual(t["exit_reason"], "TIME120")


if __name__ == "__main__":
    unittest.main()
