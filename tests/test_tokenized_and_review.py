import unittest

import main as app


class TokenizedStockTests(unittest.TestCase):
    def test_known_stock_tokens_excluded(self):
        for b in ("TQQQB", "NBISB", "SNDKB", "AAPLB", "NVDA"):
            self.assertTrue(app.is_tokenized_stock(b), b)

    def test_crypto_not_excluded(self):
        for b in ("BNB", "AB", "SOL", "PEPE", "ETHFI", "ZRO"):
            self.assertFalse(app.is_tokenized_stock(b), b)


class CReviewTests(unittest.TestCase):
    def setUp(self):
        self.saved = list(app.V90_STATE.get("trades", []))

    def tearDown(self):
        app.V90_STATE["trades"] = self.saved

    def trades(self, n, net, entry="2026-11-26T00:00:00+00:00", variant="C"):
        return [{"variant": variant, "status": "CLOSED_SHADOW", "entry_utc": entry,
                 "exit_utc": f"2026-12-01T00:{i // 60:02d}:{i % 60:02d}+00:00", "net_pct": net} for i in range(n)]

    def test_running_until_target(self):
        app.V90_STATE["trades"] = self.trades(149, -1.0)
        r = app.c_review_status()
        self.assertFalse(r["blocked"])
        self.assertEqual(r["verdict"], "DEVAM EDIYOR")

    def test_blocks_when_negative(self):
        app.V90_STATE["trades"] = self.trades(150, -0.1)
        self.assertTrue(app.c_review_status()["blocked"])

    def test_passes_when_positive(self):
        app.V90_STATE["trades"] = self.trades(160, 0.2)
        r = app.c_review_status()
        self.assertFalse(r["blocked"])
        self.assertEqual(r["verdict"], "GECTI")
        self.assertEqual(r["closed"], 150)

    def test_ignores_old_and_other_models(self):
        app.V90_STATE["trades"] = (self.trades(200, -1.0, entry="2026-11-20T00:00:00+00:00")
                                   + self.trades(200, -1.0, variant="C_TREND"))
        self.assertEqual(app.c_review_status()["closed"], 0)



class ActiveModelTests(unittest.TestCase):
    def test_only_c_and_pullback_open_new_entries(self):
        self.assertEqual(app.C_ENTRY_MODELS, ("C",))
        self.assertEqual(app.C_ACTIVE_EARLY_SHADOW_MODELS, ())
        self.assertEqual(app.C_PULLBACK_MODEL, "C-PULLBACK-5M-V1")


class PullbackReviewTests(unittest.TestCase):
    def setUp(self):
        self.saved = list(app.V90_STATE.get("trades", []))

    def tearDown(self):
        app.V90_STATE["trades"] = self.saved

    def trades(self, n, net, entry="2026-11-27T00:00:00+00:00"):
        return [{"variant": app.C_PULLBACK_MODEL, "status": "CLOSED_SHADOW", "entry_utc": entry,
                 "exit_utc": f"2026-12-01T00:{i // 60:02d}:{i % 60:02d}+00:00", "net_pct": net} for i in range(n)]

    def test_blocks_when_negative(self):
        app.V90_STATE["trades"] = self.trades(150, -0.2)
        r = app.pullback_review_status()
        self.assertTrue(r["blocked"])
        self.assertFalse(app.c_review_status()["blocked"])

    def test_counts_only_after_start(self):
        app.V90_STATE["trades"] = self.trades(150, -0.2, entry="2026-11-26T10:00:00+00:00")
        self.assertEqual(app.pullback_review_status()["closed"], 0)

    def test_c_review_starts_at_new_exit_rule(self):
        self.assertEqual(app.C_REVIEW_START_UTC, "2026-11-25T19:10:00+00:00")


if __name__ == "__main__":
    unittest.main()
