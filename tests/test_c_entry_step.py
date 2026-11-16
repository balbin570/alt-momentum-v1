"""Integration checks for one entry change; existing exit paths stay intact."""
import copy
import tempfile
import time
import unittest
from pathlib import Path
from unittest.mock import AsyncMock, patch

import main as app


class IncrementalCEntryTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        state = copy.deepcopy(app.V90_STATE)
        storage = copy.deepcopy(app.V915_STORAGE)
        self.addCleanup(lambda: (app.V90_STATE.clear(), app.V90_STATE.update(state)))
        self.addCleanup(lambda: (app.V915_STORAGE.clear(), app.V915_STORAGE.update(storage)))
        tmp = tempfile.TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        for name, value in [('V90_LOCK', None), ('C_REMOTE_STORE', None),
                            ('V90_STATE_PATH', Path(tmp.name) / 'state.json')]:
            p = patch.object(app, name, value)
            p.start()
            self.addCleanup(p.stop)
        app.V90_STATE.update(trades=[], seen={}, signals=0, filter_observations={})
        app.V915_STORAGE.update(last_restore_status='NOT_ATTEMPTED', last_save_ok=True)

    async def scan(self, breakout):
        now = int(time.time() * 1000)
        candles = [dict(close=100. + i * .1, high=100. + i * .1,
                        volume=10., close_time=now - 1000) for i in range(24)]
        for c in candles[:11]:
            c['close'] = 90.
        candles[-13]['close'] = 100.
        candles[-1].update(close=104., volume=20.)
        for c in candles[-7:-1]:
            c['high'] = 104. / (1 + breakout / 100)
        with patch.object(app, 'build_universe', AsyncMock(return_value=[{'symbol': 'TESTUSDT'}])), \
             patch.object(app, 'get_completed_5m_candles', AsyncMock(return_value=candles)), \
             patch.object(app, 'get_json', AsyncMock(return_value={'askPrice':'104', 'bidPrice':'103.95'})), \
             patch.object(app, 'v902_notify', AsyncMock()):
            return await app.v90_scan_once()

    async def test_new_entry_tag_and_unchanged_exit_version(self):
        result = await self.scan(.49)
        self.assertEqual(1, result['new_coins'], result)
        self.assertEqual(1, len(app.V90_STATE['trades']))
        t = app.V90_STATE['trades'][0]
        self.assertEqual('C', t['variant'])
        self.assertEqual('C_V2_PROTECT', t['c_rule_version'])
        self.assertEqual('C_ENTRY_BREAKOUT050_V1', t['c_entry_version'])
        self.assertEqual(.5, t['c_entry_breakout_max_pct'])
        self.assertAlmostEqual(104*.98, t['stop_price'])
        self.assertFalse(t['orders'])

    async def test_above_new_cap_rejected(self):
        result = await self.scan(.51)
        self.assertEqual(0, result['new_coins'])
        self.assertEqual([], app.V90_STATE['trades'])
        self.assertEqual(1, result['model_filters']['C']['rejected']['breakout_range'])

    async def test_existing_lower_bound_preserved(self):
        result = await self.scan(.19)
        self.assertEqual(0, result['new_coins'])
        self.assertEqual([], app.V90_STATE['trades'])

    async def test_tag_never_changes_existing_exit_semantics(self):
        now = int(time.time()*1000)
        base = dict(symbol='TESTUSDT', variant='C', status='OPEN_SHADOW',
                    entry_price=100., entry_ms=now-60000, stop_price=98.,
                    max_bid=100., min_bid=100., tp1_done=False,
                    c_rule_version='C_V2_PROTECT', c_forward_version=app.C_FORWARD_VERSION)
        old = dict(base)
        new = dict(base, c_entry_version=app.C_ENTRY_VERSION)
        app.V90_STATE['trades'] = [old, new]
        with patch.object(app, 'get_json', AsyncMock(return_value={'bidPrice':'102'})), \
             patch.object(app, 'v902_notify', AsyncMock()):
            await app.v90_monitor(None, fast_only=True)
        for t in (old, new):
            self.assertTrue(t['be_armed'])
            self.assertAlmostEqual(100.15, t['stop_price'])
        self.assertNotIn('c_entry_version', old)
        with patch.object(app, 'get_json', AsyncMock(return_value={'bidPrice':'100'})), \
             patch.object(app, 'v902_notify', AsyncMock()):
            await app.v90_monitor(None, fast_only=True)
        self.assertEqual('CLOSED_SHADOW', old['status'])
        self.assertEqual(old['exit_reason'], new['exit_reason'])
        self.assertEqual(old['net_pct'], new['net_pct'])

    async def test_read_only_cohort_separation(self):
        app.V90_STATE['trades'] = [
            dict(variant='C', status='CLOSED_SHADOW', c_rule_version=app.C_RULE_VERSION, net_pct=-1.),
            dict(variant='C', status='CLOSED_SHADOW', c_rule_version=app.C_RULE_VERSION,
                 c_entry_version=app.C_ENTRY_VERSION, net_pct=2.),
            dict(variant='C_SHADOW_V4_C_EXIT', status='CLOSED_SHADOW', net_pct=-9.)]
        before = copy.deepcopy(app.V90_STATE)
        result = await app.c_entry_experiment()
        self.assertEqual(-1., result['cohorts']['baseline_v2']['mean_net_pct'])
        self.assertEqual(2., result['cohorts'][app.C_ENTRY_VERSION]['mean_net_pct'])
        self.assertEqual(before, app.V90_STATE)
        self.assertFalse(result['orders'])
        self.assertFalse(result['trading'])
