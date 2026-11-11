import contextlib
import copy
import io
import json
from pathlib import Path
import tempfile
import time
import unittest
from unittest.mock import AsyncMock, patch

import httpx
import main as app


class PaperSafetyTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.original_state = copy.deepcopy(app.V90_STATE)
        self.original_storage = copy.deepcopy(app.V915_STORAGE)
        self.path_patch = patch.object(app, 'V90_STATE_PATH', Path(self.temp.name) / 'state.json')
        self.path_patch.start()
        app.V90_STATE.update(trades=[], seen={}, signals=0, last_scan=None,
                             filter_observations={})
        app.V915_STORAGE['last_restore_status'] = 'NOT_ATTEMPTED'

    def tearDown(self):
        app.V90_STATE.clear()
        app.V90_STATE.update(self.original_state)
        app.V915_STORAGE.clear()
        app.V915_STORAGE.update(self.original_storage)
        self.path_patch.stop()
        self.temp.cleanup()

    def candidate(self, ret=4., volume=2., z=1., breakout=.5):
        return dict(symbol='TESTUSDT', ret60_pct=ret, volume_ratio=volume,
                    z=z, breakout_pct=breakout)

    def test_independent_failures_include_overlapping_rejections(self):
        app.c_observe_filter_ranges([self.candidate(ret=0., volume=.2)], {'utc': 'now'})
        last = app.V90_STATE['filter_observations']['last_scan']
        self.assertEqual(last['independent_failures']['volume'], 1)
        self.assertEqual(last['independent_failures']['momentum'], 1)

    def test_boundaries_and_other_filters(self):
        candidates = [self.candidate(ret=3., volume=1.5), self.candidate(ret=7.),
                      self.candidate(volume=3.5), self.candidate(z=3.5),
                      self.candidate(breakout=.19)]
        app.c_observe_filter_ranges(candidates, {'utc': 'now'})
        values = app.V90_STATE['filter_observations']['range_totals']['momentum_2.5_8__volume_1.5_3.5']
        self.assertEqual(values, dict(momentum_volume_matches=4, with_other_candle_filters=2))

    def test_observations_do_not_mutate_candidates_or_trades(self):
        candidates = [self.candidate()]
        before = copy.deepcopy(candidates)
        app.c_observe_filter_ranges(candidates, {'utc': 'now'})
        self.assertEqual(before, candidates)
        self.assertEqual([], app.V90_STATE['trades'])
        self.assertEqual(0, app.V90_STATE['signals'])

    def test_checkpoint_roundtrip_preserves_history_context_and_counters(self):
        app.V90_STATE['trades'] = [dict(variant='C_TREND', status='CLOSED_SHADOW',
            entry_btc_ret60_pct=-.3, entry_alt_positive_breadth=.2, net_pct=-1.) for _ in range(1205)]
        expected = copy.deepcopy(app.V90_STATE['trades'])
        app.c_observe_filter_ranges([self.candidate()], {'utc': 'now'})
        app.v903_save_state()
        app.V90_STATE.update(trades=[], filter_observations={})
        app.v903_load_state()
        self.assertEqual(expected, app.V90_STATE['trades'])
        self.assertEqual(1, app.V90_STATE['filter_observations']['scans'])

    def test_legacy_checkpoint_is_loaded_without_backfill(self):
        rows = [dict(variant='C_TREND', status='CLOSED_SHADOW', net_pct=1.)]
        app.V90_STATE_PATH.write_text(json.dumps(dict(trades=rows, seen={}, signals=1)))
        app.v903_load_state()
        self.assertEqual(rows, app.V90_STATE['trades'])
        self.assertEqual({}, app.V90_STATE['filter_observations'])

    def test_corrupt_checkpoint_cannot_be_overwritten(self):
        original = '{broken checkpoint'
        app.V90_STATE_PATH.write_text(original)
        app.v903_load_state()
        app.v903_save_state()
        self.assertEqual(original, app.V90_STATE_PATH.read_text())
        self.assertEqual('SAVE_BLOCKED_AFTER_RESTORE_ERROR', app.V915_STORAGE['last_save_error'])

    async def test_telegram_error_never_prints_or_stores_token_url(self):
        secret = 'synthetic-token-for-test'
        request = httpx.Request('POST', f'https://api.telegram.org/bot{secret}/sendMessage')
        error = httpx.HTTPStatusError('sensitive ' + str(request.url), request=request,
                                      response=httpx.Response(401, request=request))
        output = io.StringIO()
        with patch.object(app, 'v22_telegram_send', AsyncMock(side_effect=error)):
            with contextlib.redirect_stdout(output):
                result = await app.v902_notify('paper', 'ENTRY', 'TESTUSDT')
        self.assertNotIn(secret, output.getvalue() + json.dumps(result))
        self.assertEqual('HTTPStatusError HTTP_401', result['reason'])

    async def test_diagnostics_are_read_only_and_show_open_entry_context(self):
        app.V90_STATE['trades'] = [dict(variant='C', entry_ms=1,
            entry_utc='now', entry_btc_ret60_pct=.1, entry_alt_positive_breadth=.6,
            ret60_pct=4., volume_ratio=2., spread_pct=.03, status='OPEN_SHADOW')]
        before = copy.deepcopy(app.V90_STATE)
        result = await app.c_entry_diagnostics()
        self.assertEqual(before, app.V90_STATE)
        self.assertFalse(app.V90_STATE_PATH.exists())
        self.assertEqual(.1, result['last_retained_entry']['entry_btc_ret60_pct'])
        self.assertFalse(result['orders'])
        self.assertFalse(result['trading'])

    async def test_regime_report_never_assigns_old_trades_a_context(self):
        app.V90_STATE['trades'] = [dict(variant='C_TREND', status='CLOSED_SHADOW', net_pct=1.)]
        result = await app.c_market_regime_analysis()
        self.assertEqual(1, result['closed_missing_entry_market_context'])
        self.assertEqual(0, result['closed_with_entry_market_context'])

    async def test_scan_preserves_more_than_1200_closed_records(self):
        app.V90_STATE['trades'] = [dict(variant='C_TREND', status='CLOSED_SHADOW',
            net_pct=1., entry_ms=0) for _ in range(1205)]
        with patch.object(app, 'build_universe', AsyncMock(return_value=[])), \
             patch.object(app, 'get_completed_5m_candles', AsyncMock(return_value=[])):
            await app.v90_scan_once()
        self.assertEqual(1205, len(app.V90_STATE['trades']))
        self.assertEqual(1205, len(json.loads(app.V90_STATE_PATH.read_text())['trades']))

    async def test_scan_only_opens_C_and_records_entry_hypotheses(self):
        now = int(time.time() * 1000)
        candles = [dict(close=100. + i * .1, high=100. + i * .1,
                         volume=10., close_time=now - 1000) for i in range(24)]
        for c in candles[:11]:
            c['close'] = 90.
        candles[-13]['close'] = 100.
        candles[-1].update(close=104., volume=20.)
        # A .5% breakout with a 4% one-hour return.
        for c in candles[-7:-1]:
            c['high'] = 104. / 1.005
        with patch.object(app, 'build_universe', AsyncMock(return_value=[{'symbol': 'TESTUSDT'}])), \
             patch.object(app, 'get_completed_5m_candles', AsyncMock(return_value=candles)), \
             patch.object(app, 'get_json', AsyncMock(return_value={'askPrice': '104', 'bidPrice': '103.95'})), \
             patch.object(app, 'v902_notify', AsyncMock()):
            result = await app.v90_scan_once()
        self.assertEqual(1, result['new_coins'], result)
        self.assertEqual({'C'},
                         {t['variant'] for t in app.V90_STATE['trades']})
        for trade in app.V90_STATE['trades']:
            self.assertEqual(4., round(trade['entry_btc_ret60_pct'], 6))
            self.assertEqual(1., trade['entry_alt_positive_breadth'])
        self.assertEqual(('C',), app.V93_ALL_ACTIVE)
        self.assertIn('C_TREND', app.C_FROZEN_VARIANTS)
        self.assertTrue(set(app.C_NEW_VARIANTS).issubset(app.C_FROZEN_VARIANTS))
        self.assertIn('c_loss_observation_v1', app.V90_STATE['trades'][0])

    async def test_C_loss_comparison_counts_avoided_losses_and_missed_winners(self):
        rows = []
        for net, btc in [(-3., -.5), (1., -.2), (2., .5)]:
            t = dict(variant='C', status='CLOSED_SHADOW', net_pct=net,
                     entry_btc_ret60_pct=btc, exit_utc='2026-11-11')
            t['c_loss_observation_v1'] = app.c_loss_entry_flags(t)
            rows.append(t)
        rows.append(dict(variant='C', status='CLOSED_SHADOW', net_pct=-8.))
        rows.append(dict(variant='C_TREND', status='CLOSED_SHADOW', net_pct=-99.))
        app.V90_STATE['trades'] = rows
        before = copy.deepcopy(rows)
        result = await app.c_loss_analysis()
        comparison = result['hypotheses']['btc_negative']
        self.assertEqual(4, result['summary']['closed'])
        self.assertEqual(1, comparison['losers_avoided'])
        self.assertEqual(1, comparison['winners_missed'])
        self.assertEqual(1, comparison['unknown_closed'])
        self.assertEqual(2., comparison['hypothetical_change_sum_return_pct_points'])
        self.assertFalse(result['automatic_entry_blocking'])
        self.assertEqual(before, app.V90_STATE['trades'])

    def test_missing_regime_is_unknown_and_never_assumed_safe(self):
        flags = app.c_loss_entry_flags({})['would_skip']
        self.assertTrue(all(value is None for value in flags.values()))

    async def test_frozen_exit_variants_still_close_existing_positions(self):
        now = int(time.time() * 1000)
        app.V90_STATE['trades'] = [dict(symbol='TESTUSDT', variant=variant,
            status='OPEN_SHADOW', entry_price=100., entry_ms=now-1000,
            stop_price=97., max_bid=100., min_bid=100., remaining_fraction=1.)
            for variant in app.C_NEW_VARIANTS]
        with patch.object(app, 'get_json', AsyncMock(return_value={'bidPrice': '96'})), \
             patch.object(app, 'v902_notify', AsyncMock()):
            await app.v90_monitor(None)
        self.assertTrue(all(t['status'] == 'CLOSED_SHADOW' for t in app.V90_STATE['trades']))
        self.assertTrue(all(t['exit_reason'] == 'STOP_MONITORED' for t in app.V90_STATE['trades']))


if __name__ == '__main__':
    unittest.main()
