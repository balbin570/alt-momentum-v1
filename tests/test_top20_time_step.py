"""V6 changes only the new cohort's timeout; old exit profiles stay active."""
import copy
import time
import unittest
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock, patch

import main as core
import top20_scalp as scalp


class Top20TimeStepTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        state, storage = copy.deepcopy(core.V90_STATE), copy.deepcopy(core.V915_STORAGE)
        self.addCleanup(lambda: (core.V90_STATE.clear(), core.V90_STATE.update(state)))
        self.addCleanup(lambda: (core.V915_STORAGE.clear(), core.V915_STORAGE.update(storage)))
        core.V90_STATE.update(trades=[], seen={})
        core.V915_STORAGE.update(last_save_ok=True, last_restore_status='RESTORED')
        for name, value in [('V90_LOCK', None), ('v903_save_state', Mock()),
                            ('v902_notify', AsyncMock())]:
            p = patch.object(core, name, value)
            p.start()
            self.addCleanup(p.stop)
        self.scalper = scalp.Top20Scalper()

    def trade(self, model):
        return dict(variant=model, symbol='TESTUSDT', status='OPEN_SHADOW',
                    entry_price=100., entry_ms=0, stop_price=99.,
                    take_profit_price=101., max_bid=100., min_bid=100.,
                    assumed_cost_pct=.15, research_only=True, orders=False)

    async def monitor_at(self, seconds, bid=100.):
        with patch.object(scalp.time, 'time', return_value=seconds), \
             patch.object(core, 'get_json', AsyncMock(return_value={'bidPrice':str(bid)})):
            await self.scalper.monitor(core, None)

    async def test_v6_closes_at_four_and_v5_keeps_five(self):
        new, old = self.trade(scalp.MODEL), self.trade('TOP20_SCALP_V5')
        core.V90_STATE['trades'] = [new, old]
        await self.monitor_at(239)
        self.assertEqual('OPEN_SHADOW', new['status'])
        await self.monitor_at(240)
        self.assertEqual('TIME4', new['exit_reason'])
        self.assertEqual('OPEN_SHADOW', old['status'])
        await self.monitor_at(300)
        self.assertEqual('TIME5', old['exit_reason'])

    async def test_older_v4_keeps_ten_minutes(self):
        old = self.trade('TOP20_SCALP_V4')
        core.V90_STATE['trades'] = [old]
        await self.monitor_at(300)
        self.assertEqual('OPEN_SHADOW', old['status'])
        await self.monitor_at(600)
        self.assertEqual('TIME10', old['exit_reason'])

    async def test_v5_and_v6_keep_early_momentum_exit(self):
        core.V90_STATE['trades'] = [self.trade(model) for model in ['TOP20_SCALP_V5', scalp.MODEL]]
        await self.monitor_at(60, 99.8)
        self.assertTrue(all(t['exit_reason']=='MOMENTUM_LOST' for t in core.V90_STATE['trades']))

    async def test_take_profit_and_stop_still_precede_timeout(self):
        for bid, reason in [(101.1,'TAKE_PROFIT_MONITORED'), (98.9,'STOP_MONITORED')]:
            core.V90_STATE['trades'] = [self.trade(scalp.MODEL)]
            await self.monitor_at(240, bid)
            self.assertEqual(reason, core.V90_STATE['trades'][0]['exit_reason'])

    async def test_new_entry_persists_version_timeout_and_same_risk(self):
        candidate=dict(signal_ms=100, signal_price=100., ret1_pct=.2, ret3_pct=.3,
                       ret5_pct=.4, ret10_pct=.5, volume_ratio=2., score=1.)
        quotes=[{'bidPrice':'100','askPrice':'100.01'},
                {'bidPrice':'100.05','askPrice':'100.06'},
                {'bidPrice':'100.05','askPrice':'100.06'}]
        with patch.object(core, 'build_universe', AsyncMock(return_value=[
                {'symbol':'TESTUSDT','quote_volume_24h':2000000}])), \
             patch.object(scalp, 'confirm_early_momentum', return_value=candidate), \
             patch.object(core, 'get_json', AsyncMock(side_effect=[[]]+quotes)), \
             patch.object(scalp.asyncio, 'sleep', AsyncMock()), \
             patch.object(scalp, 'time', SimpleNamespace(time=time.time,
                          monotonic=Mock(side_effect=[0.,26.]))):
            await self.scalper.scan(core, None)
        self.assertEqual(1, len(core.V90_STATE['trades']))
        t=core.V90_STATE['trades'][0]
        self.assertEqual('TOP20_SCALP_V6',t['variant'])
        self.assertEqual(4,t['time_exit_minutes'])
        self.assertAlmostEqual(t['entry_price']*.9975,t['stop_price'])
        self.assertAlmostEqual(t['entry_price']*1.004,t['take_profit_price'])
        self.assertEqual(100,t['paper_notional_usdt'])
        self.assertFalse(t['orders'])

    async def test_status_separates_v5_from_v6(self):
        core.V90_STATE['trades'] = [dict(self.trade('TOP20_SCALP_V5'),status='CLOSED_SHADOW',net_pct=-1.),
                                   dict(self.trade(scalp.MODEL),status='CLOSED_SHADOW',net_pct=.1)]
        result=self.scalper.status(core,True)
        self.assertEqual(.1,result['stats']['mean_net_pct'])
        self.assertEqual(-1.,result['baseline_v5_stats']['mean_net_pct'])
        self.assertEqual(4,result['time_exit_minutes'])
        self.assertFalse(result['orders'])
