import unittest
from candidate_review import accepts, metrics, review


class ReviewTests(unittest.TestCase):
    def row(self, **changes):
        return dict(cohort='TOP20_SCALP_V5', symbol='BTCUSDT',
                    entry_utc='2026-11-16T00:00:00+00:00',
                    exit_utc='2026-11-16T00:05:00+00:00',
                    spread_pct=0.02, net_pct=-0.10, **changes)

    def test_missing_or_nonfinite_is_unknown(self):
        for value in [None, float('nan'), float('inf'), True, '0.02']:
            row=self.row();row['spread_pct']=value
            self.assertIsNone(accepts(row, 'TOP20_SPREAD_003_V1'))

    def test_cohorts_never_pool(self):
        row=self.row();row['cohort']='TOP20_SCALP_V4'
        self.assertIsNone(accepts(row,'TOP20_SPREAD_003_V1'))
        self.assertEqual(review([row], 'TOP20_SPREAD_003_V1', '2026-11-17T00:00:00Z')['forward_baseline']['n'],0)

    def test_cost_stress_and_all_loss_pf(self):
        self.assertEqual(metrics([self.row()])['profit_factor'],0)
        self.assertAlmostEqual(metrics([self.row()],0.1)['mean_net_pct'],-0.2)
        self.assertIsNone(metrics([])['mean_net_pct'])

    def test_pre_freeze_entry_is_not_forward(self):
        row=self.row();row['entry_utc']='2026-11-15T22:44:00Z'
        result=review([row], 'TOP20_SPREAD_003_V1', '2026-11-17T00:00:00Z')
        self.assertEqual(result['forward_baseline']['n'],0)
        self.assertEqual(result['development_n'],1)

    def test_future_exit_and_overlap(self):
        a=self.row();b=self.row();b['symbol']='ETHUSDT'
        c=self.row();c['symbol']='SOLUSDT';c['exit_utc']='2026-11-19T00:00:00Z'
        result=review([a,b,c], 'TOP20_SPREAD_003_V1', '2026-11-17T00:00:00Z')
        self.assertEqual(result['forward_kept']['n'],2)
        self.assertEqual(result['nonoverlap_count'],1)
        self.assertFalse(result['promotion_allowed'])

    def test_duplicates_and_naive_times_rejected(self):
        with self.assertRaises(ValueError):
            review([self.row(),self.row()], 'TOP20_SPREAD_003_V1', '2026-11-17T00:00:00Z')
        row=self.row();row['entry_utc']='2026-11-16T00:00:00'
        with self.assertRaises(ValueError):
            review([row], 'TOP20_SPREAD_003_V1', '2026-11-17T00:00:00Z')

    def test_other_candidate_boundaries(self):
        self.assertTrue(accepts({'cohort':'C_V2_PROTECT','breakout_pct':0.5},'C_BREAKOUT_050_V1'))
        self.assertFalse(accepts({'cohort':'HIZLI_BASELINE_9a0e1d4','ret30':1.01},'HIZLI_RET30_100_V1'))


if __name__ == '__main__':
    unittest.main()
