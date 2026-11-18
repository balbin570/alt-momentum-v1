"""Offline entry-filter research. Never imported by a bot; never places orders.

Input: normalized CLOSED rows with cohort, symbol, entry_utc, exit_utc,
net_pct and entry-time features. Net returns already include baseline costs.
Filtered historical returns omit capacity/reinvestment effects: NOT portfolio PnL.
"""
import argparse
import json
import math
from datetime import datetime, timezone

FROZEN_AT = '2026-11-15T22:45:00+00:00'
CANDIDATES = {
    'TOP20_SPREAD_003_V1': ('TOP20_SCALP_V5', 'spread_pct', 0.03),
    'C_BREAKOUT_050_V1': ('C_V2_PROTECT', 'breakout_pct', 0.50),
    'HIZLI_RET30_100_V1': ('HIZLI_BASELINE_9a0e1d4', 'ret30', 1.0),
    'C_ENTRY_SPREAD010_V1': ('C_ENTRY_BREAKOUT050_V1', 'spread_pct', 0.10),
}
# Each newly proposed hypothesis starts its own untouched forward period.
# The older candidates keep their original cutoff and cohort definitions.
FROZEN_AT_BY_CANDIDATE = {
    'C_ENTRY_SPREAD010_V1': '2026-11-18T06:50:00+00:00',
}


def timestamp(value):
    result = datetime.fromisoformat(value.replace('Z', '+00:00'))
    if result.tzinfo is None:
        raise ValueError('timezone required')
    return result.astimezone(timezone.utc)


def finite(value):
    return (isinstance(value, (int, float)) and not isinstance(value, bool)
            and math.isfinite(value))


def accepts(row, candidate):
    cohort, field, ceiling = CANDIDATES[candidate]
    if row.get('cohort') != cohort:
        return None
    value = row.get(field)
    # Missing/nonfinite entry information is unknown, not a retroactive reject.
    return 0 <= value <= ceiling if finite(value) else None


def metrics(rows, extra_cost_pct=0.0):
    values = [r['net_pct'] - extra_cost_pct for r in rows]
    gains = sum(max(v, 0) for v in values)
    losses = -sum(min(v, 0) for v in values)
    return {'n': len(values), 'mean_net_pct': sum(values)/len(values) if values else None,
            'win_rate_pct': 100*sum(v > 0 for v in values)/len(values) if values else None,
            'profit_factor': gains/losses if losses else None,
            'no_losses': bool(values) and losses == 0}


def review(rows, candidate, as_of):
    frozen_at = FROZEN_AT_BY_CANDIDATE.get(candidate, FROZEN_AT)
    cutoff, end = timestamp(frozen_at), timestamp(as_of)
    seen, development, forward, unknown = set(), [], [], 0
    for row in rows:
        if row.get('cohort') != CANDIDATES[candidate][0]:
            continue
        start, exit_time = timestamp(row['entry_utc']), timestamp(row['exit_utc'])
        if exit_time < start or not finite(row.get('net_pct')) or not row.get('symbol'):
            raise ValueError('invalid closed trade')
        key = (row['cohort'], row['symbol'], start)
        if key in seen:
            raise ValueError('duplicate trade')
        seen.add(key)
        if exit_time > end:
            continue
        decision = accepts(row, candidate)
        if decision is None:
            unknown += 1
            continue
        # Pre-freeze entries closing later are NOT forward evidence.
        (forward if start >= cutoff else development).append(row)
    forward.sort(key=lambda r: (timestamp(r['entry_utc']), r['symbol']))
    kept = [r for r in forward if accepts(r, candidate)]
    # Conservative global non-overlap count; does NOT establish independence.
    nonoverlap, last_exit = 0, cutoff
    for row in kept:
        if timestamp(row['entry_utc']) >= last_exit:
            nonoverlap += 1
            last_exit = timestamp(row['exit_utc'])
    baseline = metrics(forward)
    selected = metrics(kept)
    stress = metrics(kept, 0.10)
    enough_time = end >= cutoff and (end-cutoff).total_seconds() >= 7*86400
    positive = (selected['mean_net_pct'] is not None and selected['mean_net_pct'] > 0
                and selected['profit_factor'] is not None and selected['profit_factor'] > 1
                and stress['mean_net_pct'] > 0
                and selected['mean_net_pct'] > baseline['mean_net_pct'])
    return {'candidate': candidate, 'frozen_at': frozen_at, 'as_of': as_of,
            'development_n': len(development), 'unknown_n': unknown,
            'forward_baseline': baseline, 'forward_kept': selected,
            'stress_extra_roundtrip_cost_pct': 0.10, 'stress_kept': stress,
            'nonoverlap_count': nonoverlap,
            'minimum_sample_and_time_met': enough_time and nonoverlap >= 50,
            'preliminary_positive': bool(positive),
            'promotion_allowed': False,
            'reason': 'OFFLINE_ONLY: requires separate portfolio drawdown, dependency, '
                      'fill and operational validation; never automatically deploy this module.'}


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('input', help='private normalized JSON array; never commit raw records')
    parser.add_argument('--candidate', choices=CANDIDATES, required=True)
    parser.add_argument('--as-of', required=True)
    args = parser.parse_args()
    with open(args.input, encoding='utf-8') as source:
        print(json.dumps(review(json.load(source), args.candidate, args.as_of), indent=2, allow_nan=False))

