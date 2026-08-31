import ast
from pathlib import Path

old_path = Path('/mnt/data/main.py')
new_path = Path(__file__).with_name('main.py')
old = ast.parse(old_path.read_text(encoding='utf-8'))
new = ast.parse(new_path.read_text(encoding='utf-8'))

def funcs(tree):
    return {n.name: ast.dump(n, include_attributes=False) for n in tree.body if isinstance(n, (ast.FunctionDef, ast.AsyncFunctionDef))}

o, n = funcs(old), funcs(new)
unchanged = [
    'momentum_for',
    '_first_v2_eligible_candidates',
    'record_first_v2_vote',
    'first_v2_vote',
    'consensus_confirmations',
    'store_consensus_event',
    'evaluate_consensus_variant',
    'arm_dca',
    'mark_dca_filled',
]
for name in unchanged:
    assert name in o and name in n, name
    assert o[name] == n[name], f'{name} changed unexpectedly'

# Confirm the core numeric defaults remain exactly the supplied MULTI7 values.
old_text = old_path.read_text(encoding='utf-8')
new_text = new_path.read_text(encoding='utf-8')
keys = [
    'DECISION_INTERVAL', 'TRADE_WINDOW_SECONDS', 'ENTRY_ORDER_SIZE', 'DCA_ORDER_SIZE',
    'LOOKBACK_TICKS', 'V2_ELIGIBLE_PRICE_MIN', 'V2_ELIGIBLE_PRICE_MAX',
    'V2_ELIGIBLE_MOM_MIN', 'V2_ELIGIBLE_MOM_MAX', 'SAFE_ENTRY_MOM_MIN',
    'SAFE_ENTRY_MOM_MAX', 'SAFE_ENTRY_PRICE_MIN', 'SAFE_ENTRY_PRICE_MAX',
    'C_SAFE_ENTRY_PRICE_MIN', 'C_SAFE_ENTRY_PRICE_MAX', 'C_DCA_MIN_BUY_PRICE',
    'C_DCA_MAX_BUY_PRICE', 'C_DCA_REBOUND_MOM_MIN', 'C_DCA_REBOUND_MOM_MAX',
    'DCA_ARM_PRICE', 'DCA_DEADLINE_SEC', 'CONSENSUS_WINDOW_SEC',
    'F_CONSENSUS_MIN_OTHER_TOKENS', 'G_CONSENSUS_MIN_OTHER_TOKENS',
    'H_CONSENSUS_MIN_OTHER_TOKENS', 'J_CONSENSUS_MIN_OTHER_TOKENS',
]
import re
for key in keys:
    ro = re.search(rf'^{key}\s*=\s*(.+)$', old_text, re.M)
    rn = re.search(rf'^{key}\s*=\s*(.+)$', new_text, re.M)
    assert ro and rn, key
    assert ro.group(1).strip() == rn.group(1).strip(), f'{key}: {ro.group(1)} != {rn.group(1)}'

assert 'async def report_loop' not in new_text
assert 'asyncio.create_task(report_loop())' not in new_text
assert 'BREAKEVEN_TRIGGER_MOVE = float(os.getenv("BREAKEVEN_TRIGGER_MOVE", "0.05"))' in new_text
print('Strategy parity check: OK — FIRST-V2/F/G/H/J/DCA unchanged; only BE protection/runtime/reporting changed.')
