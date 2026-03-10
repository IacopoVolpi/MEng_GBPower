"""
Careful side-by-side comparison of the SAME BMU in bids.csv vs submitted_bids.csv
to verify whether there's a sign flip.
"""
import pandas as pd
import numpy as np
from pathlib import Path

DATA_DIR = Path('data/base')

# Use 2024-01-08 since user has it open
day = '2024-01-08'

# ── Load bids.csv (real accepted) ──
bids_accepted = pd.read_csv(DATA_DIR / day / 'bids.csv', index_col=[0, 1])
vols_accepted = bids_accepted.loc[pd.IndexSlice[:, 'vol'], :]
prices_accepted = bids_accepted.loc[pd.IndexSlice[:, 'price'], :]
vols_accepted.index = vols_accepted.index.droplevel(1)
prices_accepted.index = prices_accepted.index.droplevel(1)

# ── Load submitted_bids.csv ──
sub_bids = pd.read_csv(DATA_DIR / day / 'submitted_bids.csv', parse_dates=['timestamp'])

# Pick a few BMUs that appear in BOTH files
accepted_bmus = set(bids_accepted.columns)
submitted_bmus = set(sub_bids['NationalGridBmUnit'].unique())
common = sorted(accepted_bmus & submitted_bmus)[:8]

print("=" * 90)
print(f"SIDE-BY-SIDE PRICE COMPARISON: bids.csv vs submitted_bids.csv ({day})")
print("=" * 90)
print(f"\nCommon BMUs (first 8): {common}")

for bmu in common:
    # From bids.csv (accepted)
    acc_prices = prices_accepted[bmu].dropna()
    acc_vols = vols_accepted[bmu].dropna()
    
    # From submitted_bids.csv
    sub = sub_bids[sub_bids['NationalGridBmUnit'] == bmu]
    
    print(f"\n{'─'*70}")
    print(f"BMU: {bmu}")
    print(f"  bids.csv (accepted):       prices [{acc_prices.min():.2f}, {acc_prices.max():.2f}], "
          f"mean={acc_prices.mean():.2f}, n={len(acc_prices)}")
    print(f"  submitted_bids.csv:        prices [{sub['Bid'].min():.2f}, {sub['Bid'].max():.2f}], "
          f"mean={sub['Bid'].mean():.2f}, n={len(sub)}")
    
    # Check if signs match
    acc_sign = "NEGATIVE" if acc_prices.mean() < 0 else "POSITIVE"
    sub_sign = "NEGATIVE" if sub['Bid'].mean() < 0 else "POSITIVE"
    match = "✓ SAME SIGN" if acc_sign == sub_sign else "✗ SIGN FLIPPED!"
    print(f"  bids.csv sign: {acc_sign}, submitted sign: {sub_sign} → {match}")

# ── Now check the SAME thing for the 2024-06-15 wind day ──
print("\n\n" + "=" * 90)
print("CHECKING 2024-06-15 (the wind day from previous analysis)")
print("=" * 90)

day2 = '2024-06-15'
bids2 = pd.read_csv(DATA_DIR / day2 / 'bids.csv', index_col=[0, 1])
prices2 = bids2.loc[pd.IndexSlice[:, 'price'], :]
prices2.index = prices2.index.droplevel(1)
vols2 = bids2.loc[pd.IndexSlice[:, 'vol'], :]
vols2.index = vols2.index.droplevel(1)

sub2 = pd.read_csv(DATA_DIR / day2 / 'submitted_bids.csv', parse_dates=['timestamp'])

# Check GAOFO wind units specifically
for bmu in ['GAOFO-1', 'GAOFO-2', 'GAOFO-3', 'GAOFO-4']:
    in_accepted = bmu in prices2.columns
    in_submitted = bmu in sub2['NationalGridBmUnit'].values
    
    print(f"\n{'─'*70}")
    print(f"BMU: {bmu}")
    
    if in_accepted:
        ap = prices2[bmu].dropna()
        av = vols2[bmu].dropna()
        print(f"  bids.csv (accepted):       prices [{ap.min():.2f}, {ap.max():.2f}], mean={ap.mean():.2f}")
        print(f"                             vols   [{av.min():.2f}, {av.max():.2f}], mean={av.mean():.2f}")
    else:
        print(f"  bids.csv: NOT PRESENT (not accepted on this day)")
    
    if in_submitted:
        sb = sub2[sub2['NationalGridBmUnit'] == bmu]
        print(f"  submitted_bids.csv:        prices [{sb['Bid'].min():.2f}, {sb['Bid'].max():.2f}], mean={sb['Bid'].mean():.2f}")
        print(f"                             vols   [{sb['LevelFrom'].min():.2f}, {sb['LevelFrom'].max():.2f}]")
    else:
        print(f"  submitted_bids: NOT PRESENT")
    
    if in_accepted and in_submitted:
        acc_sign = "NEGATIVE" if ap.mean() < 0 else "POSITIVE"
        sub_sign = "NEGATIVE" if sb['Bid'].mean() < 0 else "POSITIVE"
        match = "✓ SAME SIGN" if acc_sign == sub_sign else "✗ SIGN FLIPPED!"
        print(f"  → bids.csv sign: {acc_sign}, submitted sign: {sub_sign} → {match}")

# ── Also check some non-wind BMUs on 2024-06-15 ──
print(f"\n\n{'─'*70}")
print("Also checking non-wind BMUs on 2024-06-15:")
common2 = sorted(set(prices2.columns) & set(sub2['NationalGridBmUnit']))[:6]
for bmu in common2:
    ap = prices2[bmu].dropna()
    sb = sub2[sub2['NationalGridBmUnit'] == bmu]
    if len(ap) == 0 or len(sb) == 0:
        continue
    acc_sign = "NEG" if ap.mean() < 0 else "POS"
    sub_sign = "NEG" if sb['Bid'].mean() < 0 else "POS"
    match = "✓ SAME" if acc_sign == sub_sign else "✗ FLIPPED!"
    print(f"  {bmu:20s}  accepted: mean={ap.mean():8.2f} ({acc_sign})  submitted: mean={sb['Bid'].mean():8.2f} ({sub_sign})  {match}")

# ── OVERALL: check ALL common BMUs on 2024-01-08 ──
print(f"\n\n" + "=" * 90)
print("SYSTEMATIC CHECK: ALL common BMUs on 2024-01-08")
print("=" * 90)
bids1 = pd.read_csv(DATA_DIR / '2024-01-08' / 'bids.csv', index_col=[0, 1])
p1 = bids1.loc[pd.IndexSlice[:, 'price'], :]
p1.index = p1.index.droplevel(1)
s1 = pd.read_csv(DATA_DIR / '2024-01-08' / 'submitted_bids.csv', parse_dates=['timestamp'])

common_all = sorted(set(p1.columns) & set(s1['NationalGridBmUnit']))
n_same = 0
n_flip = 0
for bmu in common_all:
    ap = p1[bmu].dropna()
    sb = s1[s1['NationalGridBmUnit'] == bmu]['Bid']
    if len(ap) == 0 or len(sb) == 0:
        continue
    if (ap.mean() < 0) == (sb.mean() < 0):
        n_same += 1
    else:
        n_flip += 1
        print(f"  FLIPPED: {bmu:20s}  accepted={ap.mean():8.2f}  submitted={sb.mean():8.2f}")

print(f"\nTotal common BMUs: {len(common_all)}")
print(f"Same sign: {n_same}")
print(f"Sign flipped: {n_flip}")
