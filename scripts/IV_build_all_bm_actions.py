"""
Fetches ALL submitted bids and offers from Elexon API (not just accepted).
Outputs raw data to CSV for use in explicit balancing simulations.
"""

import logging
import requests
import pandas as pd
from io import StringIO
from tqdm import tqdm
from _elexon_helpers import robust_request

from _helpers import to_datetime, configure_logging
from _constants import build_sp_register

logger = logging.getLogger(__name__)


# to get prices of bids and offers 
trades_url = (
        'https://data.elexon.co.uk/bmrs/api/v1/balancing/bid-offer/' +
        'all?settlementDate={}&settlementPeriod={}&format=csv'
    )


def get_all_submitted_trades(date, period):
    response = robust_request(requests.get, trades_url.format(date, period), wait_time=2)
    response.raise_for_status()
    return pd.read_csv(StringIO(response.text))


def build_submitted_data(day, periods):
    all_bids = []
    all_offers = []
    
    for period in tqdm(periods, desc="Fetching bids/offers"):
        trades = get_all_submitted_trades(day, period)
        if trades.empty:
            continue
        
        timestamp = to_datetime(day, period)
        trades['timestamp'] = timestamp
        
        bids = trades[trades['PairId'] < 0][['timestamp', 'NationalGridBmUnit', 'PairId', 'Bid', 'LevelFrom', 'LevelTo']]
        offers = trades[trades['PairId'] > 0][['timestamp', 'NationalGridBmUnit', 'PairId', 'Offer', 'LevelFrom', 'LevelTo']]
        
        all_bids.append(bids)
        all_offers.append(offers)
    
    return pd.concat(all_bids, ignore_index=True), pd.concat(all_offers, ignore_index=True)


if __name__ == '__main__':
    configure_logging(snakemake)
    
    day = snakemake.wildcards.day
    sp_register = build_sp_register(day)
    periods = sp_register.settlement_period.tolist()
    
    logger.info(f"Fetching submitted bids/offers for {day}")
    
    bids_df, offers_df = build_submitted_data(day, periods)
    
    bids_df.to_csv(snakemake.output.submitted_bids, index=False)
    offers_df.to_csv(snakemake.output.submitted_offers, index=False)
    
    logger.info(f"Saved {len(bids_df)} bids, {len(offers_df)} offers")

