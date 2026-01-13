import pandas as pd


# data on daylight savings start and end dates
#pd.to_datetime is used to ensure the dates are in the correct format, it converts strings to Timestamps.
dst_start_dates = pd.to_datetime([
    '2019-03-31',
    '2020-03-29',
    '2021-03-28',
    '2022-03-27',
    '2023-03-26',
    '2024-03-31',
    '2025-03-30',
    '2026-03-29',
])
dst_end_dates = pd.to_datetime([
    '2019-10-27',
    '2020-10-25',
    '2021-10-31',
    '2022-10-30',
    '2023-10-29',
    '2024-10-27',
    '2025-10-26',
    '2026-10-25',
])


########################   BUILDING DATA REGISTER   #######################
def classify_day(day):

    if day in dst_start_dates:
        return 'start_savings'
    elif day in dst_end_dates:
        return 'end_savings'
    elif (sum(day > dst_start_dates) + sum(day > dst_end_dates)) % 2 == 0:
        return 'winter'
    elif (sum(day > dst_start_dates) + sum(day > dst_end_dates)) % 2 == 1:
        return 'summer'

    raise ValueError('Couldnt classify date.')

# build a settlement period register for a given day
def build_sp_register(day):
    mode = classify_day(day)

# convert day to start of day in UTC, accounting for local time zone (Europe/London) and DST changes
    start = pd.Timestamp(day, tz='Europe/London').tz_convert('UTC')

    if mode == 'winter' or mode == 'summer':
        periods = 48
    elif mode == 'start_savings':
        periods = 46
    elif mode == 'end_savings':
        periods = 50
    else:
        raise ValueError(f'Mode {mode} not recognised.')

# build dataframe with settlement periods as a column and datetime index 
    return pd.DataFrame(
        {
            'settlement_period': range(1, periods+1)
            },
        index=pd.date_range(start, periods=periods, freq='30min', tz='UTC')
    )
