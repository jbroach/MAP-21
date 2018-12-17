"""
inrix_may_filter_pctile.py

One-time use script to calculate average travel times for non-Memorial Day
Tu, W, Thu in May.

Utilizes unaveraged data from RITIS site.

by Kevin Saavedra, kevin.saavedra@oregonmetro.gov
"""

import os
import pandas as pd
import numpy as np


def tt_by_hour(df_tt, hour):
    """Process hourly travel time averages."""
    df_tt = df_tt[df_tt['measurement_tstamp'].dt.hour.isin([hour])]
    tmc_operations = ({'travel_time_seconds': 'mean',
                       'hour_{0}_5th_pct'.format(hour).format(hour): lambda x: np.percentile(x, 5),
                       'hour_{0}_95th_pct'.format(hour).format(hour): lambda x: np.percentile(x, 95)})
    df_tt = df_tt.groupby('tmc_code', as_index=False).agg(tmc_operations)
    df_avg_tt = df_tt.rename(
        columns={'travel_time_seconds': 'hour_{}_mean_tt_seconds'.format(hour)})
    return df_avg_tt


def main():
    drive_path = 'H:/map21/perfMeasures/phed/data/may_2017_no_averaging/'
    filename = 'may_2017_no_averaging.csv'

    full_path = drive_path + filename
    print("Loading data...")
    df = pd.read_csv(
            os.path.join(
                os.path.dirname(__file__), full_path))

    print("Filtering timestamps...")
    df['measurement_tstamp'] = pd.to_datetime(df['measurement_tstamp'])

    # Filter for Tuesday (excludes days following Memorial Day)
    df = df[df['measurement_tstamp'].dt.day.isin(
        [2, 3, 4, 9, 10, 11, 16, 17, 18, 23, 24, 25])]
    df = df.dropna()

    tmc_list = df['tmc_code'].drop_duplicates().values.tolist()
    tmc_format = {'tmc_code': tmc_list}
    df_tmc = pd.DataFrame.from_dict(tmc_format)

    # Add segment length from metadata
    print("Join TMC Metadata...")
    wd = 'H:/map21/perfMeasures/phed/data/'
    df_meta = pd.read_csv(
        os.path.join(
            os.path.dirname(__file__),
            wd +
            'TMC_Identification_NPMRDS (Trucks and passenger vehicles).csv'),
        usecols=['tmc', 'miles'])

    df_tmc = pd.merge(df_tmc, df_meta, left_on='tmc_code',
                      right_on='tmc', how='inner')
    df_tmc = df_tmc.drop(columns=['tmc'])

    hours = list(range(0, 24))
    for hour in hours:
        df['hour_{0}_95th_pct'.format(hour)] = df['travel_time_seconds']
        df['hour_{0}_5th_pct'.format(hour)] = df['travel_time_seconds']
        df_time = tt_by_hour(df, hour)
        df_tmc = pd.merge(df_tmc, df_time, on='tmc_code', how='left')

    df_tmc.to_csv('may_2017_INRIX_pctile.csv', index=False)


if __name__ == '__main__':
    main()
