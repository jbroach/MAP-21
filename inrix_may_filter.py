"""
inrix_may_filter.py

One-time use script to calculate average travel times for non-Memorial Day
Tu, W, Thu in May.

by Kevin Saavedra, kevin.saavedra@oregonmetro.gov
"""

import os
import pandas as pd


def tt_by_hour(df_tt, hour):
    """Process hourly travel time averages."""
    df_tt = df_tt[df_tt['measurement_tstamp'].dt.hour.isin([hour])]
    tmc_operations = ({'travel_time_seconds': 'mean'})
    df_tt = df_tt.groupby('tmc_code', as_index=False).agg(tmc_operations)
    df_avg_tt = df_tt.rename(
        columns={'travel_time_seconds': 'hour_{}_tt_seconds'.format(hour)})
    return df_avg_tt


def main():
    drive_path = 'H:/map21/perfMeasures/phed/data/original_data/'
    quarters = ['2017Q2']
    folder_end = '_TriCounty_Metro_15-min'
    file_end = '_NPMRDS (Trucks and passenger vehicles).csv'

    for q in quarters:
        filename = q + folder_end + file_end
        path = q + folder_end
        full_path = path + '/' + filename
        print("Loading {0} data...".format(q))
        df = pd.read_csv(
                os.path.join(
                    os.path.dirname(__file__), drive_path + full_path))

    print("Filtering timestamps...".format(q))
    df['measurement_tstamp'] = pd.to_datetime(df['measurement_tstamp'])

    # Filter for May only.
    df = df[df['measurement_tstamp'].dt.month.isin([5])]

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
        df_time = tt_by_hour(df, hour)
        df_tmc = pd.merge(df_tmc, df_time, on='tmc_code', how='left')

    df_tmc.to_csv('may_2017_INRIX.csv', index=False)


if __name__ == '__main__':
    main()
