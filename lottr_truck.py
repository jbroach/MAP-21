"""
Script to calculate Freight Reliability Metric per ODOT Guidance.

By Kevin Saavedra, Metro, kevin.saavedra@oregonmetro.gov

NOTE: SCRIPT RELIES ON PANDAS v.0.23.0 OR GREATER!
Usage:
>>>python lottr_truck.py
"""

import os
import pandas as pd
import numpy as np
import datetime as dt


def calc_freight_reliability(df_rel, nhs_pct_switch=True):
    """
    Calculates TTTR (Truck Travel Time Reliability), AKA freight reliability.
    Args: df_rel, a pandas dataframe.
          nhs_pct_switch, if True weight segment lengths by nhs_pct.
    Returns: df_rel, a pandas dataframe with new columns 'weighted_ttr'
             tttr_index, the full freight reliability index measure of the
             whole interstate system.
    """
    # Only report out for interstate segments
    df_int = df_rel.loc[df_rel['interstate'] == 1].copy()
    if nhs_pct_switch:
        nhs_prop = df_int['nhs_pct'] / 100.0
    else:
        print('NHS pct adjustment is OFF')
        nhs_prop = [1.0 for i in range(len(df_int))]
    print('mean(nhs_prop_int) = {}'.format(np.mean(nhs_prop)))
    # Total length of the interstate system
    df_int_sum = sum(df_int['miles'] * nhs_prop)
    print('df_int_sum = {}'.format(df_int_sum))
    # Calculated weighted tttr for trucks
    df_int.loc[:, 'weighted_ttr'] = df_int['miles'] * nhs_prop * df_int['tttr']
    sum_weighted = df_int['weighted_ttr'].sum()
    tttr_index =  sum_weighted / df_int_sum

    return df_int, tttr_index


def calc_ttr(df_ttr, nhs_pct_switch=True):
    """Calculates travel time reliability.
    Args: df_ttr, a pandas dataframe.
          nhs_pct_switch, if True weight segment lengths by nhs_pct.
    Returns: df_ttr, a pandas dataframe with new ttr column.
    """
    # Working vehicle occupancy assumptions:
    VOCt = 1
    df_ttr['VOLt'] = df_ttr['pct_truck'] * df_ttr['dir_aadt'] * 365

    # weight miles by nhs_pct
    if nhs_pct_switch:
        nhs_prop = df_ttr['nhs_pct'] / 100.0
    else:
        print('NHS pct adjustment is OFF')
        nhs_prop = [1.0 for i in range(len(df_ttr))]
    print('mean(nhs_prop) = {}'.format(np.mean(nhs_prop)))

    df_ttr['ttr'] = df_ttr['miles'] * nhs_prop * df_ttr['VOLt'] * VOCt

    return df_ttr


def AADT_splits(df_spl):
    """Calculates AADT by truck vehicle type.
    Args: df_spl, a pandas dataframe.
    Returns: df_spl, a pandas dataframe containing new columns:
        dir_aadt: directional aadt
        pct_truck: percentage mode splits of trucks.
    """
    df_spl['dir_aadt'] = (df_spl['aadt']/df_spl['faciltype']).round()
    df_spl['pct_truck'] = df_spl['aadt_combi'] / df_spl['dir_aadt']

    return df_spl


def get_max_ttr(df_max):
    """Returns maximum ttr calculated per TMC.
    Args: df_max, a pandas dataframe.
    Returns: df_max, a dataframe containing grouped TMCs with max tttr values.
    """
    ttr_operations = ({'tttr': 'max'})
    df_max = df_max.groupby('tmc_code', as_index=False).agg(ttr_operations)

    return df_max


def calc_lottr(df_lottr):
    """Calculates LOTTR (Level of Travel Time Reliability) using FHWA metrics.
    Args: df_lottr, a pandas dataframe.
    Returns: df_lottr, a pandas dataframe with new columns:
             95_pct_tt, 95th percentile calculation.
             50_pct_tt, 50th percentile calculation.
             tttr, completed truck travel time reliability calculation.
    """
    # df_lottr.loc[:, '95_pct_tt'] = df_lottr['travel_time_seconds']
    # df_lottr.loc[:, '50_pct_tt'] = df_lottr['travel_time_seconds']
    df_lottr['95_pct_tt'] = df_lottr['travel_time_seconds']
    df_lottr['50_pct_tt'] = df_lottr['travel_time_seconds']
    tmc_operations = ({'95_pct_tt': lambda x: np.percentile(x, 95),
                       '50_pct_tt': lambda x: np.percentile(x, 50)})

    df_lottr = df_lottr.groupby('tmc_code', as_index=False).agg(tmc_operations)
    df_lottr['tttr'] = df_lottr['95_pct_tt'] / df_lottr['50_pct_tt']

    return df_lottr


def agg_travel_times(df_tt, days):
    """Aggregates weekday truck travel time reliability values.
    Args: df_tt, a pandas dataframe.
    Returns: df_ttr_all_times, a pandas dataframe with stacked truck travel
             time reliability numbers for easy group_by characteristics.
    """
    # creates df containing all tmcs and ttrs listed vertically
    tmc_list = df_tt['tmc_code'].drop_duplicates().values.tolist()
    tmc_format = {'tmc_code': tmc_list}
    df_tmc = pd.DataFrame.from_dict(tmc_format)

    overnight = [list(range(20, 24)), list(range(0, 7))]
    overnight = [hour for lst in overnight for hour in lst]

    if days == 'MF':
        df_6_9 = df_tt[df_tt['measurement_tstamp'].dt.hour.isin(
            list(range(6, 10)))].copy()  # copy avoids slicing ambiguity
        df_10_15 = df_tt[df_tt['measurement_tstamp'].dt.hour.isin(
            list(range(10, 16)))].copy()
        df_16_19 = df_tt[df_tt['measurement_tstamp'].dt.hour.isin(
            list(range(16, 20)))].copy()

        df_20_6 = df_tt[df_tt['measurement_tstamp']
                        .dt.hour.isin(overnight)].copy()

        df_list = [df_6_9,  df_10_15, df_16_19, df_20_6]

    if days == 'SATSUN':
        df_6_19 = df_tt[df_tt['measurement_tstamp'].dt.hour.isin(
            list(range(6, 20)))].copy() # copy avoids slicing ambiguity

        df_20_6 = df_tt[df_tt['measurement_tstamp']
                        .dt.hour.isin(overnight)].copy()

        df_list = [df_6_19, df_20_6]

    df_ttr_all_times = pd.DataFrame()
    for df in df_list:
        df_temp = calc_lottr(df)
        df_ttr_all_times = pd.concat([df_ttr_all_times, df_temp], sort=False)

    return df_ttr_all_times


def main():
    """Main script to calculate TTTR."""
    startTime = dt.datetime.now()
    print('Script started at {0}'.format(startTime))
    pd.set_option('display.max_rows', None)
    # pd.set_option('mode.chained_assignment', 'raise')

    drive_path = 'H:/map21/2020/data/'
    quarters = ['']
    # quarters = ['2017Q0', '2017Q1', '2017Q2', '2017Q3', '2017Q4']
    folder_end_trucks = 'pdx-3co-mtip-2019-trucks-15min'
    folder_end_all = 'pdx-3co-mtip-2019-all-15min'
    file_end = '.csv'
    # TODO move interstate identification to TMC file parsing
    local_tmcs = 'networks/metro-2019'  # csv file with set of analysis TMCs
    nhs_pct_switch = True  # whether to weight segments by nhs_pct
    outfile = 'lottr_truck_out_2019_mtip2020.csv'

    df = pd.DataFrame()  # Empty dataframe

    for q in quarters:
        filename = q + folder_end_trucks + file_end
        path = q + folder_end_trucks
        full_path = path + '/' + filename
        print("Loading {0} data (Truck)...".format(full_path))
        df_temp = pd.read_csv(
                    os.path.join(
                        os.path.dirname(__file__), drive_path + full_path),
                        usecols=('tmc_code', 'measurement_tstamp',
                                       'travel_time_seconds'))
        df = pd.concat([df, df_temp], sort=False)

    data_start = min(df['measurement_tstamp'])
    data_stop = max(df['measurement_tstamp'])
    print('{} to {}'.format(data_start, data_stop))

    # Load all vehicle files to use where Truck travel times missing or zero
    # folder_end = 'pdx-3co-mtip-2019-all-15min'
    df2 = pd.DataFrame()  # Empty dataframe

    for q in quarters:
        filename = q + folder_end_all + file_end
        path = q + folder_end_all
        full_path = path + '/' + filename
        print("Loading {0} data (All Vehicle)...".format(full_path))
        df_temp = pd.read_csv(
                    os.path.join(
                        os.path.dirname(__file__), drive_path + full_path),
                        usecols=('tmc_code', 'measurement_tstamp',
                                       'travel_time_seconds'))
        df_temp = pd.read_csv(drive_path + full_path)
        df2 = pd.concat([df2, df_temp], sort=False)

    # we'll use all vehicle times where Truck times missing, so all vehicle
    # files define availability
    if sum(pd.isna(df['travel_time_seconds'])) != 0:
        df2 = df2.dropna(subset=['travel_time_seconds'])

    data_start = min(df2['measurement_tstamp'])
    data_stop = max(df2['measurement_tstamp'])
    print('{} to {}'.format(data_start, data_stop))

    print('Merging Truck & All Vehicle data...')
    # len1 = len(df)
    df = pd.merge(df, df2, how='right', on=('tmc_code', 'measurement_tstamp'),
                  suffixes=('', '_all'))

    # Filter by timestamps
    print("Filtering timestamps...".format(q))
    df['measurement_tstamp'] = pd.to_datetime(df['measurement_tstamp'])
    df['hour'] = df['measurement_tstamp'].dt.hour
    # wd = 'H:/map21/2020/data/networks/'

    # Join/filter on relevant Metro TMCs
    print("Join/filter on Metro TMCs...")
    df_urban = pd.read_csv(
        os.path.join(os.path.dirname(__file__), drive_path + local_tmcs
                     + '.csv'),
        usecols=('Tmc', 'interstate'))
    df = pd.merge(df, df_urban, how='inner', left_on=df['tmc_code'],
                  right_on=df_urban['Tmc'])
    # df = df.drop('key_0', axis=1)

    # Swap in All vehicle values where Truck missing or zero
    df['travel_time_seconds'] = np.where(pd.isna(df['travel_time_seconds'])
      | (df['travel_time_seconds'] == 0),
      df['travel_time_seconds_all'], df['travel_time_seconds'])

    # Apply calculation functions
    print("Applying calculation functions...")
    # Separate weekend and weekday dataframes for processing
    df_mf = df[df['measurement_tstamp'].dt.weekday.isin([0, 1, 2, 3, 4])]
    df_sat_sun = df[df['measurement_tstamp'].dt.weekday.isin([5, 6])]
    df_mf = agg_travel_times(df_mf, 'MF')
    df_sat_sun = agg_travel_times(df_sat_sun, 'SATSUN')

    # Combine weekend, weekday dataset
    df = pd.concat([df_mf, df_sat_sun], sort=False)
    df = get_max_ttr(df)

    # Add interstate back (TODO: fix this in aggregate funcs)
    df = pd.merge(df, df_urban, how='left', left_on='tmc_code',
                  right_on='Tmc')

    # Join TMC Metadata
    print("Join TMC Metadata...")
    df_meta = pd.read_csv(
        os.path.join(
            os.path.dirname(__file__),
            drive_path + folder_end_all + '/'
            + 'TMC_Identification.csv'),
        usecols=['tmc', 'miles', 'faciltype', 'aadt', 'aadt_singl',
                 'aadt_combi', 'nhs_pct', 'active_end_date'])

    # Downloaded file sometimes has duplicate records because of an apparent
    #   midnight overlap bug in the massive data downloader.
    #   Previously, this was handled in aggregate functions
    #   but those don't necessarily keep correct date range for data
    print(len(df_meta))
    df_meta = df_meta[df_meta['active_end_date'].str[:19] > data_start]
    print(len(df_meta))

    # Occasionally, TMCs duplicated (e.g. 2018 114+04459)
    # duplicate missing HPMS vars needed in calcs
    df_meta = df_meta.dropna(subset=['nhs_pct'])
    df = pd.merge(df, df_meta, left_on=df['tmc_code'],
                  right_on=df_meta['tmc'], how='inner')

    # ###########This is necessary in pandas > v.0.22.0 ####
    df = df.drop('key_0', axis=1)
    ########################################################

    # Note: superceded by single network file w/ `interstate` attribute
    # Join Interstate values
    # df_interstate = pd.read_csv(
    #     os.path.join(os.path.dirname(__file__), wd + 'interstate_tmc_092618.csv'))
    # df = pd.merge(df, df_interstate, left_on='tmc_code', right_on='Tmc',
    #               how='left')

    df = AADT_splits(df)
    # df = calc_ttr(df, nhs_pct_switch=nhs_pct_switch)  # not needed for freight
    df, reliability_index = calc_freight_reliability(df,
      nhs_pct_switch=nhs_pct_switch)
    print(reliability_index)

    if outfile:
        df.to_csv(outfile)
    endTime = dt.datetime.now()
    print("Script finished in {0}.".format(endTime - startTime))


if __name__ == '__main__':
    main()
