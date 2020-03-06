"""
Script to calculate Level of Travel Time Reliability (LOTTR) per FHWA
guidelines. Calculations for Auto/Bus traffic.

By Kevin Saavedra, Metro, kevin.saavedra@oregonmetro.gov

NOTE: SCRIPT RELIES ON PANDAS v.0.23.0 OR GREATER!
"""

import os
import pandas as pd
import numpy as np
import datetime as dt


def calc_pct_reliability(df_pct):
    """
    Calculates percent reliability of interstate and non-interstate network.
    Args: df_pct, a pandas dataframe.
    Returns: int_rel_pct, % interstate reliability.
             non_int_rel_pct, % non interstate reliability.
    """
    # Auto, Bus interstate
    df_int = df_pct.loc[df_pct['interstate'] == 1]
    df_int_sum = df_int['ttr'].sum()
    df_int_rel = df_int.loc[df_int['reliable'] == 1]
    int_rel_pct = df_int_rel['ttr'].sum() / df_int_sum

    # Auto, Bus non-interstate
    df_non_int = df_pct.loc[df_pct['interstate'] != 1]
    df_non_int_sum = df_non_int['ttr'].sum()
    df_non_int_rel = df_non_int.loc[df_non_int['reliable'] == 1]
    non_int_rel_pct = df_non_int_rel['ttr'].sum() / df_non_int_sum

    return int_rel_pct, non_int_rel_pct


def calc_ttr(df_ttr):
    """Calculate travel time reliability for auto and bus.
    Args: df_ttr, a pandas dataframe.
    Returns: df_ttr, a pandas dataframe with new columns:
             VOLa, Yearly Auto volumes.
             VOLb, Yearly Bus volumes.
             ttr,  Travel Time Reliability.
    """
    # Working vehicle occupancy assumptions:
    VOCa = 1.4
    VOCb = 12.6

    df_ttr['VOLa'] = df_ttr['pct_auto'] * df_ttr['dir_aadt'] * 365
    df_ttr['VOLb'] = df_ttr['pct_bus'] * df_ttr['dir_aadt'] * 365

    # weight miles by nhs_pct
    nhs_prop = df_ttr['nhs_pct'] / 100.0
    print('mean(nhs_prop) = {}'.format(np.mean(nhs_prop)))
    #nhs_prop = 1.0
    df_ttr['ttr'] = (df_ttr['miles'] * nhs_prop * df_ttr['VOLa'] * VOCa
                     + df_ttr['miles'] * nhs_prop * df_ttr['VOLb'] * VOCb)
    return df_ttr


def AADT_splits(df_spl):
    """Calculates AADT per vehicle type.
    Args: df_spl, a pandas dataframe.
    Returns: df_spl, a pandas dataframe containing new columns:
        dir_aadt: directional aadt
        aadt_auto: auto aadt
        pct_auto, pct_bus, pct_truck : percentage mode splits of auto and bus.
    """
    df_spl['dir_aadt'] = (df_spl['aadt']/df_spl['faciltype']).round()
    df_spl['aadt_auto'] = df_spl['dir_aadt'] - \
        (df_spl['aadt_singl'] + df_spl['aadt_combi'])
    df_spl['pct_auto'] = df_spl['aadt_auto'] / df_spl['dir_aadt']
    df_spl['pct_bus'] = df_spl['aadt_singl'] / df_spl['dir_aadt']
    return df_spl


def check_reliable(df_rel):
    """Check reliability of TMCs across time periods.
    Args: df_rel, a pandas dataframe.
    Returns: df_rel, a pandas dataframe with new column:
             reliable, with value 1 if all time periods are reliable.
    """
    df_rel.loc[:, 'reliable'] = np.where(
                                  (df_rel['MF_6_9'] < 1.5)
                                   & (df_rel['MF_10_15'] < 1.5)
                                   & (df_rel['MF_16_19'] < 1.5)
                                   & (df_rel['SATSUN_6_19'] < 1.5),
                                   1, 0)
    return df_rel


def calc_lottr(days, time_period, df_lottr):
    """Calculates LOTTR (Level of Travel Time Reliability) using FHWA metrics.
    Args: df_lottr, a pandas dataframe.
    Returns: df_lottr, a pandas dataframe with new columns:
             80_pct_tt, 95th percentile calculation.
             50_pct_tt, 50th percentile calculation.
             tttr, completed truck travel time reliability calculation.
    """
    df_lottr.loc[:, '80_pct_tt'] = df_lottr['travel_time_seconds']
    df_lottr.loc[:, '50_pct_tt'] = df_lottr['travel_time_seconds']

    tmc_operations = ({'80_pct_tt': lambda x: np.percentile(x, 80),
                       '50_pct_tt': lambda x: np.percentile(x, 50)})

    df_lottr = df_lottr.groupby('tmc_code', as_index=False).agg(tmc_operations)
    column_name = '{0}_{1}'.format(days, time_period)
    df_lottr[column_name] = df_lottr['80_pct_tt'] / df_lottr['50_pct_tt']
    #df_lottr = df_lottr.drop('travel_time_seconds', axis=1)

    return df_lottr



def agg_travel_time_sat_sun(df_tt):
    """Aggregates weekend truck travel time reliability values.
    Args: df_tt, a pandas dataframe.
    Returns: df_ttr_all_times, a pandas dataframe with stacked truck travel
             time reliability numbers for easy group_by characteristics.
    """
    # create 'clean' dataframe consisting of non-duplicated TMCs
    tmc_list = df_tt['tmc_code'].drop_duplicates().values.tolist()
    tmc_format = {'tmc_code': tmc_list}
    df_tmc = pd.DataFrame.from_dict(tmc_format)

    df_6_19 = df_tt[df_tt['measurement_tstamp'].dt.hour.isin(
        [6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19])]

    df = calc_lottr('SATSUN', '6_19', df_6_19)
    df_tmc = pd.merge(df_tmc, df, on='tmc_code', how='left')

    return df_tmc


def agg_travel_times_mf(df_tt):
    """Aggregates weekday truck travel time reliability values.
    Args: df_tt, a pandas dataframe.
    Returns: df_ttr_all_times, a pandas dataframe with stacked truck travel
             time reliability numbers for easy group_by characteristics.
    """
    # create 'clean' dataframe consisting of non-duplicated TMCs
    tmc_list = df_tt['tmc_code'].drop_duplicates().values.tolist()
    tmc_format = {'tmc_code': tmc_list}
    df_tmc = pd.DataFrame.from_dict(tmc_format)

    df_6_9 = df_tt[df_tt['measurement_tstamp'].dt.hour.isin([6, 7, 8, 9])]
    df_10_15 = df_tt[df_tt['measurement_tstamp'].dt.hour.isin([10, 11, 12, 13,
                                                               14, 15])]
    df_16_19 = df_tt[df_tt['measurement_tstamp'].dt.hour.isin([16, 17, 18, 19])]
    data = {'6_9': df_6_9, '10_15': df_10_15, '16_19': df_16_19}

    for key, value in data.items():
        df = calc_lottr('MF', key, value)
        df_tmc = pd.merge(df_tmc, df, on='tmc_code', how='left')

    return df_tmc

def main():
    """Main script to calculate LOTTR."""
    startTime = dt.datetime.now()
    print('Script started at {0}'.format(startTime))
    pd.set_option('display.max_rows', None)

    drive_path = 'H:/map21/2020/data/'
    quarters = ['']
    #quarters = ['2017Q0', '2017Q1', '2017Q2', '2017Q3', '2017Q4']

    folder_end = 'pdx-3co-mtip-2019-all-15min'
    file_end = '.csv'

    df = pd.DataFrame()  # Empty dataframe

    for q in quarters:
        filename = q + folder_end + file_end
        path = q + folder_end
        full_path = path + '/' + filename
        print("Loading {0} data...".format(full_path))
        df_temp = pd.read_csv(
                    os.path.join(
                        os.path.dirname(__file__), drive_path + full_path))
        df = pd.concat([df, df_temp], sort=False)

    # df = df.dropna()
    if sum(pd.isna(df['travel_time_seconds'])) != 0:
        df = df.dropna(subset=['travel_time_seconds'])

    # Filter by timestamps
    print("Filtering timestamps...".format(q))
    df.loc[:, 'measurement_tstamp'] = pd.to_datetime(df['measurement_tstamp'])
    df.loc[:, 'hour'] = df['measurement_tstamp'].dt.hour

    wd = 'H:/map21/2020/data/networks/'

    df = df[df['measurement_tstamp'].dt.hour.isin(
        [6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19])]

    # Join/filter on relevant Metro TMCs
    print("Join/filter on Metro TMCs...")
    df_urban = pd.read_csv(
        os.path.join(os.path.dirname(__file__), wd + 'metro-2019.csv'),
        usecols=('Tmc', 'interstate'))

    # This is necessary in pandas > v.0.22.0 ####
    #df = df.drop('key_0', axis=1)
    #############################################

    #df = pd.merge(df_urban, df, how='inner', left_on=df_urban['Tmc'],
    #              right_on=df['tmc_code'])

    df = pd.merge(df, df_urban, how='right', left_on=df['tmc_code'],
                  right_on=df_urban['Tmc'])
    df = df.drop('key_0', axis=1)

    #df.describe()
    # Apply calculation functions
    print("Applying calculation functions...")
    # df = AADT_splits(df)

    # Separate weekend and weekday dataframes for processing
    df_mf = df[df['measurement_tstamp'].dt.weekday.isin([0, 1, 2, 3, 4])]
    df_sat_sun = df[df['measurement_tstamp'].dt.weekday.isin([5, 6])]
    df_mf = agg_travel_times_mf(df_mf)
    df_sat_sun = agg_travel_time_sat_sun(df_sat_sun)

    # Combined weekend, weekday dataset
    df = pd.merge(df_mf, df_sat_sun, on='tmc_code')
    # Add interstate back (TODO: fix this in aggregate funcs)
    df = pd.merge(df, df_urban, how='left', left_on='tmc_code',
                  right_on='Tmc')
    #df = df.drop('key_0', axis=1)
    df = check_reliable(df)

    # Join TMC Metadata
    print("Join TMC Metadata...")
    df_meta = pd.read_csv(
        os.path.join(
            os.path.dirname(__file__),
            drive_path + folder_end + '/' +
            'TMC_Identification.csv'),
        usecols=['tmc', 'miles', 'tmclinear', 'faciltype', 'aadt',
                 'aadt_singl', 'aadt_combi', 'nhs_pct'])

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
    df = calc_ttr(df)
    print(calc_pct_reliability(df))

    #df.to_csv('lottr_out_2019_mtip2020_nhspct.csv')
    endTime = dt.datetime.now()
    print("Script finished in {0}.".format(endTime - startTime))


if __name__ == '__main__':
    main()
