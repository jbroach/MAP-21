"""
Script to calculate Level of Travel Time Reliability (LOTTR) per FHWA 
guidelines.

Joins Metro peaking factor csv for calibrated hourly aadt volumes and HERE
data, provided by ODOT for relevant speed limits on TMCs.

Script by Kevin Saavedra, Metro, kevin.saavedra@oregonmetro.gov

NOTE: SCRIPT RELIES ON PANDAS v.0.23.0 OR GREATER!
"""

import os
import pandas as pd
import numpy as np
import datetime as dt

def TED_summation(df_teds):
    """Calculates final TED summation.
    Args: df_teds, a pandas dataframe.
    Returns: df_teds, a pandas dataframe with new columns:
        AVOc, AVOb, AVOt (car, bus, and truck average vehicle occupancy).
    """
    # Working vehicle occupancy assumptions:
    VOCa = 1.4
    VOCb = 12.6
    VOCt = 1
    df_teds['AVOc'] = df_teds['pct_auto'] * VOCa
    df_teds['AVOb'] = df_teds['pct_bus'] * VOCb
    df_teds['AVOt'] = df_teds['pct_truck'] * VOCt
    df_teds['TED'] = (df_teds['TED_seg'] *
                      (df_teds['AVOc'] + df_teds['AVOb'] + df_teds['AVOt'])
                      )
    return df_teds


def tmc_group_operations(df_in):
    tmc_operations = ({'LENGTH': 'max',
                       'SPDLIMIT': 'max',
                       'FREEFLOW': 'mean',
                       'MEAN': 'mean',
                       'MEAN_5': lambda x: np.percentile(x, 5),
                       'MEAN_95': lambda x: np.percentile(x, 95),
                       'CONFIDENCE': 'mean'})
    df_in = df_in.groupby('TMC', as_index=False).agg(tmc_operations)
    return df_tmc


def calc_lottr(df_lottr, time_period):
    tmc_operations = ({'travel_time_seconds': 'mean'})
    df_lottr = df_lottr.groupby('tmc_code', as_index=False).agg(tmc_operations)

    column_name = 'MF_{}'.format(time_period)
    df_lottr[column_name] = np.percentile(df_lottr['travel_time_seconds'], 8) / df_lottr['travel_time_seconds']
    return df_lottr


def agg_travel_times_mf(df_tt):
    # create empty dataframe
    df_combined = pd.DataFrame()

    df_6_9 = df_tt['measurement_tstamp'].dt.hour.isin([6, 7, 8, 9])
    df_10_15 = df_tt['measurement_tstamp'].dt.hour.isin([10, 11, 12, 13, 14, 15])
    df_16_19 = df_tt['measurement_tstamp'].dt.hour.isin([16, 17, 18, 19])

    data = {df_6_9: '6_9', df_10_15: '10_15', df_16_19: '16_19'}
    for key in data.items():
        df = calc_lottr(key, value)
        df = pd.concat([df_combined, df], sort=False)

def AADT_splits(df_spl):
    """Calculates AADT per vehicle type.
    Args: df_spl, a pandas dataframe.
    Returns: df_spl, a pandas dataframe containing new columns:
        dir_aadt: directional aadt
        aadt_auto: auto aadt
        pct_auto, pct_bus, pct_truck : percentage mode splits of auto, bus and
        trucks.
    """
    df_spl['dir_aadt'] = (df_spl['aadt']/df_spl['faciltype']).round()
    df_spl['aadt_auto'] = df_spl['dir_aadt'] - \
        (df_spl['aadt_singl'] + df_spl['aadt_combi'])
    df_spl['pct_auto'] = df_spl['aadt_auto'] / df_spl['dir_aadt']
    df_spl['pct_bus'] = df_spl['aadt_singl'] / df_spl['dir_aadt']
    df_spl['pct_truck'] = df_spl['aadt_combi'] / df_spl['dir_aadt']
    return df_spl


def main():
    """Main script to calculate PHED."""
    startTime = dt.datetime.now()
    print('Script started at {0}'.format(startTime))
    pd.set_option('display.max_rows', None)

    ###############################################################
    #               UNCOMMENT FOR FULL DATASET                    #
    drive_path = 'H:/map21/perfMeasures/phed/data/original_data/'
    quarters = ['2017Q0', '2017Q1', '2017Q2', '2017Q3', '2017Q4']
    folder_end = '_TriCounty_Metro_15-min'
    file_end = '_NPMRDS (Trucks and passenger vehicles).csv'

    df = pd.DataFrame()  # Empty dataframe

    for q in quarters:
        filename = q + folder_end + file_end
        path = q + folder_end
        full_path = path + '/' + filename
        print("Loading {0} data...".format(q))
        df_temp = pd.read_csv(
                    os.path.join(
                        os.path.dirname(__file__), drive_path + full_path))
        df = pd.concat([df, df_temp], sort=False)


    # Filter by timestamps
    print("Filtering timestamps...".format(q))
    df['measurement_tstamp'] = pd.to_datetime(df['measurement_tstamp'])
    df['hour'] = df['measurement_tstamp'].dt.hour

    wd = 'H:/map21/perfMeasures/phed/data/'
    # Join peakingFactor data
    df_peak = pd.read_csv(
        os.path.join(
            os.path.dirname(__file__),
            wd + 'peakingFactors_join_edit.csv'),
        usecols=['startTime', '2015_15-min_Combined'])
    df_peak['pk_hour'] = pd.to_datetime(df_peak['startTime']).dt.hour
    df = pd.merge(
        df, df_peak, left_on=df['hour'],
        right_on=df_peak['pk_hour'], how='left')

    df = df[df['measurement_tstamp'].dt.hour.isin(
        [6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19])]

    # Join/filter on relevant urban TMCs
    print("Join/filter on urban TMCs...")
    df_urban = pd.read_csv(
        os.path.join(os.path.dirname(__file__), wd + 'urban_tmc.csv'))

    # This is necessary in pandas > v.0.22.0 ####
    df = df.drop('key_0', axis=1)
    #############################################

    df = pd.merge(df_urban, df, how='inner', left_on=df_urban['Tmc'],
                  right_on=df['tmc_code'])
    df = df.drop('key_0', axis=1)



    """
    # Join TMC Metadata
    print("Join TMC Metadata...")
    df_meta = pd.read_csv(
        os.path.join(
            os.path.dirname(__file__),
            wd +
            'TMC_Identification_NPMRDS (Trucks and passenger vehicles).csv'),
        usecols=['tmc', 'miles', 'tmclinear', 'faciltype', 'aadt',
                 'aadt_singl', 'aadt_combi'])

    df = pd.merge(df, df_meta, left_on=df['tmc_code'],
                  right_on=df_meta['tmc'], how='inner')
    # This is necessary in pandas > v.0.22.0 ####
    df = df.drop('key_0', axis=1)
    #############################################

    # Join HERE data
    df_here = pd.read_csv(
        os.path.join(
            os.path.dirname(__file__), wd +
            'HERE_OR_Static_TriCounty_edit.csv'),
        usecols=['TMC_HERE', 'SPEED_LIMIT'])

    df = pd.merge(df, df_here, left_on=df['tmc_code'],
                  right_on=df_here['TMC_HERE'], how='left', validate='m:1')
    # This is necessary in pandas > v.0.22.0 ####
    df = df.drop('key_0', axis=1)
    #############################################
    """

    # Apply calculation functions
    print("Applying calculation functions...")
    # df = AADT_splits(df)

    # Separate weekend and weekday dataframes for processing
    df_mf = df['measurement_tstamp'].dt.weekday.isin([0, 1, 2, 3, 4])
    df_sat_sun = df['measurement_tstamp'].dt.weekday.isin([5, 6])

    df_new = pd.DataFrame()
    df_lst = [df_mf, df_sat_sun]
    for df in df_lst:
        temp = agg_travel_times_mf(df)
        df_new = pd.concat([df_new, temp], sort=False)

    df_new.to_csv('lottr_out.csv')

    endTime = dt.datetime.now()
    print("Script finished in {0}.".format(endTime - startTime))


if __name__ == '__main__':
    main()
