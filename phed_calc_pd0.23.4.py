"""
Script to calculate Peak Hr. Excessive Delay per FHWA guidelines.
Joins Metro peaking factor csv for calibrated hourly aadt volumes and HERE
data, provided by ODOT for relevant speed limits on TMCs.

Script by Kevin Saavedra, Metro, kevin.saavedra@oregonmetro.gov
Adapted from Excel tables created by Rich Arnold, P.E., ODOT

NOTE: SCRIPT RELIES ON PANDAS v.0.23.0 OR GREATER!
"""

import os
import pandas as pd
import numpy as np
import datetime as dt


def per_capita_TED(sum_12_mo):
    """Calculates final Peak Hour Excessive Delay number.
    Args: sum_11_mo, the integer sum of all TED values.
    Returns: A value for Peak Hour Excessive Delay per capita.
    """
    # year_adjusted_TED = (sum_11_mo / 11) + sum_11_mo
    pop_PDX = 1577456
    # return year_adjusted_TED / pop_PDX
    return sum_12_mo / pop_PDX


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


def total_excessive_delay(df_ted):
    """Calculates Total Excessive Delay per given TMC using padas groupby
    function.
    Args: df_ted, a pandas dataframe.
    Returns: df_ted, a pandas dataframe grouped by TMC.
    """
    df_ted['TED_seg'] = (df_ted['ED'] * df_ted['PK_HR'])
    ted_operations = ({'TED_seg': 'sum',
                       'pct_auto': 'max',
                       'pct_bus': 'max',
                       'pct_truck': 'max'})
    df_ted = df_ted.groupby('tmc_code', as_index=False).agg(ted_operations)
    return df_ted


def peak_hr(df_pk):
    """Performs Peak Hour calculations by combining directional aadt values
    with vehicle hourly volume factors determined by Metro.
    Args: df_pk, a pandas dataframe.
    Returns: df_pk, a pandas dataframe conaining new PK_HR column with
    completed calculations.
    """
    df_pk['PK_HR'] = (df_pk['dir_aadt'] *
                      df_pk['2015_15-min_Combined'])
    return df_pk


def excessive_delay(df_ed):
    """Calculates Excessive Delay.
    Args: df_ed, a pandas dataframe.
    Returns: df_ed, a pandas dataframe containing new column ED with completed
    calculations."""
    df_ed['ED'] = df_ed['RSD'] / 3600  # check this value hundredths of an hour
    df_ed['ED'] = df_ed['ED']
    df_ed['ED'] = np.where(df_ed['ED'] >= 0, df_ed['ED'], 0)
    return df_ed


def RSD(df_rsd):
    """Calculates RSD (Travel Time Segment delay).
    Args: df_rsd, a pandas dataframe.
    Returns, df_rsd, a pandas dataframe with new column RSD with completed
    calculations.
    """
    df_rsd['RSD'] = df_rsd['travel_time_seconds'] - df_rsd['SD']
    df_rsd['RSD'] = np.where(df_rsd['RSD'] >= 0, df_rsd['RSD'], 0)
    return df_rsd


def segment_delay(df_sd):
    """Calculates Excessive Delay Threshold Travel Time (EDTTT).
    Args: df_sd, a pandas dataframe.
    Returns: df_sd, a pandas dataframe with new column SD with completed
    calculations.
    """
    df_sd['SD'] = (df_sd['miles'] / df_sd['TS']) * 3600
    return df_sd


def AADT_splits(df_spl):
    """Calculates AADT per vehicle type.
    Args: df_spl, a pandas dataframe.
    Returns: df_spl, a pandas dataframe containing new columns:
        dir_aadt: directional aadt
        aadt_auto: auto aadt
        pct_auto, pct_bus, pct_truck : percentage mode splits of auto, bus and
        trucks.
    """
    df_spl['dir_aadt'] = (df_spl['aadt']/df_spl['faciltype'])
    df_spl['aadt_auto'] = df_spl['dir_aadt'] - \
        (df_spl['aadt_singl'] + df_spl['aadt_combi'])
    df_spl['pct_auto'] = df_spl['aadt_auto'] / df_spl['dir_aadt']
    df_spl['pct_bus'] = df_spl['aadt_singl'] / df_spl['dir_aadt']
    df_spl['pct_truck'] = df_spl['aadt_combi'] / df_spl['dir_aadt']
    return df_spl


def threshold_speed(df_ts):
    """Calculates Threshold Speed, defined as the larger of 20mph or
    Posted Speed Limit * .6.
    Args: df_ts, a pandas dataframe.
    Returns: df_ts, A pandas dataframe with new columns:
        'posted_mult', 'TS'.
    """
    df_ts['posted_mult'] = df_ts['SPEED_LIMIT'] * .6
    df_ts['TS'] = np.where(df_ts['posted_mult'] > 20, df_ts['posted_mult'], 20)
    return df_ts


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

    ###########################################################################

    ###########################################################################
    #              UNCOMMENT TO USE ONE-MONTH TEST DATSET                     #
    # df = pd.read_csv(os.path.join(os.path.dirname(__file__),
    # 'Feb2017_test/Feb2017_test.csv'))
    ###########################################################################

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

    # Capture weekdays only
    df = df[df['measurement_tstamp'].dt.weekday.isin([0, 1, 2, 3, 4])]
    df = df[df['measurement_tstamp'].dt.hour.isin(
        [6, 7, 8, 9, 10, 15, 16, 17, 18, 19])]

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

    # Apply calculation functions
    print("Applying calculation functions...")
    df = threshold_speed(df)
    df = AADT_splits(df)
    df = segment_delay(df)
    df = RSD(df)
    df = excessive_delay(df)
    df = peak_hr(df)
    df = total_excessive_delay(df)
    df = TED_summation(df)
    df = df[['tmc_code', 'TED']]
    df.to_csv('phed_out.csv')

    result = round(per_capita_TED(df['TED'].sum()), 2)
    print("==================================================================")
    print("Calulated {} peak hour excessive delay per capita."
          .format(str(result)))
    print("==================================================================")
    endTime = dt.datetime.now()
    print("Script finished in {0}.".format(endTime - startTime))


if __name__ == '__main__':
    main()
