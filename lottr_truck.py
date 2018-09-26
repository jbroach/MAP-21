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


def calc_freight_reliability(df_rel):
    df_int = df_rel.loc[df_rel['interstate'] == 1]
    df_int_sum = df_int['miles'].sum()
    
    df_int['weighted_ttr'] = df_int['miles'] * df_int['tttr']  
    sum_weighted = df_int['weighted_ttr'].sum()

    ttr_index =  sum_weighted / df_int_sum
    return df_rel, ttr_index


def calc_ttr(df_ttr):
    """Calculate travel time reliability for auto and bus passengers.
    """
    # Working vehicle occupancy assumptions:
    VOCt = 1
    df_ttr['VOLt'] = df_ttr['pct_truck'] * df_ttr['dir_aadt'] * 365
    df_ttr['ttr'] = df_ttr['miles'] * df_ttr['VOLt'] * VOCt
    
    return df_ttr


def AADT_splits(df_spl):
    """Calculates AADT per vehicle type.
    Args: df_spl, a pandas dataframe.
    Returns: df_spl, a pandas dataframe containing new columns:
        dir_aadt: directional aadt
        pct_truck : percentage mode splits of trucks.
    """
    df_spl['dir_aadt'] = (df_spl['aadt']/df_spl['faciltype']).round()
    df_spl['pct_truck'] = df_spl['aadt_combi'] / df_spl['dir_aadt']

    return df_spl


def get_max_ttr(df_max):
    ttr_operations = ({'tttr': 'max'})
    df_max = df_max.groupby('tmc_code', as_index=False).agg(ttr_operations)
    return df_max


def calc_lottr(df_lottr):
    df_lottr['95_pct_tt'] = df_lottr['travel_time_seconds']
    df_lottr['50_pct_tt'] = df_lottr['travel_time_seconds'] 

    tmc_operations = ({'95_pct_tt': lambda x: np.percentile(x, 95),
                       '50_pct_tt': lambda x: np.percentile(x, 50)})
    
    df_lottr = df_lottr.groupby('tmc_code', as_index=False).agg(tmc_operations)
    df_lottr['tttr'] = df_lottr['95_pct_tt'] / df_lottr['50_pct_tt']
    #df_lottr = df_lottr.drop('travel_time_seconds', axis=1)

    return df_lottr
  

def agg_travel_time_sat_sun(df_tt):
    """Aggregates weekend values."""
    tmc_list = df_tt['tmc_code'].drop_duplicates().values.tolist()
    tmc_format = {'tmc_code': tmc_list}
    df_tmc = pd.DataFrame.from_dict(tmc_format)

    df_6_19 = df_tt[df_tt['measurement_tstamp'].dt.hour.isin(
        [6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19])]
    df_20_6 = df_tt[df_tt['measurement_tstamp'].dt.hour.isin(
        [20, 21, 22, 23, 0, 1, 2, 3, 4, 5, 6])]
    
    df_list = [df_6_19, df_20_6]
    
    df_ttr_all_times = pd.DataFrame()
    for df in df_list:
        df_temp = calc_lottr(df)
        df_ttr_all_times = pd.concat([df_ttr_all_times, df_temp], sort=False)
    df_ttr_all_times.to_csv('tmc_by_time_period_satsun.csv')
    return df_ttr_all_times


def agg_travel_times_mf(df_tt):
    """Aggregates weekday values"""
    # creates df containing all tmcs and ttrs listed vertically
    tmc_list = df_tt['tmc_code'].drop_duplicates().values.tolist()
    tmc_format = {'tmc_code': tmc_list}
    df_tmc = pd.DataFrame.from_dict(tmc_format)

    df_6_9 = df_tt[df_tt['measurement_tstamp'].dt.hour.isin([6, 7, 8, 9])]
    df_10_15 = df_tt[df_tt['measurement_tstamp'].dt.hour.isin([10, 11, 12, 13, 
                                                               14, 15])]
    df_16_19 = df_tt[df_tt['measurement_tstamp'].dt.hour.isin([16, 17, 18, 19])]
    df_20_6 = df_tt[df_tt['measurement_tstamp'].dt.hour.isin(
        [20, 21, 22, 23, 0, 1, 2, 3, 4, 5, 6])]
    
    df_list = [df_6_9,  df_10_15, df_16_19, df_20_6]
    
    df_ttr_all_times = pd.DataFrame()
    for df in df_list:
        df_temp = calc_lottr(df)
        df_ttr_all_times = pd.concat([df_ttr_all_times, df_temp], sort=False)
    
    df_ttr_all_times.to_csv('tmc_by_time_period_mf.csv')
    return df_ttr_all_times


def main():
    """Main script to calculate PHED."""
    startTime = dt.datetime.now()
    print('Script started at {0}'.format(startTime))
    pd.set_option('display.max_rows', None)


    drive_path = 'H:/map21/perfMeasures/phed/data/original_data/'
    #quarters = ['2017Q0']
    quarters = ['2017Q0', '2017Q1', '2017Q2', '2017Q3', '2017Q4']

    folder_end = '_TriCounty_Metro_15-min'
    file_end = '_NPMRDS (Trucks).csv'

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
    df = df.dropna()

    # Filter by timestamps
    print("Filtering timestamps...".format(q))
    df['measurement_tstamp'] = pd.to_datetime(df['measurement_tstamp'])
    df['hour'] = df['measurement_tstamp'].dt.hour
    wd = 'H:/map21/perfMeasures/phed/data/'
    
    # Join/filter on relevant Metro TMCs
    print("Join/filter on Metro TMCs...")
    df_urban = pd.read_csv(
        os.path.join(os.path.dirname(__file__), wd + 'metro_tmc_092618.csv'))   
    
    df = pd.merge(df, df_urban, how='right', left_on=df['tmc_code'], 
                  right_on=df_urban['Tmc'])
    df = df.drop('key_0', axis=1)
    #print(df.shape, df['travel_time_seconds'].sum())

    # Apply calculation functions
    print("Applying calculation functions...")

    # Separate weekend and weekday dataframes for processing
    df_mf = df[df['measurement_tstamp'].dt.weekday.isin([0, 1, 2, 3, 4])]
    df_sat_sun = df[df['measurement_tstamp'].dt.weekday.isin([5, 6])]
    df_mf = agg_travel_times_mf(df_mf)
    df_sat_sun = agg_travel_time_sat_sun(df_sat_sun)

    # Combine weekend, weekday dataset
    df = pd.concat([df_mf, df_sat_sun], sort=False)
    df = get_max_ttr(df)

    # Join TMC Metadata
    print("Join TMC Metadata...")
    df_meta = pd.read_csv(
        os.path.join(
            os.path.dirname(__file__),
            wd +
            'TMC_Identification_NPMRDS (Trucks and passenger vehicles).csv'),
        usecols=['tmc', 'miles', 'faciltype', 'aadt', 'aadt_singl', 
                 'aadt_combi'])

    df = pd.merge(df, df_meta, left_on=df['tmc_code'],
                  right_on=df_meta['tmc'], how='inner')

    # ###########This is necessary in pandas > v.0.22.0 ####
    df = df.drop('key_0', axis=1)
    ########################################################

    # Join Interstate values
    df_interstate = pd.read_csv(
        os.path.join(os.path.dirname(__file__), wd + 'interstate_tmc_092618.csv'))
    df = pd.merge(df, df_interstate, left_on='tmc_code', right_on='Tmc', 
                  how='inner')

    df = AADT_splits(df)
    df = calc_ttr(df)
    df, reliability_index = calc_freight_reliability(df)
    print(reliability_index)

    df.to_csv('lottr_truck_out.csv')
    endTime = dt.datetime.now()
    print("Script finished in {0}.".format(endTime - startTime))


if __name__ == '__main__':
    main()
