# Script by Kevin Saavedra, Metro, kevin.saavedra@oregonmetro.gov
# Adapted from Excel tables by Rich Arnold, P.E., ODOT

import os
import pandas as pd
import numpy as np
import datetime as dt

def TED_summation(df_teds):
    ########## Vehicle occupancy numbers
    VOCa = 1.4 
    VOCb = 10
    VOCt = 1
    ########## Working assumptions above
    df_teds['AVOc'] = df_teds['pct_auto'] * VOCa
    df_teds['AVOb'] = df_teds['pct_bus'] * VOCb
    df_teds['AVOt'] = df_teds['pct_truck'] * VOCt
    df_teds['TED'] = (df_teds['TED_seg'] * (df_teds['AVOc'] + df_teds['AVOb'] + df_teds['AVOt'])).round(3) 
    return df_teds

def total_excessive_delay(df_ted):
    df_ted['TED_seg'] = (df_ted['ED'] * df_ted['PK_HR']).round(2)
    df_ted['TED_seg'] = df_ted['TED_seg'].round() # Need this line to fix excel rounding weirdness
    #df_ted = df_ted[['tmc_code', 'RSD', 'PK_HR', 'ED', 'PK_HR', 'TED_seg']]
    #Groupby for summation by TMC
    
    ted_operations = ({'TED_seg' : 'sum',
                      'pct_auto' : 'max',
                      'pct_bus'  : 'max',
                      'pct_truck': 'max'})
    df_ted = df_ted.groupby('tmc_code', as_index=False).agg(ted_operations)
    
    return df_ted
    
def peak_hr(df_pk):
    ### Uncomment to use ODOT Methodology
    #df_pk['PK_HR'] = (df_pk['DirAADT_AUTO']/4).round()
    ###
    # Below uses Metro peaking calibrations.
    df_pk['PK_HR'] = (df_pk['aadt'] * df_pk['2015_15-min_Combined']).round()
    return df_pk
    
def excessive_delay(df_ed):
    df_ed['ED'] = df_ed['RSD'] / 3600 # check this value hundredths of an hour
    df_ed['ED'] = df_ed['ED'].round(3)
    df_ed['ED'] = np.where(df_ed['ED'] >= 0, df_ed['ED'], 0)
    return df_ed

def RSD(df_rsd):
    # returns travel time segment delay calculations.
    # df_rsd['RSD'] = df_rsd['SD'] - df_rsd['TS'] 
    df_rsd['RSD'] = df_rsd['travel_time_seconds'] - df_rsd['SD']
    df_rsd['RSD'] = np.where(df_rsd['RSD'] >= 0, df_rsd['RSD'], 0)
    df_rsd['RSD'] = np.where(df_rsd['RSD'] > 900, 900, df_rsd['RSD']) # Where does this show up?
    return df_rsd
        
def segment_delay(df_sd):
    """
    # returns SD spreadsheet value.
    df_sd['SD'] = df_sd['travel_time_seconds'].round()
    """
    # AKA EDTTTs value (Excessive Delay Threshold Travel Time)
    df_sd['SD'] = (df_sd['miles'] / df_sd['TS']) * 3600
    return df_sd

def AADT_splits(df_spl):
    ### Apply mode splits using ODOT methodology
    #df_spl['TOTAL_AADT'] = df_spl['DirAADT_AUTO'] + df_spl['DirAADT_BUS'] + df_spl['DirAADT_TRK']
    #df_spl['pct_auto'] = df_spl['DirAADT_AUTO']/df_spl['TOTAL_AADT']
    #df_spl['pct_bus'] = df_spl['DirAADT_BUS']/df_spl['TOTAL_AADT']
    #df_spl['pct_truck'] = df_spl['DirAADT_TRK']/df_spl['TOTAL_AADT']
    ###
    df_spl['aadt_auto'] = df_spl['aadt'] - (df_spl['aadt_singl'] + df_spl['aadt_combi'])
    df_spl['pct_auto'] = df_spl['aadt_auto']/df_spl['aadt']
    df_spl['pct_bus'] = df_spl['aadt_singl']/df_spl['aadt']
    df_spl['pct_truck'] = df_spl['aadt_combi']/df_spl['aadt']
    return df_spl

def threshold_speed(df_ts):
    # TS is the larger of 20mph or Posted Speed Limit * .6
    df_ts['posted_mult'] = df_ts['SPEED_LIMIT'] * .6 
    df_ts['TS'] = np.where(df_ts['posted_mult'] > 20, df_ts['posted_mult'], 20)
    return df_ts

def main():
    pd.set_option('display.max_rows', None)
    """
    ############ UNCOMMENT FOR FULL DATASET###################################
    drive_path = 'H:/map21/perfMeasures/phed/data/original_data/'
    quarters = ['2017Q1', '2017Q2', '2017Q3', '2017Q4'] 
    folder_end = '_TriCounty_Metro_15-min'
    file_end = '_NPMRDS (Trucks and passenger vehicles).csv'
    
    df = pd.DataFrame() #empty dataframe
    
    for q in quarters:
        filename = q + folder_end + file_end
        path = q + folder_end
        full_path = path + '/' + filename
        df_temp = pd.read_csv(os.path.join(os.path.dirname(__file__), drive_path + full_path)) #fix in script implementation
        df = pd.concat([df, df_temp])
    ############################################################################
    """
    df = pd.read_csv(os.path.join(os.path.dirname(__file__), 'Feb2017_test/Feb2017_test.csv')) #fix in script implementation

    # Filter by timestamps
    df['measurement_tstamp'] = pd.to_datetime(df['measurement_tstamp'])
    df = df[df['measurement_tstamp'].dt.weekday.isin([0, 1, 2, 3, 4])] # Capture weekdays only
    df = df[df['measurement_tstamp'].dt.hour.isin([6, 7, 8, 9, 10, 15, 16, 17, 18, 19])] # add 10, 15 to accurately capture data.
    df['hour'] = df['measurement_tstamp'].dt.hour 

    # Join peakingFactor data
    df_peak = pd.read_csv(os.path.join(os.path.dirname(__file__), 
        'H:/map21/perfMeasures/phed/data/peakingFactors_edit.csv'),
        usecols=['startTime', '2015_15-min_Combined'])
    df_peak['pk_hour'] = pd.to_datetime(df_peak['startTime']).dt.hour
    df = pd.merge(df, df_peak, left_on=df['hour'], right_on=df_peak['pk_hour'], how='inner')

    # Join relevant files
    df_meta = pd.read_csv(os.path.join(os.path.dirname(__file__), 'Feb2017_test/TMC_Identification.csv'),
                          usecols=['tmc', 'miles', 'tmclinear', 'aadt', 'aadt_singl', 'aadt_combi' ])
    df_odot = pd.read_csv(os.path.join(os.path.dirname(__file__), 'Feb2017_test/odot_edt.csv'))
    df = pd.merge(df, df_meta, left_on=df['tmc_code'], right_on=df_meta['tmc'], how='inner')
    df = pd.merge(df, df_odot, left_on=df['tmc_code'], right_on=df_odot['TMC'], how='inner')
    
    # Join HERE data
    df_here = pd.read_csv(os.path.join(os.path.dirname(__file__), 
        'H:/map21/perfMeasures/phed/data/HERE_OR_Static_TriCounty_edit.csv'),
        usecols=['TMC_HERE', 'SPEED_LIMIT'])
    df = pd.merge(df, df_here, left_on=df['tmc_code'], right_on=df_here['TMC_HERE'], how='left', validate='m:1') #This may need to be a left join.
      
    # Apply calculation functions
    df = threshold_speed(df)
    df = AADT_splits(df)
    df = segment_delay(df)
    df = RSD(df)
    df = excessive_delay(df)
    df = peak_hr(df)
    df = total_excessive_delay(df)
    df = TED_summation(df)
    
    df = df[['tmc_code','TED']]
    df.to_csv('phed_out.csv')
    # print(df['TED'].sum())
    print(df)    

    #print(df.loc[df['tmc_code'] == '114-04412'])

if __name__ == '__main__':
    main()