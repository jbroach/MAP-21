"""
inrix_may_filter_chunk.py

One-time use script to calculate average travel times for non-Memorial Day
Tu, W, Thu in May.

This script makes use of chunking in order to get around the memory limitations
imposed by the GCP free tier.

by Kevin Saavedra, kevin.saavedra@oregonmetro.gov
"""

import os
import pandas as pd


def tt_by_hour(df_tt, hour):
    """Process hourly travel time averages."""
    df_tt = df_tt[df_tt['measurement_tstamp'].dt.hour.isin([hour])]
    tmc_operations = ({'travel_time_seconds': 'min'})
    df_tt = df_tt.groupby('tmc_code', as_index=False).agg(tmc_operations)
    df_avg_tt = df_tt.rename(
        columns={'travel_time_seconds': 'hour_{}_tt_seconds'.format(hour)})
    return df_avg_tt


def main():
    drive_path = '/home/remote/KS_projects/test_dev/MAP-21/data/'
    quarters = ['2017Q2']
    folder_end = '_TriCounty_Metro_15-min'
    file_end = '_NPMRDS (Trucks and passenger vehicles).csv'

    for q in quarters:
        filename = q + folder_end + file_end
        path = q + folder_end
        full_path = path + '/' + filename
        print("Loading {0} data...".format(q))
    
        # Load as chunks
        df = pd.read_csv(os.path.join(
            os.path.dirname(__file__), drive_path + full_path),
                usecols=['tmc_code', 'measurement_tstamp', 
                         'travel_time_seconds'],
                parse_dates=[1],
                chunksize=10000)

        #pieces = [x.apply(pd.to_datetime(df['measurement_tstamp'])) 
        #    for x in df]
        # List comprehension
        # list_chunks = [chunks[i:i+n] for i in range(0, chunks.shape[0],n)]
        # print(list_chunks[0])  
        
         
        print("Filtering timestamps...".format(q))
        # Filter for May only.
        filter = [x[x['measurement_tstamp'].dt.month.isin([5])] for x in df]
        filter_d = [x[x['measurement_tstamp'].dt.day.isin(
            [2, 3, 4, 9, 10, 11, 16, 17, 18, 23, 24, 25])] for x in filter]
        
    df = pd.concat(filter_d)
        
        # check = df['measurement_tstamp'].dt.day.isin([31])
        # print(check.unique())
        
    df = df.dropna()

        ### Working aggregation
        #pieces = [x.groupby('tmc_code')['travel_time_seconds'].agg(
        #    ['sum', 'count']) for x in df]
        #agg = pd.concat(pieces).groupby(level=0).sum()
        #print(agg['sum']/agg['count'])
        ##########

   
    tmc_list = df['tmc_code'].drop_duplicates().values.tolist()
    tmc_format = {'tmc_code': tmc_list}
    df_tmc = pd.DataFrame.from_dict(tmc_format)

    """
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
    """
    
    hours = list(range(0, 24))
    for hour in hours:
        df_time = tt_by_hour(df, hour)
        df_tmc = pd.merge(df_tmc, df_time, on='tmc_code', how='left')

    df_tmc.to_csv('may_2017_INRIX.csv', index=False)


if __name__ == '__main__':
    main()
