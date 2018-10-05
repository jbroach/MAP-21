# import numpy as np
import pandas as pd
import datetime as dt
import os


def main():

    startTime = dt.datetime.now()
    print('Script started at {0}'.format(startTime))
    drive_path = 'H:/map21/perfMeasures/phed/data/original_data/'
    # quarters = ['2017Q0', '2017Q1', '2017Q2', '2017Q3', '2017Q4']
    quarters = ['2017Q0']

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
        del df_temp

    # Save to HDF5
    df.to_hdf('test.h5', 'data', mode='w', format='table')

    endTime = dt.datetime.now()
    print("Script finished in {0}.".format(endTime - startTime))


if __name__ == '__main__':
    main()
