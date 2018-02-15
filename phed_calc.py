import os
import pandas as pd
#import numpy as np

def main():
    df = pd.read_csv(os.path.join(os.path.dirname('__file__'), 'Feb2017_test/Feb2017_test.csv')) #fix in script implementation
    df_meta = pd.read_csv(os.path.join(os.path.dirname('__file__'), 'Feb2017_test/TMC_Identification.csv'), 
                          usecols=['tmc', 'road', 'direction', 'intersection', 'state', 'county', 'miles',
                                                  'tmclinear', 'aadt', 'aadt_singl', 'aadt_combi' ])
    df_odot = pd.read_csv(os.path.join(os.path.dirname('__file__'), 'Feb2017_test/odot_edt.csv'))
    df = pd.merge(df, df_meta, left_on=df['tmc_code'], right_on=['tmc'], how='inner')

    # Return a selected row for test purposes only
    print(df.loc[df['tmc_code'] == '114-04368'])

if __name__ == '__main__':
    main()