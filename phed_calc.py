import os
import pandas as pd
#import numpy as np

def main():
    df = pd.read_csv(os.path.join(os.path.dirname(__file__), 'Feb2017_test/Feb2017_test.csv'))
    df_meta = pd.read_csv(os.path.join(os.path.dirname(__file__), 'Feb2017_test/TMC_Identification.csv'))
    df = pd.merge(df, df_meta, left_on=df['tmc_code'], right_on=['tmc'], how='inner')
    print (df)

if __name__ == '__main__':
    main()