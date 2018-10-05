"""
PHED with classes. IKR?
"""

import os
import pandas as pd
import numpy as np
import datetime as dt


class Phed:

    def __init__(self):
        """Create new pandas dataframe"""
        self.df = pd.DataFrame()

    def load_metro_data(self):
        """Loads INRIX, here, data"""
        drive_path = 'H:/map21/perfMeasures/phed/data/original_data/'
        quarters = ['2017Q0', '2017Q1', '2017Q2', '2017Q3', '2017Q4']
        #quarters = ['2017Q0']
        folder_end = '_TriCounty_Metro_15-min'
        file_end = '_NPMRDS (Trucks and passenger vehicles).csv'

        for q in quarters:
            filename = q + folder_end + file_end
            path = q + folder_end
            full_path = path + '/' + filename
            print("Loading {0} data...".format(q))
            df_temp = pd.read_csv(
                        os.path.join(
                            os.path.dirname(__file__), drive_path + full_path))
            self.df = pd.concat([self.df, df_temp], sort=False)

        # Filter by timestamps
        print("Filtering timestamps...".format(q))
        self.df['measurement_tstamp'] = pd.to_datetime(
            self.df['measurement_tstamp'])
        self.df['hour'] = self.df['measurement_tstamp'].dt.hour

        wd = 'H:/map21/perfMeasures/phed/data/'
        # Join peakingFactor data
        df_peak = pd.read_csv(
            os.path.join(
                os.path.dirname(__file__),
                wd + 'peakingFactors_join_edit.csv'),
            usecols=['startTime', '2015_15-min_Combined'])
        df_peak['pk_hour'] = pd.to_datetime(df_peak['startTime']).dt.hour
        self.df = pd.merge(
            self.df, df_peak, left_on=self.df['hour'],
            right_on=df_peak['pk_hour'], how='left')

        # Capture weekdays only
        self.df = self.df[self.df['measurement_tstamp'].dt.weekday.isin(
            [0, 1, 2, 3, 4])]
        self.df = self.df[self.df['measurement_tstamp'].dt.hour.isin(
            [6, 7, 8, 9, 10, 15, 16, 17, 18, 19])]

        # Join/filter on relevant urban TMCs
        print("Join/filter on urban TMCs...")
        df_urban = pd.read_csv(
            os.path.join(os.path.dirname(__file__), wd + 'urban_tmc.csv'))
        self.df = self.df.drop('key_0', axis=1)

        self.df = pd.merge(df_urban, self.df,
                           how='inner',
                           left_on=df_urban['Tmc'],
                           right_on=self.df['tmc_code'])
        self.df = self.df.drop('key_0', axis=1)

        # Join TMC Metadata
        print("Join TMC Metadata...")
        df_meta = pd.read_csv(
            os.path.join(
                os.path.dirname(__file__),
                wd +
                'TMC_Identification_NPMRDS (Trucks and passenger vehicles).csv'),
            usecols=['tmc', 'miles', 'tmclinear', 'faciltype', 'aadt',
                     'aadt_singl', 'aadt_combi'])

        self.df = pd.merge(self.df, df_meta, left_on=self.df['tmc_code'],
                           right_on=df_meta['tmc'], how='inner')
        self.df = self.df.drop('key_0', axis=1)

        # Join HERE data
        df_here = pd.read_csv(
            os.path.join(
                os.path.dirname(__file__), wd +
                'HERE_OR_Static_TriCounty_edit.csv'),
            usecols=['TMC_HERE', 'SPEED_LIMIT'])

        self.df = pd.merge(self.df, df_here,
                           left_on=self.df['tmc_code'],
                           right_on=df_here['TMC_HERE'],
                           how='left',
                           validate='m:1')
        self.df = self.df.drop('key_0', axis=1)

    def TED_summation(self):
        """Calculates final TED summation.
        Args: self.df, a pandas dataframe.
        Returns: self.df, a pandas dataframe with new columns:
            AVOc, AVOb, AVOt (car, bus, and truck average vehicle occupancy).
        """
        # Working vehicle occupancy assumptions:
        VOCa = 1.4
        VOCb = 12.6
        VOCt = 1
        self.df['AVOc'] = self.df['pct_auto'] * VOCa
        self.df['AVOb'] = self.df['pct_bus'] * VOCb
        self.df['AVOt'] = self.df['pct_truck'] * VOCt
        self.df['TED'] = (self.df['TED_seg'] *
                     (self.df['AVOc'] + self.df['AVOb'] + self.df['AVOt']))
        return self.df

    def total_excessive_delay(self):
        """Calculates Total Excessive Delay per given TMC using padas groupby
        function.
        Args: self.df, a pandas dataframe.
        Returns: self.df, a pandas dataframe grouped by TMC.
        """
        self.df['TED_seg'] = (self.df['ED'] * self.df['PK_HR'])
        ted_operations = ({'TED_seg': 'sum',
                           'pct_auto': 'max',
                           'pct_bus': 'max',
                           'pct_truck': 'max'})
        self.df = self.df.groupby(
            'tmc_code', as_index=False).agg(ted_operations)
        return self.df

    def peak_hr(self):
        """Performs Peak Hour calculations by combining directional aadt values
        with vehicle hourly volume factors determined by Metro.
        Args: self.df, a pandas dataframe.
        Returns: self.df, a pandas dataframe conaining new PK_HR column with
        completed calculations.
        """
        self.df['PK_HR'] = self.df['dir_aadt'] * self.df['2015_15-min_Combined']
        return self.df

    def excessive_delay(self):
        """Calculates Excessive Delay.
        Args: self.df, a pandas dataframe.
        Returns: self.df, a pandas dataframe containing new column ED with
        completed calculations."""
        self.df['ED'] = self.df['RSD'] / 3600
        self.df['ED'] = self.df['ED'].round(3)
        self.df['ED'] = np.where(self.df['ED'] >= 0, self.df['ED'], 0)
        return self.df

    def RSD(self):
        """Calculates RSD (Travel Time Segment delay).
        Args: self.df, a pandas dataframe.
        Returns, self.df, a pandas dataframe with new column RSD with completed
        calculations.
        """
        self.df['RSD'] = self.df['travel_time_seconds'] - self.df['SD']
        self.df['RSD'] = np.where(self.df['RSD'] >= 0, self.df['RSD'], 0)
        return self.df

    def segment_delay(self):
        """Calculates Excessive Delay Threshold Travel Time (EDTTT).
        Args: self.df, a pandas dataframe.
        Returns: self.df, a pandas dataframe with new column SD with completed
        calculations.
        """
        self.df['SD'] = (self.df['miles'] / self.df['TS']) * 3600
        return self.df

    def AADT_splits(self):
        """Calculates AADT per vehicle type.
        Args: self.df, a pandas dataframe.
        Returns: self.df, a pandas dataframe containing new columns:
            dir_aadt: directional aadt
            aadt_auto: auto aadt
            pct_auto, pct_bus, pct_truck : percentage mode splits of auto, bus
            and trucks.
        """
        self.df['dir_aadt'] = (self.df['aadt']/self.df['faciltype']).round()
        self.df['aadt_auto'] = self.df['dir_aadt'] - \
            (self.df['aadt_singl'] + self.df['aadt_combi'])
        self.df['pct_auto'] = self.df['aadt_auto'] / self.df['dir_aadt']
        self.df['pct_bus'] = self.df['aadt_singl'] / self.df['dir_aadt']
        self.df['pct_truck'] = self.df['aadt_combi'] / self.df['dir_aadt']
        return self.df

    def threshold_speed(self):
        """Calculates Threshold Speed, defined as the larger of 20mph or
        Posted Speed Limit * .6.
        Args: self.df, a pandas dataframe.
        Returns: self.df, A pandas dataframe with new columns:
            'posted_mult', 'TS'.
        """
        self.df['posted_mult'] = self.df['SPEED_LIMIT'] * .6
        self.df['TS'] = np.where(
            self.df['posted_mult'] > 20, self.df['posted_mult'], 20)
        return self.df


def per_capita_TED(sum_12_mo):
    """Calculates final Peak Hour Excessive Delay number.
    Args: sum_12_mo, the integer sum of all TED values.
    Returns: A value for Peak Hour Excessive Delay per capita.
    """
    print(sum_12_mo)
    pop_PDX = 1577456
    return sum_12_mo / pop_PDX


def main():
    startTime = dt.datetime.now()
    print('Script started at {0}'.format(startTime))

    calcs = Phed()
    calcs.load_metro_data()
    calcs.threshold_speed()
    calcs.AADT_splits()
    calcs.segment_delay()
    calcs.RSD()
    calcs.excessive_delay()
    calcs.peak_hr()
    calcs.total_excessive_delay()
    calcs.TED_summation()

    result = round(per_capita_TED(calcs.df['TED'].sum()), 2)
    print("==================================================================")
    print("Calulated {} peak hour excessive delay per capita."
          .format(str(result)))
    print("==================================================================")
    endTime = dt.datetime.now()
    print("Script finished in {0}.".format(endTime - startTime))


if __name__ == '__main__':
    main()
