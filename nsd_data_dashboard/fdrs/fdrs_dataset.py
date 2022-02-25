"""
Module to handle FDRS data, including loading it from the API, cleaning, and processing.
"""
import requests
import os
import yaml
import pandas as pd
from nsd_data_dashboard.common import Dataset
from nsd_data_dashboard.common.cleaners import DatabankNSIDMapper, NSNamesCleaner


class FDRSDataset(Dataset):
    """
    Load FDRS data from the API, and clean and process the data.

    Parameters
    ----------
    filepath : string (required)
        Path to save the dataset when loaded, and to read the dataset from.
    """
    def __init__(self, filepath, api_key, reload=True):
        indicators = yaml.safe_load(open(os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 'common/dataset_indicators.yml')))['FDRS']
        super().__init__(filepath=filepath, reload=reload, indicators=indicators)
        self.api_key = api_key
        self.reload = reload


    def reload_data(self):
        """
        Read in data from the NS Databank API and save to file.
        """
        # Pull data from FDRS API
        response = requests.get(url=f'https://data-api.ifrc.org/api/Data?apiKey={self.api_key}')
        response.raise_for_status()

        # Unnest the response from the API into a tabular format
        data = pd.DataFrame(response.json()['data'])
        data = data.explode('data', ignore_index=True)
        data = pd.concat([data.drop(columns=['data']).rename(columns={'id': 'indicator'}),
                          pd.json_normalize(data['data']).rename(columns={'id': 'National Society ID'})], axis=1)
        data = data.explode('data', ignore_index=True)
        data = pd.concat([data.drop(columns=['data']),
                          pd.json_normalize(data['data'])], axis=1)

        if data['years'].astype(str).nunique()!=1:
            raise ValueError('Unexpected values in years column', data['years'].astype(str).unique())
        data.drop(columns=['years'], inplace=True)

        # Save the data
        data.to_csv(self.filepath, index=False)


    def process(self):
        """
        Transform and process the data, including changing the structure and selecting columns.
        """
        # Convert NS IDs to NS names
        self.data['National Society name'] = DatabankNSIDMapper(api_key=self.api_key).map(self.data['National Society ID'])

        # Make sure the NS names agree with the central list
        self.data['National Society name'] = NSNamesCleaner().clean(self.data['National Society name'])

        # Replace False for nan for boolean indicators, so that they are dropped in the next step
        get_latest_columns = ['KPI_hasFinancialStatement', 'audited', 'ar', 'sp']
        self.data.loc[(self.data['indicator'].isin(get_latest_columns)) & (self.data['value'].astype(str)=='False'), 'value'] = float('nan')
        self.data.loc[(self.data['indicator'].isin(get_latest_columns)) & (self.data['value'].astype(str)=='True'), 'value'] = 'Yes'

        # Keep only the latest values for each indicator: keep the smallest value if there are duplicates
        self.data = self.data.dropna(how='any')\
                             .sort_values(by=['year', 'value'], ascending=[False, True])\
                             .drop_duplicates(subset=['National Society name', 'indicator'], keep='first')\
                             .sort_values(by=['National Society name', 'indicator'], ascending=True)

        # Pivot the dataframe to have NSs as rows and indicators as columns
        self.data = self.data.pivot(index=['National Society name'],
                                    columns='indicator',
                                    values=['value', 'year'])\
                             .swaplevel(axis='columns')\
                             .sort_index(axis='columns', level=0, sort_remaining=False)

        # Convert NS supported and receiving support lists from NS IDs to NS names
        def split_convert_ns_ids(x):
            ns_ids = x.split(',')
            ns_names = DatabankNSIDMapper(api_key=self.api_key).map(ns_ids)
            return ', '.join(ns_names)
        for column in ['supported1', 'received_support1']:
            self.data[column, 'value'] = self.data[column, 'value'].apply(lambda x: x if x!=x else split_convert_ns_ids(x))

        # Calculate the top income sources
        def get_top_n_income_sources(row, n=3):
            total_income_column = 'KPI_IncomeLC'
            income_source_columns = {'h_gov': 'Home government', 'f_gov': 'Foreign government', 'ind': 'Individuals', 'corp': 'Corporations', 'found': 'Foundations', 'un': 'UN agencies & other multilateral agencies', 'pooled_f': 'Pooled funds', 'ngo': 'Non-governmental organizations', 'si': 'Service income', 'iga': 'Income-generating activity', 'KPI_incomeFromNSsLC': 'Other National Society', 'ifrc': 'IFRC', 'icrc': 'ICRC', 'other': 'Other source'}
            if row['KPI_IncomeLC', 'value'] != row['KPI_IncomeLC', 'value']:
                if row['KPI_IncomeLC_CHF', 'value'] != row['KPI_IncomeLC_CHF', 'value']:
                    return
                income_source_columns = {source+'_CHF': alt for source, alt in income_source_columns.items()}
                total_income_column += '_CHF'
            source_year = row[total_income_column, 'year']
            for column in income_source_columns:
                if row[column, 'year']!=source_year:
                    return row
            total_income = float(row[total_income_column, 'value'])
            income_amounts = row[[[column, 'value'] for column in income_source_columns]].astype(float).dropna().sort_values(ascending=False)
            if not income_amounts.empty:
                for i in range(0, n):
                    row[f'income_source_{i+1}', 'value'] = f'{income_source_columns[income_amounts.index[i][0]]} ({round(100*income_amounts[i]/total_income)}%)'
                    row[f'income_source_{i+1}', 'year'] = source_year
            return row

        self.data = self.data.apply(lambda row: get_top_n_income_sources(row), axis=1)
