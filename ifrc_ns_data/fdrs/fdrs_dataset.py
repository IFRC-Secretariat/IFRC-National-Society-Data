"""
Module to handle FDRS data, including loading it from the API, cleaning, and processing.
"""
import requests
import os
import yaml
import pandas as pd
from ifrc_ns_data.common import Dataset
from ifrc_ns_data.common.cleaners import DatabankNSIDMapper, NSInfoMapper


class FDRSDataset(Dataset):
    """
    Load FDRS data from the API, and clean and process the data.

    Parameters
    ----------
    filepath : string (required)
        Path to save the dataset when loaded, and to read the dataset from.
    """
    def __init__(self, api_key):
        self.name = 'FDRS'
        super().__init__()
        self.api_key = api_key


    def pull_data(self):
        """
        Read in raw data from the NS Databank API.
        """
        # Pull data from FDRS API
        response = requests.get(url=f'https://data-api.ifrc.org/api/Data?apiKey={self.api_key}')
        response.raise_for_status()

        # Unnest the response from the API into a tabular format
        data = pd.DataFrame(response.json()['data'])
        data = data.explode('data', ignore_index=True)
        data = pd.concat([data.drop(columns=['data']).rename(columns={'id': 'Indicator'}),
                          pd.json_normalize(data['data']).rename(columns={'id': 'National Society ID'})], axis=1)
        data = data.explode('data', ignore_index=True)
        data = pd.concat([data.drop(columns=['data']),
                          pd.json_normalize(data['data'])], axis=1)

        if data['years'].astype(str).nunique()!=1:
            raise ValueError('Unexpected values in years column', data['years'].astype(str).unique())
        data.drop(columns=['years'], inplace=True)

        return data


    def process_data(self, data):
        """
        Transform and process the data, including changing the structure and selecting columns.

        Parameters
        ----------
        data : pandas DataFrame (required)
            Raw data to be processed.
        """
        # Rename columns and remove nans
        data = data.rename(columns={'value': 'Value', 'year': 'Year'})\
                   .dropna(subset=['Value', 'Year', 'Indicator'], how='any')

        # Map in country and region information
        for column in self.index_columns:
            data[column] = NSInfoMapper().map(data['National Society ID'], on='National Society ID', column=column)

        # Replace False for nan for boolean indicators, so that they are dropped in the next step
        get_latest_columns = ['KPI_hasFinancialStatement', 'audited', 'ar', 'sp']
        data.loc[(data['Indicator'].isin(get_latest_columns)) & (data['Value'].astype(str)=='False'), 'Value'] = float('nan')
        data.loc[(data['Indicator'].isin(get_latest_columns)) & (data['Value'].astype(str)=='True'), 'Value'] = 'Yes'

        # Convert NS supported and receiving support lists from NS IDs to NS names
        def split_convert_ns_ids(x):
            ns_ids = x.split(',')
            ns_names = DatabankNSIDMapper(api_key=self.api_key).map(ns_ids)
            return ', '.join(ns_names)
        data['Value'] = data.apply(lambda row: split_convert_ns_ids(row['Value']) if ((row['Indicator'] in ['supported1', 'received_support1']) and (row['Value']==row['Value'])) else row['Value'], axis=1)

        # Common processing for indicator-type datasets
        data = self.process_indicator_data(data)

        return data
