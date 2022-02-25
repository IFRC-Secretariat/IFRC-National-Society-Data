"""
Module to handle data on Logistics Projects, including loading it from the data file, cleaning, and processing.
"""
import re
import os
import yaml
import pandas as pd
from nsd_data_dashboard.common import Dataset
from nsd_data_dashboard.common.cleaners import NSNamesCleaner


class LogisticsProjectsDataset(Dataset):
    """
    Load Logistics Projects data from the file, and clean and process the data.
    The filepath should be the location of the data.

    Parameters
    ----------
    filepath : string (required)
        Path to save the dataset when loaded, and to read the dataset from.
    """
    def __init__(self, filepath):
        indicators = yaml.safe_load(open(os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 'common/dataset_indicators.yml')))['Logistics Projects']
        super().__init__(filepath=filepath, reload=False, sheet_name='Append', indicators=indicators)
        pass


    def process(self):
        """
        Transform and process the data, including changing the structure and selecting columns.
        """
        # Clean the data
        self.data = self.data.drop(columns=['Region']).dropna(how='all')

        # Clean the country column and check that the NS names are consistent with the centralised list
        self.data['Country'] = self.data['Country'].str.strip()

        # Add another column level
        self.data.set_index(['Country'], inplace=True)
        self.data.columns = pd.MultiIndex.from_product([self.data.columns, ['value']])
