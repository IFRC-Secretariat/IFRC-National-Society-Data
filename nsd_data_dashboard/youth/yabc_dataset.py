"""
Module to handle YABC data, including loading it from the data file, cleaning, and processing.
"""
import re
import os
import yaml
import pandas as pd
from nsd_data_dashboard.common import Dataset
from nsd_data_dashboard.common.cleaners import NSNamesCleaner


class YABCDataset(Dataset):
    """
    Load YABC data from the file, and clean and process the data.
    The filepath should be the location of the data.

    Parameters
    ----------
    filepath : string (required)
        Path to save the dataset when loaded, and to read the dataset from.
    """
    def __init__(self, filepath):
        indicators = yaml.safe_load(open(os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 'common/dataset_indicators.yml')))['YABC']
        super().__init__(filepath=filepath, reload=False, indicators=indicators)
        pass


    def process(self):
        """
        Transform and process the data, including changing the structure and selecting columns.
        """
        # Set the columns
        self.data.columns = self.data.iloc[1]
        self.data = self.data.iloc[2:, 1:]

        # Clean up the column names
        clean_columns = {column: column.strip() for column in self.data.columns}
        self.data = self.data.rename(columns=clean_columns, errors='raise')
        self.data = self.data.loc[self.data['Country']!='TOTAL']

        # Check that the NS names are consistent with the centralised names list
        self.data['National Society name'] = NSNamesCleaner().clean(self.data['Country'].str.strip())
        self.data = self.data.drop(columns=['Country'])

        # Add another column level
        self.data = self.data.set_index(['National Society name'])
        self.data.columns = pd.MultiIndex.from_product([self.data.columns, ['value']])
