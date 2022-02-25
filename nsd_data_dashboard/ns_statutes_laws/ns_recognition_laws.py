"""
Module to handle NS Recognition Laws data, including loading it from the data file, cleaning, and processing.
"""
import re
import os
import yaml
import pandas as pd
from nsd_data_dashboard.common import Dataset
from nsd_data_dashboard.common.cleaners import NSNamesCleaner


class NSRecognitionLawsDataset(Dataset):
    """
    Load NS Recognition Laws data from the file, and clean and process the data.
    The filepath should be the location of the NS Recognition Laws data.

    Parameters
    ----------
    filepath : string (required)
        Path to save the dataset when loaded, and to read the dataset from.
    """
    def __init__(self, filepath, indicators=None):
        indicators = yaml.safe_load(open(os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 'common/dataset_indicators.yml')))['NS Recognition Laws']
        super().__init__(filepath=filepath, reload=False, indicators=indicators)
        pass


    def process(self):
        """
        Transform and process the data, including changing the structure and selecting columns.
        """
        # Set the columns from the data row
        self.data.columns = self.data.iloc[0]
        self.data = self.data.iloc[1:]

        # Clean up the column names
        self.data.rename(columns={column: column.strip() for column in self.data.columns}, inplace=True)
        self.data.rename(columns={'National Society (NS)': 'National Society name'}, inplace=True, errors='raise')

        # Check that the NS names are consistent with the centralised names list
        self.data['National Society name'] = NSNamesCleaner().clean(self.data['National Society name'].str.strip())

        # Add another column level
        self.data = self.data.set_index(['National Society name'])
        self.data.columns = pd.MultiIndex.from_product([self.data.columns, ['']])
