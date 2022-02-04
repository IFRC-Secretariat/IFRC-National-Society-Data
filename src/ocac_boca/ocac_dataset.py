"""
Module to handle OCAC data, including loading it from the downloaded data file, cleaning, and processing.
"""
import requests
import pandas as pd
from src.common import Dataset
from src.common.cleaners import NSNamesCleaner


class OCACDataset(Dataset):
    """
    Load OCAC data from the downloaded file, and clean and process the data.
    The filepath should be the location of the downloaded OCAC data.

    Parameters
    ----------
    filepath : string (required)
        Path to save the dataset when loaded, and to read the dataset from.
    """
    def __init__(self, filepath, indicators=None):
        super().__init__(filepath=filepath, reload=False, indicators=indicators)
        pass


    def process(self):
        """
        Transform and process the data, including changing the structure and selecting columns.
        """
        # Process the data into a log format, with a row for each assessment
        self.data.loc[self.data['Name'].isnull(), 'Name'] = self.data['Code']
        self.data['Name'] = self.data['Name'].str.strip()
        self.data = self.data.drop(columns=['Code'])\
                             .set_index(['Name'])\
                             .dropna(how='all')\
                             .transpose()\
                             .drop(columns=['iso', 'Region', 'SubRegion', 'Month', 'Version', 'Principal facilitator', 'Second facilitator', 'NS Focal point', 'OCAC data public', 'OCAC report public'], errors='raise')\
                             .reset_index(drop=True)\
                             .rename(columns={'National Society': 'National Society name'})

        # Check that the NS names are consistent with the centralised names list
        self.data['National Society name'] = NSNamesCleaner().clean(self.data['National Society name'])
        self.data.set_index(['National Society name', 'Year'], inplace=True)

        return self.data
