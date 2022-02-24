"""
Module to handle NS Statutes data, including loading it from the data file, cleaning, and processing.
"""
import re
import pandas as pd
from nsd_data_dashboard.common import Dataset
from nsd_data_dashboard.common.cleaners import NSNamesCleaner


class NSStatutesDataset(Dataset):
    """
    Load NS Statutes data from the file, and clean and process the data.
    The filepath should be the location of the NS Statutes data.

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
        # Set the columns
        self.data.columns = self.data.iloc[1]
        self.data = self.data.iloc[3:, :8]

        # Clean up the column names
        clean_columns = {column: re.sub("^\d.", "", column.strip()).strip().replace('\n', ' ') for column in self.data.columns}
        self.data.rename(columns=clean_columns, inplace=True, errors='raise')
        self.data.rename(columns={'National Society (NS)': 'National Society name'}, inplace=True, errors='raise')

        # Check that the NS names are consistent with the centralised names list
        self.data['National Society name'] = NSNamesCleaner().clean(self.data['National Society name'].str.strip())

        # Add another column level
        self.data = self.data.set_index(['National Society name'])
        self.data.columns = pd.MultiIndex.from_product([self.data.columns, ['']])
