"""
Module to handle NS Statutes data, including loading it from the data file, cleaning, and processing.
"""
import re
import os
import yaml
import pandas as pd
from nsd_data_dashboard.common import Dataset
from nsd_data_dashboard.common.cleaners import NSInfoCleaner, NSInfoMapper


class NSStatutesDataset(Dataset):
    """
    Load NS Statutes data from the file, and clean and process the data.
    The filepath should be the location of the NS Statutes data.

    Parameters
    ----------
    filepath : string (required)
        Path to save the dataset when loaded, and to read the dataset from.
    """
    def __init__(self, filepath):
        self.name = 'NS Statutes'
        super().__init__(filepath=filepath, reload=False)
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
        self.data.rename(columns={'National Society (NS)': 'Country'}, inplace=True, errors='raise')

        # Add in other NS information
        self.data['Country'] = NSInfoCleaner().clean_country_names(data=self.data['Country'])
        extra_columns = [column for column in self.index_columns if column!='Country']
        ns_info_mapper = NSInfoMapper()
        for column in extra_columns:
            self.data[column] = ns_info_mapper.map(data=self.data['Country'], on='Country', column=column)

        # Add another column level
        self.data = self.data.set_index(self.index_columns)
        self.data.columns = pd.MultiIndex.from_product([self.data.columns, ['Value']], names=['Indicator', None])
