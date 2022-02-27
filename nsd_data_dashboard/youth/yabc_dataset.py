"""
Module to handle YABC data, including loading it from the data file, cleaning, and processing.
"""
import re
import os
import yaml
import pandas as pd
from nsd_data_dashboard.common import Dataset
from nsd_data_dashboard.common.cleaners import NSInfoCleaner, NSInfoMapper


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
        self.name = 'YABC'
        super().__init__(filepath=filepath, reload=False)
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
        self.data['Country'] = NSInfoCleaner().clean_country_names(self.data['Country'].str.strip())
        extra_columns = [column for column in self.index_columns if column!='Country']
        ns_info_mapper = NSInfoMapper()
        for column in extra_columns:
            self.data[column] = ns_info_mapper.map(data=self.data['Country'], on='Country', column=column)

        # Add another column level
        self.data = self.data.set_index(self.index_columns)
        self.data.columns = pd.MultiIndex.from_product([self.data.columns, ['value']], names=['indicator', None])
