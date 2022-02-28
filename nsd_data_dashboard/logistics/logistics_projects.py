"""
Module to handle data on Logistics Projects, including loading it from the data file, cleaning, and processing.
"""
import re
import os
import yaml
import pandas as pd
from nsd_data_dashboard.common import Dataset
from nsd_data_dashboard.common.cleaners import NSInfoCleaner, NSInfoMapper


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
        self.name = 'Logistics Projects'
        super().__init__(filepath=filepath, reload=False, sheet_name='Append')
        pass


    def process(self):
        """
        Transform and process the data, including changing the structure and selecting columns.
        """
        # Clean the data
        self.data = self.data.drop(columns=['Region']).dropna(how='all')

        # Clean the country column and map on extra information
        self.data['Country'] = NSInfoCleaner().clean_country_names(self.data['Country'])
        extra_columns = [column for column in self.index_columns if column!='Country']
        ns_info_mapper = NSInfoMapper()
        for column in extra_columns:
            self.data[column] = ns_info_mapper.map(data=self.data['Country'], on='Country', column=column)

        # Add another column level
        self.data.set_index(self.index_columns, inplace=True)
        self.data.columns = pd.MultiIndex.from_product([self.data.columns, ['Value']], names=['Indicator', None])
