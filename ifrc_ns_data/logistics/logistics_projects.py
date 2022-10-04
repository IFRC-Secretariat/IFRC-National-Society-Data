"""
Module to handle data on Logistics Projects, including loading it from the data file, cleaning, and processing.
"""
import re
import os
import yaml
import pandas as pd
from ifrc_ns_data.common import Dataset
from ifrc_ns_data.common.cleaners import NSInfoCleaner, NSInfoMapper


class LogisticsProjectsDataset(Dataset):
    """
    Load Logistics Projects data from the file, and clean and process the data.
    The filepath should be the location of the data.

    Parameters
    ----------
    filepath : string (required)
        Path to save the dataset when loaded, and to read the dataset from.
    """
    def __init__(self, filepath, sheet_name):
        self.name = 'Logistics Projects'
        super().__init__(filepath=filepath, sheet_name=sheet_name)
        pass


    def process_data(self, data):
        """
        Transform and process the data, including changing the structure and selecting columns.
        """
        # Clean the data
        data = data.drop(columns=['Region']).dropna(how='all')

        # Clean the country column and map on extra information
        data['Country'] = NSInfoCleaner().clean_country_names(data['Country'])
        extra_columns = [column for column in self.index_columns if column!='Country']
        ns_info_mapper = NSInfoMapper()
        for column in extra_columns:
            data[column] = ns_info_mapper.map(data=data['Country'], on='Country', column=column)

        # Order the NS index columns
        data = self.order_index_columns(data)

        return data
