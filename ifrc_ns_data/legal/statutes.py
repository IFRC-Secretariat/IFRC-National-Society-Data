"""
Module to handle NS Statutes data, including loading it from the data file, cleaning, and processing.
"""
import re
import os
import warnings
import yaml
import pandas as pd
from ifrc_ns_data.common import Dataset
from ifrc_ns_data.common.cleaners import NSInfoCleaner, NSInfoMapper


class StatutesDataset(Dataset):
    """
    Load NS Statutes data from the file, and clean and process the data.
    The filepath should be the location of the NS Statutes data.

    Parameters
    ----------
    filepath : string (required)
        Path to save the dataset when loaded, and to read the dataset from.
    """
    def __init__(self, filepath=None, sheet_name=None):
        if filepath is None:
            raise ValueError('Please specify a path to the National Society statutes dataset.')
        super().__init__(name='Statutes', filepath=filepath, sheet_name=sheet_name)


    def process_data(self, data, latest=None):
        """
        Transform and process the data, including changing the structure and selecting columns.

        Parameters
        ----------
        data : pandas DataFrame (required)
            Raw data to be processed.

        latest : bool (default=None)
            Not in use.
        """
        # Print a warning if filtering is given as this does not apply
        if latest is not None:
            warnings.warn(f'Filtering latest data does not apply to dataset {self.name}')

        # Set the columns
        data.columns = data.iloc[1]
        data = data.iloc[3:, :8]

        # Clean up the column names
        clean_columns = {column: re.sub("^\d.", "", column.strip()).strip().replace('\n', ' ') for column in data.columns}
        data.rename(columns=clean_columns, inplace=True, errors='raise')
        data.rename(columns={'National Society (NS)': 'Country'}, inplace=True, errors='raise')

        # Add in other NS information
        data['Country'] = NSInfoCleaner().clean_country_names(data=data['Country'])
        extra_columns = [column for column in self.index_columns if column!='Country']
        ns_info_mapper = NSInfoMapper()
        for column in extra_columns:
            data[column] = ns_info_mapper.map(data=data['Country'], map_from='Country', map_to=column)

        # Rename and order columns
        data = self.rename_columns(data)
        data = self.order_index_columns(data)

        return data
