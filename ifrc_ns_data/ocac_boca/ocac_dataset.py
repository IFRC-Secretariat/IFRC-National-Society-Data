"""
Module to handle OCAC data, including loading it from the downloaded data file, cleaning, and processing.
"""
import requests
import os
import yaml
import pandas as pd
from ifrc_ns_data.common import Dataset
from ifrc_ns_data.common.cleaners import NSInfoCleaner, NSInfoMapper


class OCACDataset(Dataset):
    """
    Load OCAC data from the downloaded file, and clean and process the data.
    The filepath should be the location of the downloaded OCAC data.

    Parameters
    ----------
    filepath : string (required)
        Path to save the dataset when loaded, and to read the dataset from.
    """
    def __init__(self, filepath, sheet_name):
        self.name = 'OCAC'
        super().__init__(filepath=filepath, sheet_name=sheet_name)


    def process_data(self, data, filter_latest=False):
        """
        Transform and process the data, including changing the structure and selecting columns.
        """
        # Process the data into a log format, with a row for each assessment
        data = data.rename(columns={'Name': 'Indicator'})
        data.loc[data['Indicator'].isnull(), 'Indicator'] = data['Code']
        data['Indicator'] = data['Indicator'].str.strip()
        data = data.drop(columns=['Code'])\
                             .set_index(['Indicator'])\
                             .dropna(how='all')\
                             .transpose()\
                             .drop(columns=['iso', 'Region', 'SubRegion', 'Month', 'Version', 'Principal facilitator', 'Second facilitator', 'NS Focal point', 'OCAC data public', 'OCAC report public'], errors='raise')\
                             .reset_index(drop=True)\
                             .rename(columns={'National Society': 'National Society name'})

        # Check that the NS names are consistent with the centralised names list, and add extra information
        data['National Society name'] = NSInfoCleaner().clean_ns_names(data['National Society name'])
        extra_columns = [column for column in self.index_columns if column!='National Society name']
        ns_info_mapper = NSInfoMapper()
        for column in extra_columns:
            data[column] = ns_info_mapper.map(data=data['National Society name'], map_from='National Society name', map_to=column)

        # Keep only the latest assessment for each NS
        if filter_latest:
            data = data.sort_values(by=['National Society name', 'Year'], ascending=[True, False])\
                                 .drop_duplicates(subset=['National Society name'], keep='first')

        # Order columns
        data = self.order_index_columns(data)

        return data
