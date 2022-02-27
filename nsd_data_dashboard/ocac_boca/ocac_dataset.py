"""
Module to handle OCAC data, including loading it from the downloaded data file, cleaning, and processing.
"""
import requests
import os
import yaml
import pandas as pd
from nsd_data_dashboard.common import Dataset
from nsd_data_dashboard.common.cleaners import NSInfoCleaner, NSInfoMapper


class OCACDataset(Dataset):
    """
    Load OCAC data from the downloaded file, and clean and process the data.
    The filepath should be the location of the downloaded OCAC data.

    Parameters
    ----------
    filepath : string (required)
        Path to save the dataset when loaded, and to read the dataset from.
    """
    def __init__(self, filepath):
        indicators = yaml.safe_load(open(os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 'common/dataset_indicators.yml')))['OCAC']
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

        # Check that the NS names are consistent with the centralised names list, and add extra information
        self.data['National Society name'] = NSInfoCleaner().clean_ns_names(self.data['National Society name'])
        extra_columns = [column for column in self.index_columns if column!='National Society name']
        ns_info_mapper = NSInfoMapper()
        for column in extra_columns:
            self.data[column] = ns_info_mapper.map(data=self.data['National Society name'], on='National Society name', column=column)

        # Keep only the latest assessment for each NS
        self.data = self.data.sort_values(by=['National Society name', 'Year'], ascending=[True, False])\
                             .drop_duplicates(subset=['National Society name'], keep='first')\
                             .set_index(self.index_columns)

        # Add another column level
        self.data.columns = pd.MultiIndex.from_product([self.data.columns, ['value']])
