"""
Module to handle National Society general information and contact data.
This is used as the central list of NS info including names, IDs, countries, and regions.
The module can be used to pull this data from the NS Databank API, process, and clean the data.
"""
import requests
import os
import yaml
import pandas as pd
from nsd_data_dashboard.common import Dataset
from nsd_data_dashboard.common.cleaners import NSInfoCleaner


class NSContactsDataset(Dataset):
    """
    Pull NS contact information from the NS Databank API, and clean and process the data.

    Parameters
    ----------
    filepath : string (required)
        Path to save the dataset when loaded, and to read the dataset from.
    """
    def __init__(self, filepath, api_key, reload=True):
        self.name = 'NS Contacts'
        super().__init__(filepath=filepath, reload=reload)
        self.api_key = api_key
        self.reload = reload


    def reload_data(self):
        """
        Read in data from the NS Databank API and save to file, or read in as a CSV file from the given filepath.
        """
        # Pull data from FDRS API
        response = requests.get(url=f'https://data-api.ifrc.org/api/entities/ns?apiKey={self.api_key}')
        response.raise_for_status()
        data = pd.DataFrame(response.json())

        # Save the data
        data.to_csv(self.filepath, index=False)


    def process(self):
        """
        Transform and process the data, including changing the structure and selecting columns.
        """
        # Make sure the NS names agree with the central list
        self.data.rename(columns={'NSO_DON_name': 'National Society name'}, errors='raise', inplace=True)
        self.data['National Society name'] = NSInfoCleaner().clean_ns_names(self.data['National Society name'])
        self.data.set_index('National Society name', inplace=True)

        # Add another column level
        self.data.columns = pd.MultiIndex.from_product([self.data.columns, ['Value']])
