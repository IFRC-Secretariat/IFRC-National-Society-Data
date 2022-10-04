"""
Module to handle National Society general information and contact data.
This is used as the central list of NS info including names, IDs, countries, and regions.
The module can be used to pull this data from the NS Databank API, process, and clean the data.
"""
import requests
import os
import yaml
import pandas as pd
from ifrc_ns_data.common import Dataset
from ifrc_ns_data.common.cleaners import NSInfoCleaner


class NSContactsDataset(Dataset):
    """
    Pull NS contact information from the NS Databank API, and clean and process the data.

    Parameters
    ----------
    filepath : string (required)
        Path to save the dataset when loaded, and to read the dataset from.
    """
    def __init__(self, api_key):
        self.name = 'NS Contacts'
        super().__init__()
        self.api_key = api_key


    def pull_data(self):
        """
        Read in data from the NS Databank API and save to file, or read in as a CSV file from the given filepath.
        """
        # Pull data from FDRS API
        response = requests.get(url=f'https://data-api.ifrc.org/api/entities/ns?apiKey={self.api_key}')
        response.raise_for_status()
        data = pd.DataFrame(response.json())

        return data


    def process_data(self, data):
        """
        Transform and process the data, including changing the structure and selecting columns.
        """
        # Make sure the NS names agree with the central list
        data.rename(columns={'NSO_DON_name': 'National Society name'}, errors='raise', inplace=True)
        data['National Society name'] = NSInfoCleaner().clean_ns_names(data['National Society name'])

        # Rename and order columns
        data = data.rename(columns={'country': 'Country', 'iso_3': 'ISO3', 'NSO_ZON_name': 'Region'})
        data = self.rename_columns(data, drop_others=True)
        data = self.order_index_columns(data)

        return data
