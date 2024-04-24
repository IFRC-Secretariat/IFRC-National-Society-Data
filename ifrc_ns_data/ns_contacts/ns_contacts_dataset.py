"""
Module to handle National Society general information and contact data.
This is used as the central list of NS info including names, IDs, countries, and regions.
The module can be used to pull this data from the NS Databank API, process, and clean the data.
"""
import requests
import pandas as pd
from ifrc_ns_data.common import Dataset, NationalSocietiesInfo
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
        super().__init__(name='NS Contacts')
        self.api_key = api_key.strip()

    def pull_data(self, filters=None):
        """
        Read in data from the NS Databank API and save to file, or read in as a CSV file from the given filepath.

        Parameters
        ----------
        filters : dict (default=None)
            Filters to filter by country or by National Society.
            Keys can only be "Country", "National Society name", or "ISO3". Values are lists.
        """
        # Convert the provided information to NS IDs
        selected_ns = NationalSocietiesInfo().data
        for filter_name, filter_values in filters.items():
            selected_ns = [ns for ns in selected_ns if ns[filter_name] in filter_values]
        selected_ns_ids = [ns['National Society ID'] for ns in selected_ns if ns['National Society ID'] is not None]

        # Pull data from FDRS API
        response = requests.get(
            url=f'https://data-api.ifrc.org/api/entities/ns/?{",".join(selected_ns_ids)}&apiKey={self.api_key}'
        )
        response.raise_for_status()
        data = pd.DataFrame(response.json())

        return data

    def process_data(self, data):
        """
        Transform and process the data, including changing the structure and selecting columns.

        Parameters
        ----------
        data : pandas DataFrame (required)
            Raw data to be processed.
        """
        # Make sure the NS names agree with the central list
        data.rename(columns={'NSO_DON_name': 'National Society name'}, errors='raise', inplace=True)
        data['National Society name'] = NSInfoCleaner().clean_ns_names(data['National Society name'])

        # Rename and order columns
        rename_columns = {
            'country': 'Country',
            'iso_3': 'ISO3',
            'NSO_ZON_name': 'Region',
            'cur_code': 'Currency',
            'url': 'URL',
            'facebook': 'Facebook',
            'twitter': 'Twitter',
            'othersocial': 'Other social'
        }
        data = data.rename(columns=rename_columns, errors='raise')
        data = data[list(set(self.index_columns.copy() + list(rename_columns.values())))]

        # Melt into indicator format
        data = pd.melt(
            data,
            id_vars=self.index_columns,
            value_vars=[
                column for column in data.columns
                if column not in self.index_columns
            ],
            var_name="Indicator",
            value_name="Value"
        )
        data['Year'] = ''
        data = self.order_index_columns(data)

        return data
