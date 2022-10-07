"""
Module to handle operations data from the IFRC GO platform..
The module can be used to pull this data from the IFRC GO API, process, and clean the data.
"""
import requests
import os
import warnings
import yaml
import pandas as pd
from ifrc_ns_data.common import Dataset
from ifrc_ns_data.common.cleaners import NSInfoCleaner, DictColumnExpander, NSInfoMapper


class OperationsDataset(Dataset):
    """
    Pull IFRC operations information from the IFRC GO platform API, and clean and process the data.
    """
    def __init__(self):
        super().__init__(name='GO Operations')


    def pull_data(self):
        """
        Read in data from the IFRC GO API.
        """
        # Pull data from FDRS API and save the data locally
        data = []
        next_url = f'https://goadmin.ifrc.org/api/v2/appeal/?limit=100&offset=0'
        while next_url:
            response = requests.get(url=next_url)
            response.raise_for_status()
            data += response.json()['results']
            next_url = response.json()['next']
        data = pd.DataFrame(data)

        return data


    def process_data(self, data, latest=None):
        """
        Transform and process the data, including changing the structure and selecting columns.
        Process the data into a NS indicator format.

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

        # Expand dict-type columns
        expand_columns = ['dtype', 'region', 'country']
        data = DictColumnExpander().clean(data=data,
                                               columns=expand_columns,
                                               drop=True)

        # Convert the date type columns to pandas datetimes
        for column in ['start_date', 'end_date']:
            data[column].replace({'0001-01-01T00:00:00Z': float('nan')}, inplace=True)
            data[column] = pd.to_datetime(data[column], format='%Y-%m-%dT%H:%M:%SZ')

        # Drop columns that aren't needed
        data = data.rename(columns={'country.society_name': 'National Society name'})\
                             .dropna(subset=['National Society name'])\
                             .drop(columns=['country.name'])

        # Check the NS names, and merge in other information
        data = data.loc[data['National Society name']!='']
        data['National Society name'] = NSInfoCleaner().clean_ns_names(data['National Society name'])
        new_columns = [column for column in self.index_columns if column!='National Society name']
        for column in new_columns:
            data[column] = NSInfoMapper().map(data['National Society name'], map_from='National Society name', map_to=column)

        # Select only active operations
        data = data.loc[data['status_display']=='Active']
        data[['amount_funded', 'amount_requested']] = data[['amount_funded', 'amount_requested']].astype(float)
        data['funding'] = 100*(data['amount_funded']/data['amount_requested']).round(0)

        # Rename, order and select columns
        data = self.rename_columns(data, drop_others=True)
        data = self.order_index_columns(data)

        return data
