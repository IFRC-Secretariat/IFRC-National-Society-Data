"""
Module to handle projects (3W) data from the IFRC GO platform..
The module can be used to pull this data from the IFRC GO API, process, and clean the data.
"""
import requests
import os
import yaml
import pandas as pd
from ifrc_ns_data.common import Dataset
from ifrc_ns_data.common.cleaners import NSInfoCleaner, DictColumnExpander, NSInfoMapper


class ProjectsDataset(Dataset):
    """
    Pull IFRC projects (3W) information from the IFRC GO platform API, and clean and process the data.

    Parameters
    ----------
    filepath : string (required)
        Path to save the dataset when loaded, and to read the dataset from.
    """
    def __init__(self):
        self.name = 'GO Projects'
        super().__init__()


    def pull_data(self):
        """
        Read in data from the IFRC GO API and save to file.
        """
        # Pull data from FDRS API and save the data locally
        data = []
        next_url = f'https://goadmin.ifrc.org/api/v2/project/?limit=100&offset=0'
        while next_url:
            response = requests.get(url=next_url)
            response.raise_for_status()
            data += response.json()['results']
            next_url = response.json()['next']
        data = pd.DataFrame(data)

        return data


    def process_data(self, data):
        """
        Transform and process the data, including changing the structure and selecting columns.
        Process the data into a NS indicator format.
        """
        # Expand dict-type columns
        expand_columns = ['project_country_detail', 'dtype_detail', 'event_detail', 'reporting_ns_detail']
        data = DictColumnExpander().clean(data=data,
                                               columns=expand_columns,
                                               drop=True)

        # Convert the date type columns to pandas datetimes
        for column in ['start_date', 'end_date']:
            data[column] = pd.to_datetime(data[column], format='%Y-%m-%d')

        # Keep only data with a NS specified
        data = data.rename(columns={'project_country_detail.society_name': 'National Society name'})\
                             .dropna(subset=['National Society name'])

        # Clean NS names and add additional NS information
        data['National Society name'] = NSInfoCleaner().clean_ns_names(data['National Society name'])
        new_columns = [column for column in self.index_columns if column!='National Society name']
        for column in new_columns:
            data[column] = NSInfoMapper().map(data['National Society name'], on='National Society name', column=column)

        # Check all data is public, and select only ongoing projects
        if data['visibility'].unique() != ['public']:
            raise ValueError('Dataset contains non-public data.')

        # Rename, order and select columns
        data = self.rename_columns(data)
        data = self.order_index_columns(data)

        return data
