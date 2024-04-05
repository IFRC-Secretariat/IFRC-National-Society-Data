"""
Module to handle operations data from the IFRC GO platform..
The module can be used to pull this data from the IFRC GO API, process, and clean the data.
"""
import requests
import pandas as pd
from ifrc_ns_data.common import Dataset, NationalSocietiesInfo
from ifrc_ns_data.common.cleaners import NSInfoCleaner, DictColumnExpander, NSInfoMapper


class GOOperationsDataset(Dataset):
    """
    Pull IFRC operations information from the IFRC GO platform API, and clean and process the data.
    """
    def __init__(self):
        super().__init__(name='GO Operations')

    def pull_data(self, filters=None):
        """
        Read in data from the IFRC GO API.

        Parameters
        ----------
        filters : dict (default=None)
            Filters to filter by country or by National Society.
            Keys can only be "Country", "National Society name", or "ISO3". Values are lists.
        """
        # Get the list of ISO3 codes
        selected_iso3s = None
        if filters:
            selected_ns = NationalSocietiesInfo().data
            for filter_name, filter_values in filters.items():
                selected_ns = [ns for ns in selected_ns if ns[filter_name] in filter_values]
            selected_iso3s = [ns['ISO3'] for ns in selected_ns if ns['National Society ID'] is not None]

        # Pull data from the GO API
        data = []
        url = 'https://goadmin.ifrc.org/api/v2/appeal/?limit=100&offset=0'
        if selected_iso3s is None:
            next_url = url
            while next_url:
                response = requests.get(url=next_url)
                response.raise_for_status()
                data += response.json()['results']
                next_url = response.json()['next']
        else:
            for iso3 in selected_iso3s:
                next_url = f'{url}&country__iso3={iso3}'
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

        Parameters
        ----------
        data : pandas DataFrame (required)
            Raw data to be processed.
        """
        # Expand dict-type columns
        expand_columns = ['dtype', 'region', 'country']
        data = DictColumnExpander().clean(
            data=data,
            columns=expand_columns,
            drop=True
        )

        # Convert the date type columns to pandas datetimes
        for column in ['start_date', 'end_date']:
            data[column].replace({'0001-01-01T00:00:00Z': float('nan')}, inplace=True)
            data[column] = pd.to_datetime(data[column], format='%Y-%m-%dT%H:%M:%SZ')

        # Drop columns that aren't needed
        data = data.rename(columns={'country.society_name': 'National Society name'})\
            .dropna(subset=['National Society name'])\
            .drop(columns=['country.name'])

        # Check the NS names, and merge in other information
        data = data.loc[data['National Society name'] != '']
        data['National Society name'] = NSInfoCleaner().clean_ns_names(data['National Society name'])
        new_columns = [column for column in self.index_columns if column != 'National Society name']
        for column in new_columns:
            data[column] = NSInfoMapper().map(
                data['National Society name'],
                map_from='National Society name',
                map_to=column
            )

        # Select only active operations
        data = data.loc[data['status_display'] == 'Active']
        data[['amount_funded', 'amount_requested']] = data[['amount_funded', 'amount_requested']].astype(float)
        data['funding'] = 100*(data['amount_funded']/data['amount_requested']).round(0)

        # Rename, order and select columns
        data = self.rename_columns(data, drop_others=True)
        data = self.order_index_columns(data)

        return data
