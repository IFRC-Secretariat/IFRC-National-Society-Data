"""
Module to handle projects (3W) data from the IFRC GO platform..
The module can be used to pull this data from the IFRC GO API, process, and clean the data.
"""
import requests
import warnings
import pandas as pd
from ifrc_ns_data.common import Dataset, NationalSocietiesInfo
from ifrc_ns_data.common.cleaners import NSInfoCleaner, DictColumnExpander, NSInfoMapper


class GOProjectsDataset(Dataset):
    """
    Pull IFRC projects (3W) information from the IFRC GO platform API, and clean and process the data.

    Parameters
    ----------
    filepath : string (required)
        Path to save the dataset when loaded, and to read the dataset from.
    """
    def __init__(self):
        super().__init__(name='GO Projects')

    def pull_data(self, filters=None):
        """
        Read in data from the IFRC GO API and save to file.

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

        # Pull data from GO API
        data = []
        url = 'https://goadmin.ifrc.org/api/v2/project/?limit=100&offset=0'
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
        if latest:
            warnings.warn(f'Filtering latest data does not apply to dataset {self.name}')

        # Expand dict-type columns
        expand_columns = ['project_country_detail', 'dtype_detail', 'event_detail', 'reporting_ns_detail']
        data = DictColumnExpander().clean(
            data=data,
            columns=expand_columns,
            drop=True
        )

        # Convert the date type columns to pandas datetimes
        for column in ['start_date', 'end_date']:
            data[column] = pd.to_datetime(data[column], format='%Y-%m-%d')

        # Keep only data with a NS specified
        data = data.rename(columns={'project_country_detail.society_name': 'National Society name'})\
            .dropna(subset=['National Society name'])

        # Clean NS names and add additional NS information
        data['National Society name'] = NSInfoCleaner().clean_ns_names(data['National Society name'])
        new_columns = [column for column in self.index_columns if column != 'National Society name']
        for column in new_columns:
            data[column] = NSInfoMapper().map(
                data['National Society name'],
                map_from='National Society name',
                map_to=column
            )

        # Check all data is public, and select only ongoing projects
        if data['visibility'].unique() != ['public']:
            raise ValueError('Dataset contains non-public data.')

        # Rename, order and select columns
        data = self.rename_columns(data, drop_others=True)
        data = self.order_index_columns(data)

        return data
