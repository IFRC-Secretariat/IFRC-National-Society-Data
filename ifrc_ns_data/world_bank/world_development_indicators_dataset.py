"""
Module to handle World Bank data, including pulling it from the World Bank API, cleaning, and processing.
"""
import requests
import os
import yaml
import pandas as pd
from ifrc_ns_data.common import Dataset
from ifrc_ns_data.common.cleaners import DictColumnExpander, NSInfoMapper


class WorldDevelopmentIndicatorsDataset(Dataset):
    """
    Pull World Development Indicators data from the World Bank API, and clean and process the data.

    Parameters
    ----------
    filepath : string (required)
        Path to save the dataset when pulled, and to read the dataset from.
    """
    def __init__(self):
        self.name = 'World Development Indicators'
        super().__init__()


    def pull_data(self):
        """
        Pull data from the World Bank API and save to file.
        """
        data = pd.DataFrame()
        page = 1; per_page = 1000
        while True:
            api_indicators = ';'.join([indicator['source_name'] for indicator in self.dataset_info['indicators']])
            url = f'https://api.worldbank.org/v2/country/all/indicator/{api_indicators}?source=2&page={page}&format=json&per_page={per_page}'
            print(f'Requesting page {page}', end=' ')
            response = requests.get(url=url)
            data = pd.concat([data, pd.DataFrame(response.json()[1])])
            total_pages = response.json()[0]['pages']
            print(f'out of {total_pages}')
            if page == total_pages:
                break
            page += 1

        return data


    def process_data(self, data, filter_latest=False):
        """
        Transform and process the data, including changing the structure and selecting columns.
        """
        # Expand dict-type columns
        expand_columns = ['indicator', 'country']
        data = DictColumnExpander().clean(data=data, columns=['indicator', 'country'], drop=True)

        # Map ISO3 codes to NS names and add extra columns
        data['National Society name'] = NSInfoMapper().map_iso_to_ns(data=data['countryiso3code'])
        extra_columns = [column for column in self.index_columns if column!='National Society name']
        ns_info_mapper = NSInfoMapper()
        for column in extra_columns:
            data[column] = ns_info_mapper.map(data=data['National Society name'], on='National Society name', column=column)

        # The data contains regional and world-level information, drop this
        data = data.dropna(subset=['National Society name', 'indicator.value', 'value', 'date'], how='any')\
                   .rename(columns={'date': 'Year', 'indicator.id': 'Indicator', 'value': 'Value'}, errors='raise')\
                   .drop(columns=['countryiso3code', 'country.id', 'country.value', 'unit', 'obs_status', 'decimal', 'scale', 'indicator.value'])

        # Get the latest values of each indicator for each NS
        if filter_latest:
            data = data.sort_values(by=['National Society name', 'indicator.value', 'Year'], ascending=[True, True, False])\
                       .drop_duplicates(subset=['National Society name', 'Indicator'], keep='first')\

        # Select and rename indicators
        data = self.rename_indicators(data)

        return data
