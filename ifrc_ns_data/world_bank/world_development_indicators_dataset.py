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
    def __init__(self, filepath, reload=True):
        self.name = 'World Development Indicators'
        super().__init__(filepath=filepath, reload=reload)
        self.reload = reload


    def reload_data(self):
        """
        Pull data from the World Bank API and save to file.
        """
        data = pd.DataFrame()
        page = 1; per_page = 1000
        while True:
            api_indicators = ';'.join([indicator['source_name'] for indicator in self.indicators])
            response = requests.get(url=f'https://api.worldbank.org/v2/country/all/indicator/{api_indicators}?source=2&page={page}&format=json&per_page={per_page}')
            data = pd.concat([data, pd.DataFrame(response.json()[1])])
            if page == response.json()[0]['pages']: break
            page += 1

        # Save the data
        data.to_csv(self.filepath, index=False)


    def process(self):
        """
        Transform and process the data, including changing the structure and selecting columns.
        """
        # Expand dict-type columns
        expand_columns = ['indicator', 'country']
        self.data = DictColumnExpander().clean(data=self.data,
                                               columns=expand_columns,
                                               drop=True)

        # Map ISO3 codes to NS names and add extra columns
        self.data['National Society name'] = NSInfoMapper().map_iso_to_ns(data=self.data['countryiso3code'])
        extra_columns = [column for column in self.index_columns if column!='National Society name']
        ns_info_mapper = NSInfoMapper()
        for column in extra_columns:
            self.data[column] = ns_info_mapper.map(data=self.data['National Society name'], on='National Society name', column=column)

        # Get the latest values of each indicator for each NS
        self.data = self.data.dropna(subset=['National Society name', 'indicator.value', 'value', 'date'], how='any')\
                             .sort_values(by=['National Society name', 'indicator.value', 'date'], ascending=[True, True, False])\
                             .drop_duplicates(subset=['National Society name', 'indicator.value'], keep='first')\
                             .rename(columns={'date': 'Year', 'indicator.id': 'Indicator', 'value': 'Value'})\
                             .pivot(index=self.index_columns, columns='Indicator', values=['Value', 'Year'])\
                             .swaplevel(axis='columns')\
                             .sort_index(axis='columns', level=0, sort_remaining=False)
