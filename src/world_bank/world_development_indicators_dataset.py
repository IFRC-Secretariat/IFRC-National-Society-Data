"""
Module to handle World Bank data, including pulling it from the World Bank API, cleaning, and processing.
"""
import requests
import pandas as pd
from src.common import Dataset
from src.common.cleaners import DictColumnExpander, CountryNSMapper


class WorldDevelopmentIndicatorsDataset(Dataset):
    """
    Pull World Development Indicators data from the World Bank API, and clean and process the data.

    Parameters
    ----------
    filepath : string (required)
        Path to save the dataset when pulled, and to read the dataset from.
    """
    def __init__(self, filepath, reload=True, indicators=None):
        super().__init__(filepath=filepath, reload=reload, indicators=indicators)
        self.reload = reload


    def reload_data(self):
        """
        Pull data from the World Bank API and save to file.
        """
        data = pd.DataFrame()
        page = 1; per_page = 1000
        while True:
            api_indicators = ';'.join(self.indicators.keys())
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

        # Map ISO3 codes to NS names
        self.data['National Society name'] = CountryNSMapper().map(self.data['countryiso3code'])

        # Get the latest values of each indicator for each NS
        self.data = self.data.dropna(subset=['National Society name', 'indicator.value', 'value', 'date'], how='any')\
                             .sort_values(by=['National Society name', 'indicator.value', 'date'], ascending=[True, True, False])\
                             .drop_duplicates(subset=['National Society name', 'indicator.value'], keep='first')\
                             .rename(columns={'date': 'year'})\
                             .pivot(index=['National Society name'], columns='indicator.id', values=['value', 'year'])\
                             .swaplevel(axis='columns')\
                             .sort_index(axis='columns', level=0)
