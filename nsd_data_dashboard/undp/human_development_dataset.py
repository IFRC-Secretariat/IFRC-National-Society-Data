"""
Module to handle UNDP Human Development data, including pulling it from the API, cleaning, and processing.
"""
import requests
import pandas as pd
from nsd_data_dashboard.common import Dataset
from nsd_data_dashboard.common.cleaners import DictColumnExpander, CountryNSMapper


class HumanDevelopmentDataset(Dataset):
    """
    Pull UNDP Human Development data from the API, and clean and process the data.

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
        Pull data from the UNDP Human Development API and save to file.
        """
        # Pull the data for each indicator
        data = pd.DataFrame()
        for indicator in self.indicators.keys():
            response = requests.get(url='http://ec2-54-174-131-205.compute-1.amazonaws.com/API/HDRO_API.php/indicator_id=137506')
            response.raise_for_status()

            # Unnest the data from the API into a tabular format
            indicator_data = pd.DataFrame(response.json()['indicator_value'])\
                                         .reset_index()\
                                         .rename(columns={'index': 'indicator'})\
                                         .melt(id_vars='indicator', var_name='iso3')
            indicator_data = pd.concat([indicator_data.drop(columns=['value']),
                                        pd.json_normalize(indicator_data['value'])], axis=1)

            # Append to the main data
            data = pd.concat([data, indicator_data])

        # Save the data
        data.to_csv(self.filepath, index=False)


    def process(self):
        """
        Transform and process the data, including changing the structure and selecting columns.
        """
        # Map ISO3 codes to NS names
        self.data['National Society name'] = CountryNSMapper().map(self.data['iso3'])

        # Get the latest value of each indicator for each NS
        self.data = self.data.drop(columns=['iso3'])\
                             .melt(id_vars=['National Society name', 'indicator'], var_name='year')\
                             .dropna(how='any')\
                             .sort_values(by=['National Society name', 'indicator', 'year'], ascending=[True, True, False])\
                             .drop_duplicates(subset=['National Society name', 'indicator'], keep='first')
        self.data['source'] = 'UNDP'
        self.data = self.data.pivot(index=['National Society name'], columns='indicator', values=['value', 'year', 'source'])\
                             .swaplevel(axis='columns')\
                             .sort_index(axis='columns', level=0)
