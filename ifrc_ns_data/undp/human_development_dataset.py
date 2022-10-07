"""
Module to handle UNDP Human Development data, including pulling it from the API, cleaning, and processing.
"""
import requests
import os
import yaml
import pandas as pd
from ifrc_ns_data.common import Dataset
from ifrc_ns_data.common.cleaners import DictColumnExpander, NSInfoMapper


class HumanDevelopmentDataset(Dataset):
    """
    Pull UNDP Human Development data from the API, and clean and process the data.

    Parameters
    ----------
    filepath : string (required)
        Path to save the dataset when pulled, and to read the dataset from.
    """
    def __init__(self):
        super().__init__(name='UNDP Human Development')


    def pull_data(self):
        """
        Pull data from the UNDP Human Development API and save to file.
        """
        # Pull the data for each indicator
        data = pd.DataFrame()
        for indicator in self.indicators:
            response = requests.get(url=f'http://ec2-54-174-131-205.compute-1.amazonaws.com/API/HDRO_API.php/indicator_id={indicator["source_name"]}')
            response.raise_for_status()

            # Unnest the data from the API into a tabular format
            indicator_data = pd.DataFrame(response.json()['indicator_value'])\
                                         .reset_index()\
                                         .rename(columns={'index': 'Indicator'})\
                                         .melt(id_vars='Indicator', var_name='iso3')
            indicator_data = pd.concat([indicator_data.drop(columns=['value']),
                                        pd.json_normalize(indicator_data['value'])], axis=1)

            # Append to the main data
            data = pd.concat([data, indicator_data])

        return data


    def process_data(self, data, latest=False):
        """
        Transform and process the data, including changing the structure and selecting columns.

        Parameters
        ----------
        data : pandas DataFrame (required)
            Raw data to be processed.

        latest : bool (default=False)
            If True, only the latest data for each National Society and indicator will be returned.
        """
        # Map ISO3 codes to NS names, and add extra columns
        data['National Society name'] = NSInfoMapper().map_iso_to_ns(data=data['iso3'])
        extra_columns = [column for column in self.index_columns if column!='National Society name']
        ns_info_mapper = NSInfoMapper()
        for column in extra_columns:
            data[column] = ns_info_mapper.map(data=data['National Society name'], map_from='National Society name', map_to=column)

        # Melt the data into a log format
        data = data.drop(columns=['iso3'])\
                   .melt(id_vars=self.index_columns+['Indicator'], var_name='Year')\
                   .dropna(how='any')\
                   .rename(columns={'value': 'Value'})

        # Filter the latest data for each NS/ indicator
        if latest:
            data = self.filter_latest_indicators(data)

        # Select and rename indicators
        data = self.rename_indicators(data)
        data = self.order_index_columns(data, other_columns=['Indicator', 'Value', 'Year'])

        return data
