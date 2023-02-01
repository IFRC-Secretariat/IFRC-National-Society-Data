"""
Module to handle FDRS data, including loading it from the API, cleaning, and processing.
"""
import requests
import pandas as pd
from ifrc_ns_data.common import Dataset
from ifrc_ns_data.common.cleaners import NSInfoMapper


class CorruptionPerceptionIndexDataset(Dataset):
    """
    Load CPI data from the API, and clean and process the data.

    Parameters
    ----------
    filepath : string (required)
        Path to save the dataset when loaded, and to read the dataset from.
    """
    def __init__(self):
        super().__init__(name='Corruption Perception Index')


    def pull_data(self):
        """
        Read in raw data from the Transparency International CPI API.
        """
        # Pull data from the TI API
        response = requests.get(url=f'https://www.transparency.org/api/latest/cpi')
        response.raise_for_status()

        # Tidy the data
        data = pd.DataFrame(response.json())
        
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
        # Drop and rename columns
        data = data.drop(columns=['country', 'region'])
        data = data.rename(columns={'iso3': 'ISO3'})

        # Map in NS information
        new_columns = [column for column in self.index_columns if column!='ISO3']
        ns_info_mapper = NSInfoMapper()
        for column in new_columns:
            ns_id_mapped = ns_info_mapper.map(data=data['ISO3'], map_from='ISO3', map_to=column)\
                                         .rename(column)
            data = pd.concat([data.reset_index(drop=True), ns_id_mapped.reset_index(drop=True)], axis=1)

        # Reorder columns
        data = self.rename_columns(data, drop_others=True)
        data = self.order_index_columns(data)

        return data