"""
Module to handle FDRS data, including loading it from the API, cleaning, and processing.
"""
import warnings
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

    def pull_data(self, filters=None):
        """
        Read in raw data from the Transparency International CPI API.

        Parameters
        ----------
        filters : dict (default=None)
            Filters to filter by country or by National Society.
            Keys can only be "Country", "National Society name", or "ISO3". Values are lists.
            Note that this is NOT IMPLEMENTED and is only included in this method to ensure consistency
            with the parent class and other child classes.
        """
        # The data cannot be filtered from the API so raise a warning if filters are provided
        if (filters is not None) and (filters != {}):
            warnings.warn(f'Filters {filters} not applied because the API response cannot be filtered.')

        # Pull data from the TI API
        response = requests.get(url='https://www.transparency.org/api/latest/cpi')
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
        new_columns = [column for column in self.index_columns if column != 'ISO3']
        ns_info_mapper = NSInfoMapper()
        for column in new_columns:
            ns_id_mapped = ns_info_mapper.map(data=data['ISO3'], map_from='ISO3', map_to=column)\
                                         .rename(column)
            data = pd.concat([data.reset_index(drop=True), ns_id_mapped.reset_index(drop=True)], axis=1)

        # Reorder columns
        data = self.rename_columns(data, drop_others=True)
        data = self.order_index_columns(data)

        # Filter only the latest data
        data = data\
            .sort_values(by=['National Society name', 'Year'], ascending=[True, False])\
            .drop_duplicates(subset=['National Society name'], keep='first')\
            .reset_index(drop=True)

        return data
