"""
Module to access and handle IFRC Youth Engagement data.
"""
import requests
from io import StringIO
import warnings
from bs4 import BeautifulSoup
import pandas as pd
from ifrc_ns_data.common import Dataset
from ifrc_ns_data.common.cleaners import NSInfoCleaner, NSInfoMapper


class YouthEngagementDataset(Dataset):
    """
    Load IFRC Youth Engagement Survey data from the website at https://volunteeringredcross.org/en/global-youth-survey-en/, and clean and process the data.

    Parameters
    ----------
    filepath : string (required)
        Path to save the dataset when loaded, and to read the dataset from.
    """
    def __init__(self):
        super().__init__(name='Youth Engagement')


    def pull_data(self, filters=None):
        """
        Scrape data from the Youth Engagement website at https://volunteeringredcross.org/en/global-youth-survey-en/.
        
        Parameters
        ----------
        filters : dict (default=None)
            Filters to filter by country or by National Society.
            Keys can only be "Country", "National Society name", or "ISO3". Values are lists.
            Note that this is NOT IMPLEMENTED and is only included in this method to ensure consistency with the parent class and other child classes.
        """
        # The data cannot be filtered from the API so raise a warning if filters are provided
        if (filters is not None) and (filters != {}):
            warnings.warn(f'Filters {filters} not applied because the API response cannot be filtered.')

        # Pull data from the URL used on the website: add a long timeout because it can take a long time
        response = requests.get(url='https://volunteeringredcross.org/en/wp-json/visualizer/v1/action/8333/csv/', timeout=600)
        response.raise_for_status()

        # Extract the data
        data = response.json()
        string_data = StringIO(data['data']['csv'])
        data = pd.read_csv(string_data, sep=',', usecols=range(0,7), skiprows=[1])

        return data


    def process_data(self, data, latest=None):
        """
        Transform and process the data, including changing the structure and selecting columns.

        Parameters
        ----------
        data : pandas DataFrame (required)
            Raw data to be processed.
        """
        # Print a warning if filtering is given as this does not apply
        if latest:
            warnings.warn(f'Filtering latest data does not apply to dataset {self.name}')

        # Drop columns and add NS information
        data = data.drop(columns=['Region'])
        data = data.rename(columns={'National Society': 'National Society name'})
        data["National Society name"] = NSInfoCleaner().clean_ns_names(data["National Society name"])
        new_columns = [column for column in self.index_columns if column!='National Society name']
        ns_info_mapper = NSInfoMapper()
        for column in new_columns:
            data[column] = ns_info_mapper.map(data=data['National Society name'], map_from='National Society name', map_to=column)

        # Convert data types
        data['Year'] = pd.to_numeric(data['Year'], errors='raise')

        # Reorder columns
        data = self.rename_columns(data, drop_others=True)
        data = self.order_index_columns(data)

        return data