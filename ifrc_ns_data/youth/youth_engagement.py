"""
Module to access and handle IFRC Youth Engagement data.
"""
import requests
from io import StringIO
import pandas as pd
from ifrc_ns_data.common import Dataset
from ifrc_ns_data.common.cleaners import NSInfoCleaner, NSInfoMapper


class YouthEngagementDataset(Dataset):
    """
    Load IFRC Youth Engagement Survey data from the website at
    https://volunteeringredcross.org/en/global-youth-survey-en/, and clean and process the data.

    Parameters
    ----------
    filepath : string (required)
        Path to save the dataset when loaded, and to read the dataset from.
    """
    def __init__(self):
        super().__init__(name='Youth Engagement')

    def pull_data(self):
        """
        Scrape data from the Youth Engagement website at https://volunteeringredcross.org/en/global-youth-survey-en/.
        """
        # Pull data from the URL used on the website: add a long timeout because it can take a long time
        response = requests.get(
            url='https://volunteeringredcross.org/en/wp-json/visualizer/v1/action/8333/csv/',
            timeout=600
        )
        response.raise_for_status()

        # Extract the data
        data = response.json()
        string_data = StringIO(data['data']['csv'])
        data = pd.read_csv(string_data, sep=',', usecols=range(0, 7), skiprows=[1])

        return data

    def process_data(self, data):
        """
        Transform and process the data, including changing the structure and selecting columns.

        Parameters
        ----------
        data : pandas DataFrame (required)
            Raw data to be processed.
        """
        # Drop columns and add NS information
        data = data.drop(columns=['Region'])
        data = data.rename(columns={'National Society': 'National Society name'})
        data["National Society name"] = NSInfoCleaner().clean_ns_names(data["National Society name"])
        new_columns = [column for column in self.index_columns if column != 'National Society name']
        ns_info_mapper = NSInfoMapper()
        for column in new_columns:
            data[column] = ns_info_mapper.map(
                data=data['National Society name'],
                map_from='National Society name',
                map_to=column
            )

        # Convert data types
        data['Year'] = pd.to_numeric(data['Year'], errors='raise')

        # Rename and order the columns
        select_columns = ['Year', 'Youth Policy', 'Youth Engagement Strategy', 'Youth in GB', 'Youth-led structure']
        data = data[self.index_columns.copy() + select_columns]

        return data
