"""
Module to handle BOCA data, including loading it from the API, cleaning, and processing.
"""
import requests
import pandas as pd
from ifrc_ns_data.common import Dataset
from ifrc_ns_data.common.cleaners import NSInfoMapper


class BOCAAssessmentDatesDataset(Dataset):
    """
    Load BOCA assessment dates data from the API, and clean and process the data.

    Parameters
    ----------
    filepath : string (required)
        Path to save the dataset when loaded, and to read the dataset from.
    """
    def __init__(self, api_key):
        super().__init__(name='BOCA Assessment Dates')
        self.api_key = api_key.strip()

    def pull_data(self):
        """
        Read in raw data from the BOCA Assessments Dates API from the NS databank.
        """
        # Pull data from FDRS API
        response = requests.get(url=f'https://data-api.ifrc.org/api/bocapublic?apiKey={self.api_key}')
        response.raise_for_status()
        results = response.json()

        # Convert the data into a pandas DataFrame
        data = pd.DataFrame(results)

        return data

    def process_data(self, data):
        """
        Transform and process the data, including changing the structure and selecting columns.

        Parameters
        ----------
        data : pandas DataFrame (required)
            Raw data to be processed.
        """
        # Use the NS code to add other NS information
        ns_info_mapper = NSInfoMapper()
        for column in self.index_columns:
            ns_id_mapped = ns_info_mapper.map(
                data=data['NsId'],
                map_from='National Society ID',
                map_to=column,
                errors='raise'
            ).rename(column)
            data = pd.concat([data.reset_index(drop=True), ns_id_mapped.reset_index(drop=True)], axis=1)
        data = data.drop(columns=['NsId', 'NsName'])

        # Add other columns and order the columns
        data = self.rename_columns(data, drop_others=True)
        data = self.order_index_columns(data)

        return data
