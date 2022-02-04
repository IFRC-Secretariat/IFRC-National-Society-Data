"""
Module to handle FDRS data, including loading it from the API, cleaning, and processing.
"""
import requests
import pandas as pd
from src.common import Dataset
from src.common.cleaners import DatabankNSIDMapper, NSNamesChecker


class FDRSDataset(Dataset):
    """
    Load FDRS data from the API, and clean and process the data.

    Parameters
    ----------
    filepath : string (required)
        Path to save the dataset when loaded, and to read the dataset from.
    """
    def __init__(self, filepath, api_key, reload=True, indicators=None):
        super().__init__(filepath=filepath, indicators=indicators)
        self.api_key = api_key
        self.reload = reload


    def reload_data(self):
        """
        Read in data from the NS Databank API and save to file, or read in as a CSV file from the given filepath.
        """
        # Pull data from FDRS API and save the data locally
        response = requests.get(url=f'https://data-api.ifrc.org/api/Data?apiKey={self.api_key}')
        response.raise_for_status()

        # Unnest the response from the API into a tabular format
        data = pd.DataFrame(response.json()['data'])
        data = data.explode('data', ignore_index=True)
        data = pd.concat([data.drop(columns=['data']).rename(columns={'id': 'indicator'}),
                          pd.json_normalize(data['data']).rename(columns={'id': 'National Society ID'})], axis=1)
        data = data.explode('data', ignore_index=True)
        data = pd.concat([data.drop(columns=['data']),
                          pd.json_normalize(data['data'])], axis=1)

        if data['years'].astype(str).nunique()!=1:
            raise ValueError('Unexpected values in years column', data['years'].astype(str).unique())
        data.drop(columns=['years'], inplace=True)

        # Save the data
        data.to_csv(self.filepath, index=False)


    def process(self):
        """
        Transform and process the data, including changing the structure and selecting columns.
        """
        # Convert NS IDs to NS names
        self.data['National Society name'] = DatabankNSIDMapper(api_key=self.api_key).map(self.data['National Society ID'])

        # Make sure the NS names agree with the central list
        NSNamesChecker().check(self.data['National Society name'])

        # Pivot the dataframe to have NSs as rows and indicators as columns
        self.data.dropna(how='any', inplace=True)
        self.data = self.data.pivot(index=['National Society name', 'year'], columns='indicator', values='value')
