"""
Module to handle FDRS data, including loading it from the API, cleaning, and processing.
"""
import requests
import pandas as pd
from src.common.dataset import Dataset

class FDRSDataset(Dataset):
    """
    Load FDRS data from the API, and clean and process the data.
    """
    def __init__(self, filepath, cleaners):
        super().__init__(filepath, cleaners)
        self.data = pd.DataFrame()


    def __str__(self):
        return repr(self.data)


    def load_data(self, api_key, refresh=True):
        """
        Read in data from the filepath.
        """
        # Pull data from FDRS API and save the data locally
        if refresh:
            response = requests.get(url='https://data-api.ifrc.org/api/Data?apiKey='+str(api_key))
            response.raise_for_status()
            data = pd.DataFrame(response.json()['data'])
            data.to_csv(self.filepath)

        # Read the data from file
        self.data = super().load_data()
