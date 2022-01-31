"""
Module to handle National Society general information and contact data.
This is used as the central list of NS info including names, IDs, countries, and regions.
The module can be used to pull this data from the NS Databank API, process, and clean the data.
"""
import requests
import pandas as pd
from src.common import Dataset

class NSContactsDataset(Dataset):
    """
    Pull NS contact information from the NS Databank API, and clean and process the data.

    Parameters
    ----------
    filepath : string (required)
        Path to save the dataset when loaded, and to read the dataset from.
    """
    def __init__(self, filepath, api_key, refresh=True, cleaners=None, indicators=None):
        super().__init__(filepath=filepath, cleaners=cleaners, indicators=indicators)
        self.api_key = api_key
        self.refresh = refresh


    def load_data(self):
        """
        Read in data from the NS Databank API and save to file, or read in as a CSV file from the given filepath.
        """
        # Pull data from FDRS API and save the data locally
        if self.refresh:
            response = requests.get(url=f'https://data-api.ifrc.org/api/entities/ns?apiKey={self.api_key}')
            response.raise_for_status()

            # Convert to a pandas DataFrame and rename columns for consistency with other datasets
            data = pd.DataFrame(response.json())
            data.rename(columns={'KPI_DON_code': 'ns_id',
                                 'NSO_DON_name': 'ns_name',
                                 'NSO_ZON_name': 'zone_name',
                                 'ZON_Code': 'zone_code'}, errors='raise', inplace=True)

            # Save the data
            data.to_csv(self.filepath, index=False)

        # Read the data from file
        self.data = super().load_data()


    def process(self):
        """
        Transform and process the data, including changing the structure and selecting columns.
        """
        # Select the indicators
        self.data.set_index('ns_name')
        if self.indicators is not None:
            self.data = self.data[self.indicators]
