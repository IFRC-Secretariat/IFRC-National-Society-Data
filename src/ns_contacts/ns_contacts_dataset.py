"""
Module to handle National Society general information and contact data.
This is used as the central list of NS info including names, IDs, countries, and regions.
The module can be used to pull this data from the NS Databank API, process, and clean the data.
"""
import requests
import pandas as pd
from src.common import Dataset
from src.common.cleaners import DatabankNSIDMapper, NSNamesChecker


class NSContactsDataset(Dataset):
    """
    Pull NS contact information from the NS Databank API, and clean and process the data.

    Parameters
    ----------
    filepath : string (required)
        Path to save the dataset when loaded, and to read the dataset from.
    """
    def __init__(self, filepath, api_key, refresh=True, indicators=None):
        super().__init__(filepath=filepath, indicators=indicators)
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
            data.rename(columns={'KPI_DON_code': 'National Society ID',
                                 'NSO_DON_name': 'National Society name',
                                 'NSO_ZON_name': 'Region name',
                                 'ZON_Code': 'Region code'}, errors='raise', inplace=True)

            # Save the data
            data.to_csv(self.filepath, index=False)

        # Read the data from file
        super().load_data()


    def process(self):
        """
        Transform and process the data, including changing the structure and selecting columns.
        """
        # Convert NS IDs to NS names
        self.data['National Society name'] = DatabankNSIDMapper(api_key=self.api_key).map(self.data['National Society ID'])

        # Make sure the NS names agree with the central list
        NSNamesChecker().check(self.data['National Society name'])

        # Select the indicators
        self.data.set_index('National Society name')
        if self.indicators is not None:
            self.data = self.data[self.indicators]
