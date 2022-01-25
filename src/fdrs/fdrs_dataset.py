"""
Module to handle FDRS data, including loading it from the API, cleaning, and processing.
"""
import requests
import pandas as pd
from src.common.dataset import Dataset

class FDRSDataset(Dataset):
    """
    Load FDRS data from the API, and clean and process the data.

    Parameters
    ----------
    filepath : string (required)
        Path to save the dataset when loaded, and to read the dataset from.

    cleaners : list (optional)
        List of cleaners to be applied to the dataset.
    """
    def __str__(self):
        """
        Redefinen the str representation to print out the dataset as a pandas DataFrame.
        """
        return repr(self.data)


    def load_data(self, api_key, refresh=True):
        """
        Read in data from the filepath.

        Parameters
        ----------
        api_key : string (required)
            API key for the FDRS API used to authenticate.

        refresh : boolean (default=False)
            If True, the data is pulled from the API and saved locally. If False, the data is read in locally from file.
        """
        # Pull data from FDRS API and save the data locally
        if refresh:
            response = requests.get(url='https://data-api.ifrc.org/api/Data?apiKey='+str(api_key))
            response.raise_for_status()

            # Unnest the response from the API into a tabular format
            data = pd.DataFrame(response.json()['data'])
            if not (data['id'].nunique() == len(data) == 1076):
                raise RuntimeError('FDRS data does not have the expected length of 1076 indicators.')
            data = data.explode('data', ignore_index=True)
            data = pd.concat([data.drop(columns=['data']).rename(columns={'id': 'indicator_id'}),
                              pd.json_normalize(data['data']).rename(columns={'id': 'ns_id'})], axis=1)
            data = data.explode('data', ignore_index=True)
            data = pd.concat([data.drop(columns=['data']),
                              pd.json_normalize(data['data'])], axis=1)

            # Save the data
            data.to_csv(self.filepath)

        # Read the data from file
        self.data = super().load_data()
