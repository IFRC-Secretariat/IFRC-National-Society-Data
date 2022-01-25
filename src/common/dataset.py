"""
Module to define a Dataset Class with methods to load, clean, and process datasets.
"""
import pandas as pd

class Dataset:
    """
    Dataset class to handle data, including to load, clean, and process data.

    Parameters
    ----------
    filepath : string (required)
        Path to save the dataset when loaded, and to read the dataset from.
    """
    def __init__(self, filepath):
        self.filepath = filepath
        self.data = pd.DataFrame()


    def load_data(self):
        """
        Read in the data as a CSV file from the given file path.
        """
        data = pd.read_csv(self.filepath)
        return data


    def clean(self, cleaners=[]):
        """
        Loop through the data cleaners to clean the data.

        Parameters
        ----------
        cleaners : list (default=[])
            List of data cleaners to be applied to the dataset.
        """
        for cleaner in cleaners:
            self.data = cleaner.clean(self.data)
