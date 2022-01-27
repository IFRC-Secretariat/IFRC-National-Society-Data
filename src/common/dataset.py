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
    def __init__(self, filepath, cleaners=None, indicators=None):
        self.filepath = filepath
        self.data = pd.DataFrame()
        self.cleaners = [] if cleaners is None else cleaners
        self.indicators = [] if indicators is None else indicators


    def __str__(self):
        """
        Redefine the str representation to print out the dataset as a pandas DataFrame.
        """
        return repr(self.data)


    def load_data(self):
        """
        Read in the data as a CSV file from the given file path.
        """
        data = pd.read_csv(self.filepath)
        return data


    def clean(self):
        """
        Loop through the data cleaners to clean the data.
        """
        for cleaner in self.cleaners:
            self.data = cleaner.clean(self.data)


    def process(self):
        """
        Process the data, including transforming it into the required structure.
        """
        pass
