"""
Module to define a Dataset Class with methods to load, clean, and process datasets.
"""
import os
import warnings
import pandas as pd


class Dataset:
    """
    Dataset class to handle data, including to load, clean, and process data.

    Parameters
    ----------
    filepath : string (required)
        Path to save the dataset when loaded, and to read the dataset from.
    """
    def __init__(self, filepath, reload=True, indicators=None):
        self.filepath = filepath
        self.data = pd.DataFrame()
        self.reload = reload
        self.indicators = indicators


    def __str__(self):
        """
        Redefine the str representation to print out the dataset as a pandas DataFrame.
        """
        return repr(self.data)


    def load_data(self):
        """
        Read in the data as a CSV or Excel file from the given file path.
        """
        # reload the data if required
        if self.reload:
            self.reload_data()

        # Check the file extension
        extension = os.path.splitext(self.filepath)[1][1:]
        if extension=='csv':
            self.data = pd.read_csv(self.filepath)
        elif extension in ['xlsx', 'xls']:
            self.data = pd.read_excel(self.filepath)
        else:
            raise ValueError(f'Unknown file extension {extension}')


    def reload_data(self):
        """
        Reload the data by pulling it from source.
        """
        warnings.warn("No method implemented to reload data for this dataset.")


    def process(self):
        """
        Process the data, including transforming it into the required structure.
        """
        # Clean the dataset
        self.clean()
