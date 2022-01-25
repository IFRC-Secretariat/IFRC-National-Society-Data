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

    cleaners : list (optional)
        List of cleaners to be applied to the dataset.
    """
    def __init__(self, filepath, cleaners):
        self.filepath = filepath
        self.cleaners = cleaners

    def load_data(self):
        data = pd.read_csv(self.filepath)
        return data

    def clean(self, data):
        for cleaner in self.cleaners:
            data = cleaner.clean(data)
