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

    reload : boolean (default=True)
        If True, the data will be reloaded from source, e.g. pulled from an API, and saved to filepath.

    indicators : dict (default=None)
        Dict of indicators mapping the source name to a new name. This can be used to filter and rename the indicator columns in the dataset.
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


    @property
    def columns(self):
        """
        Return the columns of the pandas DataFrame in the data attribute.
        """
        return self.data.columns


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
        raise NotImplementedError


    def add_indicator_info(self):
        """
        Merge in indicator information if set.
        """
        # Rename and select indicators
        rename_indicators = {indicator['source_name']: indicator['name'] for indicator in self.indicators}
        self.data = self.data.rename(columns=rename_indicators, errors='raise', level=0)[rename_indicators.values()]

        # Add in extra information for each indicator
        for indicator in self.indicators:
            if 'extra_info' in indicator:
                for column, value in indicator['extra_info'].items():
                    self.data[indicator['name'], column] = value

        # Order the columns
        def order_columns(x):
            order = ['value', 'year', 'source']
            order_map = {item: order.index(item) for item in order}
            return x.map(order_map)

        self.data = self.data.sort_index(axis='columns', level=1, key=lambda x: order_columns(x))\
                             .sort_index(axis='columns', level=0, sort_remaining=False)
