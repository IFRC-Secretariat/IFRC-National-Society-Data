"""
Module to define a Dataset Class with methods to load, clean, and process datasets.
"""
import os
import warnings
import pandas as pd
import yaml


class Dataset:
    """
    Dataset class to handle data, including to load, clean, and process data.

    Parameters
    ----------
    filepath : string (required)
        Path to save the dataset when loaded, and to read the dataset from.

    reload : boolean (default=True)
        If True, the data will be reloaded from source, e.g. pulled from an API, and saved to filepath.
    """
    def __init__(self, filepath, sheet_name=0, reload=True):
        self.filepath = filepath
        self.sheet_name = sheet_name
        self.data = pd.DataFrame()
        self.reload = reload
        self.index_columns = ['National Society name', 'Country', 'ISO3', 'Region']
        self.dataset_info = yaml.safe_load(open(os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 'common/dataset_indicators.yml')))[self.name]


    def __str__(self):
        """
        Redefine the str representation to print out the dataset as a pandas DataFrame.
        """
        return repr(self.data)


    @property
    def meta(self):
        """
        Get meta information about the dataset.
        """
        dataset_info = self.dataset_info
        dataset_meta = dataset_info['meta'] if 'meta' in dataset_info.keys() else None
        return dataset_meta


    @property
    def indicators(self):
        """
        Get the list of dataset indicators.
        """
        dataset_info = self.dataset_info
        return dataset_info['indicators']


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
            self.data = pd.read_excel(self.filepath, sheet_name=self.sheet_name)
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
        indicator_names = [indicator['name'] for indicator in self.indicators]
        rename_indicators = {indicator['source_name']: indicator['name'] for indicator in self.indicators}
        self.data = self.data.rename(columns=rename_indicators, errors='raise', level=0)[indicator_names]

        # Add in extra information
        if self.meta:
            for column, value in self.meta.items():
                for indicator in indicator_names:
                    self.data[indicator, column] = value

        # Order the column hierarchies
        subcolumns_order = ['value', 'year', 'link']
        if 'meta' in self.indicators:
            subcolumns_order += self.indicators['meta'].keys()
        def order_columns(x):
            order_map = {item: subcolumns_order.index(item) for item in subcolumns_order}
            order = x.map(order_map)
            return order
        self.data = self.data.sort_index(axis='columns', level=1, key=lambda x: order_columns(x), sort_remaining=False)\
                             .sort_index(axis='columns', level=0, sort_remaining=False)

        # Verify that the index names and level names are correct
        if self.data.index.names != ['National Society name', 'Country', 'ISO3', 'Region']:
            print(self.data)
            raise ValueError(f'Index names of dataset {self.name} does not match expected: {self.data.index.names}')
        if self.data.columns.names != ['indicator', None]:
            print(self.data)
            raise ValueError(f'Column names of dataset {self.name} does not match expected: {self.data.columns.names}')
