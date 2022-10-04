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
    filepath : string (default=None)
        If reading from file, the location of the source file data.
    """
    def __init__(self, filepath=None, sheet_name=None):
        self.filepath = filepath
        self.sheet_name = sheet_name
        self.index_columns = ['National Society name', 'Country', 'ISO3', 'Region']
        self.dataset_info = yaml.safe_load(open(os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 'common/dataset_indicators.yml')))[self.name]


    @property
    def meta(self):
        """
        Get meta information about the dataset.
        """
        dataset_meta = self.dataset_info['meta'] if 'meta' in self.dataset_info.keys() else None
        return dataset_meta


    @property
    def indicators(self):
        """
        Get the list of dataset indicators.
        """
        dataset_info = self.dataset_info
        return dataset_info['indicators']


    def get_data(self):
        """
        Pull the raw data, process it, and return the final dataset.

        Parameters
        ----------
        """
        raw_data = self.load_data()
        processed_data = self.process_data(data=raw_data)
        processed_data['Dataset'] = self.name

        return processed_data


    def load_data(self):
        """
        Read in the data from the source: either as a CSV or Excel file from a given file path, or pull from an API.
        """
        # Read in the data from a CSV or Excel file
        if self.filepath is not None:
            extension = os.path.splitext(self.filepath)[1][1:]
            if extension=='csv':
                data = pd.read_csv(self.filepath)
            elif extension in ['xlsx', 'xls']:
                data = pd.read_excel(self.filepath, sheet_name=self.sheet_name)
            else:
                raise ValueError(f'Unknown file extension {extension}')
        # Pull data from an API
        else:
            data = self.pull_data()

        return data


    def pull_data(self):
        """
        Pull the data from an API.
        """
        raise NotImplementedError


    def process_data(self, data):
        """
        Process the data, including transforming it into the required structure.

        Parameters
        ----------
        data : Pandas DataFrame (required)
            Raw dataset to be processed.
        """
        raise NotImplementedError


    def rename_indicators(self, data):
        """
        Rename indicators in the 'Indicator' column in the dataset using the names in the yml file.
        """
        # Get a map of indicator current names to verbose names
        rename_indicators = {indicator['source_name']: indicator['name'] for indicator in self.dataset_info['indicators']}

        # Raise an error if any indicators are missing from the dataset
        missing_indicators = [indicator for indicator in rename_indicators if indicator not in data['Indicator'].unique()]
        if missing_indicators:
            raise KeyError(f"{missing_indicators} not found in columns")

        # Rename and select indicators
        data['Indicator'] = data['Indicator'].replace(rename_indicators, regex=False)
        data = data.loc[data['Indicator'].isin(rename_indicators.values())]

        # Order columns
        data = self.order_index_columns(data, other_columns=['Indicator', 'Dataset', 'Value', 'Year'], missing='raise')

        return data


    def rename_columns(self, data, drop_others=False):
        """
        Rename columns in the dataset using the names in the yml file.
        """
        # Get a map of indicator current names to verbose names, and rename
        rename_columns = {column['source_name']: column['name'] for column in self.dataset_info['columns']}
        data = data.rename(columns=rename_columns, errors='raise')

        # Drop columns that were not in the rename list
        if drop_others:
            data = data[self.index_columns+list(rename_columns.values())]

        return data


    def order_index_columns(self, data, other_columns=None, missing='ignore'):
        """
        Move the index columns containing NS information to the front of the dataset.

        Parameters
        ----------
        data : pandas DataFrame (required)
            Dataset to order the columns of.

        other_columns : list (default=None)
            If not None, this will be used to order the other columns following the index columns.

        missing : string (default='ignore')
            If other_columns is not None and missing is 'raise' then an error will be raised if there are columns in data which are not in other_columns.
        """
        # Create a list giving the required order of columns
        columns_order = self.index_columns
        if other_columns is not None:
            if missing:
                missing_columns = [column for column in data.columns if column not in columns_order]
                if missing_columns:
                    raise ValueError(f'{missing_columns} missing from dataset')
            columns_order += other_columns
        else:
            columns_order+=[column for column in data.columns if column not in self.index_columns]

        # Order columns
        data = data[columns_order]

        return data


    def filter_latest(self, data):
        """
        Filter the dataset to only return the latest data for each National Society for each indicator.
        """
        # Keep only the latest values for each indicator: keep the smallest value if there are duplicates
        data = data.sort_values(by=['Year', 'Value'], ascending=[False, True])\
                    .drop_duplicates(subset=['National Society name', 'Indicator'], keep='first')\
                    .sort_values(by=['National Society name', 'Indicator'], ascending=True)

        return data
