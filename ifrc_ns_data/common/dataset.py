"""
Module to define a Dataset Class with methods to load, clean, and process datasets.
"""
import os
import pandas as pd
import yaml
from ifrc_ns_data.definitions import DATASETS_CONFIG_PATH
from .national_societies_info import NationalSocietiesInfo


class Dataset:
    """
    Dataset class to handle data, including to load, clean, and process data.

    Parameters
    ----------
    filepath : string (default=None)
        If reading from file, this is the location of the source file data.
        This is required for datasets which are not pulled from elsewhere (e.g. API).

    sheet_name : string (default=None)
        Required when the filepath is a path to an Excel document.
    """
    def __init__(self, name, filepath=None, sheet_name=None):
        self.name = name

        # Validate the filepath and sheet_name
        if filepath is not None:
            extension = os.path.splitext(filepath)[1][1:]
            if extension in ['xlsx', 'xls']:
                if sheet_name is None:
                    raise ValueError('Excel file must have sheet_name specified')
            elif extension not in ['csv']:
                raise ValueError('File specified in filepath must be Excel (xlsx or xls) or CSV (csv)')
        self.filepath = filepath
        self.sheet_name = sheet_name
        self.index_columns = ['National Society name', 'Country', 'ISO3', 'Region']

        # Set information about the dataset as attributes
        dataset_info = yaml.safe_load(open(DATASETS_CONFIG_PATH, encoding='utf-8'))[self.name]
        for info in dataset_info:
            setattr(self, info.lower(), dataset_info[info])

    def get_data(self, latest=None, iso3=None, country=None, ns=None):
        """
        Pull the raw data from file or API. Process the data.
        Filter the dataset by National Society name/ Country/ ISO3,
        and optionally by latest (gets only the latest data for each dataset).

        Parameters
        ----------
        iso3 : string or list (default=None)
            String or list of country ISO3 codes to filter the dataset.

        country : string or list (default=None)
            String or list of country names to filter the dataset.

        ns : string or list (default=None)
            String or list of National Society names to filter the dataset.

        latest : bool (default=None)
            If True, only the latest data for each National Society and indicator will be returned.
        """
        # Process the input parameters
        inputs = {'ISO3': iso3, 'Country': country, 'National Society name': ns}
        filters = {}
        for name, val in inputs.items():
            if val is None:
                continue
            elif isinstance(val, str):
                filters[name] = [val]
            elif isinstance(val, list):
                filters[name] = val
            else:
                raise TypeError(f'{val} is not a list or string')

        # Check all countries and NS names are valid
        if filters:
            ns_info = NationalSocietiesInfo()
            check_values = {
                'ISO3': ns_info.iso3_list,
                'Country': ns_info.country_list,
                'National Society name': ns_info.ns_list
            }
            for filter_name, val_list in filters.items():
                unrecognised_values = [item for item in val_list if item not in check_values[filter_name]]
                if unrecognised_values:
                    raise ValueError(
                        f'Unrecognised values {unrecognised_values}.\
                        \n\nThe allowed values are: {check_values[filter_name]}'
                    )

        # Get the data from file or API
        raw_data = self.load_source_data(filters)

        # Process the data, get latest data if this is an option (if not catch the TypeError)
        try:
            processed_data = self.process_data(data=raw_data, latest=latest)
        except TypeError:
            processed_data = self.process_data(data=raw_data)

        # Filter the processed data by country/ NS name/ ISO3
        for filter_name, filter_values in filters.items():
            # If the country column is a list of countries
            if isinstance(processed_data[filter_name].dropna().iloc[0], list):
                processed_data = processed_data[
                    processed_data[filter_name].apply(
                        lambda x: bool(set(x) & set(filter_values))
                    )
                ]
            else:
                processed_data = processed_data.loc[processed_data[filter_name].isin(filter_values)]
        processed_data.reset_index(inplace=True, drop=True)

        self.data = processed_data

        return processed_data

    def load_source_data(self, filters=None):
        """
        Read in the data from the source: either as a CSV or Excel file from a given file path, or pull from an API.

        Parameters
        ----------
        filters : dict (default=None)
            Dict mapping filter names to lists of values, e.g. {'iso3': ['AFG', 'ALB']}.
        """
        # Read in the data from a CSV or Excel file
        if self.filepath is not None:
            extension = os.path.splitext(self.filepath)[1][1:]
            if extension == 'csv':
                data = pd.read_csv(self.filepath)
            elif extension in ['xlsx', 'xls']:
                data = pd.read_excel(self.filepath, sheet_name=self.sheet_name)
            else:
                raise ValueError(f'Unknown file extension {extension}')
        # Pull data from an API, if possible apply the filters
        else:
            try:
                data = self.pull_data(filters=filters)
            except TypeError:
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

    def rename_indicators(self, data, missing='raise'):
        """
        Rename indicators in the 'Indicator' column in the dataset using the names in the yml file.

        Parameters
        ----------
        data : pandas DataFrame (required)
            Dataset to rename indicators in.

        missing : string (default='raise')
            If 'raise', raise an error if there are indicators in rename_indicators which are not in the dataset.
        """
        # Get a map of indicator current names to verbose names
        rename_indicators = {indicator['source_name']: indicator['name'] for indicator in self.indicators}

        # Raise an error if any indicators are missing from the dataset
        if missing == 'raise':
            missing_indicators = [
                indicator
                for indicator in rename_indicators
                if indicator not in data['Indicator'].unique()
            ]
            if missing_indicators:
                raise KeyError(f"{missing_indicators} not found in dataset indicators")

        # Rename and select indicators
        data['Indicator'] = data['Indicator'].replace(rename_indicators, regex=False)
        data = data.loc[data['Indicator'].isin(rename_indicators.values())]

        return data

    def order_index_columns(self, data, other_columns=None, drop_others=False):
        """
        Move the index columns containing NS information to the front of the dataset.

        Parameters
        ----------
        data : pandas DataFrame (required)
            Dataset to order the columns of.

        other_columns : list (default=None)
            If not None, this will be used to order the other columns following the index columns.

        drop_others : bool (default=False)
            If other_columns is not None and drop_others is True, drop columns
            which are not specified in other_columns or are index columns.
        """
        # Create a list giving the required order of columns
        columns_order = self.index_columns.copy()
        if other_columns is not None:
            columns_order += other_columns
            if not drop_others:
                columns_order += [column for column in data.columns if column not in columns_order]
        else:
            columns_order += [column for column in data.columns if column not in columns_order]

        # Order columns
        data = data[columns_order]

        return data

    def filter_latest_indicators(self, data):
        """
        Filter an indicator-style dataset to only return the latest data for each National Society for each indicator.

        Parameters
        ----------
        data : pandas DataFrame
            Dataset to filter.
        """
        # Keep only the latest values for each indicator: keep the smallest value if there are duplicates
        data = data\
            .sort_values(by=['Year', 'Value'], ascending=[False, True])\
            .drop_duplicates(subset=['National Society name', 'Indicator'], keep='first')\
            .sort_values(by=['National Society name', 'Indicator'], ascending=True)

        return data
