"""
Module to define data cleaners.
"""
import os
from ast import literal_eval
import warnings
import requests
import pandas as pd
import yaml


class DatabankNSIDMapper:
    """
    Convert National Society IDs for data in the NS Databank, to names.

    Parameters
    ----------
    api_key : string (required)
        API key for the NS databank.
    """
    api_response = None

    def __init__(self, api_key):
        self.api_key = api_key


    def map(self, data):
        """
        Convert National Society IDs from the NS Databank, to National Society names.

        Parameters
        ----------
        data : pandas Series (required)
            Series of NS IDs from the NS Databank to be mapped to NS names.
        """
        # Pull the data from the databank API
        if DatabankNSIDMapper.api_response is None:
            DatabankNSIDMapper.api_response = requests.get(url=f'https://data-api.ifrc.org/api/entities/ns?apiKey={self.api_key}')
            DatabankNSIDMapper.api_response.raise_for_status()

        # Get a map of NS IDs to NS names
        ns_ids_names_map = pd.DataFrame(DatabankNSIDMapper.api_response.json()).set_index('KPI_DON_code')['NSO_DON_name'].to_dict()

        # Check if there are any unkown IDs
        unknown_ids = set(data).difference(ns_ids_names_map.keys())
        if unknown_ids:
            warnings.warn(f'Unknown NSs IDs in data will not be converted to NS names: {unknown_ids}')

        # Map the names depending on the data type, and return
        if isinstance(data, pd.Series):
            results = data.map(ns_ids_names_map)
        elif isinstance(data, list):
            results = [ns_ids_names_map[ns_id] if (ns_id in ns_ids_names_map) else ns_id for ns_id in data]

        return results


class NSNamesCleaner:
    """
    Compare a list of National Society names against a central list of National
    Society names to ensure that all names are recognised and consistent.
    Run some basic cleaning including stripping whitespace.
    """
    ns_info = None

    def __init__(self):
        if NSNamesCleaner.ns_info is None:
            NSNamesCleaner.ns_info = yaml.safe_load(open(os.path.join(os.path.dirname(os.path.abspath(__file__)), 'national_societies_info.yml')))


    def clean(self, data):
        """
        Compare the NS names in the provided data series to a known list of National Society names.

        Parameters
        ----------
        data : pandas Series (required)
            Series of a pandas DataFrame to be cleaned.
        """
        # Map the list of alternative names to the main name
        ns_names_map = {}
        for ns in NSNamesCleaner.ns_info:
            if 'Alternative names' in ns.keys():
                for alt_name in ns['Alternative names']:
                    ns_names_map[alt_name] = ns['National Society name']
            if 'Country names' in ns.keys():
                for country_name in ns['Country names']:
                    ns_names_map[country_name] = ns['National Society name']

        data = data.replace(ns_names_map)

        # Read in the known list of National Society names
        known_ns_names = [ns['National Society name'] for ns in NSNamesCleaner.ns_info]
        unrecognised_ns_names = set(data.str.strip()).difference(known_ns_names)
        if unrecognised_ns_names:
            raise ValueError('Unknown NS names in data', unrecognised_ns_names)

        return data


class CountryNSMapper:
    """
    Take a list of country ISO3 codes and map them to NS names.
    """
    ns_info = None

    def __init__(self):
        if CountryNSMapper.ns_info is None:
            CountryNSMapper.ns_info = yaml.safe_load(open(os.path.join(os.path.dirname(os.path.abspath(__file__)), 'national_societies_info.yml')))


    def map(self, data):
        """
        Map the country ISO3 codes in the provided data series to National Society names.

        Parameters
        ----------
        data : pandas Series (required)
            Series of a pandas DataFrame to be mapped to National Society names.
        """
        # Map the list of alternative names to the main name
        ns_names_map = {ns['ISO3']: ns['National Society name'] for ns in CountryNSMapper.ns_info}

        # Map the NS names to the NS IDs in the provided data
        return data.map(ns_names_map)


class DictColumnExpander:
    """
    Class to expand a dict-type column in a pandas DataFrame into multiple columns.
    """
    def __init__(self):
        pass


    def clean(self, data, columns, drop=False):
        """
        Expand the dict-type column into multiple columns
        Names of the new columns will be in the format column+dict_key.

        Parameters
        ----------
        data : pandas DataFrame (required)
            Pandas DataFrame containing the columns to expand.

        columns : string or list (required)
            List of columns, or the name of one column, to expand.

        drop : bool (default=False)
            If True, the original column(s) will be dropped from the DataFrame.
        """
        # Convert columns to a list if it is a string
        if not isinstance(columns, list):
            columns = [columns]
        if not isinstance(drop, list):
            drop = [drop]*len(columns)

        # Loop through the columns to expand, rename them, and append them to the original dataframe
        for column, drop_column in zip(columns, drop):
            data[column] = data[column].apply(lambda x: x if x!=x else literal_eval(str(x)))
            expanded_column = pd.json_normalize(data[column])
            expanded_column.rename(columns={dict_key: f'{column}.{dict_key}' for dict_key in expanded_column.columns},
                                   errors='raise',
                                   inplace=True)
            data = pd.concat([data, expanded_column], axis=1)
            if drop_column:
                data.drop(columns=[column], inplace=True)

        return data
