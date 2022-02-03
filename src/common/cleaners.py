"""
Module to define data cleaners.
"""
import requests
import pandas as pd
import yaml
import os


class DatabankNSIDMapper:
    """
    Convert National Society IDs for data in the NS Databank, to names.

    Parameters
    ----------
    ns_ids : pandas series (required)
        Pandas serise of
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

        # Map the NS names to the NS IDs in the provided data
        unknown_ids = set(data).difference(ns_ids_names_map.keys())
        if unknown_ids:
            raise ValueError('Unknown NSs IDs in data', unknown_ids)
        return data.map(ns_ids_names_map)


class NSNamesChecker:
    """
    Compare a list of National Society names against a central list of National
    Society names to ensure that all names are recognised and consistent.
    Run some basic cleaning including stripping whitespace.
    """
    ns_names = None

    def __init__(self):
        if NSNamesChecker.ns_names is None:
            NSNamesChecker.ns_names = yaml.safe_load(open(os.path.join(os.path.dirname(os.path.abspath(__file__)), 'ns_names.yml')))


    def check(self, data):
        """
        Compare the NS names in the provided data series to a known list of National Society names.

        Parameters
        ----------
        data : pandas Series (required)
            Series of a pandas DataFrame to be cleaned.
        """
        # Read in the known list of National Society names
        unrecognised_ns_names = set(data.str.strip()).difference(NSNamesChecker.ns_names)
        if unrecognised_ns_names:
            raise ValueError('Unknown NS names in data', unrecognised_ns_names)
