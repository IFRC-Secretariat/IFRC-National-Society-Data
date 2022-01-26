"""
Module to define data cleaners.
"""
import requests
import pandas as pd
import yaml
import os


class DatabankNSIDConverter:
    """
    Convert National Society IDs for data in the NS Databank, to names.

    Parameters
    ----------
    ns_ids : pandas series (required)
        Pandas serise of
    """
    def __init__(self, api_key, ids_column='ns_id', names_column='ns_name'):
        self.api_key = api_key
        self.ids_column = ids_column
        self.names_column = names_column


    def clean(self, data):
        """
        Convert National Society IDs from the NS Databank, to National Society names.
        """
        # Pull the data from the databank API
        response = requests.get(url='https://data-api.ifrc.org/api/entities/ns?apiKey='+str(self.api_key))
        response.raise_for_status()

        # Get a map of NS IDs to NS names
        ns_ids_names_map = pd.DataFrame(response.json()).set_index('KPI_DON_code')['NSO_DON_name'].to_dict()

        # Map the NS names to the NS IDs in the provided data
        unknown_ids = set(data[self.ids_column]).difference(ns_ids_names_map.keys())
        if unknown_ids:
            raise ValueError('Unknown NSs IDs in data', unknown_ids)
        data[self.names_column] = data[self.ids_column].map(ns_ids_names_map)

        return data


class NSNamesCleaner:
    """
    Compare a list of National Society names against a central list of National
    Society names to ensure that all names are recognised and consistent.
    Run some basic cleaning including stripping whitespace.
    """
    def __init__(self, column='ns_name'):
        self.column = column
        self.ns_names = yaml.safe_load(open(os.path.join(os.path.dirname(os.path.abspath(__file__)), 'ns_names.yml')))


    def clean(self, data):
        """
        Compare the NS names in a column of the dataset to a known list of National Society names.
        """
        # Read in the known list of National Society names
        unrecognised_ns_names = set(data[self.column]).difference(self.ns_names)
        if unrecognised_ns_names:
            raise ValueError('Unknown NS names in data', unrecognised_ns_names)
