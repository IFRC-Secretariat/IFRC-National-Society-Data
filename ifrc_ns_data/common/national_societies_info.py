"""
Module to handle National Society general information and contact data.
This is used as the central list of NS info including names, IDs, countries, and regions.
The module can be used to pull this data from the NS Databank API, process, and clean the data.
"""
import os
import yaml
import pandas as pd


class NationalSocietiesInfo:
    """
    Pull NS contact information from the NS Databank API, and clean and process the data.

    Parameters
    ----------
    filepath : string (required)
        Path to save the dataset when loaded, and to read the dataset from.
    """
    data = None

    def __init__(self):
        if NationalSocietiesInfo.data is None:
            NationalSocietiesInfo.data = yaml.safe_load(open(os.path.join(os.path.dirname(os.path.abspath(__file__)), 'national_societies_info.yml')))


    @property
    def ns_names(self):
        """
        Get the list of NS names.
        """
        ns_names = [ns['National Society name'] for ns in NationalSocietiesInfo.data]
        return ns_names


    @property
    def df(self):
        """
        Get a pandas DataFrame of the NS data.
        """
        return pd.DataFrame(self.data)
