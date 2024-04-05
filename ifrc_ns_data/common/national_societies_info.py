"""
Module to handle National Society general information and contact data.
This is used as the central list of NS info including names, IDs, countries, and regions.
The module can be used to pull this data from the NS Databank API, process, and clean the data.
"""
import os
import yaml
import pandas as pd
from ifrc_ns_data.definitions import ROOT_DIR


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
            NationalSocietiesInfo.data = yaml.safe_load(open(
                    os.path.join(ROOT_DIR, 'ifrc_ns_data', 'common', 'national_societies_info.yml'),
                    encoding='utf-8'
            ))

    @property
    def ns_list(self):
        """
        Get the list of NS names.
        """
        ns_list = [ns['National Society name'] for ns in NationalSocietiesInfo.data]
        return ns_list

    @property
    def country_list(self):
        """
        Get the list of NS names.
        """
        country_list = [ns['Country'] for ns in NationalSocietiesInfo.data]
        return country_list

    @property
    def iso3_list(self):
        """
        Get the list of NS names.
        """
        iso3_list = [ns['ISO3'] for ns in NationalSocietiesInfo.data]
        return iso3_list

    @property
    def df(self):
        """
        Get a pandas DataFrame of the NS data.
        """
        return pd.DataFrame(self.data)
