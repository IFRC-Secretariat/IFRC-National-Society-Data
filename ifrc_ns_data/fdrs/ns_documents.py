"""
Module to handle NS documents data from FDRS, including loading it from the API, cleaning, and processing.
"""
import requests
import os
import yaml
import pandas as pd
from ifrc_ns_data.common import Dataset
from ifrc_ns_data.common.cleaners import DatabankNSIDMap, NSInfoMapper


class NSDocumentsDataset(Dataset):
    """
    Load NS documents data from the NS databank API, and clean and process the data.

    Parameters
    ----------
    filepath : string (required)
        Path to save the dataset when loaded, and to read the dataset from.
    """
    def __init__(self, api_key):
        self.name = 'NS Documents'
        super().__init__()
        self.api_key = api_key


    def pull_data(self):
        """
        Read in data from the NS Databank API and save to file.
        """
        # Pull data from FDRS API, looping through NSs
        ns_ids_names_map = DatabankNSIDMap(api_key=self.api_key).get_map()
        ns_ids_string = ','.join(ns_ids_names_map.keys())
        response = requests.get(url=f'https://data-api.ifrc.org/api/documents?ns={ns_ids_string}&apiKey={self.api_key}')
        response.raise_for_status()
        data_list = []
        for ns_response in response.json():
            ns_documents = pd.DataFrame(ns_response['documents'])
            ns_documents['National Society ID'] = ns_response['code']
            data_list.append(ns_documents)
        data = pd.concat(data_list, axis='rows')

        return data


    def process_data(self, data):
        """
        Transform and process the data, including changing the structure and selecting columns.

        Parameters
        ----------
        data : pandas DataFrame (required)
            Raw data to be processed.
        """
        # Add extra NS and country information based on the NS ID
        data = data[['National Society ID', 'name', 'document_type', 'year', 'url']]
        ns_info_mapper = NSInfoMapper()
        for column in self.index_columns:
            data[column] = ns_info_mapper.map(data=data['National Society ID'], on='National Society ID', column=column, errors='raise')

        # Keep only the latest document for each document type and NS
        data = data.dropna(subset=['National Society name', 'document_type', 'year'], how='any')\
                             .sort_values(by=['National Society name', 'document_type'], ascending=True)\
                             .rename(columns={'url': 'Value', 'document_type': 'Indicator', 'year': 'Year'})
        data['Indicator'] = data['Indicator'].str.strip()

        # Drop columns which are not needed
        data = data.drop(columns=['name'])

        # Select and rename indicators
        data = self.rename_indicators(data)

        return data
