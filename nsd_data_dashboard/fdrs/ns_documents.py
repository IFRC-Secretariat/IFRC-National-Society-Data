"""
Module to handle NS documents data from FDRS, including loading it from the API, cleaning, and processing.
"""
import requests
import os
import yaml
import pandas as pd
from nsd_data_dashboard.common import Dataset
from nsd_data_dashboard.common.cleaners import DatabankNSIDMap, NSInfoMapper


class NSDocumentsDataset(Dataset):
    """
    Load NS documents data from the NS databank API, and clean and process the data.

    Parameters
    ----------
    filepath : string (required)
        Path to save the dataset when loaded, and to read the dataset from.
    """
    def __init__(self, filepath, api_key, reload=True):
        self.name = 'NS Documents'
        super().__init__(filepath=filepath, reload=reload)
        self.api_key = api_key
        self.reload = reload


    def reload_data(self):
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

        # Save the data
        data.to_csv(self.filepath, index=False)


    def process(self):
        """
        Transform and process the data, including changing the structure and selecting columns.
        """
        # Add extra NS and country information based on the NS ID
        self.data = self.data[['National Society ID', 'name', 'document_type', 'year', 'url']]
        ns_info_mapper = NSInfoMapper()
        for column in self.index_columns:
            self.data[column] = ns_info_mapper.map(data=self.data['National Society ID'], on='National Society ID', column=column, errors='raise')

        # Keep only the latest document for each document type and NS
        self.data = self.data.dropna(subset=['National Society name', 'document_type', 'year'], how='any')\
                             .sort_values(by=['year', 'name'], ascending=[False, True])\
                             .drop_duplicates(subset=['National Society name', 'document_type'], keep='first')\
                             .sort_values(by=['National Society name', 'document_type'], ascending=True)\
                             .rename(columns={'url': 'link', 'document_type': 'indicator'})
        self.data['value'] = self.data['indicator'].str.strip().str.replace('^Our', '', regex=True).str.strip()+' - '+self.data['year'].astype(str)
        self.data = self.data.loc[self.data['indicator']!='Other']

        # Pivot the dataframe to have NSs as rows and indicators as columns
        self.data = self.data.pivot(index=self.index_columns,
                                    columns='indicator',
                                    values=['value', 'year', 'link'])\
                             .swaplevel(axis='columns')
