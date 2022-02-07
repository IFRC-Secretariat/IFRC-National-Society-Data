"""
Module to handle operations data from the IFRC GO platform..
The module can be used to pull this data from the IFRC GO API, process, and clean the data.
"""
import requests
import pandas as pd
from src.common import Dataset
from src.common.cleaners import DatabankNSIDMapper, NSNamesCleaner, DictColumnExpander


class OperationsDataset(Dataset):
    """
    Pull IFRC operations information from the IFRC GO platform API, and clean and process the data.

    Parameters
    ----------
    filepath : string (required)
        Path to save the dataset when loaded, and to read the dataset from.
    """
    def __init__(self, filepath, reload=True, indicators=None):
        super().__init__(filepath=filepath, reload=reload, indicators=indicators)
        self.reload = reload


    def reload_data(self):
        """
        Read in data from the IFRC GO API and save to file.
        """
        # Pull data from FDRS API and save the data locally
        data = []
        next_url = f'https://goadmin.ifrc.org/api/v2/appeal/?limit=100&offset=0'
        while next_url:
            response = requests.get(url=next_url)
            response.raise_for_status()
            data += response.json()['results']
            next_url = response.json()['next']
        data = pd.DataFrame(data)

        # Save the data
        data.to_csv(self.filepath, index=False)


    def process(self):
        """
        Transform and process the data, including changing the structure and selecting columns.
        Process the data into a NS indicator format.
        """
        # Expand dict-type columns
        expand_columns = ['dtype', 'region', 'country']
        self.data = DictColumnExpander().clean(data=self.data,
                                               columns=expand_columns,
                                               drop=True)

        # Drop columns that aren't needed
        self.data = self.data.drop(columns=['aid', 'sector', 'dtype.id', 'dtype.summary', 'atype', 'status', 'code', 'real_data_update', 'created_at', 'modified_at', 'event', 'needs_confirmation', 'country.iso', 'country.id', 'country.record_type', 'country.record_type_display', 'country.region', 'country.independent', 'country.is_deprecated', 'country.fdrs', 'region.name', 'region.id', 'region.region_name', 'region.label', 'id', 'country.name', 'country.iso3'])\
                              .rename(columns={'country.society_name': 'National Society name',
                                               'atype_display': 'Type',
                                               'status_display': 'Status',
                                               'dtype.name': 'Disaster type',
                                               'name': 'Operation name',
                                               'amount_requested': 'Requested amount',
                                               'amount_funded': 'Funded amount'})\
                              .dropna(subset=['National Society name'])

        # Check the names of NSs, and select only active operations
        self.data = self.data.loc[self.data['National Society name']!='']
        self.data['National Society name'] = NSNamesCleaner().clean(self.data['National Society name'])
        self.data = self.data.loc[self.data['Status']=='Active']
        self.data = self.data.drop(columns=['Status'])
        self.data['Funding'] = 100*(self.data['Funded amount']/self.data['Requested amount']).round(0)

        # Convert the date type columns to pandas datetimes
        for column in ['start_date', 'end_date']:
            self.data[column].replace({'0001-01-01T00:00:00Z': float('nan')}, inplace=True)
            self.data[column] = pd.to_datetime(self.data[column], format='%Y-%m-%dT%H:%M:%SZ')
