"""
Module to handle projects (3W) data from the IFRC GO platform..
The module can be used to pull this data from the IFRC GO API, process, and clean the data.
"""
import requests
import pandas as pd
from src.common import Dataset
from src.common.cleaners import DatabankNSIDMapper, NSNamesCleaner, DictColumnExpander


class ProjectsDataset(Dataset):
    """
    Pull IFRC projects (3W) information from the IFRC GO platform API, and clean and process the data.

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
        next_url = f'https://goadmin.ifrc.org/api/v2/project/?limit=100&offset=0'
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
        expand_columns = ['project_country_detail', 'dtype_detail', 'event_detail', 'reporting_ns_detail']
        self.data = DictColumnExpander().clean(data=self.data,
                                               columns=expand_columns,
                                               drop=True)

        # Convert the date type columns to pandas datetimes
        for column in ['start_date', 'end_date']:
            self.data[column] = pd.to_datetime(self.data[column], format='%Y-%m-%d')

        # Keep only data with a NS specified, and clean NS names
        self.data = self.data.rename(columns={'project_country_detail.society_name': 'National Society name'})\
                             .dropna(subset=['National Society name'])
        self.data['National Society name'] = NSNamesCleaner().clean(self.data['National Society name'])

        # Check all data is public, and select only ongoing projects
        if self.data['visibility'].unique() != ['public']:
            raise ValueError('Dataset contains non-public data.')
        self.data = self.data.loc[self.data['status_display']=='Ongoing']

        # Concatenate the columns to list multiple emergencies in each cell
        self.data = self.data.sort_values(by='modified_at', ascending=False)\
                              .drop_duplicates(subset=['National Society name', 'name'], keep='first')\
                              .groupby('National Society name').agg(lambda x: '\n'.join([str(item) for item in x]))
