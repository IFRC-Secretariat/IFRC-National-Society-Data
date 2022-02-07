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

        # Drop columns that aren't needed
        self.data = self.data.drop(columns=['id', 'project_country_detail.iso',
                                            'project_country_detail.iso3', 'project_country_detail.id', 'project_country_detail.record_type', 'project_country_detail.record_type_display', 'project_country_detail.region', 'project_country_detail.independent', 'project_country_detail.is_deprecated', 'project_country_detail.fdrs', 'project_country_detail.name',
                                            'project_districts_detail', 'programme_type',
                                            'reporting_ns_detail.iso', 'reporting_ns_detail.iso3', 'reporting_ns_detail.id', 'reporting_ns_detail.record_type', 'reporting_ns_detail.record_type_display', 'reporting_ns_detail.region', 'reporting_ns_detail.independent', 'reporting_ns_detail.is_deprecated', 'reporting_ns_detail.fdrs', 'reporting_ns_detail.name', 'reporting_ns_detail.society_name',
                                            'dtype_detail.id', 'dtype_detail.summary',
                                            'regional_project_detail',
                                            'event_detail.dtype', 'event_detail.id', 'event_detail.parent_event', 'event_detail.slug',
                                            'target_male', 'target_female', 'target_other', 'reached_male', 'reached_female', 'reached_other',
                                            'user', 'reporting_ns', 'project_country', 'event', 'dtype', 'regional_project', 'project_districts', 'modified_by'])\
                             .rename(columns={'dtype_detail.name': 'Disaster type',
                                              'project_country_detail.society_name': 'National Society name',
                                              'event_detail.name': 'Event description'})\
                             .dropna(subset=['National Society name'])

        # Check all data is public, and select only ongoing projects
        if self.data['visibility'].unique() != ['public']:
            raise ValueError('Dataset contains non-public data.')
        self.data = self.data.loc[self.data['status_display']=='Ongoing']
        self.data.drop(columns=['visibility_display', 'visibility', 'status_display', 'status'], inplace=True)

        # Clean NS names
        self.data['National Society name'] = NSNamesCleaner().clean(self.data['National Society name'])

        # Convert the date type columns to pandas datetimes
        for column in ['start_date', 'end_date']:
            self.data[column] = pd.to_datetime(self.data[column], format='%Y-%m-%d')
