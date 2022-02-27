"""
Module to handle INFORM Risk data, including pulling it from the INFORM API, cleaning, and processing.
"""
import requests
import os
import yaml
from datetime import date
import pandas as pd
from nsd_data_dashboard.common import Dataset
from nsd_data_dashboard.common.cleaners import DictColumnExpander, NSInfoMapper


class INFORMRiskDataset(Dataset):
    """
    Pull INFORM Risk data from the INFORM API, and clean and process the data.

    Parameters
    ----------
    filepath : string (required)
        Path to save the dataset when pulled, and to read the dataset from.
    """
    def __init__(self, filepath, reload=True):
        self.name = 'INFORM Risk'
        super().__init__(filepath=filepath, reload=reload)
        self.reload = reload


    def reload_data(self):
        """
        Pull data from the INFORM API and save to file.
        """
        # Get the workflow ID of the latest dataset
        year = date.today().year
        response = requests.get(f'https://drmkc.jrc.ec.europa.eu/Inform-Index/API/InformAPI/workflows/GetByWorkflowGroup/INFORM{year}')
        if not response.json():
            year -= 1
            response = requests.get(f'https://drmkc.jrc.ec.europa.eu/Inform-Index/API/InformAPI/workflows/GetByWorkflowGroup/INFORM{year}')
            if not response.json():
                raise RuntimeError(f'No INFORM Risk data available for {year+1} or {year}.')
        workflow_name = f'INFORM Risk {year}'
        latest_workflow = [workflow for workflow in response.json() if workflow['Name']==workflow_name]
        if not latest_workflow:
            raise ValueError(f'Missing workflow "{workflow_name}" from INFORM Risk workflows list.')
        if len(latest_workflow)>1:
            raise ValueError(f'Multiple workflows "{workflow_name}" in INFORM Risk workflows list.')
        workflow_id = latest_workflow[0]['WorkflowId']

        # Pull the data for each indicator and save in a pandas DataFrame
        data = pd.DataFrame()
        for indicator in self.indicators.keys():
            response = requests.get(f'https://drmkc.jrc.ec.europa.eu/Inform-Index/API/InformAPI/countries/Scores/?WorkflowId={workflow_id}&IndicatorId={indicator}')
            response.raise_for_status()

            df_indicator = pd.DataFrame(response.json())
            df_indicator.rename(columns={'IndicatorId': 'indicator',
                                         'IndicatorScore': 'value'}, inplace=True)
            df_indicator['year'] = year
            data = pd.concat([data, df_indicator])

        # Save the data
        data.to_csv(self.filepath, index=False)


    def process(self):
        """
        Transform and process the data, including changing the structure and selecting columns.
        """
        # Map ISO3 codes to NS names and add extra columns
        self.data['National Society name'] = NSInfoMapper().map_iso_to_ns(self.data['Iso3'])
        extra_columns = [column for column in self.index_columns if column!='National Society name']
        ns_info_mapper = NSInfoMapper()
        for column in extra_columns:
            self.data[column] = ns_info_mapper.map(data=self.data['National Society name'], on='National Society name', column=column)

        # Get the latest values of each indicator for each NS
        self.data = self.data.dropna(subset=['National Society name', 'indicator', 'value', 'year'], how='any')\
                             .pivot(index=self.index_columns, columns='indicator', values=['value', 'year'])\
                             .swaplevel(axis='columns')\
                             .sort_index(axis='columns', level=0, sort_remaining=False)
