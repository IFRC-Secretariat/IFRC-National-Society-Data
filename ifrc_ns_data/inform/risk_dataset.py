"""
Module to handle INFORM Risk data, including pulling it from the INFORM API, cleaning, and processing.
"""
import requests
from datetime import date
import pandas as pd
from ifrc_ns_data.common import Dataset
from ifrc_ns_data.common.cleaners import NSInfoMapper


class INFORMRiskDataset(Dataset):
    """
    Pull INFORM Risk data from the INFORM API, and clean and process the data.

    Parameters
    ----------
    filepath : string (required)
        Path to save the dataset when pulled, and to read the dataset from.
    """
    def __init__(self):
        super().__init__(name='INFORM Risk')

    def pull_data(self):
        """
        Pull data from the INFORM API and save to file.
        """
        # Get the workflow ID of the latest dataset
        year = date.today().year
        response = requests.get(
            f'https://drmkc.jrc.ec.europa.eu/Inform-Index/API/InformAPI/workflows/GetByWorkflowGroup/INFORM{year}'
        )
        if not response.json():
            year -= 1
            response = requests.get(
                f'https://drmkc.jrc.ec.europa.eu/Inform-Index/API/InformAPI/workflows/GetByWorkflowGroup/INFORM{year}'
            )
            if not response.json():
                raise RuntimeError(f'No INFORM Risk data available for {year+1} or {year}.')
        workflow_name = f'INFORM Risk {year}'
        latest_workflow = [workflow for workflow in response.json() if workflow['Name'] == workflow_name]
        if not latest_workflow:
            raise ValueError(f'Missing workflow "{workflow_name}" from INFORM Risk workflows list.')
        if len(latest_workflow) > 1:
            raise ValueError(f'Multiple workflows "{workflow_name}" in INFORM Risk workflows list.')
        workflow_id = latest_workflow[0]['WorkflowId']

        # Pull the data for each indicator and save in a pandas DataFrame
        response = requests.get(
            'https://drmkc.jrc.ec.europa.eu/Inform-Index/API/InformAPI/countries/Scores/?'
            f'WorkflowId={workflow_id}&'
            f'IndicatorId=INFORM'
        )
        response.raise_for_status()
        data = pd.DataFrame(response.json())
        data.rename(
            columns={'IndicatorId': 'Indicator', 'IndicatorScore': 'Value'},
            inplace=True
        )
        data['Year'] = year

        return data

    def process_data(self, data, latest=False):
        """
        Transform and process the data, including changing the structure and selecting columns.

        Parameters
        ----------
        data : pandas DataFrame (required)
            Raw data to be processed.

        latest : bool (default=False)
            If True, only the latest data for each National Society and indicator will be returned.
        """
        # Map ISO3 codes to NS names and add extra columns
        ns_info_mapper = NSInfoMapper()
        data['National Society name'] = ns_info_mapper.map_iso_to_ns(data['Iso3'])
        extra_columns = [column for column in self.index_columns if column != 'National Society name']
        for column in extra_columns:
            data[column] = ns_info_mapper.map(
                data=data['National Society name'],
                map_from='National Society name',
                map_to=column
            )

        # Set the indicator name and drop columns
        data = data.drop(columns=['Iso3', 'IndicatorName', 'nodelevel', 'ValidityYear', 'Unit', 'Note'])

        # Rename indicators
        rename_indicators = {'INFORM': 'INFORM Risk Index'}
        data['Indicator'] = data['Indicator'].replace(rename_indicators, regex=False)
        data = data.loc[data['Indicator'].isin(rename_indicators.values())]

        # Select and order columns
        columns_order = self.index_columns.copy() + ['Indicator', 'Value', 'Year']
        data = data[columns_order + [col for col in data.columns if col not in columns_order]]

        # Filter the dataset if required
        if latest:
            data = self.filter_latest_indicators(data).reset_index(drop=True)

        return data
