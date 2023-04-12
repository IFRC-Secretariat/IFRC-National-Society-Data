"""
Module to handle INFORM Risk data, including pulling it from the INFORM API, cleaning, and processing.
"""
import warnings
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


    def pull_data(self, filters=None):
        """
        Pull data from the INFORM API and save to file.

        Parameters
        ----------
        filters : dict (default=None)
            Filters to filter by country or by National Society.
            Keys can only be "Country", "National Society name", or "ISO3". Values are lists.
            Note that this is NOT IMPLEMENTED and is only included in this method to ensure consistency with the parent class and other child classes.
        """
        # The data cannot be filtered from the API so raise a warning if filters are provided
        if filters is not None:
            warnings.warn(f'Filters {filters} not applied because the API response cannot be filtered.')

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
        for indicator in self.indicators:
            response = requests.get(f'https://drmkc.jrc.ec.europa.eu/Inform-Index/API/InformAPI/countries/Scores/?WorkflowId={workflow_id}&IndicatorId={indicator["source_name"]}')
            response.raise_for_status()

            df_indicator = pd.DataFrame(response.json())
            df_indicator.rename(columns={'IndicatorId': 'Indicator',
                                         'IndicatorScore': 'Value'}, inplace=True)
            df_indicator['Year'] = year
            data = pd.concat([data, df_indicator])

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
        extra_columns = [column for column in self.index_columns if column!='National Society name']
        for column in extra_columns:
            data[column] = ns_info_mapper.map(data=data['National Society name'], map_from='National Society name', map_to=column)

        # Set the indicator name and drop columns
        data = data.drop(columns=['Iso3', 'IndicatorName', 'nodelevel', 'ValidityYear', 'Unit', 'Note'])

        # Select and rename indicators
        data = self.rename_indicators(data)
        data = self.order_index_columns(data, other_columns=['Indicator', 'Value', 'Year'])

        # Filter the dataset if required
        if latest:
            data = self.filter_latest_indicators(data).reset_index(drop=True)

        return data
