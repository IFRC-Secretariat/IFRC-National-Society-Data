"""
Module to handle OCAC data, including loading it from the downloaded data file, cleaning, and processing.
"""
import requests
import warnings
import pandas as pd
from ifrc_ns_data.common import Dataset
from ifrc_ns_data.common.cleaners import NSInfoCleaner, NSInfoMapper


class OCACDataset(Dataset):
    """
    Load OCAC data from the downloaded file, and clean and process the data.
    The filepath should be the location of the downloaded OCAC data.

    Parameters
    ----------
    filepath : string (required)
        Path to save the dataset when loaded, and to read the dataset from.
    """
    def __init__(self, filepath=None, sheet_name=None):
        if filepath is None:
            raise TypeError(
                'Please specify a path to the OCAC dataset. The data '
                'can be downloaded as an Excel file from the OCAC site at '
                'https://data-api.ifrc.org/Backoffice/OCAC/Form?app=ocac '
                '(there is not yet an API).')
        super().__init__(name='OCAC', filepath=filepath, sheet_name=sheet_name)

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
        # Process the data into a log format, with a row for each assessment
        data = data.rename(columns={'Name': 'Indicator'})
        data.loc[data['Indicator'].isnull(), 'Indicator'] = data['Code']
        data['Indicator'] = data['Indicator'].str.strip()
        data = data\
            .drop(columns=['Code'])\
            .set_index(['Indicator'])\
            .dropna(how='all')\
            .transpose()\
            .drop(
                columns=[
                    'iso', 'Region', 'SubRegion', 'Month', 'Version',
                    'Principal facilitator', 'Second facilitator',
                    'NS Focal point', 'OCAC data public', 'OCAC report public'
                ],
                errors='ignore'
            )\
            .reset_index(drop=True)\
            .rename(columns={'National Society': 'National Society name'})

        # Check that the NS names are consistent with the centralised names list, and add extra information
        data['National Society name'] = NSInfoCleaner().clean_ns_names(data['National Society name'])
        extra_columns = [column for column in self.index_columns if column != 'National Society name']
        ns_info_mapper = NSInfoMapper()
        for column in extra_columns:
            data[column] = ns_info_mapper.map(
                data=data['National Society name'],
                map_from='National Society name',
                map_to=column
            )

        # Convert data types
        data['Year'] = pd.to_numeric(data['Year'], errors='raise')

        # Keep only the latest assessment for each NS
        if latest:
            data = data.sort_values(by=['National Society name', 'Year'], ascending=[True, False])\
                                 .drop_duplicates(subset=['National Society name'], keep='first')

        # Order columns
        data = self.order_index_columns(data)

        return data


class OCACAssessmentDatesDataset(Dataset):
    """
    Load OCAC data from the downloaded file, and clean and process the data.
    The filepath should be the location of the downloaded OCAC data.

    Parameters
    ----------
    filepath : string (required)
        Path to save the dataset when loaded, and to read the dataset from.
    """
    def __init__(self, api_key):
        super().__init__(name='OCAC Assessment Dates')
        self.api_key = api_key.strip()

    def pull_data(self, filters=None):
        """
        Read in raw data from the OCAC Assessments Dates API from the NS databank.

        Parameters
        ----------
        filters : dict (default=None)
            Filters to filter by country or by National Society.
            Keys can only be "Country", "National Society name", or "ISO3". Values are lists.
            Note that this is NOT IMPLEMENTED and is only included in this method to ensure
            consistency with the parent class and other child classes.
        """
        # The data cannot be filtered from the API so raise a warning if filters are provided
        if (filters is not None) and (filters != {}):
            warnings.warn(f'Filters {filters} not applied because the API response cannot be filtered.')

        # Pull data from FDRS API
        response = requests.get(url=f'https://data-api.ifrc.org/api/ocacpublic?apiKey={self.api_key}')
        response.raise_for_status()
        results = response.json()

        # Convert the data into a pandas DataFrame
        data = pd.DataFrame(results)

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
        # Use the NS code to add other NS information
        ns_info_mapper = NSInfoMapper()
        for column in self.index_columns:
            ns_id_mapped = ns_info_mapper.map(
                data=data['NsId'],
                map_from='National Society ID',
                map_to=column,
                errors='raise'
            ).rename(column)
            data = pd.concat([data.reset_index(drop=True), ns_id_mapped.reset_index(drop=True)], axis=1)
        data = data.drop(columns=['NsId', 'NsName'])

        # Add other columns and order the columns
        data = self.rename_columns(data, drop_others=True)
        data = self.order_index_columns(data)

        return data
