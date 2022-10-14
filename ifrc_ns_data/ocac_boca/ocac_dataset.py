"""
Module to handle OCAC data, including loading it from the downloaded data file, cleaning, and processing.
"""
import pandas as pd
import numpy as np
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
        data = data.drop(columns=['Code'])\
                             .set_index(['Indicator'])\
                             .dropna(how='all')\
                             .transpose()\
                             .drop(columns=['iso', 'Region', 'SubRegion', 'Month', 'Version', 'Principal facilitator', 'Second facilitator', 'NS Focal point', 'OCAC data public', 'OCAC report public'], errors='ignore')\
                             .reset_index(drop=True)\
                             .rename(columns={'National Society': 'National Society name'})

        # Check that the NS names are consistent with the centralised names list, and add extra information
        data['National Society name'] = NSInfoCleaner().clean_ns_names(data['National Society name'])
        extra_columns = [column for column in self.index_columns if column!='National Society name']
        ns_info_mapper = NSInfoMapper()
        for column in extra_columns:
            data[column] = ns_info_mapper.map(data=data['National Society name'], map_from='National Society name', map_to=column)

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
    def __init__(self, filepath=None, sheet_name=None):
        if filepath is None:
            raise TypeError('Please specify a path to the OCAC dataset. The data'
                'can be downloaded as an Excel file from the OCAC site at '
                'https://data-api.ifrc.org/Backoffice/OCAC/Form?app=ocac '
                '(there is not yet an API).')
        self.name = 'OCAC Assessment Dates'
        super().__init__(name='OCAC Assessment Dates', filepath=filepath, sheet_name=sheet_name)


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
        # Use the OCACDataset class to process the data
        ocac_dataset = OCACDataset(filepath=self.filepath, sheet_name=self.sheet_name)
        ocac_data = ocac_dataset.process_data(data=data, latest=latest)

        # Select only dates of OCAC assessment and NOT results
        data = ocac_data[self.index_columns+['Year']].copy()

        # Double check that column names are only these, and that the Year data type is numeric
        expected_columns = ['National Society name', 'Country', 'ISO3', 'Region', 'Year']
        if data.columns.to_list()!=expected_columns:
            unexpected_columns = [column for column in data.columns if column not in expected_columns]
            raise ValueError(f'Unexpected columns {unexpected_columns}')
        if data['Year'].dtype != np.int64:
            raise TypeError(f'Expected int64 type in Year column')

        # Change to indicator format so that Value is the year of assessment
        data['Indicator'] = 'OCAC assessment date'
        data = data.rename(columns={'Year': 'Value'})
        data['Year'] = None
        data = self.order_index_columns(data, other_columns=['Indicator', 'Value', 'Year'])

        return data
