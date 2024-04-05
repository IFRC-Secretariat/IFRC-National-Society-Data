"""
Module to handle FDRS data, including loading it from the API, cleaning, and processing.
"""
import warnings
import requests
import pandas as pd
from ifrc_ns_data.common import Dataset
from ifrc_ns_data.common.cleaners import DatabankNSIDMapper, NSInfoMapper


class FDRSDataset(Dataset):
    """
    Load FDRS data from the API, and clean and process the data.

    Parameters
    ----------
    filepath : string (required)
        Path to save the dataset when loaded, and to read the dataset from.
    """
    def __init__(self, api_key):
        super().__init__(name='FDRS')
        self.api_key = api_key.strip()

    def pull_data(self, filters=None):
        """
        Read in raw data from the NS Databank API.

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
        response = requests.get(url=f'https://data-api.ifrc.org/api/Data?apiKey={self.api_key}')
        response.raise_for_status()

        # Unnest the response from the API into a tabular format
        data = pd.DataFrame(response.json()['data'])
        data = data.explode('data', ignore_index=True)
        data = pd.concat([data.drop(columns=['data']).rename(columns={'id': 'Indicator'}),
                          pd.json_normalize(data['data']).rename(columns={'id': 'National Society ID'})], axis=1)
        data = data.explode('data', ignore_index=True)
        data = pd.concat([data.drop(columns=['data']),
                          pd.json_normalize(data['data'])], axis=1)

        if data['years'].astype(str).nunique() != 1:
            raise ValueError('Unexpected values in years column', data['years'].astype(str).unique())
        data.drop(columns=['years'], inplace=True)

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
        # Rename columns and remove nans
        data = data.rename(columns={'value': 'Value', 'year': 'Year'})\
                   .dropna(subset=['Value', 'Year', 'Indicator'], how='any')

        # Add in the FDRS page URL
        data['URL'] = 'https://data.ifrc.org/FDRS/national-society/'+data['National Society ID']

        # Map in country and region information
        for column in self.index_columns:
            data[column] = NSInfoMapper().map(
                data['National Society ID'],
                map_from='National Society ID',
                map_to=column
            )
        data = data.drop(columns=['National Society ID'])

        # Convert NS supported and receiving support lists from NS IDs to NS names
        def split_convert_ns_ids(x):
            # Conver the string to a list and remove invalid IDs
            invalid_values = ['IFRC', 'DBE004']
            ns_ids = [
                item.strip()
                for item in x.replace(';', ',').split(',')
                if (item.strip() != '') and (item not in invalid_values)
            ]
            # Some IDs have been changed; replace these
            changed_ids = {'DCS001': 'DRS001'}
            ns_ids = [changed_ids[id] if id in changed_ids else id for id in ns_ids]
            # Convert NS IDs to NS names
            ns_names = DatabankNSIDMapper(api_key=self.api_key).map(ns_ids, clean_names=True)
            return ', '.join(ns_names)
        data['Value'] = data['Value'].replace(
            'One of our staff was sent for support to DRC-Congo on a surge',
            'Red Cross of the Democratic Republic of the Congo'
        )
        data['Value'] = data.apply(
            lambda row:
                split_convert_ns_ids(row['Value'])
                if ((row['Indicator'] in ['supported1', 'received_support1']) and (row['Value'] == row['Value']))
                else row['Value'],
            axis=1
        )

        # Replace True and False with Yes and No, for readability
        latest_columns_names = {
            'KPI_hasFinancialStatement': 'Year of latest financial statement',
            'audited': 'Year of latest audited financial statement',
            'ar': 'Year of latest annual report',
            'sp': 'Year of latest strategic plan'
        }
        data.loc[
            (data['Indicator'].isin(latest_columns_names.keys())) & (data['Value'].astype(str) == 'False'),
            'Value'
        ] = 'No'
        data.loc[
            (data['Indicator'].isin(latest_columns_names.keys())) & (data['Value'].astype(str) == 'True'),
            'Value'
        ] = 'Yes'

        # Add in year of latest financial statement, and year of latest audited financial statement
        latest_available = data.loc[(data['Indicator'].isin(latest_columns_names)) & (data['Value'] == 'Yes')]\
            .sort_values(by=['National Society name', 'Year'], ascending=False)\
            .drop_duplicates(subset=['National Society name', 'Indicator'], keep='first')
        latest_available['Indicator'] = latest_available['Indicator'].apply(
            lambda indicator: latest_columns_names.get(indicator)
        )
        latest_available['Value'] = latest_available['Year']
        data = pd.concat([data, latest_available]).reset_index(drop=True)

        # Select and rename indicators
        data = self.rename_indicators(data)
        data = self.order_index_columns(data, other_columns=['Indicator', 'Value', 'Year', 'URL'])

        # Filter the dataset if required
        if latest:
            data = self.filter_latest_indicators(data).reset_index(drop=True)

        return data
