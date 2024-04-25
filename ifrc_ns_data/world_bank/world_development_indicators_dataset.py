"""
Module to handle World Bank data, including pulling it from the World Bank API, cleaning, and processing.
"""
import requests
import pandas as pd
from ifrc_ns_data.common import Dataset, NationalSocietiesInfo
from ifrc_ns_data.common.cleaners import DictColumnExpander, NSInfoMapper


class WorldDevelopmentIndicatorsDataset(Dataset):
    """
    Pull World Development Indicators data from the World Bank API, and clean and process the data.

    Parameters
    ----------
    filepath : string (required)
        Path to save the dataset when pulled, and to read the dataset from.
    """
    def __init__(self):
        super().__init__(name='World Development Indicators')

    def pull_data(self, filters=None):
        """
        Pull data from the World Bank API.
        """
        # Get the list of NSs to filter by
        if filters:
            selected_ns = NationalSocietiesInfo().data
            for filter_name, filter_values in filters.items():
                selected_ns = [ns for ns in selected_ns if ns[filter_name] in filter_values]
            selected_countries = ';'.join([ns['ISO3'] for ns in selected_ns if ns['National Society ID'] is not None])
        else:
            selected_countries = 'all'

        # Pull data from the API
        data = pd.DataFrame()
        page = 1
        per_page = 1000
        # When testing pull only 5 pages because otherwise it takes a long time
        total_pages = None
        while True:
            api_indicators = ';'.join([
                'SP.POP.TOTL', 'NY.GDP.MKTP.CD', 'SI.POV.NAHC',
                'NY.GNP.PCAP.CD', 'SP.DYN.LE00.IN', 'SE.ADT.LITR.ZS',
                'SP.URB.TOTL.IN.ZS'
            ])
            url = 'https://api.worldbank.org/v2/country/'\
                f'{selected_countries}/indicator/{api_indicators}?'\
                f'source=2&page={page}&format=json&per_page={per_page}'
            response = requests.get(url=url)
            response.raise_for_status()
            data = pd.concat([data, pd.DataFrame(response.json()[1])])
            if total_pages is None:
                total_pages = response.json()[0]['pages']
            print(f'out of {total_pages}')
            if page == total_pages:
                break
            page += 1

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
        # Expand dict-type columns
        data = DictColumnExpander().clean(data=data, columns=['indicator', 'country'], drop=True)

        # Map ISO3 codes to NS names and add extra columns
        data['National Society name'] = NSInfoMapper().map_iso_to_ns(data=data['countryiso3code'])
        extra_columns = [column for column in self.index_columns if column != 'National Society name']
        ns_info_mapper = NSInfoMapper()
        for column in extra_columns:
            data[column] = ns_info_mapper.map(
                data=data['National Society name'],
                map_from='National Society name',
                map_to=column
            )

        # The data contains regional and world-level information, drop this
        data = data\
            .dropna(subset=['National Society name', 'indicator.value', 'value', 'date'], how='any')\
            .rename(columns={'date': 'Year', 'indicator.id': 'Indicator', 'value': 'Value'}, errors='raise')\
            .drop(
                columns=[
                    'countryiso3code', 'country.id', 'country.value',
                    'unit', 'obs_status',
                    'decimal', 'scale', 'indicator.value'
                ],
                errors='ignore'
            )

        # Get the latest values of each indicator for each NS
        if latest:
            data = self.filter_latest_indicators(data)

        # Rename indicators
        rename_indicators = {
            'SP.POP.TOTL': 'Population, total',
            'NY.GDP.MKTP.CD': 'GDP (US dollars)',
            'SI.POV.NAHC': 'Poverty headcount ratio at national poverty lines (% of population)',
            'NY.GNP.PCAP.CD': 'GNI per capita, Atlas method (current US$)',
            'SP.DYN.LE00.IN': 'Life expectancy at birth, total years',
            'SE.ADT.LITR.ZS': 'Literacy rate, adult total (% of people ages 15 and above)',
            'SP.URB.TOTL.IN.ZS': 'Urban population (% of total)'
        }
        data['Indicator'] = data['Indicator'].replace(rename_indicators, regex=False)
        data = data.loc[data['Indicator'].isin(rename_indicators.values())]

        # Select and order columns
        columns_order = self.index_columns.copy() + ['Indicator', 'Value', 'Year']
        data = data[columns_order + [col for col in data.columns if col not in columns_order]]

        return data
