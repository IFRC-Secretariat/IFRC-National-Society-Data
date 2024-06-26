"""
Module to handle YABC data, including loading it from the data file, cleaning, and processing.
"""
from ifrc_ns_data.common import Dataset
from ifrc_ns_data.common.cleaners import NSInfoCleaner, NSInfoMapper


class YABCDataset(Dataset):
    """
    Load YABC data from the file, and clean and process the data.
    The filepath should be the location of the data.

    Parameters
    ----------
    filepath : string (required)
        Path to save the dataset when loaded, and to read the dataset from.
    """
    def __init__(self, filepath=None, sheet_name=None):
        if filepath is None:
            raise TypeError('Please specify a path to the IFRC Youth YABC dataset.')
        super().__init__(name='YABC', filepath=filepath, sheet_name=sheet_name)

    def process_data(self, data):
        """
        Transform and process the data, including changing the structure and selecting columns.

        Parameters
        ----------
        data : pandas DataFrame (required)
            Raw data to be processed.
        """
        # Set the columns
        data.columns = data.iloc[1]
        data = data.iloc[2:, 1:]
        data = data.dropna(how='all', axis=0).dropna(how='all', axis=1)

        # Clean up the column names
        clean_columns = {column: column.strip() for column in data.columns}
        data = data.rename(columns=clean_columns, errors='raise')
        data = data.loc[data['Country'] != 'TOTAL']

        # Check that the NS names are consistent with the centralised names list
        data.loc[:, 'Country'] = NSInfoCleaner().clean_country_names(data.loc[:, 'Country'].str.strip())
        extra_columns = [column for column in self.index_columns if column != 'Country']
        ns_info_mapper = NSInfoMapper()
        for column in extra_columns:
            data[column] = ns_info_mapper.map(data=data['Country'], map_from='Country', map_to=column)

        # Rename and order the columns
        rename_columns = {
            'number of YABC trainings to date': 'Number of YABC trainings to date',
            'peer educators': 'Number of peer educators',
            'trainers': 'Number of trainers',
            'Comment': 'Comment'
        }
        data = data.rename(columns=rename_columns, errors='raise')
        data = data[self.index_columns.copy() + list(rename_columns.values())]

        return data
