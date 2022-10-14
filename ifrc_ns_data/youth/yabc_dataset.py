"""
Module to handle YABC data, including loading it from the data file, cleaning, and processing.
"""
import warnings
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


    def process_data(self, data, latest=None):
        """
        Transform and process the data, including changing the structure and selecting columns.

        Parameters
        ----------
        data : pandas DataFrame (required)
            Raw data to be processed.

        latest : bool (default=None)
            Not in use.
        """
        # Print a warning if filtering is given as this does not apply
        if latest is not None:
            warnings.warn(f'Filtering latest data does not apply to dataset {self.name}')

        # Set the columns
        data.columns = data.iloc[1]
        data = data.iloc[2:, 1:]
        data = data.dropna(how='all', axis=0).dropna(how='all', axis=1)

        # Clean up the column names
        clean_columns = {column: column.strip() for column in data.columns}
        data = data.rename(columns=clean_columns, errors='raise')
        data = data.loc[data['Country']!='TOTAL']

        # Check that the NS names are consistent with the centralised names list
        data['Country'] = NSInfoCleaner().clean_country_names(data['Country'].str.strip())
        extra_columns = [column for column in self.index_columns if column!='Country']
        ns_info_mapper = NSInfoMapper()
        for column in extra_columns:
            data[column] = ns_info_mapper.map(data=data['Country'], map_from='Country', map_to=column)

        # Rename and select columns
        data = self.rename_columns(data, drop_others=True)
        data = self.order_index_columns(data)

        return data
