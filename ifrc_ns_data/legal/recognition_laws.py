"""
Module to handle NS Recognition Laws data, including loading it from the data file, cleaning, and processing.
"""
import pandas as pd
from ifrc_ns_data.common import Dataset
from ifrc_ns_data.common.cleaners import NSInfoCleaner, NSInfoMapper


class RecognitionLawsDataset(Dataset):
    """
    Load NS Recognition Laws data from the file, and clean and process the data.
    The filepath should be the location of the NS Recognition Laws data.

    Parameters
    ----------
    filepath : string (required)
        Path to save the dataset when loaded, and to read the dataset from.
    """
    def __init__(self, filepath=None, sheet_name=None):
        if filepath is None:
            raise TypeError('Please specify a path to the National Society recognition laws dataset.')
        super().__init__(name='Recognition Laws', filepath=filepath, sheet_name=sheet_name)

    def process_data(self, data):
        """
        Transform and process the data, including changing the structure and selecting columns.

        Parameters
        ----------
        data : pandas DataFrame (required)
            Raw data to be processed.

        latest : bool (default=None)
            Not in use.
        """
        # Set the columns from the data row
        data.columns = data.iloc[0]
        data = data.iloc[1:]
        data = data.dropna(how='all')

        # Clean up the column names
        data.rename(columns={column: column.strip() for column in data.columns}, inplace=True)
        data.rename(columns={'National Society (NS)': 'Country'}, inplace=True, errors='raise')

        # Check that the NS names are consistent with the centralised names list
        data['Country'] = NSInfoCleaner().clean_country_names(data['Country'].str.strip())
        extra_columns = [column for column in self.index_columns if column != 'Country']
        ns_info_mapper = NSInfoMapper()
        for column in extra_columns:
            ns_id_mapped = ns_info_mapper.map(data=data['Country'], map_from='Country', map_to=column)\
                                         .rename(column)
            data = pd.concat([data.reset_index(drop=True), ns_id_mapped.reset_index(drop=True)], axis=1)

        # Rename and order columns
        data = self.rename_columns(data)
        data = self.order_index_columns(data)

        return data
