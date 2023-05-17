"""
Module to handle data on Logistics Projects, including loading it from the data file, cleaning, and processing.
"""
import warnings
from ifrc_ns_data.common import Dataset
from ifrc_ns_data.common.cleaners import NSInfoCleaner, NSInfoMapper


class LogisticsProjectsDataset(Dataset):
    """
    Load Logistics Projects data from the file, and clean and process the data.
    The filepath should be the location of the data.

    Parameters
    ----------
    filepath : string (required)
        Path to save the dataset when loaded, and to read the dataset from.
    """
    def __init__(self, filepath=None, sheet_name=None):
        if filepath is None:
            raise TypeError('Please specify a path to the IFRC logistics projects dataset.')
        super().__init__(name='Logistics Projects', filepath=filepath, sheet_name=sheet_name)
        pass


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
        if latest:
            warnings.warn(f'Filtering latest data does not apply to dataset {self.name}')

        # Clean the data
        data = data.drop(columns=['Region']).dropna(how='all')

        # Clean the country column and map on extra information
        data['Country'] = NSInfoCleaner().clean_country_names(data['Country'])
        extra_columns = [column for column in self.index_columns if column!='Country']
        ns_info_mapper = NSInfoMapper()
        for column in extra_columns:
            data[column] = ns_info_mapper.map(data=data['Country'], map_from='Country', map_to=column)

        # Order the NS index columns
        data = self.order_index_columns(data)

        return data
