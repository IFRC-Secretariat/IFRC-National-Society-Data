import os
import unittest
import pandas as pd
import yaml
import ifrc_ns_data
from ifrc_ns_data.definitions import DATASETS_CONFIG_PATH, ROOT_DIR
"""
Test the examples in the README.
"""


class TestExamples(unittest.TestCase):
    def setUp(self):
        self.fdrs_api_key = os.environ.get('FDRS_PUBLIC_API_KEY')


    def test_get_individual_dataset(self):
        """
        Test getting the FDRS dataset individually.
        """
        # Initiate the class to access FDRS data
        fdrs_dataset = ifrc_ns_data.FDRSDataset(api_key=self.fdrs_api_key)
        print(fdrs_dataset.name)

        # Call the get_data method to pull the data from the API, process the data, and return the processed data
        fdrs_dataset.get_data()
        print(fdrs_dataset.data)

        # Get only the latest data
        fdrs_dataset.get_data(latest=True)
        print(fdrs_dataset.data)


    def test_get_multiple_datasets(self):
        """
        Test getting multiple datasets at once using the DataCollector.
        """
        data_collector = ifrc_ns_data.DataCollector()

        # Get all available datasets
        # This will only return datasets not requiring arguments -
        # datasets requiring arguments will be skipped and a warning will be printed.
        all_datasets = data_collector.get_data()

        # Get only the FDRS and OCAC datasets, supplying the required arguments
        fdrs_ocac_datasets = data_collector.get_data(datasets=['FDRS', 'OCAC'],
                                                     dataset_args={'FDRS': {'api_key': self.fdrs_api_key},
                                                                   'OCAC': {'filepath': os.path.join(ROOT_DIR, 'tests', 'data', 'ocac.csv')}})

        # Get all external public datasets not requiring arguments
        public_datasets = data_collector.get_data(filters={'privacy': 'public', 'type': 'external'})

        # Get all data but only including the latest data for each National Society and indicator
        latest_data = data_collector.get_data(latest=True)

        # Loop through the datasets and print the name, the data, and the columns
        for dataset in latest_data:
            print(dataset.name) # Print the dataset name
            print(dataset.data) # Print the data as a pandas DataFrame
            print(dataset.data.columns) # Print the columns of the pandas DataFrame


    def test_get_indicator_data(self):
         """
         Test getting all the indicator data at once using the DataCollector.
         """
         data_collector = ifrc_ns_data.DataCollector()

         # Get all indicator-style datasets as a single pandas DataFrame
         df = data_collector.get_indicators_data()
         print(df.columns) # Print the columns
