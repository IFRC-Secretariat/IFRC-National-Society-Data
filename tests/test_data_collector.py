import os
import unittest
import pandas as pd
import yaml
import ifrc_ns_data
from definitions import DATASETS_CONFIG_PATH


class TestDataCollector(unittest.TestCase):
    def setUp(self):
        self.index_columns = ['National Society name', 'Country', 'ISO3', 'Region']
        self.indicator_dataset_columns = self.index_columns+['Indicator', 'Value', 'Year', 'Dataset']
        self.fdrs_api_key = os.environ.get('FDRS_PUBLIC_API_KEY')
        self.data_collector = ifrc_ns_data.DataCollector()
        self.dataset_args = {'FDRS': {'api_key': self.fdrs_api_key},
                             'NS Contacts': {'api_key': self.fdrs_api_key},
                             'NS Documents': {'api_key': self.fdrs_api_key}}
        self.datasets_info = yaml.safe_load(open(DATASETS_CONFIG_PATH))


    def test_get_data(self):
        """
        Test getting all the data with the data collector.
        """
        # Get all datasets
        all_datasets = self.data_collector.get_data(dataset_args=self.dataset_args)
        for dataset in all_datasets:
            self.assertTrue(isinstance(dataset.data, pd.DataFrame))
            self.assertFalse(dataset.data.empty)


    def test_get_public_data(self):
        """
        Test getting all public datasets.
        """
        # Get all datasets and check all returned contain non-empty pandas DataFrames with privacy public
        all_datasets = self.data_collector.get_data(dataset_args=self.dataset_args, filters={'privacy': 'public'})
        for dataset in all_datasets:
            self.assertTrue(isinstance(dataset.data, pd.DataFrame))
            self.assertFalse(dataset.data.empty)
            self.assertEqual(dataset.privacy, 'public')


    def test_get_indicator_data(self):
        """
        Test getting all the indicator data.
        """
        # Get the indicator data and check the return is a non-empty pandas DataFrame
        indicator_dataset = self.data_collector.get_merged_indicator_data(dataset_args=self.dataset_args)
        self.assertTrue(isinstance(indicator_dataset, pd.DataFrame))
        self.assertFalse(indicator_dataset.empty)


    def test_get_public_indicator_data(self):
        """
        Test getting all the indicator data.
        """
        # Get the indicator data filtered by privacy public
        indicator_dataset = self.data_collector.get_merged_indicator_data(dataset_args=self.dataset_args, filters={'privacy': 'public'})
        self.assertTrue(isinstance(indicator_dataset, pd.DataFrame))
        self.assertFalse(indicator_dataset.empty)

        # Check all datasets are public
        for dataset_name in indicator_dataset['Dataset'].unique():
            self.assertEqual(self.datasets_info[dataset_name]['privacy'], 'public')
