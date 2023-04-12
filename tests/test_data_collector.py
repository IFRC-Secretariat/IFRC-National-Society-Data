import os
import unittest
import pandas as pd
import yaml
import numpy as np
import ifrc_ns_data
from ifrc_ns_data.definitions import DATASETS_CONFIG_PATH, ROOT_DIR


class TestAllData(unittest.TestCase):
    def setUp(self):
        self.index_columns = ['National Society name', 'Country', 'ISO3', 'Region']
        self.indicator_dataset_columns = self.index_columns+['Indicator', 'Value', 'Year', 'Dataset']
        self.fdrs_api_key = os.environ.get('FDRS_PUBLIC_API_KEY')
        self.data_collector = ifrc_ns_data.DataCollector()
        test_datasets_path = os.path.join(ROOT_DIR, 'tests', 'data')
        self.dataset_args = {'FDRS': {'api_key': self.fdrs_api_key},
                             'NS Contacts': {'api_key': self.fdrs_api_key},
                             'NS Documents': {'api_key': self.fdrs_api_key},
                             'Statutes': {'filepath': os.path.join(test_datasets_path, 'statutes.csv')},
                             'Recognition laws': {'filepath': os.path.join(test_datasets_path, 'recognition_laws.csv')},
                             'Logistics projects': {'filepath': os.path.join(test_datasets_path, 'logistics_projects.csv')},
                             'OCAC': {'filepath': os.path.join(test_datasets_path, 'ocac.csv')},
                             'OCAC assessment dates': {'filepath': os.path.join(test_datasets_path, 'ocac.csv')},
                             'YABC': {'filepath': os.path.join(test_datasets_path, 'yabc.csv')}}
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

    def test_get_single_iso3_data(self):
        """
        Test getting data for only one country, filtering by ISO3.
        """
        # Get country data for all datasets
        all_datasets = self.data_collector.get_data(dataset_args=self.dataset_args, 
                                                    iso3='AFG')
        for dataset in all_datasets:
            self.assertTrue(isinstance(dataset.data, pd.DataFrame))
            self.assertFalse(dataset.data.empty)
            self.assertEqual(dataset.data['ISO3'].unique(), ['AFG'])

    def test_get_single_country_data(self):
        """
        Test getting data for only one country, filtering by country name.
        """
        # Get country data for all datasets
        all_datasets = self.data_collector.get_data(dataset_args=self.dataset_args, 
                                                    country='Afghanistan')
        for dataset in all_datasets:
            self.assertTrue(isinstance(dataset.data, pd.DataFrame))
            self.assertFalse(dataset.data.empty)
            self.assertEqual(dataset.data['Country'].unique(), ['Afghanistan'])

    def test_get_single_ns_data(self):
        """
        Test getting data for only one country, filtering by National Society name.
        """
        # Get country data for all datasets
        all_datasets = self.data_collector.get_data(dataset_args=self.dataset_args, 
                                                    ns='Afghan Red Crescent')
        for dataset in all_datasets:
            self.assertTrue(isinstance(dataset.data, pd.DataFrame))
            self.assertFalse(dataset.data.empty)
            self.assertEqual(dataset.data['National Society name'].unique(), ['Afghan Red Crescent'])

    def test_get_multi_iso3_data(self):
        """
        Test getting data for multiple countries, filtering by ISO3.
        """
        # Get country data for all datasets
        all_datasets = self.data_collector.get_data(dataset_args=self.dataset_args, 
                                                    iso3=['AFG', 'ZWE'])
        for dataset in all_datasets:
            self.assertTrue(isinstance(dataset.data, pd.DataFrame))
            self.assertFalse(dataset.data.empty)
            self.assertEqual(sorted(dataset.data['ISO3'].unique()), ['AFG', 'ZWE'])

    def test_get_multi_country_data(self):
        """
        Test getting data for multiple countries, filtering by country name.
        """
        # Get country data for all datasets
        all_datasets = self.data_collector.get_data(dataset_args=self.dataset_args, 
                                                    country=['Afghanistan', 'Zimbabwe'])
        for dataset in all_datasets:
            self.assertTrue(isinstance(dataset.data, pd.DataFrame))
            self.assertFalse(dataset.data.empty)
            self.assertEqual(sorted(dataset.data['Country'].unique()), ['Afghanistan', 'Zimbabwe'])

    def test_get_multi_ns_data(self):
        """
        Test getting data for multiple countries, filtering by National Society name.
        """
        # Get country data for all datasets
        all_datasets = self.data_collector.get_data(dataset_args=self.dataset_args, 
                                                    ns=['Afghan Red Crescent', 'Zimbabwe Red Cross Society'])
        for dataset in all_datasets:
            self.assertTrue(isinstance(dataset.data, pd.DataFrame))
            self.assertFalse(dataset.data.empty)
            self.assertEqual(sorted(dataset.data['National Society name'].unique()), ['Afghan Red Crescent', 'Zimbabwe Red Cross Society'])


class TestIndicatorData(unittest.TestCase):
    def setUp(self):
        self.index_columns = ['National Society name', 'Country', 'ISO3', 'Region']
        self.indicator_dataset_columns = self.index_columns+['Indicator', 'Value', 'Year', 'Dataset']
        self.fdrs_api_key = os.environ.get('FDRS_PUBLIC_API_KEY')
        self.data_collector = ifrc_ns_data.DataCollector()
        test_datasets_path = os.path.join(ROOT_DIR, 'tests', 'data')
        self.dataset_args = {'FDRS': {'api_key': self.fdrs_api_key},
                             'NS Contacts': {'api_key': self.fdrs_api_key},
                             'NS Documents': {'api_key': self.fdrs_api_key},
                             'Statutes': {'filepath': os.path.join(test_datasets_path, 'statutes.csv')},
                             'Recognition laws': {'filepath': os.path.join(test_datasets_path, 'recognition_laws.csv')},
                             'Logistics projects': {'filepath': os.path.join(test_datasets_path, 'logistics_projects.csv')},
                             'OCAC': {'filepath': os.path.join(test_datasets_path, 'ocac.csv')},
                             'OCAC assessment dates': {'filepath': os.path.join(test_datasets_path, 'ocac.csv')},
                             'YABC': {'filepath': os.path.join(test_datasets_path, 'yabc.csv')}}
        self.datasets_info = yaml.safe_load(open(DATASETS_CONFIG_PATH))

    def test_get_indicator_data(self):
        """
        Test getting all the indicator data.
        """
        # Get the indicator data and check the return is a non-empty pandas DataFrame
        indicator_dataset = self.data_collector.get_indicators_data(dataset_args=self.dataset_args)
        self.assertTrue(isinstance(indicator_dataset, pd.DataFrame))
        self.assertFalse(indicator_dataset.empty)
        self.assertEqual(indicator_dataset.columns.tolist(), self.indicator_dataset_columns)

    def test_get_public_indicator_data(self):
        """
        Test getting all the indicator data.
        """
        # Get the indicator data filtered by privacy public
        indicator_dataset = self.data_collector.get_indicators_data(dataset_args=self.dataset_args, filters={'privacy': 'public'})
        self.assertTrue(isinstance(indicator_dataset, pd.DataFrame))
        self.assertFalse(indicator_dataset.empty)

        # Check all datasets are public
        for dataset_name in indicator_dataset['Dataset'].unique():
            self.assertEqual(self.datasets_info[dataset_name]['privacy'], 'public')

    def test_get_latest_indicator_data(self):
        """
        Test getting the latest indicator data, only returning the latest result for each NS/ indicator.
        """
        # Get the indicator data and check the return is a non-empty pandas DataFrame
        indicator_dataset = self.data_collector.get_indicators_data(dataset_args=self.dataset_args,
                                                                          latest=True)
        self.assertTrue(isinstance(indicator_dataset, pd.DataFrame))
        self.assertFalse(indicator_dataset.empty)

        # Check that there is only one result for each NS/ indicator/ dataset
        counts = indicator_dataset.groupby(['National Society name', 'Indicator', 'Dataset']).size()
        self.assertEqual(counts.unique(), [1])

    def test_get_quantitative_indicator_data(self):
        """
        Test getting the merged indicator data, but filtered by quantitative only.
        """
        # Get the indicator data and check the return value is numeric
        indicator_dataset = self.data_collector.get_indicators_data(dataset_args=self.dataset_args,
                                                                          quantitative=True)
        self.assertTrue(isinstance(indicator_dataset, pd.DataFrame))
        self.assertFalse(indicator_dataset.empty)
        self.assertEqual(indicator_dataset['Value'].dtype, np.float64)

        # Get the indicator data and check the return value is numeric
        indicator_dataset = self.data_collector.get_indicators_data(dataset_args=self.dataset_args,
                                                                          quantitative=False)
        self.assertTrue(isinstance(indicator_dataset, pd.DataFrame))
        self.assertFalse(indicator_dataset.empty)
        self.assertEqual(indicator_dataset['Value'].dtype, 'object')