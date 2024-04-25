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
        self.indicator_dataset_columns = self.index_columns +\
            ['Indicator', 'Value', 'Year', 'Description', 'URL', 'Dataset']
        self.fdrs_api_key = os.environ.get('FDRS_PUBLIC_API_KEY')
        self.data_collector = ifrc_ns_data.DataCollector()
        test_datasets_path = os.path.join(ROOT_DIR, 'tests', 'data')

        # Define dataset classes and arguments
        self.datasets = [
            {
                'name': 'Evaluations',
                'class': ifrc_ns_data.EvaluationsDataset,
            },
            {
                'name': 'FDRS',
                'class': ifrc_ns_data.FDRSDataset,
                'args': {'api_key': self.fdrs_api_key}
            },
            {
                'name': 'NS Documents',
                'class': ifrc_ns_data.NSDocumentsDataset,
                'args': {'api_key': self.fdrs_api_key}
            },
            {
                'name': 'NS Contacts',
                'class': ifrc_ns_data.NSContactsDataset,
                'args': {'api_key': self.fdrs_api_key}
            },
            {
                'name': 'OCAC',
                'class': ifrc_ns_data.OCACDataset,
                'args': {'filepath': os.path.join(test_datasets_path, 'ocac.csv')}
            },
            {
                'name': 'OCAC Assessment Dates',
                'class': ifrc_ns_data.OCACAssessmentDatesDataset,
                'args': {'api_key': self.fdrs_api_key}
            },
            {
                'name': 'BOCA Assessment Dates',
                'class': ifrc_ns_data.BOCAAssessmentDatesDataset,
                'args': {'api_key': self.fdrs_api_key}
            },
            {
                'name': 'GO Operations',
                'class': ifrc_ns_data.GOOperationsDataset,
            },
            {
                'name': 'GO Projects',
                'class': ifrc_ns_data.GOProjectsDataset,
            },
            {
                'name': 'INFORM Risk',
                'class': ifrc_ns_data.INFORMRiskDataset,
            },
            {
                'name': 'Recognition Laws',
                'class': ifrc_ns_data.RecognitionLawsDataset,
                'args': {'filepath': os.path.join(test_datasets_path, 'recognition_laws.csv')}
            },
            {
                'name': 'Statutes',
                'class': ifrc_ns_data.StatutesDataset,
                'args': {'filepath': os.path.join(test_datasets_path, 'statutes.csv')}
            },
            {
                'name': 'Logistics Projects',
                'class': ifrc_ns_data.LogisticsProjectsDataset,
                'args': {'filepath': os.path.join(test_datasets_path, 'logistics_projects.csv')}
            },
            {
                'name': 'World Development Indicators',
                'class': ifrc_ns_data.WorldDevelopmentIndicatorsDataset,
            },
            {
                'name': 'YABC',
                'class': ifrc_ns_data.YABCDataset,
                'args': {'filepath': os.path.join(test_datasets_path, 'yabc.csv')}
            },
            {
                'name': 'ICRC Presence',
                'class': ifrc_ns_data.ICRCPresenceDataset,
            },
            {
                'name': 'IFRC Disaster Law',
                'class': ifrc_ns_data.IFRCDisasterLawDataset,
            },
            {
                'name': 'Corruption Perception Index',
                'class': ifrc_ns_data.CorruptionPerceptionIndexDataset,
            },
            {
                'name': 'Youth Engagement',
                'class': ifrc_ns_data.YouthEngagementDataset,
            },
        ]

        # Add in data from data_cache
        for details in self.datasets:
            raw_data_path = os.path.join(ROOT_DIR, 'tests', 'data_cache', f'{details["name"]}.csv')
            details['raw_data'] = None
            if os.path.exists(raw_data_path):
                raw_data = pd.read_csv(raw_data_path)
                details['raw_data'] = raw_data

        # Get a dict of arguments and raw data
        self.dataset_args = {}
        self.raw_data = {}
        for details in self.datasets:
            dataset_name = details['name']
            if 'args' in details:
                self.dataset_args[dataset_name] = details['args'].copy()
            if 'raw_data' in details:
                self.raw_data[dataset_name] = details['raw_data'].copy()

        # Set dataset arguments
        self.datasets_info = yaml.safe_load(open(DATASETS_CONFIG_PATH))

    def test_individual_datasets(self):
        """
        Test individual classes for each dataset.
        """
        for details in self.datasets:
            # Setup the dataset class and get data
            dataset = details['class'](
                **({} if 'args' not in details else details['args'])
            )
            data = dataset.get_data(
                raw_data=details['raw_data']
            )
            # Tests
            self.assertTrue(isinstance(data, pd.DataFrame))
            self.assertFalse(data.empty)
            self.assertEqual(data.columns.to_list()[:4], self.index_columns)
            # Save the raw data (pulled but unprocessed)
            raw_data_path = os.path.join(ROOT_DIR, 'tests', 'data_cache', f'{details["name"]}.csv')
            dataset.raw_data.to_csv(raw_data_path, index=False)

    def test_get_data(self):
        """
        Test getting all the data with the data collector.
        """
        # Get all datasets
        all_datasets = self.data_collector.get_data(
            dataset_args=self.dataset_args,
            raw_data=self.raw_data
        )
        # Check total datasets and all not empty
        self.assertEqual(len(all_datasets), 19)
        for dataset in all_datasets:
            self.assertTrue(isinstance(dataset.data, pd.DataFrame))
            self.assertFalse(dataset.data.empty)

    def test_get_public_data(self):
        """
        Test getting all public datasets.
        """
        # Get all datasets and check all returned contain non-empty pandas DataFrames with privacy public
        all_datasets = self.data_collector.get_data(
            dataset_args=self.dataset_args,
            raw_data=self.raw_data,
            filters={'privacy': 'public'}
        )
        # Check total datasets and no private datasets
        self.assertEqual(len(all_datasets), 14)
        dataset_names = [dataset.name for dataset in all_datasets]
        private_datsets = ['OCAC', 'Statutes', 'Recognition Laws', 'YABC', 'Logistics Projects']
        for dataset_name in private_datsets:
            self.assertNotIn(dataset_name, dataset_names)

        # Check none empty
        for dataset in all_datasets:
            self.assertTrue(isinstance(dataset.data, pd.DataFrame))
            self.assertFalse(dataset.data.empty)
            self.assertEqual(dataset.privacy, 'public')

    def test_get_single_country_data(self):
        """
        Test getting data for only one country, filtering by country name.
        """
        # Get country data for all datasets
        all_datasets = self.data_collector.get_data(
            dataset_args=self.dataset_args,
            raw_data=self.raw_data,
            country='Afghanistan'
        )
        for dataset in all_datasets:
            self.assertTrue(isinstance(dataset.data, pd.DataFrame))
            if dataset.name not in ['BOCA Assessment Dates', 'Evaluations']:
                self.assertFalse(dataset.data.empty)
                self.assertEqual(dataset.data['Country'].unique(), ['Afghanistan'])

    def test_get_indicator_data(self):
        """
        Test getting all the indicator data.
        """
        # Get the indicator data and check the return is a non-empty pandas DataFrame
        indicator_dataset = self.data_collector.get_indicators_data(
            dataset_args=self.dataset_args,
            raw_data=self.raw_data
        )
        self.assertTrue(isinstance(indicator_dataset, pd.DataFrame))
        self.assertFalse(indicator_dataset.empty)
        self.assertEqual(indicator_dataset.columns.tolist(), self.indicator_dataset_columns)

    def test_get_public_indicator_data(self):
        """
        Test getting all the indicator data.
        """
        # Get the indicator data filtered by privacy public
        indicator_dataset = self.data_collector.get_indicators_data(
            dataset_args=self.dataset_args,
            raw_data=self.raw_data,
            filters={'privacy': 'public'}
        )
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
        indicator_dataset = self.data_collector.get_indicators_data(
            dataset_args=self.dataset_args,
            raw_data=self.raw_data,
            latest=True
        )
        self.assertTrue(isinstance(indicator_dataset, pd.DataFrame))
        self.assertFalse(indicator_dataset.empty)

        # Check that there is only one result for each NS/ indicator/ dataset
        counts = indicator_dataset.groupby(['National Society name', 'Indicator', 'Dataset']).size()
        self.assertEqual(counts.unique().tolist(), [1])

    def test_get_quantitative_indicator_data(self):
        """
        Test getting the merged indicator data, but filtered by quantitative only.
        """
        # Get the indicator data and check the return value is numeric
        indicator_dataset = self.data_collector.get_indicators_data(
            dataset_args=self.dataset_args,
            raw_data=self.raw_data,
            quantitative=True
        )
        self.assertTrue(isinstance(indicator_dataset, pd.DataFrame))
        self.assertFalse(indicator_dataset.empty)
        self.assertEqual(indicator_dataset['Value'].dtype, np.float64)

        # Get the indicator data and check the return value is numeric
        indicator_dataset = self.data_collector.get_indicators_data(
            dataset_args=self.dataset_args,
            raw_data=self.raw_data,
            quantitative=False
        )
        self.assertTrue(isinstance(indicator_dataset, pd.DataFrame))
        self.assertFalse(indicator_dataset.empty)
        self.assertEqual(indicator_dataset['Value'].dtype, 'object')
