import os
import unittest
import pandas as pd
import ifrc_ns_data


class TestDatasets(unittest.TestCase):
    def setUp(self):
        self.index_columns = ['National Society name', 'Country', 'ISO3', 'Region']
        self.indicator_dataset_columns = self.index_columns+['Indicator', 'Value', 'Year', 'Dataset']
        self.fdrs_api_key = os.environ.get('FDRS_PUBLIC_API_KEY')

    def test_fdrs(self):
        fdrs_data = ifrc_ns_data.fdrs.FDRSDataset(api_key=self.fdrs_api_key)
        data = fdrs_data.get_data()
        self.assertTrue(isinstance(data, pd.DataFrame))
        self.assertFalse(data.empty)
        self.assertEqual(data.columns.to_list(), self.indicator_dataset_columns)

    def test_ns_documents(self):
        ns_documents = ifrc_ns_data.fdrs.NSDocumentsDataset(api_key=self.fdrs_api_key)
        data = ns_documents.get_data()
        self.assertTrue(isinstance(data, pd.DataFrame))
        self.assertFalse(data.empty)
        self.assertEqual(data.columns.to_list(), self.indicator_dataset_columns)

    def test_go_operations(self):
        operations_data = ifrc_ns_data.go.OperationsDataset()
        data = operations_data.get_data()
        self.assertTrue(isinstance(data, pd.DataFrame))
        self.assertFalse(data.empty)
        self.assertEqual(data.columns.to_list()[:4], self.index_columns)

    def test_go_projects(self):
        projects_data = ifrc_ns_data.go.ProjectsDataset()
        data = projects_data.get_data()
        self.assertTrue(isinstance(data, pd.DataFrame))
        self.assertFalse(data.empty)
        self.assertEqual(data.columns.to_list()[:4], self.index_columns)

    def test_inform_risk(self):
        inform_risk_data = ifrc_ns_data.inform.INFORMRiskDataset()
        data = inform_risk_data.get_data()
        self.assertTrue(isinstance(data, pd.DataFrame))
        self.assertFalse(data.empty)
        self.assertEqual(data.columns.to_list(), self.indicator_dataset_columns)

    def test_ns_contacts(self):
        ns_contacts_data = ifrc_ns_data.ns_contacts.NSContactsDataset(api_key=self.fdrs_api_key)
        data = ns_contacts_data.get_data()
        self.assertTrue(isinstance(data, pd.DataFrame))
        self.assertFalse(data.empty)
        self.assertEqual(data.columns.to_list()[:4], self.index_columns)

    def test_world_bank(self):
        world_bank_data = ifrc_ns_data.world_bank.WorldDevelopmentIndicatorsDataset()
        data = world_bank_data.get_data()
        self.assertTrue(isinstance(data, pd.DataFrame))
        self.assertFalse(data.empty)
        self.assertEqual(data.columns.to_list(), self.indicator_dataset_columns)
