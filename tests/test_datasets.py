import os
import unittest
import pandas as pd
import ifrc_ns_data
from definitions import ROOT_DIR


class TestDatasets(unittest.TestCase):
    def setUp(self):
        self.index_columns = ['National Society name', 'Country', 'ISO3', 'Region']
        self.indicator_dataset_columns = self.index_columns+['Indicator', 'Value', 'Year']
        self.fdrs_api_key = os.environ.get('FDRS_PUBLIC_API_KEY')
        self.test_datasets_path = os.path.join(ROOT_DIR, 'tests', 'data')

    def test_fdrs(self):
        fdrs_data = ifrc_ns_data.FDRSDataset(api_key=self.fdrs_api_key)
        data = fdrs_data.get_data()
        self.assertTrue(isinstance(data, pd.DataFrame))
        self.assertFalse(data.empty)
        self.assertEqual(data.columns.to_list(), self.indicator_dataset_columns)

    def test_ns_documents(self):
        ns_documents = ifrc_ns_data.NSDocumentsDataset(api_key=self.fdrs_api_key)
        data = ns_documents.get_data()
        self.assertTrue(isinstance(data, pd.DataFrame))
        self.assertFalse(data.empty)
        self.assertEqual(data.columns.to_list(), self.indicator_dataset_columns)

    def test_go_operations(self):
        operations_data = ifrc_ns_data.GOOperationsDataset()
        data = operations_data.get_data()
        self.assertTrue(isinstance(data, pd.DataFrame))
        self.assertFalse(data.empty)
        self.assertEqual(data.columns.to_list()[:4], self.index_columns)

    def test_go_projects(self):
        projects_data = ifrc_ns_data.GOProjectsDataset()
        data = projects_data.get_data()
        self.assertTrue(isinstance(data, pd.DataFrame))
        self.assertFalse(data.empty)
        self.assertEqual(data.columns.to_list()[:4], self.index_columns)

    def test_inform_risk(self):
        inform_risk_data = ifrc_ns_data.INFORMRiskDataset()
        data = inform_risk_data.get_data()
        self.assertTrue(isinstance(data, pd.DataFrame))
        self.assertFalse(data.empty)
        self.assertEqual(data.columns.to_list(), self.indicator_dataset_columns)

    def test_recognition_laws(self):
        recognition_laws_data = ifrc_ns_data.RecognitionLawsDataset(filepath=os.path.join(self.test_datasets_path, 'recognition_laws.csv'))
        data = recognition_laws_data.get_data()
        self.assertTrue(isinstance(data, pd.DataFrame))
        self.assertFalse(data.empty)
        self.assertEqual(data.columns.to_list()[:4], self.index_columns)

    def test_statutes(self):
        statutes_data = ifrc_ns_data.StatutesDataset(filepath=os.path.join(self.test_datasets_path, 'statutes.csv'))
        data = statutes_data.get_data()
        self.assertTrue(isinstance(data, pd.DataFrame))
        self.assertFalse(data.empty)
        self.assertEqual(data.columns.to_list()[:4], self.index_columns)

    def test_logistics_projects(self):
        logistics_projects_data = ifrc_ns_data.LogisticsProjectsDataset(filepath=os.path.join(self.test_datasets_path, 'logistics_projects.csv'))
        data = logistics_projects_data.get_data()
        self.assertTrue(isinstance(data, pd.DataFrame))
        self.assertFalse(data.empty)
        self.assertEqual(data.columns.to_list()[:4], self.index_columns)

    def test_yabc(self):
        yabc_data = ifrc_ns_data.YABCDataset(filepath=os.path.join(self.test_datasets_path, 'yabc.csv'))
        data = yabc_data.get_data()
        self.assertTrue(isinstance(data, pd.DataFrame))
        self.assertFalse(data.empty)
        self.assertEqual(data.columns.to_list()[:4], self.index_columns)

    def test_ns_contacts(self):
        ns_contacts_data = ifrc_ns_data.NSContactsDataset(api_key=self.fdrs_api_key)
        data = ns_contacts_data.get_data()
        self.assertTrue(isinstance(data, pd.DataFrame))
        self.assertFalse(data.empty)
        self.assertEqual(data.columns.to_list()[:4], self.index_columns)

    def test_ocac(self):
        ocac_data = ifrc_ns_data.OCACDataset(filepath=os.path.join(self.test_datasets_path, 'ocac.csv'))
        data = ocac_data.get_data()
        self.assertTrue(isinstance(data, pd.DataFrame))
        self.assertFalse(data.empty)
        self.assertEqual(data.columns.to_list()[:4], self.index_columns)

    def test_ocac_assessment_dates(self):
        ocac_assessment_dates = ifrc_ns_data.OCACAssessmentDatesDataset(filepath=os.path.join(self.test_datasets_path, 'ocac.csv'))
        data = ocac_assessment_dates.get_data()
        self.assertTrue(isinstance(data, pd.DataFrame))
        self.assertFalse(data.empty)
        self.assertEqual(data.columns.to_list(), self.indicator_dataset_columns)

    def test_world_bank(self):
        world_bank_data = ifrc_ns_data.WorldDevelopmentIndicatorsDataset()
        data = world_bank_data.get_data()
        self.assertTrue(isinstance(data, pd.DataFrame))
        self.assertFalse(data.empty)
        self.assertEqual(data.columns.to_list(), self.indicator_dataset_columns)
