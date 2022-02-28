"""
Script to generate an Excel document (dashboard) containing data on National Societies.
"""
import os, sys
import yaml
import pandas as pd
from collections import Counter
import warnings

sys.path.append('.')
from nsd_data_dashboard.common import NationalSocietiesInfo
from nsd_data_dashboard.fdrs import FDRSDataset, NSDocumentsDataset
from nsd_data_dashboard.ocac_boca import OCACDataset
from nsd_data_dashboard.ns_statutes_laws import NSStatutesDataset, NSRecognitionLawsDataset
from nsd_data_dashboard.youth import YABCDataset
from nsd_data_dashboard.logistics import LogisticsProjectsDataset
from nsd_data_dashboard.go import OperationsDataset, ProjectsDataset
from nsd_data_dashboard.world_bank import WorldDevelopmentIndicatorsDataset
from nsd_data_dashboard.undp import HumanDevelopmentDataset
from nsd_data_dashboard.inform import INFORMRiskDataset
from nsd_data_dashboard.builder import NSDDashboardBuilder

"""
SETUP
- Pull in required environment variables
- Read in the indicators file which defines contains information about the indicators required for the different datasets
"""
# Define constants and set environment variables
ROOT_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
FDRS_PUBLIC_API_KEY = os.environ.get('FDRS_PUBLIC_API_KEY')
if FDRS_PUBLIC_API_KEY is None:
    raise ValueError('FDRS API key not set.')

# Read in the config file
config = yaml.safe_load(open(os.path.join(ROOT_DIR, 'scripts/config.yml')))
reload = config['reload_data'] if 'reload_data' in config else True

# Get the general NS information
ns_general_info = NationalSocietiesInfo()
df_ns_general_info = ns_general_info.df.set_index('National Society name')[config['national_society_indicators']]
df_ns_general_info.columns = pd.MultiIndex.from_product([df_ns_general_info.columns, ['']])


"""
PULL AND PROCESS DATA
- Loop through datasets and optionally pull the data from source (e.g. from the API)
- Run processing and cleaning
- Return the dataset in a consistent format with NS names as the index
"""
# Load, clean, and process the datasets
"""
indicator_datasets = [
    FDRSDataset(
        filepath=os.path.join(ROOT_DIR, 'data/fdrs/fdrs_api_response.csv'),
        api_key=FDRS_PUBLIC_API_KEY,
        reload=reload,
        ),
    OCACDataset(
        filepath=os.path.join(ROOT_DIR, 'data/ocac/ocac_website_download.xlsx'),
        ),
    NSStatutesDataset(
        filepath=os.path.join(ROOT_DIR, 'data/ns_statutes_laws/ns_statutes.xls'),
    ),
    NSDocumentsDataset(
        filepath=os.path.join(ROOT_DIR, 'data/fdrs/ns_documents_api_response.csv'),
        api_key=FDRS_PUBLIC_API_KEY,
        reload=reload,
    ),
    YABCDataset(
        filepath=os.path.join(ROOT_DIR, 'data/youth/yabc_data.xlsx'),
    ),
    NSRecognitionLawsDataset(
        filepath=os.path.join(ROOT_DIR, 'data/ns_statutes_laws/ns_recognition_laws.xls'),
    ),
    LogisticsProjectsDataset(
        filepath=os.path.join(ROOT_DIR, 'data/logistics/logistics_projects.xlsx'),
    ),
    OperationsDataset(
        filepath=os.path.join(ROOT_DIR, 'data/go/go_operations_api_response.csv'),
        reload=reload,
        ),
    ProjectsDataset(
        filepath=os.path.join(ROOT_DIR, 'data/go/go_projects_api_response.csv'),
        reload=reload,
        ),
    WorldDevelopmentIndicatorsDataset(
        filepath=os.path.join(ROOT_DIR, 'data/world_bank/world_development_indicators_api_response.csv'),
        reload=reload,
        ),
    HumanDevelopmentDataset(
        filepath=os.path.join(ROOT_DIR, 'data/undp/undp_human_development_api_response.csv'),
        reload=reload,
        ),
    INFORMRiskDataset(
        filepath=os.path.join(ROOT_DIR, 'data/inform/inform_risk_api_response.csv'),
        reload=reload,
    ),
]"""
indicator_datasets = [
    FDRSDataset(
        filepath=os.path.join(ROOT_DIR, 'data/fdrs/fdrs_api_response.csv'),
        api_key=FDRS_PUBLIC_API_KEY,
        reload=reload,
        ),
]
# Create a list of the datasets including the name, meta information, and the loaded and processed data
for dataset in indicator_datasets:
    dataset.load_data()
    dataset.process()
    dataset.add_indicator_info()


"""
EXCEL DASHBOARD GENERATION
- Merge datasets and categorise
"""
# Use the NSDDashboardBuilder class to generate the NSD Excel document
dashboard_generator = NSDDashboardBuilder(save_folder=os.path.join(ROOT_DIR, 'data/'), # Path to a folder where the outputs will be saved
                                          file_name='NSD Data Dashboard')
dashboard_generator.generate_dashboard(indicator_datasets=indicator_datasets,
                                       categories=config['data_categories'],
                                       ns_info=df_ns_general_info,
                                       #protect_sheets=True, # Currently this makes the cells not clickable
                                       protect_workbook=True,
                                       excel_password=os.environ.get('EXCEL_PASSWORD'))
