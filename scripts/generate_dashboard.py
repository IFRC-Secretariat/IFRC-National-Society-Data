"""
Script to generate an Excel document (dashboard) containing data on National Societies.
"""
import os, sys
import yaml
import pandas as pd
from collections import Counter

from nsd_data_dashboard.fdrs import FDRSDataset
from nsd_data_dashboard.ocac_boca import OCACDataset
from nsd_data_dashboard.ns_contacts import NSContactsDataset
from nsd_data_dashboard.go import OperationsDataset, ProjectsDataset
from nsd_data_dashboard.world_bank import WorldDevelopmentIndicatorsDataset
from nsd_data_dashboard.undp import HumanDevelopmentDataset

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

# Read in the indicators list, convert to a pandas DataFrame
dashboard_indicators = yaml.safe_load(open(os.path.join(ROOT_DIR, 'scripts/dashboard_indicators.yml')))
dashboard_indicators_list = []
for category, indicators in dashboard_indicators.items():
    for indicator in indicators:
        indicator['category'] = category
        dashboard_indicators_list.append(indicator)
df_indicators = pd.DataFrame(dashboard_indicators_list)


"""
PULL AND PROCESS DATA
- Loop through datasets and optionally pull the data from source (e.g. from the API)
- Run processing and cleaning
- Return the dataset in a consistent format with NS names as the index
"""
# Restructure the dashboard_indicators to be per dataset
dataset_indicators = {}
for dataset in df_indicators['dataset'].unique():
    dataset_indicators[dataset] = df_indicators.loc[df_indicators['dataset']==dataset].set_index('source_name')['name'].to_dict()

# Load, clean, and process the datasets
datasets = {
    'FDRS': FDRSDataset(
        filepath=os.path.join(ROOT_DIR, 'data/fdrs/fdrs_api_response.csv'),
        api_key=FDRS_PUBLIC_API_KEY,
        reload=False,
        indicators=dataset_indicators['FDRS']
        ),
    'NS Contacts': NSContactsDataset(
        filepath=os.path.join(ROOT_DIR, 'data/ns_contacts/ns_contacts_api_response.csv'),
        api_key=FDRS_PUBLIC_API_KEY,
        reload=False,
        indicators=dataset_indicators['NS Contacts']
        ),
    'OCAC': OCACDataset(
        filepath=os.path.join(ROOT_DIR, 'data/ocac/ocac_website_download.xlsx'),
        indicators=dataset_indicators['OCAC']
        ),
    'GO Operations': OperationsDataset(
        filepath=os.path.join(ROOT_DIR, 'data/go/go_operations_api_response.csv'),
        reload=False,
        indicators=dataset_indicators['GO Operations']
        ),
    'GO Projects': ProjectsDataset(
        filepath=os.path.join(ROOT_DIR, 'data/go/go_projects_api_response.csv'),
        reload=False,
        indicators=dataset_indicators['GO Projects']
        ),
    'World Development Indicators': WorldDevelopmentIndicatorsDataset(
        filepath=os.path.join(ROOT_DIR, 'data/world_bank/world_development_indicators_api_response.csv'),
        reload=False,
        indicators=dataset_indicators['World Development Indicators']
        ),
    'UNDP Human Development': HumanDevelopmentDataset(
        filepath=os.path.join(ROOT_DIR, 'data/undp/undp_human_development_api_response.csv'),
        reload=False,
        indicators=dataset_indicators['UNDP Human Development']
        )
    }
for dataset_name, dataset in datasets.items():
    dataset.load_data()
    dataset.process()
    dataset.select_indicators()


"""
EXCEL DASHBOARD GENERATION
- Merge datasets and categorise
"""
# Define the writer for the Excel document
writer = pd.ExcelWriter(path=os.path.join(ROOT_DIR, 'data/NSD Data Dashboard.xlsx'),
                        engine='xlsxwriter')

# Loop through categories and merge the required data and indicators for each
for category in df_indicators['category'].unique():
    df_category_indicators = df_indicators.loc[df_indicators['category']==category]

    # Get the required indicators from each dataset and merge the datasets together
    category_datasets = []
    for dataset in df_category_indicators['dataset'].unique():
        indicators = df_category_indicators.loc[df_category_indicators['dataset']==dataset, 'name'].unique()
        category_datasets.append(datasets[dataset].data[indicators])
    df_catetory = pd.concat(category_datasets, axis='columns')

    # Remove index names, order columns, and write to the Excel document
    df_catetory = pd.concat(category_datasets, axis='columns')
    df_catetory.index.name = None
    df_catetory.columns.names = (None, None)
    df_catetory = df_catetory[df_category_indicators['name'].to_list()]
    df_catetory.to_excel(writer, sheet_name=category)

writer.save()
