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

# Read in the indicators list, convert to a pandas DataFrame
"""
dashboard_indicators = config['dashboard_indicators']
dashboard_indicators_list = []
for category, indicators in dashboard_indicators.items():
    for indicator in indicators:
        indicator['category'] = category
        dashboard_indicators_list.append(indicator)
df_indicators = pd.DataFrame(dashboard_indicators_list)
"""

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
]
"""
indicator_datasets = [
    FDRSDataset(
        filepath=os.path.join(ROOT_DIR, 'data/fdrs/fdrs_api_response.csv'),
        api_key=FDRS_PUBLIC_API_KEY,
        reload=reload,
        ),
]"""
# Create a list of the datasets including the name, meta information, and the loaded and processed data
for dataset in indicator_datasets:
    dataset.load_data()
    dataset.process()
    dataset.add_indicator_info()

# Get data on documents
documents = NSDocumentsDataset(
    filepath=os.path.join(ROOT_DIR, 'data/fdrs/ns_documents_api_response.csv'),
    api_key=FDRS_PUBLIC_API_KEY,
    reload=reload,
)
documents.load_data()
documents.process()
documents.add_indicator_info()


"""
EXCEL DASHBOARD GENERATION
- Merge datasets and categorise
"""
# Use the NSDDashboardBuilder class to generate the NSD Excel document
dashboard_generator = NSDDashboardBuilder(save_folder=os.path.join(ROOT_DIR, 'data/'), # Path to a folder where the outputs will be saved
                                          file_name='NSD Data Dashboard')
dashboard_generator.generate_dashboard(indicator_datasets=indicator_datasets,
                                       categories=config['data_categories'],
                                       documents=documents,
                                       protect_sheets=True,
                                       protect_workbook=True,
                                       excel_password=os.environ.get('EXCEL_PASSWORD'))

exit()









# Define the writer for the Excel document
writer = pd.ExcelWriter(path=os.path.join(ROOT_DIR, 'data/NSD Data Dashboard.xlsx'), engine='xlsxwriter')
all_category_datasets = {}

# Loop through categories and merge the required data and indicators for each
for category in df_indicators['category'].unique():
    df_category_indicators = df_indicators.loc[df_indicators['category']==category]

    # Get the required indicators from each dataset and merge the datasets together
    category_datasets = []
    for dataset in df_category_indicators['dataset'].unique():
        indicators = df_category_indicators.loc[df_category_indicators['dataset']==dataset, 'name'].unique()
        if dataset in datasets:
            category_datasets.append(datasets[dataset].data[indicators])
    if not category_datasets: continue
    df_category = pd.concat(category_datasets, axis='columns')

    # Remove index names, order columns, and write to the Excel document
    df_category = pd.concat(category_datasets, axis='columns')
    df_category.index.name = None
    df_category.columns.names = ('National Society', None)
    df_category = df_category[df_category_indicators['name'].to_list()].sort_index()

    # Filter by NS or country
    if category not in ['Logistics projects']:
        if 'national_societies' in config:
            missing_ns = [ns for ns in config['national_societies'] if ns not in df_category.index]
            if missing_ns:
                warnings.warn(f'The following National Societies missing from the {category} dataset: {missing_ns}')
            df_category = df_category.reindex(ns_general_info.ns_names)
            df_category = df_category[df_category.index.isin(config['national_societies'])].sort_index()
    else:
        if 'countries' in config:
            missing_countries = [country for country in config['countries'] if country not in df_category.index]
            if missing_countries:
                warnings.warn(f'The following countries are missing from the {category} dataset: {missing_countries}')
            df_category = df_category[df_category.index.isin(config['countries'])]

    # Merge in the general country info and write to Excel
    df_category = pd.merge(df_ns_general_info, df_category, left_index=True, right_index=True)
    df_category.to_excel(writer, sheet_name=category)

    # Merge to all data
    all_category_datasets[category] = df_category

writer.save()

exit()








"""
Create a new Excel document with a sheet per NS
"""
# Loop through all datasets
all_data = pd.DataFrame()
for category in df_indicators['category'].unique():
    if category not in all_category_datasets: continue
    if category in ['Active operations', '3W - Ongoing projects', 'Logistics projects']: continue
    df_category = all_category_datasets[category]

    # Melt the dataset into a log format
    df_melt = df_category.unstack()\
                         .unstack(level=1)\
                         .reset_index()\
                         .rename(columns={'National Society': 'Indicator', 'level_1': 'National Society'})
    if '' in df_melt.columns:
        if 'value' in df_melt.columns:
            df_melt.loc[df_melt['value'].isnull(), 'value'] = df_melt['']
        else:
            df_melt['value'] = df_melt['']
        df_melt.drop(columns=[''], inplace=True)
    df_melt['Category'] = category

    # Order the indicators by the config file
    sort_order = [indicator['name']+' - '+category for category in config['dashboard_indicators'] for indicator in config['dashboard_indicators'][category]]
    order_map = {indicator: sort_order.index(indicator) for indicator in sort_order}
    df_melt['sort'] = (df_melt['Indicator']+' - '+df_melt['Category']).map(order_map)
    df_melt = df_melt.sort_values(by='sort').drop(columns='sort')

    all_data = pd.concat([all_data, df_melt], axis='rows')

# Loop through NSs and write the Excel document
rename_ns = {'Red Cross Society of the Republic of Moldova': 'Red Cross Rep. of Moldova'}
writer = pd.ExcelWriter(path=os.path.join(ROOT_DIR, 'data/NSD Data Dashboard - NS.xlsx'), engine='xlsxwriter')
for national_society in all_data['National Society'].unique():
    ns_data = all_data.loc[all_data['National Society']==national_society][['National Society', 'Category', 'Indicator', 'value', 'year', 'source']]
    sheet_name = national_society if national_society not in rename_ns else rename_ns[national_society]
    ns_data.to_excel(writer, sheet_name=sheet_name, index=False)

writer.save()
