"""
Module to access and return multiple datasets at once.
"""
import warnings
import yaml
import datetime
import pandas as pd
import ifrc_ns_data
from ifrc_ns_data.common import NationalSocietiesInfo
from ifrc_ns_data.definitions import DATASETS_CONFIG_PATH


class DataCollector:
    """
    Class to handle multiple datasets.

    Parameters
    ----------
    """
    def __init__(self):
        self.datasets_info = yaml.safe_load(open(DATASETS_CONFIG_PATH, encoding='utf-8'))
        archived_datasets = ['UNDP Human Development'] # Archived because the API has stopped working
        self.dataset_names = [name for name in self.datasets_info if name not in archived_datasets]


    def get_data(self, datasets=None, dataset_args=None, iso3=None, country=None, ns=None, filters=None, latest=None):
        """
        Get all available datasets.

        Parameters
        ----------
        datasets : list (default=None)
            List of names of datasets to return. If None, all datasets are returned.

        dataset_args : dict (default=None)
            Arguments that need to be passed to each dataset class, e.g. API key or filepath. Note that if no arguments are required for a datset then this does not need to be provided.
            Should be a dict with keys as dataset names, and values a dict with arguments required for the dataset class.
            E.g. {'FDRS': {'api_key': 'xxxxxx'}, 'OCAC': {'filepath': 'ocac_data.xlsx', 'sheet_name': 'Sheet1'}}

        iso3 : string or list (default=None)
            String or list of country ISO3 codes to filter the dataset.

        country : string or list (default=None)
            String or list of country names to filter the dataset.

        ns : string or list (default=None)
            String or list of National Society names to filter the dataset.

        filters : dict (default=None)
            Filters to apply to the datasets list, e.g. {'format': 'indicators'} to get all indicator datasets, or {'privacy': 'public'} to get all public datasets.
            Note that if datasets is also provided, it will be filtered by this filter.

        latest : bool (default=None)
            If True, only the latest data will be returned for the dataset, and older data will not be included.
            For datasets where this is not valid the whole dataset is returned and a warning is printed.

        Returns
        -------
        dataset_instances : list of Dataset objects
            List of Dataset objects. The attribute 'data' is a pandas DataFrame containing the data. Other attributes contain information about the dataset such as source, focal_point, and privacy.
        """
        # Validate the provided dataset names, or set to all
        if datasets is not None:
            dataset_names = self.validate_dataset_names(datasets)
        else:
            dataset_names = self.dataset_names.copy()

        # Process the iso3/ country/ ns input parameters
        inputs = {'ISO3': iso3, 'Country': country, 'National Society name': ns}
        country_filters = {}
        for name, val in inputs.items():
            if val is None:
                continue
            elif isinstance(val, str):
                country_filters[name] = [val]
            elif isinstance(val, list):
                country_filters[name] = val
            else:
                raise TypeError(f'{val} is not a list or string')

        # Check all countries and NS names are valid
        if country_filters:
            ns_info = NationalSocietiesInfo()
            check_values = {'ISO3': ns_info.iso3_list,
                            'Country': ns_info.country_list,
                            'National Society name': ns_info.ns_list}
            for filter_name, val_list in country_filters.items():
                unrecognised_values = [item for item in val_list if item not in check_values[filter_name]]
                if unrecognised_values:
                    raise ValueError(f'Unrecognised values {unrecognised_values}.\n\nThe allowed values are: {check_values[filter_name]}')

        # Deal with filters
        if (filters is not None) and (filters != {}):

            # Check filters provided are valid
            valid_filters = []
            for dataset_name, dataset_info in self.datasets_info.items():
                valid_filters.extend(map(str.lower, dataset_info.keys()))
            valid_filters = sorted([item for item in list(set(valid_filters)) if item not in ['indicators', 'columns']])
            invalid_filters = [item.strip().lower() for item in filters if item not in valid_filters]
            if invalid_filters:
                raise ValueError(f'{invalid_filters} are not valid filters. Choose from {valid_filters}')

            # Filter the datasets list
            filtered_datasets = []
            for dataset_name in dataset_names:
                dataset_info = self.datasets_info[dataset_name]
                if any(filter not in dataset_info for filter in filters):
                    continue
                if all(dataset_info[filter]==filters[filter] for filter in filters):
                    filtered_datasets.append(dataset_name)

        else:
            filtered_datasets = dataset_names.copy()

        # Initiate the dataset classes list
        print(f'Getting datasets {filtered_datasets}...')
        dataset_instances = self.initiate_datasets(datasets=filtered_datasets,
                                                   dataset_args=dataset_args)

        # Load the data from the source and process
        names_params = {'ISO3': 'iso3', 
                        'Country': 'country', 
                        'National Society name': 'ns'}
        country_filters = {names_params[name]: country_filters[name] for name in country_filters}
        for dataset in dataset_instances:
            print(f'Getting {dataset.name} data...')
            dataset.get_data(latest=latest, **country_filters)

        return dataset_instances


    def get_indicators_data(self, datasets=None, dataset_args=None, filters=None, latest=None, quantitative=None):
        """
        Get a dataset in indicators format of data on National Societies.
        Indicator format contains columns about the National Society/ country, an indicator name, the indicator value, the year, and optionally a description and URL.
        This includes whole datasets which are already in "indicator" format (e.g. the FDRS dataset), or parts of other datasets, such as ICRC presence.

        Parameters
        ----------
        datasets : list (default=None)
            List of names of datasets to return. If None, all datasets are returned.

        dataset_args : dict (default=None)
            Arguments that need to be passed to each dataset class, e.g. API key or filepath. Note that if no arguments are required for a datset then this does not need to be provided.
            Should be a dict with keys as dataset names, and values a dict with arguments required for the dataset class.
            E.g. {'FDRS': {'api_key': 'xxxxxx'}, 'OCAC': {'filepath': 'ocac_data.xlsx', 'sheet_name': 'Sheet1'}}

        latest : bool (default=None)
            If True, only the latest data will be returned for the dataset, and older data will not be included.
            For datasets where this is not valid the whole dataset is returned and a warning is printed.

        quantitative : bool (default=None)
            If True, only return quantitative data (some datasets contain a mix of qualitative and quantitative indicators so this cannot be filtered at dataset-level).
        """
        # Get each dataset and turn into indicator-format
        indicator_datasets = ["NS Contacts", "FDRS", "NS Documents", "OCAC Assessment Dates", "World Development Indicators", "INFORM Risk", "ICRC Presence", "IFRC Disaster Law", "Corruption Perception Index"]
        indicator_datasets_lower = [dataset.lower() for dataset in indicator_datasets]
        column_names = ['National Society name', 'Country', 'ISO3', 'Region', 'Indicator', 'Value', 'Year', 'Description', 'URL']
        if datasets is None:
            datasets = indicator_datasets
        else:
            invalid_datasets = [dataset for dataset in datasets if dataset.lower() not in indicator_datasets_lower]
            if invalid_datasets:
                warnings.warn(f'Dropping datasets {invalid_datasets} because they cannot be formatted in indcator format.')
                datasets = [dataset for dataset in datasets if dataset.lower() in indicator_datasets_lower]

        # Initiate the dataset classes for these datasets and get data
        dataset_instances = self.get_data(datasets=datasets,
                                          dataset_args=dataset_args,
                                          filters=filters,
                                          latest=latest)
        if not dataset_instances:
            return

        # Reformat datasets to be in indicator format
        indicator_data = pd.DataFrame()
        for dataset in dataset_instances:

            # These datasets are already in indicator format
            if dataset.name in ["NS Contacts", "FDRS", "NS Documents", "OCAC Assessment Dates", "World Development Indicators", "INFORM Risk"]:
                continue

            # ICRC presence
            elif dataset.name == 'ICRC Presence':
                dataset.data = dataset.data.rename(columns={'ICRC presence': 'Value'})\
                                            .drop(columns=['Key operation'])
                dataset.data['Indicator'] = 'ICRC presence'
                dataset.data['Year'] = datetime.date.today().year

            # IFRC Disaster Law
            elif dataset.name == 'IFRC Disaster Law':
                dataset.data = dataset.data.rename(columns={'Description': 'Value'})\
                                           .drop(columns=['ID'])
                dataset.data['Indicator'] = 'IFRC Disaster Law'
                dataset.data['Year'] = datetime.date.today().year

            # CPI
            elif dataset.name == 'Corruption Perception Index':
                dataset.data = dataset.data.rename(columns={'Score': 'Value'})\
                                           .drop(columns=['Standard error', 'Sources', 'Rank'])
                dataset.data['Indicator'] = 'Corruption Perception Index'

            else:
                raise RuntimeError(f'Unrecognised dataset {dataset.name}')

        # Merge all datasets
        for dataset in dataset_instances:
            optional_columns = ['Description', 'URL']
            for column in optional_columns:
                if column not in dataset.data.columns:
                    dataset.data[column] = ""
            if set(dataset.data.columns) != set(column_names):
                extra_columns = [column for column in dataset.data.columns if column not in column_names]
                missing_columns = [column for column in column_names if column not in dataset.data.columns]
                raise ValueError(f'Columns of dataset {dataset.name} do not match the indicator format. Extra columns: {extra_columns}. Missing columns: {missing_columns}')
            dataset.data['Dataset'] = dataset.name
            indicator_data = pd.concat([dataset.data for dataset in dataset_instances])

        # Tidy: sort columns, sort rows
        indicator_data = indicator_data[column_names+['Dataset']]
        indicator_data = indicator_data.sort_values(by=['Dataset', 'National Society name', 'Indicator', 'Year', 'Value'])\
                                        .reset_index(drop=True)

        # Filter for only quantitative data or only qualitative data
        if quantitative==True:
            indicator_data = indicator_data.loc[indicator_data['Value'].astype(str).str.isnumeric()]
            indicator_data['Value'] = indicator_data['Value'].astype(float)
        elif quantitative==False:
            indicator_data = indicator_data.loc[~indicator_data['Value'].astype(str).str.isnumeric()]

        return indicator_data


    def validate_dataset_names(self, datasets):
        """
        Check whether all names in a list are valid dataset names (case insensitive).
        """
        # Check provided datasets are in recognised list
        available_datasets = [item.lower().strip() for item in self.datasets_info]
        case_map = {item.lower().strip(): item for item in self.datasets_info}
        valid_datasets = []
        if datasets is not None:
            for dataset in datasets:
                if dataset.lower().strip() not in available_datasets:
                    warnings.warn(f'Dataset {dataset} not recognised, skipping. Dataset options are: {list(self.datasets_info.keys())}')
                else:
                    valid_datasets.append(case_map[dataset.lower().strip()])

        return valid_datasets


    def initiate_datasets(self, datasets=None, dataset_args=None):
        """
        Get all available datasets.

        Parameters
        ----------
        datasets : list (default=None)
            List of names of datasets to return. If None, all datasets are returned.

        dataset_args : dict (default=None)
            Arguments that need to be passed to each dataset class, e.g. API key or filepath. Note that if no arguments are required for a datset then this does not need to be provided.
            Should be a dict with keys as dataset names, and values a dict with arguments required for the dataset class.
            E.g. {'FDRS': {'api_key': 'xxxxxx'}, 'OCAC': {'filepath': 'ocac_data.xlsx', 'sheet_name': 'Sheet1'}}
        """
        # Lowercase all provided arguments
        if dataset_args is not None:
            dataset_args = {k.lower().strip(): v for k, v in dataset_args.items()}
        else:
            dataset_args = {}
        datasets = [item.lower().strip() for item in datasets]

        # Add in empty args for datasets not in dataset_args
        for dataset in datasets:
            if dataset not in dataset_args:
                dataset_args[dataset] = {}

        # Initiate all dataset classes including providing arguments. Skip when arguments are not provided.
        class_names = {'FDRS': ifrc_ns_data.FDRSDataset,
                       'NS Documents': ifrc_ns_data.NSDocumentsDataset,
                       'NS Contacts': ifrc_ns_data.NSContactsDataset,
                       'OCAC': ifrc_ns_data.OCACDataset,
                       'OCAC Assessment Dates': ifrc_ns_data.OCACAssessmentDatesDataset,
                       'GO Operations': ifrc_ns_data.GOOperationsDataset,
                       'GO Projects': ifrc_ns_data.GOProjectsDataset,
                       'INFORM Risk': ifrc_ns_data.INFORMRiskDataset,
                       'Recognition laws': ifrc_ns_data.RecognitionLawsDataset,
                       'Statutes': ifrc_ns_data.StatutesDataset,
                       'Logistics projects': ifrc_ns_data.LogisticsProjectsDataset,
                       'World Development Indicators': ifrc_ns_data.WorldDevelopmentIndicatorsDataset,
                       'YABC': ifrc_ns_data.YABCDataset,
                       'ICRC Presence': ifrc_ns_data.ICRCPresenceDataset,
                       'IFRC Disaster Law': ifrc_ns_data.IFRCDisasterLawDataset,
                       'Corruption Perception Index': ifrc_ns_data.CorruptionPerceptionIndexDataset,
                       }
        class_names = {k.lower(): v for k, v in class_names.items()}
        dataset_instances = []
        for dataset_name in datasets:
            try:
                dataset_instances.append(class_names[dataset_name](**dataset_args[dataset_name]))
            except TypeError as err:
                warnings.warn(f'Arguments for dataset "{dataset_name}" not provided so skipping.\n{err}')

        return dataset_instances
