"""
Module to access and return multiple datasets at once.
"""
import warnings
import yaml
import pandas as pd
import ifrc_ns_data
from ifrc_ns_data.definitions import DATASETS_CONFIG_PATH


class DataCollector:
    """
    Class to handle multiple datasets.

    Parameters
    ----------
    """
    def __init__(self):
        self.datasets_info = yaml.safe_load(open(DATASETS_CONFIG_PATH))
        archived_datasets = ['UNDP Human Development'] # Archived because the API has stopped working
        self.dataset_names = [name for name in self.datasets_info if name not in archived_datasets]


    def get_data(self, datasets=None, dataset_args=None, filters=None, latest=None):
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

        # Deal with filters
        if filters is not None:

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
        for dataset in dataset_instances:
            print(f'Getting {dataset.name} data...')
            dataset.get_data(latest=latest)

        return dataset_instances


    def get_merged_indicator_data(self, datasets=None, dataset_args=None, filters=None, latest=None, quantitative=None):
        """
        Get a dataset of all indicator-format data on National Societies.

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
        # Initiate the dataset classes for these datasets and get data
        if filters is None: filters = {}
        filters = {**filters, **{'format': 'indicators'}}
        dataset_instances = self.get_data(datasets=datasets,
                                          dataset_args=dataset_args,
                                          filters=filters,
                                          latest=latest)

        # Merge all of the datasets together
        if not dataset_instances: return
        for dataset in dataset_instances:
            dataset.data['Dataset'] = dataset.name
        all_indicator_data = pd.concat([dataset.data for dataset in dataset_instances])
        all_indicator_data = all_indicator_data.sort_values(by=['Dataset', 'National Society name', 'Indicator', 'Year', 'Value'])\
                                               .reset_index(drop=True)

        # Filter for only quantitative data or only qualitative data
        if quantitative==True:
            all_indicator_data = all_indicator_data.loc[all_indicator_data['Value'].astype(str).str.isnumeric()]
            all_indicator_data['Value'] = all_indicator_data['Value'].astype(float)
        elif quantitative==False:
            all_indicator_data = all_indicator_data.loc[~all_indicator_data['Value'].astype(str).str.isnumeric()]

        return all_indicator_data


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
                       }
        class_names = {k.lower(): v for k, v in class_names.items()}
        dataset_instances = []
        for dataset_name in datasets:
            try:
                dataset_instances.append(class_names[dataset_name](**dataset_args[dataset_name]))
            except TypeError as err:
                warnings.warn(f'Arguments for dataset "{dataset_name}" not provided so skipping.\n{err}')

        return dataset_instances
