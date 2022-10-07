"""
Module to access and return multiple datasets at once.
"""
import os
import warnings
import yaml
import pandas as pd
import ifrc_ns_data
from definitions import DATASETS_CONFIG_PATH


class DataCollector:
    """
    Class to handle multiple datasets.

    Parameters
    ----------
    """
    def __init__(self):
        self.datasets_info = yaml.safe_load(open(DATASETS_CONFIG_PATH))
        self.dataset_names = list(self.datasets_info)


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
        dataset_classes : list of Dataset objects
            List of Dataset objects. The attribute 'data' is a pandas DataFrame containing the data. Other attributes contain information about the dataset such as source, focal_point, and privacy.
        """
        # Validate the provided dataset names, or set to all
        if datasets is not None:
            dataset_names = validate_dataset_names(datasets)
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
        dataset_classes = self.initiate_datasets(datasets=filtered_datasets, dataset_args=dataset_args)

        # Load the data from the source and process
        for dataset in dataset_classes:
            print(f'Getting {dataset.name} data...')
            dataset.get_data(latest=latest)

        return dataset_classes


    def get_merged_indicator_data(self, datasets=None, dataset_args=None, filters=None, latest=None):
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
        """
        # Initiate the dataset classes for these datasets and get data
        if filters is None: filters = {}
        filters = {**filters, **{'format': 'indicators'}}
        dataset_classes = self.get_data(datasets=datasets,
                                        dataset_args=dataset_args,
                                        filters=filters,
                                        latest=latest)

        # Merge all of the datasets together
        if not dataset_classes: return
        for dataset in dataset_classes:
            dataset.data['Dataset'] = dataset.name
        all_indicator_data = pd.concat([dataset.data for dataset in dataset_classes])
        all_indicator_data = all_indicator_data.sort_values(by=['Dataset', 'National Society name', 'Indicator', 'Year', 'Value'])\
                                               .reset_index(drop=True)

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

        # Initiate all dataset classes including providing arguments. Skip when arguments are not provided.
        dataset_classes = []
        if 'fdrs' in datasets:
            if 'fdrs' in dataset_args:
                dataset_classes.append(ifrc_ns_data.fdrs.FDRSDataset(**dataset_args['fdrs']))
            else:
                warnings.warn('FDRS arguments not provided so skipping')
        if 'ns documents' in datasets:
            if 'ns documents' in dataset_args:
                dataset_classes.append(ifrc_ns_data.fdrs.NSDocumentsDataset(**dataset_args['ns documents']))
            else:
                warnings.warn('NS Documents arguments not provided so skipping')
        if 'go operations' in datasets:
            dataset_classes.append(ifrc_ns_data.go.OperationsDataset())
        if 'go projects' in datasets:
            dataset_classes.append(ifrc_ns_data.go.ProjectsDataset())
        if 'inform risk' in datasets:
            dataset_classes.append(ifrc_ns_data.inform.INFORMRiskDataset())
        if 'recognition laws' in datasets:
            if 'recognition laws' in dataset_args:
                dataset_classes.append(ifrc_ns_data.legal.RecognitionLawsDataset(**dataset_args['recognition laws']))
            else:
                warnings.warn('Recognition Laws arguments not provided so skipping')
        if 'statutes' in datasets:
            if 'statutes' in dataset_args:
                dataset_classes.append(ifrc_ns_data.legal.Statutes(**dataset_args['statutes']))
            else:
                warnings.warn('Statutes arguments not provided so skipping')
        if 'logistics projects' in datasets:
            if 'logistics projects' in dataset_args:
                dataset_classes.append(ifrc_ns_data.logistics.LogisticsProjectsDataset(**dataset_args['logistics projects']))
            else:
                warnings.warn('Logistics Projects arguments not provided so skipping')
        if 'ns contacts' in datasets:
            if 'ns contacts' in dataset_args:
                dataset_classes.append(ifrc_ns_data.ns_contacts.NSContactsDataset(**dataset_args['ns contacts']))
            else:
                warnings.warn('NS Contacts arguments not provided so skipping')
        if 'ocac' in datasets:
            if 'ocac' in dataset_args:
                dataset_classes.append(ifrc_ns_data.ocac_boca.OCACDataset(**dataset_args['ocac']))
            else:
                warnings.warn('OCAC arguments not provided so skipping')
        if 'ocac assessment dates' in datasets:
            if 'ocac assessment dates' in dataset_args:
                dataset_classes.append(ifrc_ns_data.ocac_boca.OCACAssessmentDatesDataset(**dataset_args['ocac assessment dates']))
            else:
                warnings.warn('OCAC arguments not provided so skipping')
        if 'world development indicators' in datasets:
            dataset_classes.append(ifrc_ns_data.world_bank.WorldDevelopmentIndicatorsDataset())
        if 'yabc' in datasets:
            if 'yabc' in dataset_args:
                dataset_classes.append(ifrc_ns_data.youth.YABCDataset(**dataset_args['yabc']))
            else:
                warnings.warn('YABC arguments not provided so skipping')

        return dataset_classes