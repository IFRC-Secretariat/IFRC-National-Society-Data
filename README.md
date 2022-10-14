# IFRC National Society Data

This library can be used for getting data on a National Society/ country level from different sources in a consistent format. This includes pulling data from different APIs (internally and externally), and processing that data.

## Setup

### Requirements

Requires Python 3.8+. Required Python packages can be installed from the requirements file:

```bash
pip3 install -r requirements.txt
```

### Installation

To install the package, run the following command in a bash terminal from the root directory:

```bash
pip3 install .
```

## Usage

The library can be used to access datasets individually, or to get multiple datasets at once. These are both described with examples below.


### Getting individual datasets

Individual datasets can be accessed using the specific class for that dataset. Depending on the source of the data, different arguments have to be provided:

- Datasets pulled from an API not requiring an API key: no arguments
- Datasets pulled from an API requiring an API key: ```api_key```
- Datasets not accessible via API: ```filepath``` pointing to an Excel or CSV document, and ```sheet_name``` if that document is an Excel document

The available datasets are given in the [datasets table](#datasets-table), including a description, the source, the class name, and the arguments required. More information is available in the ```datasets_config.yml``` file and in each class module.

Data for each of these datasets can be accessed from the dedicated dataset class by using the ```get_data``` method. This pulls the data from source (API or loading from file), and processes the data, including ensuring National Society names are consistent with a central list, and restructuring and renaming as required. An optional ```latest``` argument can be provided; if ```true```, this will only return the latest data for each National Society and indicator. Note that this does not apply to every dataset: for datasets where this is not applicable a warning is printed.

The ```get_data``` method returns an instance of the dataset class. The ```data``` attribute can be used to access the data in the format of a pandas DataFrame. Other information, such as ```name```, ```source```, ```type```, ```privacy```, and ```link``` can also be accessed as attributes.

```python
import ifrc_ns_data

# Initiate the class to access FDRS data
fdrs_dataset = ifrc_ns_data.FDRSDataset(api_key='xxxxxxx')
print(fdrs_dataset.name)

# Call the get_data method to pull the data from the API, process the data, and return the processed data
fdrs_data = fdrs_dataset.get_data()
print(fdrs_dataset.data)

# Get only the latest data
fdrs_data = fdrs_dataset.get_data(latest=True)
print(fdrs_dataset.data)
```


### Getting multiple datasets

Multiple datasets can be got in one go using the ```DataCollector``` class. This class has the following optional parameters:

- ```datasets```: list of dataset names to get data for. If ```None```, all available datasets are returned.
- ```dataset_args```: arguments required for each dataset class, e.g. ```api_key``` or ```filepath```. These are described in more detail in the [datasets table](#datasets-table).
- ```filters```: filters to apply to the datasets, e.g. ```{'privacy': 'public', 'type': 'external'}``` will return all external public datasets.
- ```latest```: as for individual datasets, if ```true```, only the latest data for each National Society and indicator will be returned. This does not apply to all datasets: where it does not apply, a warning is printed.

The ```get_data``` method returns a list of instances of the dataset classes. The ```data``` attribute can be used to access the data in the format of a pandas DataFrame. Other information, such as ```name```, ```source```, ```type```, ```privacy```, and ```link``` can also be accessed as attributes.

```python
import ifrc_ns_data

data_collector = ifrc_ns_data.DataCollector()

# Get all available datasets
# This will only return datasets not requiring arguments -
# datasets requiring arguments will be skipped and a warning will be printed.
all_datasets = data_collector.get_data()

# Get only the FDRS and OCAC datasets, supplying the required arguments
fdrs_ocac_datasets = data_collector.get_data(datasets=['FDRS', 'OCAC'],
                                             dataset_args={'FDRS': {'api_key': 'xxxxxxxxx'},
                                                           'OCAC': {'filepath': 'my_downloaded_ocac_data.xlsx',
                                                                    'sheet_name': 'Sheet1'}})

# Get all external public datasets not requiring arguments
public_datasets = data_collector.get_data(filters={'privacy': 'public', 'type': 'external'})

# Get all data but only including the latest data for each National Society and indicator
latest_data = data_collector.get_data(latest=True)

# Loop through the datasets and print the name, the data, and the columns
for dataset in latest_data:
  print(dataset.name) # Print the dataset name
  print(dataset.data) # Print the data as a pandas DataFrame
  print(dataset.data.columns) # Print the columns of the pandas DataFrame
```

Several of the datasets have the same "indicator" style structure, with a row for each National Society/ indicator/ year. These all have the same columns: ```National Society name```, ```Country```, ```ISO3```, ```Region```, ```Indicator```, ```Value```, ```Year```. The ```get_merged_indicator_data``` method can be used to get all of this data in a single pandas DataFrame. This method has the same parameters as the ```get_data``` method (```datasets```, ```dataset_args```, ```filters```, and ```latest```). As with ```get_data```, if ```dataset_args``` is not provided as an argument then only the datasets not requiring parameters will be returned, and warnings will be printed for the others.

```python
import ifrc_ns_data

data_collector = ifrc_ns_data.DataCollector()

# Get all indicator-style datasets as a single pandas DataFrame
df = data_collector.get_merged_indicator_data()
print(df.columns) # Print the columns
```

## Datasets table

| Dataset   |      Description      |      Source      |  Class name       | Arguments       |
|-----------|----------------------|------------------|------------------|-------------------|
| FDRS |  FDRS data gives an overview of a National Society in a number of indicators | FDRS API | FDRSDataset | api_key |
| NS documents | Links to key documents including annual plans and financial statements | FDRS API | NSDocumentsDataset | api_key |
| NS contacts | Contact information and social media links | FDRS API | NSContactsDataset | api_key |
| OCAC | Results of OCAC assessment which assesses the strength of a NS | OCAC website Excel download | OCACDataset | filename, sheet_name |
| OCAC assessment dates | Dates of OCAC assessments run by a NS | OCAC website Excel download | NSContactsDataset | filename, sheet_name |
| GO operations | List of operations | GO API | GOOperationsDataset | |
| GO projects | List of projects | GO API | GOProjectsDataset | |
| Recognition laws | NS recognition laws | Excel document | RecognitionLawsDataset | filename, sheet_name |
| Statutes | NS statutes | Excel document | StatutesDataset | filename, sheet_name |
| Logistics projects | List of IFRC logistics projects | Excel document | LogisticsProjectsDataset | filename, sheet_name |
| YABC | IFRC youth YABC projects | Excel document | YABCDataset | filename, sheet_name |
| INFORM risk | INFORM Risk data | INFORM API | INFORMRiskDataset | |
| World development indicators | World development indicators | World Bank API | WorldDevelopmentIndicatorsDataset | |
