# IFRC National Society Data

This library can be used for getting data on a National Society/ country level from different sources in a consistent format. This includes pulling data from different APIs (internally and externally), and processing that data.

## Setup

These instructions are for setting up on Windows.

1. This package requires Python 3.8+. If you don't have it already, download and install Python from the [Microsoft Store](https://apps.microsoft.com/detail/python-3-11/9NRWMJP3717K?hl=en-gb&gl=CH). You also need to download and install [Microsoft C++ Build Tools](https://visualstudio.microsoft.com/visual-cpp-build-tools/).

2. First, clone the repository from Github. From the [repository Github page](https://github.com/IFRC-Secretariat/IFRC-National-Society-Data), click the green `Code` button, and then click `Open with GitHub Desktop`. Github Desktop will then copy the repository to your computer.

3. Next, open a terminal and change directory to inside the repository on your computer. Required Python packages can be installed from the requirements file:

    ```bash
    python -m pip install -r requirements.txt
    ```

4. To install the package, run the following command in a bash terminal from the root directory:

    ```bash
    python -m pip install .
    ```

    To check that the package has installed correctly, and check the version and location, run the following:

    ```bash
    python -m pip show ifrc_ns_data
    ```

Note that depending on your setup, you may need to use ```pip3``` instead in these examples.


## Pulling updates

If updates have been pushed to Github, you can pull those locally as follows:

1. Open a terminal, and change directory to the location where you cloned the repository (you can see this in Github Desktop).

2. Pull the updates from Github with: `git pull`

3. Install the updates with pip: `python -m pip install .`


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

Several of the datasets have the same "indicator" style structure, with a row for each National Society/ indicator/ year. These all have the same columns: ```National Society name```, ```Country```, ```ISO3```, ```Region```, ```Indicator```, ```Value```, ```Year```. The ```get_indicators_data``` method can be used to get all of this data in a single pandas DataFrame. This method has the same parameters as the ```get_data``` method (```datasets```, ```dataset_args```, ```filters```, and ```latest```). As with ```get_data```, if ```dataset_args``` is not provided as an argument then only the datasets not requiring parameters will be returned, and warnings will be printed for the others.

```python
import ifrc_ns_data

data_collector = ifrc_ns_data.DataCollector()

# Get indicator-style datasets for the specified datasets as a single pandas DataFrame (the xxxxx should be replaced by the relevant values (API keys or filepaths))
# Change the 'latest' argument to True to get only latest data
df = data_collector.get_indicators_data(
    latest=False, 
    datasets=[
        'FDRS', 
        'NS Documents', 
        'NS Contacts', 
        'INFORM risk', 
        'World development indicators', 
        'OCAC Assessment Dates'
    ], 
    dataset_args={
        'FDRS': {'api_key': 'xxxxx'}, 
        'NS Documents': {'api_key': 'xxxxx'}, 
        'NS Contacts': {'api_key': 'xxxxx'}, 
        'OCAC Assessment Dates': {'filepath': 'xxxxxxxx', 'sheet_name': 'Sheet1'}
    }
)
print(df) # Print the dataframe
```

### Power BI

The package can be used to import data into Power BI. First, follow the [Power BI instructions](https://learn.microsoft.com/en-us/power-bi/connect-data/desktop-python-scripts) to setup Python for Power BI. Next, install the IFRC NS data library from the Windows Powershell (see [Setup](#setup)).

The package can now be used in Power BI. E.g. the following Power Query script can be used to get all indicator-style data for the specified datasets (replacing the ```xxxxx``` by the relevant values - API keys or filepaths). ```latest``` can be changed to ```True``` to get only latest data.

```
let
    Source = Python.Execute("
import ifrc_ns_data
data_collector = ifrc_ns_data.DataCollector()
data = data_collector.get_indicators_data(latest=False, datasets=['FDRS', 'NS Documents', 'NS Contacts', 'INFORM risk', 'World development indicators', 'OCAC Assessment Dates'], dataset_args={'FDRS': {'api_key': 'xxxxx'}, 'NS Documents': {'api_key': 'xxxxx'}, 'NS Contacts': {'api_key': 'xxxxx'}, 'OCAC Assessment Dates': {'filepath': 'xxxxx', 'sheet_name': 'Sheet1'}})
    "),
    #"All indicator data" = Source{[Name="data"]}[Value]
in
    #"All indicator data"
```

Individual datasets can also be accessed, e.g. the following example in Power Query M will import FDRS data (replace ```xxxxxxx``` with your FDRS API key).

```
let
    Source = Python.Execute("
import ifrc_ns_data
fdrs_dataset = ifrc_ns_data.FDRSDataset(api_key='xxxxxxx')
fdrs_data = fdrs_dataset.get_data()
    "),
    fdrs_data = Source{[Name="fdrs_data"]}[Value]
in
    fdrs_data
```


## Datasets table

| Dataset   |      Description      |      Source      |  Class name       | Arguments       |
|-----------|----------------------|------------------|------------------|-------------------|
| Evaluations | IFRC evaluation documents | IFRC public website | EvaluationsDataset | |
| FDRS |  FDRS data gives an overview of a National Society in a number of indicators | NS Databank API | FDRSDataset | api_key |
| NS documents | Links to key documents including annual plans and financial statements | FDRS API | NSDocumentsDataset | api_key |
| NS contacts | Contact information and social media links | FDRS API | NSContactsDataset | api_key |
| OCAC | Results of OCAC assessment which assesses the strength of a NS | OCAC website Excel download | OCACDataset | filepath, sheet_name |
| OCAC assessment dates | Dates of OCAC assessments run by a NS | NS Databank API | OCACAssessmentDatesDataset | api_key |
| BOCA assessment dates | Dates of BOCA assessments run by NS branches | NS Databank API | BOCAAssessmentDatesDataset | api_key |
| GO operations | List of operations | GO API | GOOperationsDataset | |
| GO projects | List of projects | GO API | GOProjectsDataset | |
| Recognition laws | NS recognition laws | Excel document | RecognitionLawsDataset | filepath, sheet_name |
| Statutes | NS statutes | Excel document | StatutesDataset | filepath, sheet_name |
| Logistics projects | List of IFRC logistics projects | Excel document | LogisticsProjectsDataset | filepath, sheet_name |
| YABC | IFRC youth YABC projects | Excel document | YABCDataset | filepath, sheet_name |
| INFORM risk | INFORM Risk data | INFORM API | INFORMRiskDataset | |
| World development indicators | World development indicators | World Bank API | WorldDevelopmentIndicatorsDataset | |
| ICRC Presence | Presence of ICRC | ICRC website | ICRCPresenceDataset | |
| IFRC Disaster Law | IFRC Disaster Law overview | IFRC Disaster Law website | IFRCDisasterLawDataset | |
| Corruption Perception Index | The CPI ranks 180 countries and territories around the world by their perceived levels of public sector corruption, scoring on a scale of 0 (highly corrupt) to 100 (very clean). | Transparency International | CorruptionPerceptionIndexDataset | |
| Youth Engagement | IFRC Youth engagement global survey | IFRC VODPLA website | YouthEngagementDataset | |


## Contributing

To add a new dataset to this module, do the following steps:

1. Create a new file (in a new or existing folder) in `ifrc_ns_data`. If adding a new folder, add the folder name to be imported in `ifrc_ns_data/__init__.py`, and add an `__init__.py` file to the new folder to import the new class.

2. Create a new class for the dataset. Use an existing file as a template to add the `pull_data` and `process_data` methods.

3. Add the dataset with information to the `ifrc_ns_data/datasets_config.yml` file.

4. Add the new dataset class name to the `class_names` list in `ifrc_ns_data/data_collector.py`. If the dataset is in indicators format, add it to the `indicator_datasets` list in `get_indicators_data`.

5. Add the new dataset to the [README datasets table](#datasets-table).

6. Update the tests, including adding the dataset and any arguments to `self.datasets` in `setUp`, and updating the `test_get_data` and `test_get_public_data` dataset lengths in the `TestAllData` class.

## Testing

The tests can be run using pytest. If you don't have pytest installed, you can install it with:
```bash
python -m pip install pytest
```

To run the tests, you first need to set environment variables. From a Windows terminal you can do this as follows (replacing `xxxxx` with your API keys):
```bash
$env:FDRS_PUBLIC_API_KEY = 'xxxxx'
$env:GO_API_KEY = 'xxxxx'
$env:IPC_API_KEY = 'xxxxx'
```

Next, run the tests:
```bash
python -m pytest
```

Note that to save time, the raw data (data pulled from sourced but not processed) is saved in `tests/data_cache/`. This is then passed to the tests to avoid having to pull the data multiple times, as this is time consuming. If any of the `pull_data` methods are changed, the corresponding data file in `tests/data_cache/` should be deleted before the tests are run.