"""
Module to access and handle ICRC data.
"""
import requests
from bs4 import BeautifulSoup
import pandas as pd
from ifrc_ns_data.common import Dataset
from ifrc_ns_data.common.cleaners import NSInfoCleaner, NSInfoMapper


class IFRCDisasterLawDataset(Dataset):
    """
    Load IFRC Disaster Law presence data from the website, and clean and process the data.

    Parameters
    ----------
    filepath : string (required)
        Path to save the dataset when loaded, and to read the dataset from.
    """
    def __init__(self):
        super().__init__(name='IFRC Disaster Law')


    def pull_data(self):
        """
        Scrape data from the IFRC Disaster Law website at https://disasterlaw.ifrc.org/where-we-work.
        """
        # Loop through regions to get the list of countries for each region
        home_url = "https://disasterlaw.ifrc.org/"
        region_names = ["africa", "americas", "asia-and-pacific", "middle-east-north-africa", "europe-central-asia"]
        country_list = []
        for region in region_names:
            response = requests.get(f'{home_url}{region}')
            soup = BeautifulSoup(response.content, 'html.parser')
            try:
                country_options = soup.find("select", {"data-drupal-selector": "edit-country"})\
                                .find_all("option")
            except Exception as err:
                continue

            # Loop through countries and get information
            for option in country_options[1:]:
                country_name = option.text
                country_id = option["value"]
                country_url = f'https://disasterlaw.ifrc.org/node/{country_id}'
                # Get the description from the country page
                description = None
                try:
                    country_page = requests.get(country_url)
                    country_soup = BeautifulSoup(response.content, 'html.parser')
                    description = country_soup.find("div", {"class": "field--name-field-paragraphs"})\
                                              .find_all("p")
                    description = "\n".join([para.text for para in description])
                except Exception as err:
                    pass
                # Add all information to the country list
                country_list.append({"Country": country_name,
                                    "ID": country_id,
                                    "URL": country_url,
                                    "Description": description})
        data = pd.DataFrame(country_list)

        return data


    def process_data(self, data, latest=None):
        """
        Transform and process the data, including changing the structure and selecting columns.

        Parameters
        ----------
        data : pandas DataFrame (required)
            Raw data to be processed.
        """
        # Print a warning if filtering is given as this does not apply
        if latest is not None:
            warnings.warn(f'Filtering latest data does not apply to dataset {self.name}')

        # Remove regional responses, check country names, then merge in other information
        data["Country"] = NSInfoCleaner().clean_country_names(data["Country"])
        new_columns = [column for column in self.index_columns if column!='Country']
        ns_info_mapper = NSInfoMapper()
        for column in new_columns:
            ns_id_mapped = ns_info_mapper.map(data=data['Country'], map_from='Country', map_to=column)\
                                         .rename(column)
            data = pd.concat([data.reset_index(drop=True), ns_id_mapped.reset_index(drop=True)], axis=1)

        # Reorder columns
        data = self.rename_columns(data, drop_others=True)
        data = self.order_index_columns(data)

        return data