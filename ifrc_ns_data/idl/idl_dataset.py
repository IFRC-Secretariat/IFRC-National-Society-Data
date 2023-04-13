"""
Module to access and handle ICRC data.
"""
import requests
import warnings
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


    def pull_data(self, filters=None):
        """
        Scrape data from the IFRC Disaster Law website at https://disasterlaw.ifrc.org/where-we-work.
        
        Parameters
        ----------
        filters : dict (default=None)
            Filters to filter by country or by National Society.
            Keys can only be "Country", "National Society name", or "ISO3". Values are lists.
            Note that this is NOT IMPLEMENTED and is only included in this method to ensure consistency with the parent class and other child classes.
        """
        # The data cannot be filtered from the API so raise a warning if filters are provided
        if filters is not None:
            warnings.warn(f'Filters {filters} not applied because the API response cannot be filtered.')

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
            duplicated_countries = (('Republic of the Congo', '921'),)
            for option in country_options[1:]:
                country_name = option.text
                country_id = option["value"]
                if (country_name.strip(), str(country_id)) in duplicated_countries:
                    continue
                country_url = f'https://disasterlaw.ifrc.org/node/{country_id}'
                # Get the description from the country page
                description = None
                try:
                    country_page = requests.get(country_url)
                    country_soup = BeautifulSoup(country_page.content, 'html.parser')
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