"""
Module to access and handle ICRC data.
"""
import requests
from bs4 import BeautifulSoup
import pandas as pd
from ifrc_ns_data.common import Dataset
from ifrc_ns_data.common.cleaners import NSInfoCleaner, NSInfoMapper


class ICRCPresenceDataset(Dataset):
    """
    Load ICRC presence data from the website, and clean and process the data.

    Parameters
    ----------
    filepath : string (required)
        Path to save the dataset when loaded, and to read the dataset from.
    """
    def __init__(self):
        super().__init__(name='ICRC Presence')

    def pull_data(self):
        """
        Scrape data from the ICRC website at https://www.icrc.org/en/where-we-work.
        """
        # Get the home page
        response = requests.get(
            url='https://www.icrc.org/en/where-we-work',
            headers={'User-Agent': ''}
        )
        response.raise_for_status()
        soup = BeautifulSoup(response.content, 'html.parser')

        # Get the countries information from the "Where we work" page
        regions_list = soup.find("div", {"id": "blockRegionalList"})\
                           .find_all("ul", {"class": "list"})
        country_list = []
        for region in regions_list:
            for country in region.find_all("li", {"class": "item"}):
                # Get key information
                name = country.text.strip()
                url = country.find("a")["href"] if country.find("a") else None
                presence = True if url else False
                key_operation = True if "keyOperations" in country["class"] else False
                # Get the description from the country page
                description = None
                if url:
                    try:
                        country_page = requests.get(url=url, headers={'User-Agent': ''})
                        country_page.raise_for_status()
                        country_soup = BeautifulSoup(country_page.content, 'html.parser')
                        description = country_soup\
                            .find("div", {"class": "block-introduction"})\
                            .find_all()[2]\
                            .text.strip()
                    except Exception:
                        pass
                # Append all the information to the list
                country_list.append({
                    "Country": name,
                    "ICRC presence": presence,
                    "URL": url,
                    "Key operation": key_operation,
                    "Description": description
                })
        data = pd.DataFrame(country_list)

        return data

    def process_data(self, data):
        """
        Transform and process the data, including changing the structure and selecting columns.

        Parameters
        ----------
        data : pandas DataFrame (required)
            Raw data to be processed.
        """
        # Remove regional responses, check country names, then merge in other information
        data = data.loc[~data["Country"].isin(["Lake Chad", "Sahel", "test"])]
        data.loc[:, "Country"] = NSInfoCleaner().clean_country_names(data.loc[:, "Country"])
        new_columns = [column for column in self.index_columns if column != 'Country']
        ns_info_mapper = NSInfoMapper()
        for column in new_columns:
            ns_id_mapped = ns_info_mapper.map(
                data=data['Country'],
                map_from='Country',
                map_to=column
            ).rename(column)
            data = pd.concat(
                [data.reset_index(drop=True), ns_id_mapped.reset_index(drop=True)],
                axis=1
            )

        # Rename and order the columns
        select_columns = ['ICRC presence', 'Key operation', 'URL', 'Description']
        data = data[self.index_columns.copy() + select_columns]

        return data
