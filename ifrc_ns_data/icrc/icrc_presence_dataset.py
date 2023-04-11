"""
Module to access and handle ICRC data.
"""
import requests
import warnings
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


    def pull_data(self, filters):
        """
        Scrape data from the ICRC website at https://www.icrc.org/en/where-we-work.
        
        Parameters
        ----------
        filters : dict (default=None)
            Filters to filter by country or by National Society.
            Keys can only be "Country", "National Society name", or "ISO3". Values are lists.
            Note that this is NOT IMPLEMENTED and is only included in this method to ensure consistency with the parent class and other child classes.
        """
        # The data cannot be filtered from the API so raise a warning if filters are provided
        if filters is not None:
            warnings.warn(f'Filters {filters} not applied because the response cannot be filtered.')

        # Get the home page
        response = requests.get(url='https://www.icrc.org/en/where-we-work',
                                headers={'User-Agent': ''})
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
                        description = country_soup.find("div", {"class": "block-introduction"}).find_all()[2].text.strip()
                    except Exception as err:
                        pass
                # Append all the information to the list
                country_list.append({"Country": name,
                                     "ICRC presence": presence,
                                     "URL": url,
                                     "Key operation": key_operation,
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
        data = data.loc[~data["Country"].isin(["Lake Chad", "Sahel"])]
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