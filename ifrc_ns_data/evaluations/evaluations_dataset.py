"""
Module to access and handle ICRC data.
"""
import requests
from bs4 import BeautifulSoup
import pandas as pd
from ifrc_ns_data.common import Dataset
from ifrc_ns_data.common.cleaners import NSInfoCleaner, NSInfoMapper


class EvaluationsDataset(Dataset):
    """
    Load IFRC evaluations data from the IFRC public website, and clean and process the data.

    Parameters
    ----------
    filepath : string (required)
        Path to save the dataset when loaded, and to read the dataset from.
    """
    def __init__(self):
        super().__init__(name='Evaluations')

    def pull_data(self):
        """
        Scrape data from the IFRC public website at https://www.ifrc.org/evaluations.
        """
        home_url = 'https://www.ifrc.org'
        page = 0
        evaluations_data = []
        user_agent = """Mozilla/5.0 (Macintosh; Intel Mac OS X 10_11_5) \
        AppleWebKit/537.36 (KHTML, like Gecko) Chrome/50.0.2661.102 Safari/537.36"""
        while True:

            # Get the home page
            list_page = requests.get(
                url=f'{home_url}/evaluations',
                params={'page': page},
                headers={'User-Agent': user_agent}
            )
            list_page.raise_for_status()
            soup = BeautifulSoup(list_page.content, "html.parser")
            evaluations_table = soup.find('table', {'class': 'views-table'})
            if (evaluations_table is None):
                break

            # Loop through the evaluations
            evaluations_list = evaluations_table.find("tbody").find_all("tr")
            for evaluation_row in evaluations_list:

                # Get the content from the evaluation report page
                evaluation_title = evaluation_row.find("td", {'data-label': 'Title'}).text.strip()
                evaluation_page_url = evaluation_row.find("td", {'data-label': 'Title'}).find("a")['href']

                # Access meta info on the evaluation
                evaluation_info = {
                    'Country': evaluation_row.find("td", {'data-label': 'Location'}).text.strip(),
                    'Title': evaluation_title,
                    'Categories': [
                        category.strip()
                        for category in evaluation_row.find("td", {'data-label': 'Category'}).text.strip().split(',')
                    ],
                    'Type': [
                        type.strip()
                        for type in evaluation_row.find("td", {'data-label': 'Type'}).text.strip().split(',')
                    ],
                    'Organization': [
                        org.strip()
                        for org in evaluation_row.find("td", {'data-label': 'Organization'}).text.strip().split(',')
                    ],
                    'Date': evaluation_row.find("td", {'data-label': 'Date'}).text.strip(),
                    'Management response': evaluation_row.find(
                        "td", {'data-label': 'Management response'}
                    ).text.strip(),
                    'URL': f'{home_url}{evaluation_page_url}'
                }

                """
                Request the content of the web page for a single evaluation.
                Extract the evaluation file from the download section of the web page, and save the file locally.
                """
                # Download the document
                evaluation_page = requests.get(
                    url=f'{home_url}{evaluation_page_url}',
                    headers={'User-Agent': user_agent}
                )
                evaluation_page.raise_for_status()
                evaluation_page_soup = BeautifulSoup(evaluation_page.content, "html.parser")

                # Check if the document is valid
                download_area = evaluation_page_soup.find("div", {'class': 'download-links'})
                if download_area is None:
                    raise RuntimeError(f'ERROR: no download area {home_url}{evaluation_page_url}')
                download_links = download_area.find("div", {'class': 'download-links__links-content'}).find_all("a")
                if (len(download_links) != 1):
                    raise RuntimeError(f'ERROR: {len(download_links)} download links found at {evaluation_page_url}')

                # Add the document URL
                download_url = download_links[0]['href']
                evaluation_info['Document URL'] = download_url
                evaluations_data.append(evaluation_info)

            page += 1

        data = pd.DataFrame(evaluations_data)

        return data

    def process_data(self, data):
        """
        Transform and process the data, including changing the structure and selecting columns.

        Parameters
        ----------
        data : pandas DataFrame (required)
            Raw data to be processed.
        """
        # Expand the country column
        def rename_countries_commas(txt):
            country_renames_commas = {
                'Korea, Republic Of': 'Republic of Korea',
                'Iran, Islamic Republic Of': 'Iran',
                'Moldova, Republic Of': 'Moldova',
                'Taiwan, Province of China': 'Taiwan',
                'Congo, The Democratic Republic Of The': 'Democratic Republic of the Congo',
                "Korea, Democratic People'S Republic Of": "Democratic people's republic of Korea",
                'Palestinian Territory, Occupied': 'Palestine',
                'Micronesia, Federated States Of': 'Micronesia',
                'Tanzania, United Republic Of': 'Tanzania'
            }
            if txt:
                for key, repl in country_renames_commas.items():
                    txt = txt.replace(key, repl)
            return txt
        data['Country'] = data['Country'].str.strip().apply(
            lambda country_string: rename_countries_commas(country_string).strip().split(',')
        )
        remove_items = [
            '-', 'nan',
            'global',
            'europe', 'middle east and north africa', 'africa', 'asia pacific', 'americas'
        ]
        data['Country'] = data['Country'].apply(
            lambda country_list: [
                x.strip() for x in country_list
                if (x.strip().lower() not in remove_items) and (x == x)
            ]
        )
        data = data.dropna(subset=['Country'])

        # Clean the country names
        country_names = data["Country"].explode().drop_duplicates().dropna()
        cleaned_country_names = NSInfoCleaner().clean_country_names(country_names)
        rename_countries = dict(zip(country_names.tolist(), cleaned_country_names.tolist()))
        data['Country'] = data['Country'].apply(
            lambda country_list: [
                rename_countries[country] if country in rename_countries
                else country
                for country in country_list
            ]
        )

        # Add in other country columns: NS name, ISO3, region
        ns_info_mapper = NSInfoMapper()
        for column in ['National Society name', 'ISO3', 'Region']:
            data[column] = data['Country'].apply(
                lambda country_list: ns_info_mapper.map(
                    data=country_list,
                    map_from="Country",
                    map_to=column
                )
            )

        # Reorder columns
        data = self.order_index_columns(data)

        return data
