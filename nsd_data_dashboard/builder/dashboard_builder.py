"""
Module to generate an Excel dashboard with NSD data.

"""
import pandas as pd
import shutil
import yaml
import ast
from datetime import date
import os
import sys
from openpyxl.workbook.protection import WorkbookProtection
from openpyxl.styles import Font, PatternFill, Alignment, Side, Border, borders
from openpyxl.utils import get_column_letter
from openpyxl.formatting.rule import CellIsRule
from nsd_data_dashboard.builder.excel_handler import ExcelHandler


class NSDDashboardBuilder(ExcelHandler):
    """
    Class to generate an Excel document containing data on IFRC National Societies.

    Parameters
    ----------
    save_folder : string (required)
        The location to save the final Excel document and datasets.

    file_name : string (default='NSD Data Dashboard')
        The name to give the generated Excel dashboard file.
    """
    def __init__(self, save_folder, file_name='NSD Data Dashboard'):
        # Set other variables
        self.save_folder = save_folder
        self.formula_prefix = '_xlfn.'

        # Get the static file directory and excel template path
        __location__ = os.path.realpath(os.path.join(os.getcwd(), os.path.dirname(__file__)))
        self.static_folder = os.path.join(__location__, 'static/')
        excel_template = os.path.join(self.static_folder, 'dashboard_template.xlsx')

        # Copy the Excel template into save_folder and initiate the parent class, setting the file path as the path to this copy
        self.excel_file_path = f'{self.save_folder}/{file_name}.xlsx'
        shutil.copyfile(excel_template, self.excel_file_path)
        super().__init__(file_path=self.excel_file_path)


    def generate_dashboard(self, indicator_datasets, documents=None, protect_sheets=False, protect_workbook=False, excel_password=None):
        """
        Generate the Excel document containing information on IFRC National Societies.

        Parameters
        ----------
        indicator_datasets : list of Dataset objects (required)
            List of Dataset objects, containing information on datasets to write to the Excel document.

        documents : pandas DataFrame (default=None)
            Pandas DataFrame containing information on documents, to write to the Excel dashboard.

        protect_sheets : boolean or list of strings (default=False)
            List of sheets to protect. If True, all of the sheets in the Excel document will be protected.

        protect_workbook : boolean (default=False)
            If True, the workbook will be protected and the structure will be locked.

        excel_password : string (default=None)
            If protect_sheets is set to True, this password will be used to protect the sheets.
        """
        # Set attributes
        self.indicator_datasets = indicator_datasets
        self.documents = documents

        # Process the data into a "log" type dataset and write to the Excel document
        self.df_log = self.process_log_data()
        self.write_log(data=self.df_log)

        # Process a list of indicators and write to the Excel document
        df_indicators = self.process_indicators_list()
        self.write_indicators_list(data=df_indicators)

        # Process and write the documents list to the Excel document
        if documents is not None:
            df_documents = self.process_documents_data()
            self.write_documents(data=df_documents)

        # Write the update date to the About sheet and add the header image
        self.write_update_date()
        self.add_image(image_path=os.path.join(self.static_folder, 'header.png'),
                       sheets='About',
                       loc='A1',
                       atts={'height': 195, 'width': 810})

        # Set the sheet order
        sheets = ['About', 'Documents', 'All data', 'List of indicators']
        self.order_sheets(order=sheets, hidden=[])

        # Protect the sheets
        if protect_sheets is True:
            protect_sheets = self.book.sheetnames
        if protect_sheets:
            self.lock_worksheets(sheets=protect_sheets, password=excel_password)

        # Protect the workbook and structure
        if protect_workbook:
            self.book.security = WorkbookProtection(workbookPassword=excel_password, revisionsPassword=excel_password, lockWindows=True, lockStructure=True, lockRevision=True)

        # Print out a final message
        self.save()
        print('Excel document generated and saved to', self.excel_file_path)


    def process_log_data(self):
        """
        Process all of the datasets into a log format.
        """
        # Convert the datasets into a log format and append them together
        log_datasets = []
        for dataset in self.indicator_datasets:
            df_unstack = dataset.data.stack(level=0)
            if df_unstack.index.names != ['National Society name', 'Country', 'ISO3', 'Region', 'indicator']:
                raise ValueError(f'Columns missing from dataset {dataset.name}')
            log_datasets.append(df_unstack)
        df_log = pd.concat(log_datasets, axis='rows')

        # Sort the dataset, rename columns
        rename_columns = {column: column.capitalize().replace('_',' ') for column in ['indicator', 'value', 'year', 'source', 'type', 'link', 'focal_point']}
        df_log = df_log.reset_index()\
                       .sort_values(by=['year', 'indicator', 'Country'])\
                       .rename(columns=rename_columns, errors='raise')

        # Order columns
        df_log = df_log[['National Society name', 'Country', 'ISO3', 'Region', 'Indicator', 'Value', 'Year', 'Source', 'Type', 'Link', 'Focal point']]

        return df_log


    def process_indicators_list(self):
        """
        Process a dataset containing a list of all the indicators, including data sources.
        """
        # Get the list of indicators from the log data
        indicator_data = []
        for dataset in self.indicator_datasets:
            dataset_indicators = dataset.data.stack(level=0).reset_index().drop_duplicates(subset=['indicator'])[['indicator']]
            for column, value in dataset.meta.items():
                dataset_indicators[column] = value
            indicator_data.append(dataset_indicators)
        df_indicators = pd.concat(indicator_data, axis='rows')

        # Rename the datasets
        rename_columns = {column: column.capitalize().replace('_',' ') for column in ['indicator', 'source', 'type', 'link', 'focal_point']}
        df_indicators = df_indicators.rename(columns=rename_columns)\
                                     .sort_values(by=['Type', 'Source', 'Indicator'], ascending=[False, True, True])

        # Order columns
        df_indicators = df_indicators[['Indicator', 'Source', 'Type', 'Link', 'Focal point']]

        return df_indicators


    def process_documents_data(self):
        """
        Process the data on documents.
        """
        # Rename columns for readability, and sort
        df_documents = self.documents.data.rename(columns={'value': 'Document', 'year': 'Year', 'link': 'Document link', 'source': 'Source', 'type': 'Type'}, errors='raise', level=1)\
                                           .reset_index()\
                                           .sort_values(by=['Country'])

        # Rename column list names
        df_documents.columns = df_documents.columns.rename({'indicator': None})

        return df_documents


    def write_log(self, data, sheet_name="All data"):
        """
        Write all of the data to a "log" sheet in the Excel document.

        Parameters
        ----------
        data : pandas DataFrame (required)
            Dataset of processed data to write to the data sheet.

        sheet_name : string (default='All data')
            Name of the Excel sheet.
        """
        # Set the formatting
        side = Side(border_style='thin', color='FFFFFF')
        header_styles = {'font': Font(name='Calibri', bold=True, color='ffffff', size=11),
                         'alignment': Alignment(horizontal="left", vertical="center"),
                         'fill': PatternFill(start_color="567EBB", end_color="567EBB", fill_type = "solid"),
                         'border': Border(left=side, right=side, top=side, bottom=side)}
        body_styles = {'border': Border(left=side, right=side, top=side, bottom=side),
                       'alignment': Alignment(horizontal="left", vertical="top")}
        column_widths = {'National Society name': 40, 'Country': 25, 'ISO3': 10, 'Region': 30, 'Indicator': 50, 'Value': 25, 'Year': 15, 'Source': 30, 'Type': 25, 'Link': 40, 'Focal point': 30}

        # Write the data to a sheet in the Excel document
        self.write_data_sheet(data=data,
                              sheet_name=sheet_name,
                              index=False, overwrite=True,
                              header_styles=header_styles,
                              body_styles=body_styles,
                              alternate_row_background=['BBCBE3', 'DCE4F1'],
                              column_widths=column_widths,
                              sorting=True)


    def write_indicators_list(self, data, sheet_name="List of indicators"):
        """
        Write a dataset containing the list of indicators a sheet in the Excel document.

        Parameters
        ----------
        data : pandas DataFrame (required)
            Dataset of processed data to write to the data sheet.

        sheet_name : string (default='Indicators')
            Name of the Excel sheet.
        """
        # Set the formatting
        side = Side(border_style='thin', color='D9D9D9')
        header_styles = {'font': Font(name='Calibri', bold=True, color='ffffff', size=11),
                         'alignment': Alignment(horizontal="left", vertical="center"),
                         'fill': PatternFill(start_color="567EBB", end_color="567EBB", fill_type = "solid"),
                         'border': Border(left=side, right=side, top=side, bottom=side)}
        body_styles = {'border': Border(left=side, right=side, top=side, bottom=side),
                       'alignment': Alignment(horizontal="left", vertical="top"),}
        column_widths = {'Indicator': 50, 'Source': 30, 'Type': 25, 'Link': 40, 'Focal point': 30}

        # Write the data to a sheet in the Excel document
        self.write_data_sheet(data=data,
                              sheet_name=sheet_name,
                              index=False, overwrite=True,
                              header_styles=header_styles,
                              body_styles=body_styles,
                              alternate_row_background=['F2F2F2', 'FFFFFF'],
                              column_widths=column_widths,
                              sorting=True)


    def write_documents(self, data, sheet_name='Documents'):
        """
        Write a dataset containing the list of documents to the Excel document.

        Parameters
        ----------
        data : pandas DataFrame (required)
            Dataset containing the data to be written.

        sheet_name : string (required)
            Name of the Excel sheet.
        """
        # Set the formatting
        side = Side(border_style='thin', color='D9D9D9')
        header_styles = {'font': Font(name='Calibri', bold=True, color='ffffff', size=11),
                         'alignment': Alignment(horizontal="center", vertical="center"),
                         'fill': PatternFill(start_color="007F81", end_color="007F81", fill_type = "solid"),
                         'border': Border(left=side, right=side, top=side, bottom=side)}
        body_styles = {'border': Border(left=side, right=side, top=side, bottom=side),
                       'alignment': Alignment(horizontal="left", vertical="top"),}

        # Get column widths
        data = data.set_index(['National Society name', 'Country', 'ISO3', 'Region'])
        index_column_widths = {'National Society name': 40, 'Country': 25, 'ISO3': 10, 'Region': 30}
        level_column_widths = {1: {'Document': 25, 'Year': 10, 'Document link': 20, 'Source': 10, 'Type': 12}}
        #column_widths = {list(data.index.names).index(name): column_widths_index[name] for name in column_widths_index}
        #for column in column_widths_level_1:
        #    column_widths.update({(i+len(data.index.names)): column_widths_level_1[column] for i, x in enumerate(list(data.columns.get_level_values(1))) if x==column})

        # Write the data to a sheet in the Excel document
        self.write_data_sheet(data=data,
                              sheet_name=sheet_name,
                              index=True,
                              overwrite=True,
                              header_styles=header_styles,
                              body_styles=body_styles,
                              alternate_row_background=['F2F2F2', 'FFFFFF'],
                              index_column_widths=index_column_widths,
                              level_column_widths=level_column_widths,
                              sorting=True,
                              hidden=None)


    def write_update_date(self):
        """
        Write a sheet with the publication date to the Excel document.
        """
        # Read in the 'About' worksheet
        worksheet = self.book.get_sheet_by_name('About')

        # Write the date to the 'About' sheet in the Excel doc
        today = date.today().strftime('%d %B %Y')
        worksheet['A3'] = f'Updated: {today}'