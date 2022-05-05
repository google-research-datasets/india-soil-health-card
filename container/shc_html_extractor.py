"""TODO(rshashwat): DO NOT SUBMIT without one-line documentation for extract.

TODO(rshashwat): DO NOT SUBMIT without a detailed description of extract.
"""

import collections
from typing import Dict

from bs4 import BeautifulSoup
from bs4 import Tag
from html_parser_utils import parse_html_table_row
from html_parser_utils import parse_html_table_rows_with_header_and_multi_cols
from html_parser_utils import parse_html_table_rows_with_two_cols


class ShcHtmlExtractor:
  """Extracts the information from a Soil Health Card HTML file."""

  def __init__(self, file_content: str):
    self.file_content = file_content

  def extract(self) -> Dict[str, str]:
    """Reads content of a HTML file and extracts the soil health card info.

    Returns:
      A dict mapping all extracted information to their corresponding values.
    """
    soup = BeautifulSoup(self.file_content, 'html.parser')
    info = {}
    all_pages = soup.find(
        'div', attrs={
            'id': 'ReportViewer1_ctl09'
        }).find('table').find('table').find('table')
    all_pages_rows = all_pages.find_all('tr')
    info.update(self.process_page1(all_pages_rows[1]))
    info.update(self.process_page2(all_pages_rows[3]))
    return info

  def process_page1_front(self, front_table: Tag) -> Dict[str, str]:
    """The front side of page-1 is expected to contain the following information.

    1. soil health card number and it's validity.
    2. farmer's details like name, father's name, address phone, etc. Since,
    this
    is PII data we don't collect this information.
    3. Soil Sample Details like date of collection, farm location, size, etc.

    Args:
      front_table: A <table> tag containing the table at the back of the page-1.

    Returns:
      A dict mapping all extracted information to their corresponding values.
    """
    info = {}
    rows = front_table.find_all('tr')
    info['soil_health_card_number'] = parse_html_table_row(
        rows[4])[0]  # 5th row is Soil Health Card Number
    info['validity'] = parse_html_table_row(rows[5])[0]  # 6th row is validity
    #info['farmer_info'] = parse_html_table_rows_with_two_cols(
    #    rows[7:13])  # 8th to 13th is farmer details
    info['soil_sample_details'] = parse_html_table_rows_with_two_cols(
        rows[14:18])  # 15th to 18th is soil sample details
    return info

  def fetch_soil_test_resuls(self,
                             soil_test_result_table: Tag) -> Dict[str, str]:
    all_rows = soil_test_result_table.tbody.find_all('tr', recursive=False)
    return parse_html_table_rows_with_header_and_multi_cols(all_rows[1:])

  def process_page1_back(self, back_table: Tag) -> Dict[str, str]:
    """The back side of page-1 is expected to contain Soil Test Results and recommendation.

    Args:
      back_table: A <table> tag containing the table at the back of the page-1.

    Returns:
      A dict mapping all extracted information to their corresponding values.
    """
    info = {}
    all_rows = back_table.tbody.find_all('tr', recursive=False)
    info['soil_tests'] = self.fetch_soil_test_resuls(all_rows[9].find(
        'table').find('table'))  # last row is soil test results
    return info

  def process_page1(self, page1_table_row: Tag) -> Dict[str, str]:
    """Extracts information from page-1.

    Args:
      page1_table_row: A "tr" tag containing columns for front and back of
        page-1.

    Returns:
      A dict mapping all extracted information to their corresponding values.
    """
    page1 = collections.defaultdict()
    cols = page1_table_row.find_all('td', recursive=False)
    page1.update(self.process_page1_back(cols[2].find('table').find('table')))
    page1.update(
        self.process_page1_front(
            cols[5].find('table').find('table').find('table')))
    return page1

  def process_page2(self, page2_table_row: Tag) -> Dict[str, str]:
    """Extract information from page-2 (contains mostly recommendations).

    Args:
      page2_table_row: A <tr> tag containing all info in page-2.

    Returns:
      An empty dict as this page is not needed as of today.
    """
    return {}