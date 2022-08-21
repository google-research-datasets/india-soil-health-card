# Copyright 2022 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""This class is responsible to extract contents from a soil health card.

Sample Usage:
```
your_html_soil_health_card_path = '/cns/...'
ShcHtmlExtractor(html_file_path = your_html_soil_health_card_path).extract
```
"""

import collections
from typing import Dict

from bs4 import BeautifulSoup
from bs4 import Tag
from extractor.html_parser_utils import parse_html_table_row
from extractor.html_parser_utils import parse_html_table_rows_with_header_and_multi_cols
from extractor.html_parser_utils import parse_html_table_rows_with_two_cols
from extractor.html_parser_utils import parse_page2_table1
from extractor.html_parser_utils import parse_page2_table2
from extractor.html_parser_utils import parse_page2_fruits_table1
from extractor.html_parser_utils import parse_page2_fruits_table2

class ShcHtmlExtractor:
  """Extracts the information from a Soil Health Card HTML file."""

  def __init__(self, html_blob: str):
    self.html_blob = html_blob
    self._default = 'unknown'

  def extract(self) -> Dict[str, Dict[str, str]]:
    """Reads content of a HTML file and extracts the soil health card info.

    Returns:
      A dict mapping all extracted information to their corresponding values.
    """

    soup = BeautifulSoup(self.html_blob, 'html.parser')
    info = {}
    all_pages = soup.find(
        'div', attrs={
            'id': 'ReportViewer1_ctl09'
        }).find('table').find('table').find('table')
    all_pages_rows = all_pages.find_all('tr')
    info.update(self.process_page1(all_pages_rows[1]))
    info.update(
        self.process_page2(all_pages.tbody.find_all('tr', recursive=False)[3]))
    return info

  def process_page1_front(self, front_table: Tag) -> Dict[str, Dict[str, str]]:
    """The front side of page-1 is expected to contain the following information.

    1. soil health card number and it's validity.
    2. farmer's details like name, father's name, address phone, etc. Since,
    this
    is PII data we don't collect this information.
    3. Soil Sample Details like date of collection, farm location, size, etc.

    Args:
      front_table: A <table> tag containing the table at the front of page-1.

    Returns:
      A dict mapping all extracted information to their corresponding values.
    """
    info = {}
    rows = front_table.find_all('tr')
    info['details'] = {'soil_health_card_number': parse_html_table_row(
        rows[4])[0]}  # 5th row is Soil Health Card Number
    info['details'].update({'validity': parse_html_table_row(
        rows[5])[0]})  # 6th row is validity
    info['soil_sample_details'] = parse_html_table_rows_with_two_cols(
        rows[14:18])  # 15th to 18th is soil sample details
    return info

  def fetch_soil_test_resuls(
      self, soil_test_result_table: Tag) -> Dict[str, Dict[str, str]]:
    all_rows = soil_test_result_table.tbody.find_all('tr', recursive=False)

    info = {}
    soil_tests = parse_html_table_rows_with_header_and_multi_cols(all_rows[1:])

    default_num = 1
    for soil_test in soil_tests:
      param = self._get_param_name(soil_test.get('Parameter', 'notfound'))
      if param == self._default:
        info[f'soil_test_default_{default_num}'] = soil_test
        default_num += 1
      else:
        info[f'soil_test_{param}'] = soil_test

    return info

  def fetch_general_recommendations(
      self, general_recommendation_table: Tag) -> Dict[str, str]:
    row = parse_html_table_row(general_recommendation_table)

    if len(row) < 2:  # recommendations table is empty
      return {'': ''}
    elif len(row) == 2:  # only substance name but no recommendation given
      return {row[1]: ''}
    return {row[1]: row[2]}  # both substance and recommendation given

  def fetch_secondary_recommendations(
      self, secondary_recommendation_table: Tag) -> Dict[str, Dict[str, str]]:

    if secondary_recommendation_table.find('table') is None:
      return {}

    all_rows = secondary_recommendation_table.find('table').find(
        'table').tbody.find_all(
            'tr', recursive=False)[2].find('table').tbody.find_all(
                'tr', recursive=False)[1].find_all(
                    'td', recursive=False)

    soil_recoms = parse_html_table_rows_with_header_and_multi_cols(
        all_rows[1].find('table').find('table').tbody.find_all(
            'tr', recursive=False)[1:])
    spray_recoms = parse_html_table_rows_with_header_and_multi_cols(
        all_rows[3].find('table').find('table').tbody.find_all(
            'tr', recursive=False)[1:])

    info = {}
    info['secondary_recommendations_through_soil'] = {
        soil_recom.get('Parameter', ''): soil_recom.get('Through Soil', '')
        for soil_recom in soil_recoms
    }
    info['secondary_recommendations_through_spray'] = {
        soil_recom.get('Parameter', ''): spray_recom.get('Through Spray', '')
        for soil_recom, spray_recom in zip(soil_recoms, spray_recoms)
    }

    return info

  def _get_param_name(self, parameter: str) -> str:
    """Gets parameter symbol from full name description."""

    if parameter == 'notfound':
      return self._default

    words = parameter.split()
    # case where parameter is an empty string
    if not words:
      return self._default

    word = words[-1]
    return word[1:-1] if word[0] == '(' else word

  def process_page1_back(self, back_table: Tag) -> Dict[str, Dict[str, str]]:
    """The back side of page-1 is expected to contain Soil Test Results and recommendation.

    Args:
      back_table: A <table> tag containing the table at the back of the page-1.

    Returns:
      A dict mapping all extracted information to their corresponding values.
    """

    info = {}
    all_rows = back_table.tbody.find_all('tr', recursive=False)
    info['general_recommendations'] = self.fetch_general_recommendations(
        all_rows[4])
    info.update(self.fetch_secondary_recommendations(all_rows[1]))
    info['soil_test_results'] = {
        'soil_test_lab': parse_html_table_row(all_rows[7])[0],
        'soil_type': parse_html_table_row(all_rows[8])[0]
    }  # row 7 and 8 are testing lab and soil type respectively
    info.update(self.fetch_soil_test_resuls(all_rows[9].find('table').find(
        'table')))  # last row is soil test results

    return info

  def process_page1(self, page1_table_row: Tag) -> Dict[str, Dict[str, str]]:
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

  def process_page2(self, page2_table_row: Tag) -> Dict[str, Dict[str, str]]:
    """Extract information from page-2 (contains mostly recommendations).

    Args:
      page2_table_row: A <tr> tag containing all info in page-2.

    Returns:
      A dict mapping all extracted information to their corresponding values.
    """

    info = {}

    try:
      page2 = page2_table_row.find_all(
          'td', recursive=False)[2].find('table').find('table').tbody.find_all(
              'tr', recursive=False)[3].find_all(
                  'td', recursive=False)
      option1 = page2[1].find('table').find('table').tbody.find_all(
          'tr', recursive=False)[1].find('table').find('table').tbody.find_all(
              'tr', recursive=False)[2:]
      option2 = page2[3].find('table').find('table').tbody.find_all(
          'tr', recursive=False)[1].find('table').find('table').tbody.find_all(
              'tr', recursive=False)[2:]
      
      info['fertilizer_option_1'] = parse_page2_table1(option1)
      info['fertilizer_option_2'] = parse_page2_table2(option2)

    except:
      pass
      # print('page 2 upper failed')

    try:
      page2_lower = page2_table_row.find_all(
          'td', recursive=False)[2].find('table').find('table').tbody.find_all(
              'tr', recursive=False)[4].find_all(
                  'td', recursive=False)
      option1_fruits = page2_lower[1].find('table').find('table').tbody.find_all(
          'tr', recursive=False)[2].find('table').find('table').tbody.find_all(
              'tr', recursive=False)[1:]
      option2_fruits = page2_lower[3].find('table').find('table').tbody.find_all(
          'tr', recursive=False)[2].find('table').find('table').tbody.find_all(
              'tr', recursive=False)[1:]

      info['fertilizer_fruit_option_1'] = parse_page2_fruits_table1(option1_fruits)
      info['fertilizer_fruit_option_2'] = parse_page2_fruits_table2(option2_fruits)

    except:
      pass
      # print('fertilizer combinations for fruit/vegetable crops not present')

    return info
