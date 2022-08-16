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

"""Pipeline between main file and html-extractor."""

import asyncio
import os
import os.path
from datetime import datetime
from google.cloud import spanner

from card_info_parser import CardInfoParser
from extractor.shc_html_extractor import ShcHtmlExtractor
import storage
from protos import card_pb2

import time


class CardExtractor:
  """Pipeline between main file and html-extractor."""

  def __init__(self, card: dict[str, str], india_shape=None):
    self.card = card
    self.cols = []
    self.vals = []
    self.india_shape = india_shape
    self.spanner_client = spanner.Client()
    self.spanner_instance_id = os.getenv('SPANNER_INSTANCE_ID', 0)
    self.spanner_database_id = os.getenv('SPANNER_DATABASE_ID', 0)
    self.instance = self.spanner_client.instance(self.spanner_instance_id)
    self.database = self.instance.database(self.spanner_database_id)

    # assert self.india_shape is not None

  def extract_card(self, overwrite) -> bool:
    """Extracts card info and stores it in the Cards_info spanner table.

    Returns:
      A bool value indicating if the extraction was successful.
    """
    t2 = time.time()

    # get file path 
    file_path = self.get_html_file_path()     
    t1, t2 = t2, time.time()
    print(f'got the path\ntime = {t2-t1}\n')

    # increase extract attempt number
    self.inc_extract_attempt()
    t1, t2 = t2, time.time()
    print(f'updated extract attempt\ntime = {t2-t1}\n')

    # get html blob
    html_blob = self.get_html_blob(file_path)
    t1, t2 = t2, time.time()
    print(f'got the html blob\ntime = {t2-t1}\n')
    
    # get card info (bs4)
    card_info = self.get_card_info(html_blob)
    t1, t2 = t2, time.time()
    print(f'got the card\ntime = {t2-t1}\n')
    
    # parse the card(regex)
    parsed_card = self.parse_card(card_info)
    t1, t2 = t2, time.time()
    print(f'parsed the card\ntime = {t2-t1}\n')

    # update the spanner table
    self.update_table()
    t1, t2 = t2, time.time()
    print(f'updated the table\ntime = {t2-t1}\n')

    # store json file on gcs
    self.upload_json(file_path, parsed_card)
    t1, t2 = t2, time.time()
    print(f'uploaded json to cloud\ntime = {t2-t1}\n')    
    
    return True
    

  def get_html_file_path(self):
    return storage.getFilePath(self.card['state_id'], self.card['district_id'],
                               self.card['mandal_id'],
                               self.card['village_id'], self.card['sample'],
                               self.card['sr_no'])

  def get_html_blob(self, html_file_path: str):
    """Fetches contents of html file."""
    contents = storage.downloadFile(html_file_path)
    return contents

  def get_card_info(self, html_blob: str) -> dict[str, dict[str, str]]:
    """Extracts card information from the html blob."""
    return ShcHtmlExtractor(html_blob).extract()

  def parse_card(self, card_info: dict[str, dict[str, str]]):
    """Adds a new entry in the cards_info spanner table."""

    card_parser = CardInfoParser(card_info, india_shape=self.india_shape)
    soil_sample_details = card_parser.get_soil_sample_details()
    soil_tests = card_parser.get_soil_tests()
    recommendations = card_parser.get_recommendations()
    fertilizer_combinations = card_parser.get_fertilizer_combinations()

    # village view
    self.cols = ['SampleNo', 'SrNo', 'VillageId', 'SubDistrictId', 'DistrictId', 'StateId']
    self.vals = [self.card.get('sample'), self.card.get('sr_no'), self.card.get('village_id'), self.card.get('mandal_id'), self.card.get('district_id'), self.card.get('state_id')]
    
    # soil sample details
    fields = soil_sample_details.ListFields()
    for field_descriptor, value in fields:
      field_name = field_descriptor.name
      self.cols.append(field_name)
      self.vals.append(value)
    
    # soil tests
    for parameter, soil_test in soil_tests.items():
      fields = soil_test.ListFields()
      for field_descriptor, value in fields:
        field_name = f'{parameter}_{field_descriptor.name}'
        self.cols.append(field_name)
        self.vals.append(value)
    
    # errors
    self.cols.append('error_log')
    self.vals.append(card_parser.get_error_log())
    
    # recommendations and fertilizer combinations
    self.cols.append('recommendations')
    self.vals.append(recommendations)
    self.cols.append('fertilizer_combinations')
    self.vals.append(fertilizer_combinations)

    return card_parser.get_full_card()

  def update_table(self):
    self.database.run_in_transaction(lambda transaction: transaction.insert_or_update(
      table='Cards_info',
      columns=self.cols,
      values=[self.vals],
    ))

  def upload_json(self, file_path, parsed_card):
    storage.uploadParsedCard(file_path, parsed_card)

  def is_card_extracted(self, overwrite=False):
    if overwrite:
      return False

    with self.database.snapshot() as snapshot:
      marked = snapshot.execute_sql(
        """SELECT Extracted, extract_attempt
        FROM Cards
        WHERE VillageId = @villageid
        AND Sample = @sample
        AND SrNo = @srno""",
        params={
          "villageid": self.card['village_id'],
          "sample": self.card['sample'],
          "srno": self.card['sr_no'],
        },
        param_types={
          "villageid": spanner.param_types.INT64,
          "sample": spanner.param_types.STRING,
          "srno": spanner.param_types.INT64,
        },
      )

    marked = list(marked)[0]
    if marked[0]:
      print('card already extracted')
      return True
    elif marked[1] >= 3:
      print('attempt limit reached')
      return True

    return False

  def inc_extract_attempt(self):
    self.database.run_in_transaction(lambda transaction: transaction.insert_or_update(
      table='Cards',
      columns = ['VillageId', 'Sample', 'SrNo', 'extract_attempt'],
      values = [
        [self.card['village_id'], self.card['sample'], self.card['sr_no'], self.card['extract_attempt']+1]
      ]
    ))

