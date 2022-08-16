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

"""Utility functions to parse HTML tables."""
from typing import Dict, List
from bs4 import Tag


def parse_html_table_row(row: Tag,
                         allow_empty_fields: bool = False) -> List[str]:
  """Reads a "tr" and converts all it's non-empty columns to list."""
  vals = []
  for col in row.find_all("td"):
    if col.is_empty_element or col.div is None or col.div.is_empty_element:
      if allow_empty_fields:
        vals.append("")
      continue
    vals.append(str(col.div.text).strip())
  return vals


def parse_html_table_rows_with_two_cols(rows: List[Tag]) -> Dict[str, str]:
  """Useful to process key-value kind of tables, like farmer details."""
  res = {}
  for row in rows:
    cols = parse_html_table_row(row)
    if len(cols) != 2:
      if len(cols) == 1:
        res[cols[0]] = ""
        continue
      raise "not a two column row"
    res[cols[0]] = cols[1]
  return res


def parse_html_table_rows_with_header_and_multi_cols(
    rows: List[Tag]) -> List[Dict[str, str]]:
  """Convert tables with multiple columns and first row headers to list of dictionary."""
  row_dicts = []
  headers = []
  for row in rows:
    cols = parse_html_table_row(row, allow_empty_fields=True)
    if not headers:
      headers = cols
      continue
    row_dict = {}
    for vals in zip(headers, cols):
      row_dict[vals[0]] = vals[1]
    row_dicts.append(row_dict)
  return row_dicts

def parse_page2_table1(rows: List[Tag]):
  """Convert page2 table 1 with multiple columns and first row headers to list of dictionary."""

  if not rows or len(rows) % 3 != 0:
    return {}

  header = [
      'Crop Variety', 'Organic Fertilizer & Quantity',
      'Bio Fertilizer & Quantity'
  ]

  outer = {}
  for i in range(0, len(rows), 3):
    row1 = parse_html_table_row(rows[i], allow_empty_fields=True)
    row2 = parse_html_table_row(rows[i + 1], allow_empty_fields=True)
    row3 = parse_html_table_row(rows[i + 2], allow_empty_fields=True)

    info = {}
    crop_variety = row2[2] + row3[2]
    info[header[0]] = crop_variety

    if row1[3]:
      info[row1[3]] = row1[5]
    if row2[3]:
      info[row2[3]] = row2[5]
    if row3[3]:
      info[row3[3]] = row3[5]

    info[header[1]] = row1[6] + row2[6] + row3[6]
    info[header[2]] = row1[7] + row2[7] + row3[7]

    outer[crop_variety] = str(info)

  return outer


def parse_page2_table2(rows: List[Tag]):
  """Convert page2 table 2 with multiple columns and first row headers to list of dictionary."""

  if not rows or len(rows) % 3 != 0:
    return {}

  header = [
      'Crop Variety', 'Organic Fertilizer & Quantity',
      'Bio Fertilizer & Quantity', 'Reference Yield'
  ]

  outer = {}
  for i in range(0, len(rows), 3):
    row1 = parse_html_table_row(rows[i], allow_empty_fields=True)
    row2 = parse_html_table_row(rows[i + 1], allow_empty_fields=True)
    row3 = parse_html_table_row(rows[i + 2], allow_empty_fields=True)

    info = {}
    crop_variety = row2[1] + row3[1]
    info[header[0]] = crop_variety

    if row1[2]:
      info[row1[2]] = row1[4]
    if row2[2]:
      info[row2[2]] = row2[4]
    if row3[2]:
      info[row3[2]] = row3[4]

    info[header[1]] = row1[5] + row2[5] + row3[5]
    info[header[2]] = row1[6] + row2[6] + row3[6]
    info[header[3]] = row1[7] + row2[7] + row3[7]

    outer[crop_variety] = str(info)

  return outer


def parse_page2_fruits_table1(rows: List[Tag]):
  """Convert page2 fruits table 1 with multiple columns and first row headers to list of dictionary."""

  if not rows or len(rows) % 3 != 0:
    return {}

  header = [
      'Crop Stage', 'Crop Variety', 'Organic Fertilizer & Quantity',
      'Bio Fertilizer & Quantity'
  ]

  outer = {}
  for i in range(0, len(rows), 3):
    row1 = parse_html_table_row(rows[i], allow_empty_fields=True)
    row2 = parse_html_table_row(rows[i + 1], allow_empty_fields=True)
    row3 = parse_html_table_row(rows[i + 2], allow_empty_fields=True)

    info = {}
    info[header[0]] = row1[1] + row2[1] + row3[1]
    crop_variety = row2[2] + row3[2]
    info[header[1]] = crop_variety

    if row1[3]:
      info[row1[3]] = row1[5]
    if row2[3]:
      info[row2[3]] = row2[5]
    if row3[3]:
      info[row3[3]] = row3[5]

    info[header[2]] = row1[6] + row2[6] + row3[6]
    info[header[3]] = row1[7] + row2[7] + row3[7]

    outer[crop_variety] = str(info)

  return outer


def parse_page2_fruits_table2(rows: List[Tag]):
  """Convert page2 fruits table 2 with multiple columns and first row headers to list of dictionary."""

  if not rows or len(rows) % 3 != 0:
    return {}

  header = [
      'Crop Variety', 'Organic Fertilizer & Quantity',
      'Bio Fertilizer & Quantity', 'Reference Yield'
  ]

  outer = {}
  for i in range(0, len(rows), 3):
    row1 = parse_html_table_row(rows[i], allow_empty_fields=True)
    row2 = parse_html_table_row(rows[i + 1], allow_empty_fields=True)
    row3 = parse_html_table_row(rows[i + 2], allow_empty_fields=True)

    info = {}
    crop_variety = row2[1] + row3[1]
    info[header[0]] = crop_variety

    if row1[2]:
      info[row1[2]] = row1[4]
    if row2[2]:
      info[row2[2]] = row2[4]
    if row3[2]:
      info[row3[2]] = row3[4]

    info[header[1]] = row1[5] + row2[5] + row3[5]
    info[header[2]] = row1[6] + row2[6] + row3[6]
    info[header[3]] = row1[7] + row2[7] + row3[7]

    outer[crop_variety] = str(info)

  return outer
