"""Utility functions to parse HTML tables."""
from typing import Dict, List

from bs4 import Tag


def parse_html_table_row(row: Tag) -> List[str]:
  """Reads a "tr" and converts all it's non-empty columns to list."""
  vals = []
  for col in row.find_all("td"):
    if col.is_empty_element or col.div is None or col.div.is_empty_element:
      continue
    vals.append(str(col.div.string))
  return vals


def parse_html_table_rows_with_two_cols(rows: List[Tag]) -> Dict[str, str]:
  """Useful to process key-value kind of tables, like farmer details."""
  res = {}
  for row in rows:
    cols = parse_html_table_row(row)
    if len(cols) != 2:
      raise "not a two column row"
    res[cols[0]] = cols[1]
  return res


def parse_html_table_rows_with_header_and_multi_cols(
    rows: List[Tag]) -> List[Dict[str, str]]:
  """Convert tables with multiple columns and first row headers to list of dictionary."""
  row_dicts = []
  headers = []
  for row in rows:
    cols = parse_html_table_row(row)
    if not headers:
      headers = cols
      continue
    row_dict = {}
    for vals in zip(headers, cols):
      row_dict[vals[0]] = vals[1]
    row_dicts.append(row_dict)
  return row_dicts