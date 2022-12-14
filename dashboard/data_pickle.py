# imports

from google.cloud import spanner
from google.cloud import storage
from pprint import pprint
from bs4 import BeautifulSoup
import random
import pickle
import json
import re
import os
import math
import pandas as pd
import numpy as np
from matplotlib import pyplot as plt
from datetime import datetime as dtm
import matplotlib as mpl
import datetime
import gcsfs


print('fetching database...')
# fetching database

os.environ["GOOGLE_CLOUD_PROJECT"] = 'soil-health-card-india'
spanner_client = spanner.Client(project='soil-health-card-india')
spanner_instance_id = 'tfgen-spanid-20220525101635457'
spanner_database_id = 'metadata'
instance = spanner_client.instance(spanner_instance_id)
database = instance.database(spanner_database_id)


# fetching gcs bucket

storage_client = storage.Client()
bucket = storage_client.bucket('anthrokrishi-shcs')

pickle_file_path = 'pickle_files/pickleFile'

all_tables = {}

print('declaring all functions...')
# all functions

# downloads file from gcs
def download_file(file_path):
  blob = bucket.blob(file_path)
  contents = blob.download_as_string()
  return contents

###############################################################################################################

# upload to gcs
def upload_file(path, content):
  blob = bucket.blob(path)
  blob.upload_from_string(content)
  return

###############################################################################################################

# constructs file name from sample and srno
def get_file_name(sample, srno):
  if srno:
    return sample.replace('/', '-') + '_' + str(srno) + ')'
  else:
    return sample.replace('/', '-')

###############################################################################################################

# constructs file path from village view details
def get_file_path(stateid, districtid, subdistrictid, villageid, sample, srno, ext):
  if ext == 'json':
    file_prefix = 'gs://anthrokrishi-shcs/ExtractedCards'
  else:
    file_prefix = 'gs://anthrokrishi-shcs/shcs'

  return f"{file_prefix}/{stateid}/{districtid}/{subdistrictid}/{villageid}/{get_file_name(sample, srno)}.{ext}"

###############################################################################################################

# get only a part of the card

# get whole card
def get_card(blob):
  soup = BeautifulSoup(blob, 'html.parser')
  card = soup.find('div', attrs={'id': 'ReportViewer1_ctl09'}).find('table').find('table').find('table')
  return card

# get only the test table present on page1 back side
def get_test_table(blob):
  soup = BeautifulSoup(blob, 'html.parser')
  all_pages = soup.find('div', attrs={'id': 'ReportViewer1_ctl09'}).find('table').find('table').find('table')
  all_pages_rows = all_pages.tbody.find_all('tr', recursive=False)
  page1_back = all_pages_rows[1].find_all('td', recursive='False')[2].find('table').find('table').tbody.find_all('tr', recursive=False)
  # test_table = page1_back[9].find('table').find('table')
  test_table = page1_back[9]
  return test_table

# get whole page1 front side which has farmer and farm details
def get_page1_front(blob):
  soup = BeautifulSoup(blob, 'html.parser')
  all_pages = soup.find('div', attrs={'id': 'ReportViewer1_ctl09'}).find('table').find('table').find('table')
  all_pages_rows = all_pages.tbody.find_all('tr', recursive=False)
  page1_front = all_pages_rows[1].find_all('td', recursive=False)[5]
  # page2 = all_pages_rows[3]
  return page1_front

# get page 2 of the card which has fertilizer combinations
def get_page2(blob):
  soup = BeautifulSoup(blob, 'html.parser')
  all_pages = soup.find('div', attrs={'id': 'ReportViewer1_ctl09'}).find('table').find('table').find('table')
  all_pages_rows = all_pages.tbody.find_all('tr', recursive=False)
  page2 = all_pages_rows[3]
  return page2

###############################################################################################################

def get_card_from_shc_no(shc_no):
  with database.snapshot() as snapshot:
    data = snapshot.execute_sql(
      f"""SELECT StateId, DistrictId, SubDistrictId, VillageId, SampleNo, SrNo
      FROM Cards_info
      WHERE soil_health_card_number = '{shc_no}'
      """
    )

  data = list(data)
  data = data[0]

  file_path = get_file_path(data[0], data[1], data[2], data[3], data[4], data[5], 'html')
  blob = download_file(file_path)

  return blob

###############################################################################################################

def get_card_info_from_shc_no(shc_no):
  with database.snapshot() as snapshot:
    data = snapshot.execute_sql(
      f"""SELECT StateId, DistrictId, SubDistrictId, VillageId, SampleNo, SrNo
      FROM Cards_info
      WHERE soil_health_card_number = '{shc_no}'
      """
    )

  data = list(data)
  data = data[0]

  file_path = get_file_path(data[0], data[1], data[2], data[3], data[4], data[5], 'json')
  card_info = download_file(file_path)

  return card_info


print('getting statewise progress for scraping/extraction...')
# get scraping/extraction progress statewise

with database.snapshot() as snapshot:
  state_names = snapshot.execute_sql(
      """SELECT stateid, name
      FROM States
      ORDER BY 1"""
  )
state_names = list(state_names)
state_names.append([0,'All India'])
state_names.sort(key=lambda row: (row[0]))

###############################################################################################################

with database.snapshot() as snapshot:
  num_cards = snapshot.execute_sql(
      """SELECT stateid, COUNT(*)
      FROM Cards
      GROUP BY 1
      ORDER BY 1"""
  )
num_cards = list(num_cards)

###############################################################################################################

with database.snapshot() as snapshot:
  num_scraped = snapshot.execute_sql(
      """SELECT stateid, COUNT(*)
      FROM Cards
      WHERE ingested is true
      GROUP BY 1
      ORDER BY 1"""
  )
num_scraped = list(num_scraped)

###############################################################################################################

with database.snapshot() as snapshot:
  num_extracted = snapshot.execute_sql(
      """SELECT stateid, COUNT(*)
      FROM Cards_info
      GROUP BY 1
      ORDER BY 1"""
  )
num_extracted = list(num_extracted)

###############################################################################################################

progress = {}
for state in state_names:
  progress[state[0]] = {}
  progress[state[0]]['stateid'] = state[0]
  progress[state[0]]['state'] = state[1]
  progress[state[0]]['num_cards'] = 0
  progress[state[0]]['num_scraped'] = 0
  progress[state[0]]['num_extracted'] = 0

for card in num_cards:
  progress[card[0]]['num_cards'] = card[1]
  progress[0]['num_cards'] += card[1]

for card in num_scraped:
  progress[card[0]]['num_scraped'] = card[1]
  progress[0]['num_scraped'] += card[1]

for card in num_extracted:
  progress[card[0]]['num_extracted'] = card[1]
  progress[0]['num_extracted'] += card[1]

progress = pd.DataFrame(progress).T
progress = progress.sort_values('stateid')

#------------------------------------------------------
all_tables['state_names'] = state_names
all_tables['progress'] = progress
print('got progress')
#------------------------------------------------------


# Get all missing data shcs
print('getting all shc numbers with missing stuff...')
# all shcs
# with database.snapshot() as snapshot:
#   all_shcs = snapshot.execute_sql(
#       """SELECT soil_health_card_number, StateId, DistrictId, SubDistrictId, VillageId, SampleNo, SrNo
#       FROM Cards_info
#       """
#   )

# all_shcs = list(all_shcs)
# all_shcs = {x[0]: [x[1:]] for x in all_shcs}

###############################################################################################################

# shcs where all test values are missing
with database.snapshot() as snapshot:
  shcs_all_test_value_missing = snapshot.execute_sql(
    """SELECT soil_health_card_number, StateId, DistrictId, SubDistrictId, VillageId, SampleNo, SrNo
    FROM Cards_info
    WHERE pH_value is null
    AND EC_value is null
    AND OC_value is null
    AND N_value is null
    AND P_value is null
    AND K_value is null
    AND S_value is null
    AND Zn_value is null
    AND B_value is null
    AND Fe_value is null
    AND Mn_value is null
    AND Cu_value is null
    LIMIT 100
    """
  )

shcs_all_test_value_missing = list(shcs_all_test_value_missing)
# shcs_all_test_value_missing = [x[0] for x in shcs_all_test_value_missing]

###############################################################################################################

# shcs where atleast one test value is missing
with database.snapshot() as snapshot:
  shcs_any_test_value_missing = snapshot.execute_sql(
    """SELECT soil_health_card_number, StateId, DistrictId, SubDistrictId, VillageId, SampleNo, SrNo
    FROM Cards_info
    WHERE pH_value is null
    OR EC_value is null
    OR OC_value is null
    OR N_value is null
    OR P_value is null
    OR K_value is null
    OR S_value is null
    OR Zn_value is null
    OR B_value is null
    OR Fe_value is null
    OR Mn_value is null
    OR Cu_value is null
    LIMIT 100
    """
  )

shcs_any_test_value_missing = list(shcs_any_test_value_missing)
# shcs_any_test_value_missing = [x[0] for x in shcs_any_test_value_missing]

###############################################################################################################

# shcs where all normal levels are missing
with database.snapshot() as snapshot:
  shcs_all_normal_level_missing = snapshot.execute_sql(
    """SELECT soil_health_card_number, StateId, DistrictId, SubDistrictId, VillageId, SampleNo, SrNo
    FROM Cards_info
    WHERE pH_min_normal_level is null
    AND EC_min_normal_level is null
    AND OC_min_normal_level is null
    AND N_min_normal_level is null
    AND P_min_normal_level is null
    AND K_min_normal_level is null
    AND S_min_normal_level is null
    AND Zn_min_normal_level is null
    AND B_min_normal_level is null
    AND Fe_min_normal_level is null
    AND Mn_min_normal_level is null
    AND Cu_min_normal_level is null
    LIMIT 100
    """
  )

shcs_all_normal_level_missing = list(shcs_all_normal_level_missing)
# shcs_all_normal_level_missing = [x[0] for x in shcs_all_normal_level_missing]

###############################################################################################################

# shcs where atleast one normal level is missing
with database.snapshot() as snapshot:
  shcs_any_normal_level_missing = snapshot.execute_sql(
    """SELECT soil_health_card_number, StateId, DistrictId, SubDistrictId, VillageId, SampleNo, SrNo
    FROM Cards_info
    WHERE pH_min_normal_level is null
    OR EC_min_normal_level is null
    OR OC_min_normal_level is null
    OR N_min_normal_level is null
    OR P_min_normal_level is null
    OR K_min_normal_level is null
    OR S_min_normal_level is null
    OR Zn_min_normal_level is null
    OR B_min_normal_level is null
    OR Fe_min_normal_level is null
    OR Mn_min_normal_level is null
    OR Cu_min_normal_level is null
    LIMIT 100
    """
  )

shcs_any_normal_level_missing = list(shcs_any_normal_level_missing)
# shcs_any_normal_level_missing = [x[0] for x in shcs_any_normal_level_missing]

###############################################################################################################

# shcs where geoposition is missing
with database.snapshot() as snapshot:
  shcs_geopos_missing = snapshot.execute_sql(
    """SELECT soil_health_card_number, StateId, DistrictId, SubDistrictId, VillageId, SampleNo, SrNo
    FROM Cards_info
    where latitude is null
    LIMIT 100
    """
  )

shcs_geopos_missing = list(shcs_geopos_missing)
# shcs_geopos_missing = [x[0] for x in shcs_geopos_missing]

#------------------------------------------------------
# all_tables['all_shcs'] = all_shcs
all_tables['shcs_all_test_value_missing'] = shcs_all_test_value_missing
all_tables['shcs_all_normal_value_missing'] = shcs_all_normal_level_missing
all_tables['shcs_any_normal_level_missing'] = shcs_any_normal_level_missing
all_tables['shcs_any_test_value_missing'] = shcs_any_test_value_missing
all_tables['shcs_geopos_missing'] = shcs_geopos_missing
print('got all missing shcs etc')
#------------------------------------------------------


# Get srno1 scraping progress
print('getting progress for srno 1 cards...')
progress_srno1 = {}

with database.snapshot() as snapshot:
  data1 = snapshot.execute_sql(
      """SELECT stateid, count(*)
      FROM Cards
      WHERE SrNo=1
      group by 1 order by 1"""
  )

data1 = list(data1)
data1 = {x[0]:x[1] for x in data1}

with database.snapshot() as snapshot:
  data2 = snapshot.execute_sql(
      """SELECT stateid, count(*)
      FROM Cards
      WHERE ingested is true
      AND SrNo=1
      group by 1 order by 1"""
  )

data2 = list(data2)
data2 = {x[0]:x[1] for x in data2}

with database.snapshot() as snapshot:
  data3 = snapshot.execute_sql(
      """SELECT stateid, count(*)
      FROM Cards
      WHERE ingested is true and extracted is true
      AND SrNo=1
      group by 1 order by 1"""
  )

data3 = list(data3)
data3 = {x[0]:x[1] for x in data3}

for stnm in state_names:
  progress_srno1[stnm[0]] = {}
  progress_srno1[stnm[0]]['state_id'] = stnm[0]
  progress_srno1[stnm[0]]['state'] = stnm[1]
  progress_srno1[stnm[0]]['num_cards'] = data1.get(stnm[0], 0)
  progress_srno1[stnm[0]]['num_scraped'] = data2.get(stnm[0], 0)
  progress_srno1[stnm[0]]['num_extracted'] = data3.get(stnm[0], 0)

progress_srno1[0]['num_cards'] = sum(list(data1.values()))
progress_srno1[0]['num_scraped'] = sum(list(data2.values()))
progress_srno1[0]['num_extracted'] = sum(list(data3.values()))

progress_srno1 = pd.DataFrame(progress_srno1).T.sort_values('state_id')

#------------------------------------------------------
all_tables['progress_srno1'] = progress_srno1
print('got progress for srno 1 cards')
#------------------------------------------------------


print('getting missing test values stats...')
# missing test values stats
parameters = ['pH_value', 'EC_value', 'OC_value', 'N_value', 'P_value', 'K_value', 'S_value', 'Zn_value', 'B_value', 'Fe_value', 'Mn_value', 'Cu_value']

test_values_missing_statewise = {}
num_cards_extracted_statewise = {}

for parameter in parameters:
  with database.snapshot() as snapshot:
    data = snapshot.execute_sql(
        f"""SELECT stateid, count(*) 
        FROM Cards_info
        WHERE {parameter} is null
        GROUP BY 1 ORDER BY 1"""
    )

  data = list(data)

  test_values_missing_statewise[parameter] = {x[0]: x[1] for x in data}

for k,v in test_values_missing_statewise.items():
  a = sum(list(v.values()))
  test_values_missing_statewise[k][0] = a  

table_tval_missing = []
stateids = sorted([x[0] for x in state_names])

for param in parameters:
  row = []
  for stateid in stateids:
    row.append(round(test_values_missing_statewise[param].get(stateid, 0)/max(1, progress['num_extracted'][stateid])*100, 2))
  table_tval_missing.append(row)

#------------------------------------------------------
all_tables['table_tval_missing'] = table_tval_missing
print('got test value missing table')
#------------------------------------------------------


print('getting statewise missing data stats...')
# missing data stats
with database.snapshot() as snapshot:
  data = snapshot.execute_sql(
      """SELECT stateid, count(*)
      FROM Cards_info
      WHERE pH_value is Null
      OR EC_value is Null
      OR OC_value is Null
      OR N_value is Null
      OR P_value is Null
      OR K_value is Null
      OR S_value is Null
      OR Zn_value is Null
      OR B_value is Null
      OR Fe_value is Null
      OR Mn_value is Null
      OR Cu_value is Null
      GROUP BY 1 ORDER BY 1"""
  )
tval_missing_cnt = list(data)
tval_missing_cnt.append([0, sum(x[1] for x in tval_missing_cnt)])
tval_missing_cnt.sort()
tval_missing_cnt = {x[0]: x[1] for x in tval_missing_cnt}

#######################################################################################################################

with database.snapshot() as snapshot:
  data = snapshot.execute_sql(
      """SELECT stateid, count(*)
      FROM Cards_info
      WHERE latitude is null
      OR longitude is null
      GROUP BY 1 ORDER BY 1"""
  )
geopos_missing_cnt = list(data)
geopos_missing_cnt.append([0, sum(x[1] for x in geopos_missing_cnt)])
geopos_missing_cnt.sort()
geopos_missing_cnt = {x[0]: x[1] for x in geopos_missing_cnt}

#######################################################################################################################

with database.snapshot() as snapshot:
  data = snapshot.execute_sql(
      """SELECT stateid, count(*)
      FROM Cards_info
      WHERE farm_size is null
      OR farm_size_unit is null
      OR CHAR_LENGTH(farm_size_unit) = 0
      GROUP BY 1 ORDER BY 1"""
  )
farmsize_missing_cnt = list(data)
farmsize_missing_cnt.append([0, sum(x[1] for x in farmsize_missing_cnt)])
farmsize_missing_cnt.sort()
farmsize_missing_cnt = {x[0]: x[1] for x in farmsize_missing_cnt}

#######################################################################################################################

with database.snapshot() as snapshot:
  data = snapshot.execute_sql(
      """SELECT stateid, count(*)
      FROM Cards_info
      WHERE CHAR_LENGTH(soil_type) = 0
      GROUP BY 1 ORDER BY 1"""
  )
soiltype_missing_cnt = list(data)
soiltype_missing_cnt.append([0, sum(x[1] for x in soiltype_missing_cnt)])
soiltype_missing_cnt.sort()
soiltype_missing_cnt = {x[0]: x[1] for x in soiltype_missing_cnt}

table_data_missing = []
table_data_missing.append([round(tval_missing_cnt.get(x, 0)/max(1, progress['num_extracted'][x])*100, 2) for x in stateids])
table_data_missing.append([round(geopos_missing_cnt.get(x, 0)/max(1, progress['num_extracted'][x])*100, 2) for x in stateids])
table_data_missing.append([round(farmsize_missing_cnt.get(x, 0)/max(1, progress['num_extracted'][x])*100, 2) for x in stateids])
table_data_missing.append([round(soiltype_missing_cnt.get(x, 0)/max(1, progress['num_extracted'][x])*100, 2) for x in stateids])

#------------------------------------------------------
all_tables['table_data_missing'] = table_data_missing
print('got statewise missing data counts table')
#------------------------------------------------------


print('getting srno1 progress card count...')

with database.snapshot() as snapshot:
  total_srno1_cards = snapshot.execute_sql(
      """SELECT COUNT(*)
      FROM Cards
      WHERE SrNo=1"""
  )
total_srno1_cards = list(total_srno1_cards)[0][0]

with database.snapshot() as snapshot:
  current_srno1_cards = snapshot.execute_sql(
      """SELECT COUNT(*)
      FROM Cards
      WHERE Ingested is true
      AND SrNo=1"""
  )
current_srno1_cards = list(current_srno1_cards)[0][0]

current_time = dtm.now()

# add data point to existing database

#------------------------------------------------------
all_tables['total_srno1_cards'] = total_srno1_cards
all_tables['current_srno1_cards'] = current_srno1_cards
print('got srno1 total and current count')
#------------------------------------------------------


print('getting labwise missing data stats...')
# labwise missing data stats

with database.snapshot() as snapshot:
  all_labs = snapshot.execute_sql(
    """SELECT soil_test_lab, COUNT(*)
    FROM Cards_info
    GROUP BY 1 ORDER BY 1"""
  )
all_labs = list(all_labs)

######################################################################################################

with database.snapshot() as snapshot:
  tval_missing_labwise = snapshot.execute_sql(
    """SELECT soil_test_lab, COUNT(*)
    FROM Cards_info
    WHERE pH_value is Null
    OR EC_value is Null
    OR OC_value is Null
    OR N_value is Null
    OR P_value is Null
    OR K_value is Null
    OR S_value is Null
    OR Zn_value is Null
    OR B_value is Null
    OR Fe_value is Null
    OR Mn_value is Null
    OR Cu_value is Null
    GROUP BY 1 ORDER BY 1"""
  )
tval_missing_labwise = list(tval_missing_labwise)
tval_missing_labwise = {x[0]:x[1] for x in tval_missing_labwise}

######################################################################################################

with database.snapshot() as snapshot:
  farmsize_missing_labwise = snapshot.execute_sql(
    """SELECT soil_test_lab, COUNT(*)
    FROM Cards_info
    WHERE CHAR_LENGTH(farm_size_unit) = 0
    OR farm_size_unit is NULL
    OR farm_size is NULL
    GROUP BY 1 ORDER BY 1"""
  )
farmsize_missing_labwise = list(farmsize_missing_labwise)
farmsize_missing_labwise = {x[0]:x[1] for x in farmsize_missing_labwise}

######################################################################################################

with database.snapshot() as snapshot:
  geopos_missing_labwise = snapshot.execute_sql(
    """SELECT soil_test_lab, COUNT(*)
    FROM Cards_info
    WHERE latitude is NULL
    OR longitude is NULL
    GROUP BY 1 ORDER BY 1"""
  )
geopos_missing_labwise = list(geopos_missing_labwise)
geopos_missing_labwise = {x[0]:x[1] for x in geopos_missing_labwise}

######################################################################################################

with database.snapshot() as snapshot:
  soiltype_missing_labwise = snapshot.execute_sql(
    """SELECT soil_test_lab, COUNT(*)
    FROM Cards_info
    WHERE soil_type is NULL
    OR CHAR_LENGTH(soil_type) = 0
    GROUP BY 1 ORDER BY 1"""
  )
soiltype_missing_labwise = list(soiltype_missing_labwise)
soiltype_missing_labwise = {x[0]:x[1] for x in soiltype_missing_labwise}

######################################################################################################

with database.snapshot() as snapshot:
  any_missing_labwise = snapshot.execute_sql(
    """SELECT soil_test_lab, COUNT(*)
    FROM Cards_info
    WHERE pH_value is Null
    OR EC_value is Null
    OR OC_value is Null
    OR N_value is Null
    OR P_value is Null
    OR K_value is Null
    OR S_value is Null
    OR Zn_value is Null
    OR B_value is Null
    OR Fe_value is Null
    OR Mn_value is Null
    OR Cu_value is Null
    OR CHAR_LENGTH(farm_size_unit) = 0
    OR farm_size_unit is NULL
    OR farm_size is NULL
    OR latitude is NULL
    OR longitude is NULL
    OR soil_type is NULL
    OR CHAR_LENGTH(soil_type) = 0
    GROUP BY 1 ORDER BY 1"""
  )
any_missing_labwise = list(any_missing_labwise)
any_missing_labwise = {x[0]:x[1] for x in any_missing_labwise}

######################################################################################################

missing_data_labwise = {}
for lab in all_labs:
  missing_data_labwise[lab[0]] = {}
  missing_data_labwise[lab[0]]['num_cards'] = lab[1]
  missing_data_labwise[lab[0]]['test_values'] = int(tval_missing_labwise.get(lab[0], 0))
  missing_data_labwise[lab[0]]['geoposition'] = int(geopos_missing_labwise.get(lab[0], 0))
  missing_data_labwise[lab[0]]['soil_type'] = int(soiltype_missing_labwise.get(lab[0], 0))
  missing_data_labwise[lab[0]]['farm_size'] = int(farmsize_missing_labwise.get(lab[0], 0))
  missing_data_labwise[lab[0]]['any missing'] = int(any_missing_labwise.get(lab[0], 0))
  missing_data_labwise[lab[0]]['any missing(%)'] = round(any_missing_labwise.get(lab[0], 0)/max(lab[1],1)*100, 2)

missing_data_labwise = pd.DataFrame(missing_data_labwise).T

#------------------------------------------------------
all_tables['missing_data_labwise'] = missing_data_labwise
print('got labwise missing data table')
#------------------------------------------------------


print('getting dates for burndown...')
# burndown chart stats update
with database.snapshot() as snapshot:
  data = snapshot.execute_sql(
      """SELECT CAST(ingestedat as DATE), count(*)
      FROM Cards
      where srno = 1
      AND ingestedat is not null
      group by 1 order by 1 desc"""
  )
data = list(data)

with database.snapshot() as snapshot:
  data1 = snapshot.execute_sql(
      """SELECT count(*)
      FROM Cards
      where srno = 1
      AND not ingested is true"""
  )
data1 = list(data1)[0][0]

with database.snapshot() as snapshot:
  data2 = snapshot.execute_sql(
      """SELECT CAST(ingestedat as DATE), count(*)
      FROM Cards
      where ingestedat is not null
      group by 1 order by 1 desc"""
  )
data2 = list(data2)

with database.snapshot() as snapshot:
  data3 = snapshot.execute_sql(
      """SELECT count(*)
      FROM Cards
      where not ingested is true"""
  )
data3 = list(data3)[0][0]

burndown_srno1 = []
for item in data:
  burndown_srno1.append([item[0], item[1]+data1])
  data1 += item[1]

burndown_all = []
for item in data2:
  burndown_all.append([item[0], item[1]+data3])
  data3 += item[1]


#------------------------------------------------------
all_tables['burndown_srno1'] = burndown_srno1
all_tables['burndown_all'] = burndown_all
print('burndown done')
#------------------------------------------------------

######################################################################################################

# data comparison mismatch
print('getting cards with data comparison mismatch')

with database.snapshot() as snapshot:
  data1 = snapshot.execute_sql(
    """SELECT Cards.data_comparison_mismatch, Cards.StateId, Cards.DistrictId, Cards.SubDistrictId, Cards.VillageId, Cards.Sample, Cards.SrNo
    FROM Cards
    INNER JOIN Cards_info
    ON Cards.VillageId = Cards_info.VillageId AND Cards.Sample = Cards_info.SampleNo AND Cards.SrNo = Cards_Info.SrNo
    WHERE data_comparison_mismatch IS NOT NULL
    AND (REGEXP_CONTAINS(data_comparison_mismatch, r'soilsampledtls') IS TRUE
    OR REGEXP_CONTAINS(data_comparison_mismatch, r'shcdtls') IS TRUE
    OR REGEXP_CONTAINS(data_comparison_mismatch, r'soiltestresults') IS TRUE
    OR REGEXP_CONTAINS(data_comparison_mismatch, r'recommendations') IS TRUE)"""
  )

with database.snapshot() as snapshot:
  data2 = snapshot.execute_sql(
    """SELECT Cards.data_comparison_mismatch, Cards.StateId, Cards.DistrictId, Cards.SubDistrictId, Cards.VillageId, Cards.Sample, Cards.SrNo
    FROM Cards
    INNER JOIN Cards_info
    ON Cards.VillageId = Cards_info.VillageId AND Cards.Sample = Cards_info.SampleNo AND Cards.SrNo = Cards_Info.SrNo
    WHERE data_comparison_mismatch IS NOT NULL
    AND (REGEXP_CONTAINS(data_comparison_mismatch, r'cropsfertoption1') IS TRUE
    OR REGEXP_CONTAINS(data_comparison_mismatch, r'cropsfertoption2') IS TRUE)"""
  )

  data1 = list(data1)
  data2 = list(data2)

#------------------------------------------------------
  all_tables['data_comparison_mismatch_page1'] = data1
  all_tables['data_comparison_mismatch_page2'] = data2
  print('data comparison mismatch done')
#------------------------------------------------------



######################################################################################################

print('pickling all tables and dumping on gcs...')
# pickle and upload to gcs
fs = gcsfs.GCSFileSystem(project='soil-health-card-india')
with fs.open('anthrokrishi-shcs/pickle_files/pickleFile', 'wb') as file:
  pickle.dump(all_tables, file)

print('all done see you in an hour!')
