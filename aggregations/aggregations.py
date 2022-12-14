from google.cloud import spanner
import random
import math
import numpy as np
import os

spanner_client = spanner.Client()
# Retrieve Job-defined env var
if "CLOUD_RUN_TASK_INDEX" in os.environ:
  TASK_INDEX = os.getenv("CLOUD_RUN_TASK_INDEX", 0)
elif "JOB_COMPLETION_INDEX" in os.environ:
  TASK_INDEX = os.getenv("JOB_COMPLETION_INDEX", 0)
elif "TASK_INDEX" in os.environ:
  TASK_INDEX = os.getenv("TASK_INDEX", 0)
else:
  TASK_INDEX = "0"
if "CLOUD_RUN_TASK_COUNT" in os.environ:
  TASK_COUNT = os.getenv("CLOUD_RUN_TASK_COUNT", 0)
elif "TASK_COUNT" in os.environ:
  TASK_COUNT = os.getenv("TASK_COUNT", 0)
else:
  TASK_COUNT = "1"
MODE = os.getenv("MODE", 0)
SPANNER_INSTANCE_ID = os.getenv("SPANNER_INSTANCE_ID", 0)
SPANNER_DATABASE_ID = os.getenv("SPANNER_DATABASE_ID", 0)
instance = spanner_client.instance(SPANNER_INSTANCE_ID)
database = instance.database(SPANNER_DATABASE_ID)

TASK_INDEX = int(TASK_INDEX)


parameters = ['pH', 'EC', 'OC', 'N', 'P', 'K', 'S', 'Zn', 'B', 'Fe', 'Mn', 'Cu']
irrigation_methods = ['Irrigated(Borewell)', 'Irrigated(Canal)', 'Irrigated(Pond)', 'Irrigated(Tube well)', 'Irrigated(Well)', 'Rainfed']
iterations = 10000


def fn1(param):
  with database.snapshot() as snapshot:
    data = snapshot.execute_sql(
      """SELECT districtid, count(*)
      from Cards_info
      group by 1
      order by 1"""
    )
  data = list(data)

  district_counts = {x[0]: x[1] for x in data}

  print('got the villages')

  ###################################################################################################################

  cols = ['district_id', 'card_count', f'{param}_notnull_count', f'{param}_avg', f'{param}_bootstrapped_avg']
  vals = []

  with database.snapshot() as snapshot:
    data = snapshot.execute_sql(
      f"""SELECT districtid, {param}_value
      from Cards_info
      where {param}_value is not null
      order by 1"""
    )
  data = list(data)

  print('got the data') 

  ###################################################################################################################

  values_districtwise = {}
  total_cnt = 0
  for x in district_counts.keys():
    values_districtwise[x] = []

  for x in data:
    values_districtwise[x[0]].append(x[1])

  values_districtwise_np = {}
  for district, values in values_districtwise.items():
    values_districtwise_np[district] = np.array(values)
    total_cnt += len(values_districtwise_np[district])

  print(f'total cnt: {total_cnt}')

  ###################################################################################################################

  cnt = 0
  district_cnt = 0
  for district, values in values_districtwise_np.items():
    if len(values) == 0:
      continue

    district_cnt += 1
    cnt += len(values)
    avgs = []
    for _ in range(iterations):
      # sample = random.choices(values, k=len(values))
      sample = np.random.choice(values, size=len(values), replace=True)
      # total = math.fsum(sample)
      total = np.sum(sample)
      avgs.append(total/len(values))

    bootstrapped_avg = math.fsum(avgs)/iterations
    avg = math.fsum(values)/len(values)

    vals.append([district, district_counts[district], len(values), avg, bootstrapped_avg])

    print(f'{[district, district_counts[district], len(values), avg, bootstrapped_avg]}, total cards done: {cnt}/{total_cnt}')

    if len(vals) == 100:
      database.run_in_transaction(lambda transaction: transaction.insert_or_update(
        table='district_aggregations',
        columns=cols,
        values=vals,
      ))

      vals = []
      print(f'added averages for {district_cnt} districts')

  ###################################################################################################################

  if len(vals) > 0:
    database.run_in_transaction(lambda transaction: transaction.insert_or_update(
      table='district_aggregations',
      columns=cols,
      values=vals,
    ))
    print(f'added averages for {district_cnt} districts')
  print(f'\nadded {param} averages to table\n')



def fn2(param):
  with database.snapshot() as snapshot:
    village_counts = snapshot.execute_sql(
      """SELECT Cards_info.villageid, Villages.Name, count(*)
      from Cards_info
      inner join Villages
      on Villages.VillageId = Cards_info.VillageId
      group by 1, 2
      order by 1"""
    )
  village_counts = list(village_counts)

  print('got the villages')

  ####################################################################################################################

  cols = ['village_id', f'{param}_notnull_count', f'{param}_avg', f'{param}_bootstrapped_avg']
  vals = []

  with database.snapshot() as snapshot:
    data = snapshot.execute_sql(
      f"""SELECT villageid, {param}_value
      from Cards_info
      where {param}_value is not null
      order by 1"""
    )
  data = list(data)

  print('got the data') 

  ####################################################################################################################

  values_villagewise = {}
  total_cnt = 0

  for x in village_counts:
    values_villagewise[x[0]] = []

  for x in data:
    values_villagewise[x[0]].append(x[1])

  values_villagewise_np = {}
  for village, values in values_villagewise.items():
    values_villagewise_np[village] = np.array(values)
    total_cnt += len(values_villagewise_np[village])

  print(f'total cnt: {total_cnt}')

  ####################################################################################################################

  cnt = 0
  village_cnt = 0
  for village, values in values_villagewise_np.items():
    if len(values) == 0:
      continue

    village_cnt += 1
    cnt += len(values)
    avgs = []
    for _ in range(iterations):
      # sample = random.choices(values, k=len(values))
      sample = np.random.choice(values, size=len(values), replace=True)
      # total = math.fsum(sample)
      total = np.sum(sample)
      avgs.append(total/len(values))

    bootstrapped_avg = math.fsum(avgs)/iterations
    avg = math.fsum(values)/len(values)

    vals.append([village, len(values), avg, bootstrapped_avg])

    print(f'{[village, len(values), avg, bootstrapped_avg]}, total cards done: {cnt}/{total_cnt}')

    if len(vals) == 1000:
      database.run_in_transaction(lambda transaction: transaction.insert_or_update(
        table='village_aggregations',
        columns=cols,
        values=vals,
      ))

      vals = []
      print(f'added averages for {village_cnt} villages')

  ####################################################################################################################

  if len(vals) > 0:
    database.run_in_transaction(lambda transaction: transaction.insert_or_update(
      table='village_aggregations',
      columns=cols,
      values=vals,
    ))

    print(f'added averages for {village_cnt} villages')

  print(f'\nadded {param} averages to table\n')



def fn3(param):
  pass



def fn4(param, irrigate_method):
  with database.snapshot() as snapshot:
    data = snapshot.execute_sql(
      """SELECT districtid
      from Districts"""
    )
  data = list(data)
  district_ids = [x[0] for x in data]

  print('got the districts')

  ###################################################################################################################

  cols = ['district_id', 'parameter', 'irrigation_method', 'card_count', 'average', f'bootstrapped_average']
  vals = []

  with database.snapshot() as snapshot:
    data = snapshot.execute_sql(
      f"""SELECT districtid, {param}_value
      from Cards_info
      where {param}_value is not null
      and irrigation_method = "{irrigate_method}"
      order by 1"""
    )
  data = list(data)

  print('got the data') 

  ###################################################################################################################

  values_districtwise = {}
  total_cnt = 0
  for x in district_ids:
    values_districtwise[x] = []

  for x in data:
    values_districtwise[x[0]].append(x[1])

  values_districtwise_np = {}
  for district, values in values_districtwise.items():
    values_districtwise_np[district] = np.array(values)
    total_cnt += len(values_districtwise_np[district])

  print(f'total cnt: {total_cnt}')

  ###################################################################################################################

  cnt = 0
  district_cnt = 0
  for district, values in values_districtwise_np.items():
    if len(values) == 0:
      continue

    district_cnt += 1
    cnt += len(values)
    avgs = []
    for _ in range(iterations):
      # sample = random.choices(values, k=len(values))
      sample = np.random.choice(values, size=len(values), replace=True)
      # total = math.fsum(sample)
      total = np.sum(sample)
      avgs.append(total/len(values))

    bootstrapped_avg = math.fsum(avgs)/iterations
    avg = math.fsum(values)/len(values)

    vals.append([district, param, irrigate_method, len(values), avg, bootstrapped_avg])

    print(f'{[district, param, irrigate_method, len(values), avg, bootstrapped_avg]}, total cards done: {cnt}/{total_cnt}')

    if len(vals) == 100:
      database.run_in_transaction(lambda transaction: transaction.insert_or_update(
        table='district_irrigation_aggregations',
        columns=cols,
        values=vals,
      ))

      vals = []
      print(f'added averages for {district_cnt} districts')

  ###################################################################################################################

  if len(vals) > 0:
    database.run_in_transaction(lambda transaction: transaction.insert_or_update(
      table='district_irrigation_aggregations',
      columns=cols,
      values=vals,
    ))
    print(f'added averages for {district_cnt} districts')
  print(f'\nadded {param} averages to table\n')



def fn5(param, soil_type):
  with database.snapshot() as snapshot:
    data = snapshot.execute_sql(
      """SELECT districtid
      from Districts"""
    )
  data = list(data)
  district_ids = [x[0] for x in data]

  print('got the districts')

  ###################################################################################################################

  cols = ['district_id', 'parameter', 'soil_type', 'card_count', 'average', 'bootstrapped_average']
  vals = []

  with database.snapshot() as snapshot:
    data = snapshot.execute_sql(
      f"""SELECT districtid, {param}_value
      from Cards_info
      where {param}_value is not null
      and soil_type = "{soil_type}"
      order by 1"""
    )
  data = list(data)

  print('got the data') 

  ###################################################################################################################

  values_districtwise = {}
  total_cnt = 0
  for x in district_ids:
    values_districtwise[x] = []

  for x in data:
    values_districtwise[x[0]].append(x[1])

  values_districtwise_np = {}
  for district, values in values_districtwise.items():
    values_districtwise_np[district] = np.array(values)
    total_cnt += len(values_districtwise_np[district])

  print(f'total cnt: {total_cnt}')

  ###################################################################################################################

  cnt = 0
  district_cnt = 0
  for district, values in values_districtwise_np.items():
    if len(values) == 0:
      continue

    district_cnt += 1
    cnt += len(values)
    avgs = []
    for _ in range(iterations):
      # sample = random.choices(values, k=len(values))
      sample = np.random.choice(values, size=len(values), replace=True)
      # total = math.fsum(sample)
      total = np.sum(sample)
      avgs.append(total/len(values))

    bootstrapped_avg = math.fsum(avgs)/iterations
    avg = math.fsum(values)/len(values)

    vals.append([district, param, soil_type, len(values), avg, bootstrapped_avg])

    print(f'{[district, param, soil_type, len(values), avg, bootstrapped_avg]}, total cards done: {cnt}/{total_cnt}')

    if len(vals) == 100:
      database.run_in_transaction(lambda transaction: transaction.insert_or_update(
        table='district_soil_type_aggregations',
        columns=cols,
        values=vals,
      ))

      vals = []
      print(f'added averages for {district_cnt} districts')

  ###################################################################################################################

  if len(vals) > 0:
    database.run_in_transaction(lambda transaction: transaction.insert_or_update(
      table='district_soil_type_aggregations',
      columns=cols,
      values=vals,
    ))
    print(f'added averages for {district_cnt} districts')

  print(f'\nadded {param} averages to table\n')




if TASK_INDEX < 12:
  param = parameters[TASK_INDEX]
  print(f'district aggregations, parameter - {param}')
  fn1(param)

elif TASK_INDEX < 24:
  param = parameters[TASK_INDEX - 12]
  print(f'village aggregations, parameter - {param}')
  fn2(param)

elif TASK_INDEX < 36:
  param = parameters[TASK_INDEX - 24]
  print(f'soil test lab aggregations, parameter - {param}')
  fn3(param)

elif TASK_INDEX < 108:
  param = parameters[(TASK_INDEX - 36)%12]
  irrigate_method = irrigation_methods[(TASK_INDEX - 36)//12]
  print(f'district x irrigation method aggregations, parameter - {param}, irrigation method - {irrigate_method}')
  fn4(param, irrigate_method)

elif TASK_INDEX < 744:
  print('district x soil type aggregations')

  with database.snapshot() as snapshot:
    data = snapshot.execute_sql(
      """SELECT distinct soil_type
      from Cards_info
      where soil_type != ''
      """
    )
  data = list(data)
  soil_types = [x[0] for x in data]

  param = parameters[(TASK_INDEX - 108)%12]
  soil_type = soil_types[(TASK_INDEX - 108)//12]
  print(f'parameter - {param}, soil type - {soil_type}')
  fn5(param, soil_type)
