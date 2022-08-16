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

"""This class provides utility functions for parsing values from soil health card."""

from datetime import datetime
import re
from protos import card_pb2
from google.protobuf import json_format
from shapely import geometry
import json

class CardInfoParser:
  """This class provides utility functions for parsing values from soil health card."""

  def __init__(self, card_info: dict[str, dict[str, str]], india_shape_file: str = '', india_shape=None):
    self.card = card_info
    self.params = ['pH', 'EC', 'OC', 'N', 'P', 'K', 'S', 'Zn', 'B', 'Fe', 'Mn', 'Cu']
    self.error_log = ''
    self.card_proto = card_pb2.Card()
    self.india_shape_file = india_shape_file
    self.india_shape = india_shape

  def get_error_log(self):
    return self.error_log

  def get_soil_health_card_number(self, card_number: str) -> str:
    # Soil Health Card Number - xxx
    if not re.match(r'^Soil Health Card Number - .+$', card_number):
      # error log
      self.error_log += f'shc_no - {card_number}\n'
      return ''

    return card_number.split()[-1]

  def get_sample_collection_date(self, sample_date: str):
    # date column empty
    if not sample_date:
      return ''
    # date format on card: DD-MM-YYYY
    try:
      date = datetime.strptime(sample_date, '%d-%m-%Y').strftime('%Y-%m-%d')
    except:
      # error log
      self.error_log += f'date - {sample_date}\n'
      return ''
    return date

  def get_farm_size(self, farm_size: str):
    # numerical_value followed by unit or irrigation method or both or none
    if re.match(r'^[0-9]+(\.[0-9]+)?( .+)?$', farm_size):
      return float(farm_size.split()[0])
    # no numerical value present
    elif re.match(r'^[a-zA-z() ]+$', farm_size):
      return None
    self.error_log += f'farm_size - {farm_size}\n'
    return None

  def get_farm_size_unit(self, farm_size: str):
    # starts with a numerical value meaning unit should be present 
    # otherwise error
    if re.match(r'^[0-9]+(\.[0-9]+)?.*$', farm_size):
      f_size = farm_size.split()
      if len(f_size) <= 1 or (f_size[1] != 'Acre' and f_size[1] != 'Hectares'):
        self.error_log += f'farm_size_unit - {farm_size}\n'
        return ''
      return f_size[1]
    return ''

  def get_irrigation_method(self, farm_size: str):
    # numerical_value unit irrigation_method
    if re.match(r'^[0-9]+(\.[0-9]+)? +(Acre|Hectares) +.+$', farm_size):
      return ' '.join(farm_size.split()[2:])
    # numerical_value unit
    elif re.match(r'^[0-9]+(\.[0-9]+)? +(Acre|Hectares)$', farm_size):
      return ''
    # numerical_value irrigation_method
    elif re.match(r'^[0-9]+(\.[0-9]+)? +.+$', farm_size):
      return ' '.join(farm_size.split()[1:])
    # only irrigation method
    elif re.match(r'^[a-zA-Z() ]*$', farm_size):
      return farm_size
    # only numerical_value
    elif re.match(r'^[0-9]+(\.[0-9]+)?$', farm_size):
      return ''
    self.error_log += f'irrigation_method - {farm_size}\n'
    return ''

  def get_soil_type(self, soil_type: str) -> str:
    # ex: "Soil Type: Acidic hill soil", "Soil Type:", "Soil Type: Sandy soil"
    if re.match(r'^Soil Type:.*$', soil_type):
      return soil_type[10:]
    # error log
    self.error_log += f'soil type ({soil_type}) doesn\'t match regex\n'
    return soil_type

  def check_geoposition(self, gp: str) -> bool:
    if re.match(r'^Latitude [0-9]+(,[0-9]+)?\.[0-9]+Â?°N +Longitude [0-9]+(,[0-9]+)?\.[0-9]+Â?°E$', gp):
      return True
    elif re.match(r'#Error', gp):
      return False
    # error log
    self.error_log += f'geoposition - {gp}\n'
    return False

  def get_geoposition(self, geo_position: str):
    if re.match(r'^Latitude [0-9]+(,[0-9]+)?\.[0-9]+Â°N +Longitude [0-9]+(,[0-9]+)?\.[0-9]+Â°E$',
                geo_position):
      lat = geo_position.split()[1][0:-3]
      long = geo_position.split()[3][0:-3]
    else:
      lat = geo_position.split()[1][0:-2]
      long = geo_position.split()[3][0:-2]
    # return float(lat.replace(',', '')), float(long.replace(',', ''))
    lat = float(lat.replace(',', ''))
    long = float(long.replace(',', ''))

    if self.validate_geopos(lat, long):
      return lat, long
    else:
      self.error_log += f'geoposition not in india - {geo_position}\n'
    return None, None

  def get_normal_level(self, normal_level: str):
    """Parses the minimum, maximum and unit of normal level."""
    # pH normal level: "7,  Neutral"
    if re.match(r'^7, +Neutral$', normal_level):
      return 7, 7, None
    # num1 - num2 unit, example: "145 -337 kg/ha", "0-1 dS/m", "0.51 - 0.75%"
    elif re.match(r'^[0-9]+(\.[0-9]+)? *- *[0-9]+(\.[0-9]+)?.*$', normal_level):
      mini = float(normal_level.split('-')[0].strip())
      maxi = normal_level.split('-')[1].strip()
      endpoint = re.search(r'(\d)[^\d]*$', maxi)
      if endpoint is not None:
        unit = maxi[endpoint.start() + 1:].strip()
        maxi = float(maxi[:endpoint.start() + 1])
        return mini, maxi, unit
    # example: "> 10 ppm". "> 0.6 ppm"
    elif re.match(r'^> [0-9]+(\.[0-9]+)? *[a-z]+$', normal_level):
      return float(normal_level.split()[1]), None, normal_level.split()[2]

    self.error_log += f'normal_level - {normal_level}\n'
    return None, None, None

  def get_soil_sample_details(self) -> card_pb2.SoilSampleDetails:
    soil_sample_details = card_pb2.SoilSampleDetails()
    # shc no
    soil_sample_details.soil_health_card_number = self.get_soil_health_card_number(self.card.get('details', {}).get('soil_health_card_number', ''))
    # validity
    soil_sample_details.validity = self.card.get('details', {}).get('validity', '')
    # collection date
    soil_sample_details.sample_collection_date = self.get_sample_collection_date(self.card.get('soil_sample_details', {}).get('Date of Sample Collection', ''))
    if not soil_sample_details.sample_collection_date:
      soil_sample_details.ClearField('sample_collection_date')
    # survey no
    soil_sample_details.survey_no = self.card.get('soil_sample_details', {}).get('Survey No., Khasra No./ Dag No.', '')
    # farm size, farm size unit, irrigation method
    if self.card.get('soil_sample_details', {}).get('Farm Size'):
      farm_size = self.card['soil_sample_details']['Farm Size']
      # farm size
      num = self.get_farm_size(farm_size)
      if num is not None: soil_sample_details.farm_size = num
      # farm size unit
      soil_sample_details.farm_size_unit = self.get_farm_size_unit(farm_size)
      # irrigation method
      soil_sample_details.irrigation_method = self.get_irrigation_method(farm_size) 
    # lat/long
    if self.card.get('soil_sample_details', {}).get('Geo Position (GPS)'):
      gp = self.card['soil_sample_details']['Geo Position (GPS)']
      if self.check_geoposition(gp):
        latitude, longitude = self.get_geoposition(gp)
        if latitude and longitude:
          soil_sample_details.latitude = latitude
          soil_sample_details.longitude = longitude
    # soil test lab
    soil_sample_details.soil_test_lab = self.card.get('soil_test_results', {}).get('soil_test_lab', '')
    # soil type
    soil_sample_details.soil_type = self.get_soil_type(self.card.get('soil_test_results', {}).get('soil_type', ''))

    self.card_proto.soil_sample_details.CopyFrom(soil_sample_details)
    return soil_sample_details

  def get_recommendations(self):
    recommendations = card_pb2.Recommendations()
    # general recommendations
    if self.card.get('general_recommendations'):
      for param, recom in self.card['general_recommendations'].items():
        genrecom = card_pb2.Recommendations.GeneralRecommendation()
        genrecom.parameter = param
        genrecom.recommendation = recom
        recommendations.general_recommendations.append(genrecom)
    # secondary recommendations
    if self.card.get('secondary_recommendations_through_soil'):
      for param, recom in self.card['secondary_recommendations_through_soil'].items():
        secrecom = card_pb2.Recommendations.SecondaryRecommendation()
        secrecom.parameter = param
        secrecom.recommendation_through_soil = recom
        secrecom.recommendation_through_spray = self.card['secondary_recommendations_through_spray'].get(param, '')
        recommendations.secondary_recommendations.append(secrecom)

    self.card_proto.recommendations.CopyFrom(recommendations)
    return json_format.MessageToJson(recommendations)

  def get_soil_tests(self) -> dict[str, card_pb2.SoilTestResult]:
    soil_tests = {}
    for param in self.params:
      soil_test = self.card.get(f'soil_test_{param}')
      if soil_test is None:
        continue

      soil_test_result = card_pb2.SoilTestResult()
      # parameter
      soil_test_result.parameter = param
      # test value
      if soil_test.get('Test Value') and soil_test['Test Value'] != '--':
        soil_test_result.value = float(soil_test['Test Value'])
      # unit
      if soil_test.get('Unit'):
        soil_test_result.unit = soil_test['Unit']
      # rating
      if soil_test.get('Rating'):
        soil_test_result.rating = soil_test['Rating']
      # normal level
      if soil_test.get('Normal Level'):
        normal_level = soil_test['Normal Level']

        mini, maxi, unit = self.get_normal_level(normal_level)
        if mini is not None:
          soil_test_result.min_normal_level = mini
        if maxi is not None:
          soil_test_result.max_normal_level = maxi
        if unit is not None:
          soil_test_result.unit_normal_level = unit

      soil_tests[param] = soil_test_result
      self.card_proto.soil_test.append(soil_test_result)

    return soil_tests

  def validate_geopos(self, latitude, longitude):
    if not self.india_shape:
      print('Caluculating shape using shapely...\n')
      if not self.india_shape_file:
        self.india_shape_file = './testdata/india_shape.geojson'
      
      file = open(self.india_shape_file, 'r')
      india_gj_raw = file.read()
      file.close()
      india_gj = json.loads(india_gj_raw)

      self.india_shape = geometry.shape(india_gj['features'][0]['geometry'])

    return self.india_shape.contains(geometry.Point(longitude, latitude))

  def get_fertilizer_combinations(self) -> card_pb2.FertilizerCombinations:
    fertilizer_combinations = card_pb2.FertilizerCombinations()
    # option 1
    if self.card.get('fertilizer_option_1'):
      for crop_variety, combination in self.card['fertilizer_option_1'].items():
        fertilizer_combination = card_pb2.FertilizerCombinations.FertilizerCombination()
        fertilizer_combination.option_type = 1
        fertilizer_combination.crop_variety = crop_variety
        fertilizer_combination.combination = combination

        fertilizer_combinations.combinations.append(fertilizer_combination)
    # option 2
    if self.card.get('fertilizer_option_2'):
      for crop_variety, combination in self.card['fertilizer_option_2'].items():
        fertilizer_combination = card_pb2.FertilizerCombinations.FertilizerCombination()
        fertilizer_combination.option_type = 2
        fertilizer_combination.crop_variety = crop_variety
        fertilizer_combination.combination = combination

        fertilizer_combinations.combinations.append(fertilizer_combination)

    self.card_proto.fertilizer_combinations.CopyFrom(fertilizer_combinations)
    return json_format.MessageToJson(fertilizer_combinations)

  def get_full_card(self):
    return json_format.MessageToJson(self.card_proto)

  def validate_district(self, latitude, longitude, districtid):
    pass
