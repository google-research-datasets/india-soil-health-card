import os
from google.cloud import bigquery
import re
import geojson
from dateutil import parser

client = bigquery.Client()
dataset_id = os.getenv("BIGQUERY_DATASET")
dataset_id_full = f"{client.project}.{dataset_id}"
dataset = bigquery.Dataset(dataset_id_full)
"""
{
    'soil_tests': [{
        'Sr.No.': '1',
        'Parameter': 'pH',
        'Test Value': '5.00',
        'Unit': 'Highly acidic   ',
        'Rating': '7,  Neutral'
    }, {
        'Sr.No.': '2',
        'Parameter': 'EC',
        'Test Value': '--',
        'Unit': 'dS/m'
    }, {
        'Sr.No.': '3',
        'Parameter': 'Organic Carbon (OC)',
        'Test Value': '1.20',
        'Unit': '%',
        'Rating': 'Very High',
        'Normal Level': '0.51 - 0.75%'
    }, {
        'Sr.No.': '4',
        'Parameter': 'Available Nitrogen (N)',
        'Test Value': '376.00',
        'Unit': 'kg/ha',
        'Rating': 'Medium',
        'Normal Level': '280 - 560 kg/ha'
    }, {
        'Sr.No.': '5',
        'Parameter': 'Available Phosphorus (P)',
        'Test Value': '9.20',
        'Unit': 'kg/ha',
        'Rating': 'Very Low',
        'Normal Level': '23 - 57 kg/ha'
    }, {
        'Sr.No.': '6',
        'Parameter': 'Available Potassium (K)',
        'Test Value': '81.60',
        'Unit': 'kg/ha',
        'Rating': 'Low',
        'Normal Level': '145 - 337 kg/ha'
    }, {
        'Sr.No.': '7',
        'Parameter': 'Available Sulphur (S)',
        'Test Value': '--',
        'Unit': 'ppm'
    }, {
        'Sr.No.': '8',
        'Parameter': 'Available Zinc (Zn)',
        'Test Value': '--',
        'Unit': 'ppm'
    }, {
        'Sr.No.': '9',
        'Parameter': 'Available Boron (B)',
        'Test Value': '--',
        'Unit': 'ppm'
    }, {
        'Sr.No.': '10',
        'Parameter': 'Available Iron (Fe)',
        'Test Value': '--',
        'Unit': 'ppm'
    }, {
        'Sr.No.': '11',
        'Parameter': 'Available Manganese (Mn)',
        'Test Value': '--',
        'Unit': 'ppm'
    }, {
        'Sr.No.': '12',
        'Parameter': 'Available Copper (Cu)',
        'Test Value': '--',
        'Unit': 'ppm'
    }],
    'soil_health_card_number': 'Soil Health Card Number - MZ/2016-17/10343339/1',
    'validity': 'Validity - From:   To: ',
    'soil_sample_details': {
        'Date of Sample Collection': '12-04-2016',
        'Survey No., Khasra No./ Dag No.': '-',
        'Farm Size': '3.00  Hectares     Rainfed',
        'Geo Position (GPS)': 'Latitude 23.051111°N  Longitude 93.099167°E'
    }
}
"""
def insertCard(state_id,district_id,mandal_id,village_id,sample, sr_no, extracted):
    pos_test = extracted['soil_sample_details']['Geo Position (GPS)']
    result = re.match('Latitude (.*)°N.*Longitude (.*)°E', pos_test)
    if result:
        extracted['soil_sample_details']['latitude'] = result.groups()[0]
        extracted['soil_sample_details']['longitude'] = result.groups()[1]
        # ST_GEOGPOINT(longitude, latitude)
	
    errors = client.insert_rows_json(f"{dataset_id_full}.soil_health_cards", [toRow(state_id,district_id,mandal_id,village_id,sample, sr_no, extracted)])
    if errors == []:
        print("New rows have been added.")
    else:
        print("Encountered errors while inserting rows: {}".format(errors))
    
def toRow(state_id,district_id,mandal_id,village_id,sample, sr_no, extracted):
    result = {
        'id': f"{sample}_{sr_no}",
        'sample': sample,
        'sr_no': sr_no,
        'state_id': state_id,
        'district_id': district_id,
        'subdistrict_id': mandal_id,
        'village_id': village_id,
        'soil_tests': [formatSoilTest(soil_test) for soil_test in extracted['soil_tests'] ],
        'soil_health_card_number': extracted['soil_health_card_number'],
        'validity': extracted['validity'],
        'soil_sample_details': {
            'survey_no': extracted['soil_sample_details']['Survey No., Khasra No./ Dag No.'],
            'farm_size': extracted['soil_sample_details']['Farm Size'],
            'geo_position_string': extracted['soil_sample_details']['Geo Position (GPS)']
        },
    }
    if 'latitude' in extracted['soil_sample_details'] and 'longitude' in extracted['soil_sample_details']:
        latitude = float(extracted['soil_sample_details']['latitude'])
        longitude = float(extracted['soil_sample_details']['longitude'])
        if latitude > 90 or latitude < -90:
            s = latitude 
            latitude = longitude
            longitude = s
        result['soil_sample_details']['geo_position'] = geojson.dumps(geojson.Point([longitude, latitude]))
    
    if 'Date of Sample Collection' in extracted['soil_sample_details']:
        date = parser.parse(extracted['soil_sample_details']['Date of Sample Collection']).strftime("%Y-%m-%d")
        result['soil_sample_details']['date_of_sample_collection'] = date

    return result

def formatSoilTest(org):
    result = {}
    if 'Sr.No.' in org:
        result['sr_no'] = org['Sr.No.']    
    if 'Parameter' in org:
        result['parameter'] = org['Parameter']    
    if 'Test Value' in org:
        result['value'] = org['Test Value']    
    if 'Unit' in org:
        result['unit'] = org['Unit']    
    if 'Rating' in org:
        result['rating'] = org['Rating']    
    if 'Normal Level' in org:
        result['normal_level'] = org['Normal Level']    
    return result

states = [{"endpoint":"http://soilhealth.dac.gov.in/HealthCard/HealthCard/HealthCardPNew","id":"35","name":"Andaman And Nicobar Islands"},{"endpoint":"http://soilhealth.dac.gov.in/HealthCard/HealthCard/HealthCardPNew","id":"12","name":"Arunachal Pradesh"},{"endpoint":"http://soilhealth.dac.gov.in/HealthCard/HealthCard/HealthCardPNew","id":"18","name":"Assam"},{"endpoint":"http://soilhealth.dac.gov.in/HealthCard/HealthCard/HealthCardPNew","id":"10","name":"Bihar"},{"endpoint":"http://soilhealth.dac.gov.in/HealthCard/HealthCard/HealthCardPNew","id":"4","name":"Chandigarh"},{"endpoint":"http://soilhealth.dac.gov.in/HealthCard/HealthCard/HealthCardPNew","id":"7","name":"Delhi"},{"endpoint":"http://soilhealth.dac.gov.in/HealthCard/HealthCard/HealthCardPNew","id":"30","name":"Goa"},{"endpoint":"http://soilhealth.dac.gov.in/HealthCard/HealthCard/HealthCardPNew","id":"6","name":"Haryana"},{"endpoint":"http://soilhealth.dac.gov.in/HealthCard/HealthCard/HealthCardPNew","id":"2","name":"Himachal Pradesh"},{"endpoint":"http://soilhealth.dac.gov.in/HealthCard/HealthCard/HealthCardPNew","id":"1","name":"Jammu And Kashmir"},{"endpoint":"http://soilhealth.dac.gov.in/HealthCard/HealthCard/HealthCardPNew","id":"20","name":"Jharkhand"},{"endpoint":"http://soilhealth.dac.gov.in/HealthCard/HealthCard/HealthCardPNew","id":"37","name":"Ladakh"},{"endpoint":"http://soilhealth.dac.gov.in/HealthCard/HealthCard/HealthCardPNew","id":"31","name":"Lakshadweep"},{"endpoint":"http://soilhealth.dac.gov.in/HealthCard/HealthCard/HealthCardPNew","id":"14","name":"Manipur"},{"endpoint":"http://soilhealth.dac.gov.in/HealthCard/HealthCard/HealthCardPNew","id":"17","name":"Meghalaya"},{"endpoint":"http://soilhealth.dac.gov.in/HealthCard/HealthCard/HealthCardPNew","id":"15","name":"Mizoram"},{"endpoint":"http://soilhealth.dac.gov.in/HealthCard/HealthCard/HealthCardPNew","id":"13","name":"Nagaland"},{"endpoint":"http://soilhealth.dac.gov.in/HealthCard/HealthCard/HealthCardPNew","id":"34","name":"Puducherry"},{"endpoint":"http://soilhealth.dac.gov.in/HealthCard/HealthCard/HealthCardPNew","id":"3","name":"Punjab"},{"endpoint":"http://soilhealth.dac.gov.in/HealthCard/HealthCard/HealthCardPNew","id":"8","name":"Rajasthan"},{"endpoint":"http://soilhealth.dac.gov.in/HealthCard/HealthCard/HealthCardPNew","id":"11","name":"Sikkim"},{"endpoint":"http://soilhealth.dac.gov.in/HealthCard/HealthCard/HealthCardPNew","id":"16","name":"Tripura"},{"endpoint":"http://soilhealth.dac.gov.in/HealthCard/HealthCard/HealthCardPNew","id":"19","name":"West Bengal"}]