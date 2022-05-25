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

"""Download soil health cards for the given state.
"""
from bs4 import BeautifulSoup

import logging
import storage
import utils

import requests
from requests import Request
from requests.adapters import HTTPAdapter, Retry
import time
import os
from pyppeteer.network_manager import Request
import urllib.parse

import nest_asyncio
nest_asyncio.apply()

import asyncio
import pyppeteer

requests.adapters.DEFAULT_RETRIES = 5

class ReportServerUpdating(Exception):
     pass

class UnableToDownloadCard(Exception):
     pass

async def req_intercept(req: Request):
    req.headers.update({'X-Requested-With': 'XMLHttpRequest'})
    await req.continue_(overrides={'headers': req.headers})

def offlineStates():
  return {'35': {'name': 'Andaman And Nicobar Islands', 'id': '35', 'endpoint': 'http://soilhealth.dac.gov.in'}, '12': {'name': 'Arunachal Pradesh', 'id': '12', 'endpoint': 'http://soilhealth.dac.gov.in'}, '18': {'name': 'Assam', 'id': '18', 'endpoint': 'http://soilhealth.dac.gov.in'}, '10': {'name': 'Bihar', 'id': '10', 'endpoint': 'http://soilhealth.dac.gov.in'}, '4': {'name': 'Chandigarh', 'id': '4', 'endpoint': 'http://soilhealth.dac.gov.in'}, '7': {'name': 'Delhi', 'id': '7', 'endpoint': 'http://soilhealth.dac.gov.in'}, '30': {'name': 'Goa', 'id': '30', 'endpoint': 'http://soilhealth.dac.gov.in'}, '6': {'name': 'Haryana', 'id': '6', 'endpoint': 'http://soilhealth.dac.gov.in'}, '2': {'name': 'Himachal Pradesh', 'id': '2', 'endpoint': 'http://soilhealth.dac.gov.in'}, '1': {'name': 'Jammu And Kashmir', 'id': '1', 'endpoint': 'http://soilhealth.dac.gov.in'}, '20': {'name': 'Jharkhand', 'id': '20', 'endpoint': 'http://soilhealth.dac.gov.in'}, '37': {'name': 'Ladakh', 'id': '37', 'endpoint': 'http://soilhealth.dac.gov.in'}, '31': {'name': 'Lakshadweep', 'id': '31', 'endpoint': 'http://soilhealth.dac.gov.in'}, '14': {'name': 'Manipur', 'id': '14', 'endpoint': 'http://soilhealth.dac.gov.in'}, '17': {'name': 'Meghalaya', 'id': '17', 'endpoint': 'http://soilhealth.dac.gov.in'}, '15': {'name': 'Mizoram', 'id': '15', 'endpoint': 'http://soilhealth.dac.gov.in'}, '13': {'name': 'Nagaland', 'id': '13', 'endpoint': 'http://soilhealth.dac.gov.in'}, '34': {'name': 'Puducherry', 'id': '34', 'endpoint': 'http://soilhealth.dac.gov.in'}, '3': {'name': 'Punjab', 'id': '3', 'endpoint': 'http://soilhealth.dac.gov.in'}, '8': {'name': 'Rajasthan', 'id': '8', 'endpoint': 'http://soilhealth.dac.gov.in'}, '11': {'name': 'Sikkim', 'id': '11', 'endpoint': 'http://soilhealth.dac.gov.in'}, '16': {'name': 'Tripura', 'id': '16', 'endpoint': 'http://soilhealth.dac.gov.in'}, '19': {'name': 'West Bengal', 'id': '19', 'endpoint': 'http://soilhealth.dac.gov.in'}, '28': {'name': 'Andhra Pradesh', 'id': '28', 'endpoint': 'http://soilhealth6.dac.gov.in'}, '22': {'name': 'Chhattisgarh', 'id': '22', 'endpoint': 'http://soilhealth3.dac.gov.in'}, '36': {'name': 'Telangana', 'id': '36', 'endpoint': 'http://soilhealth3.dac.gov.in'}, '24': {'name': 'Gujarat', 'id': '24', 'endpoint': 'http://soilhealth9.dac.gov.in'}, '32': {'name': 'Kerala', 'id': '32', 'endpoint': 'http://soilhealth9.dac.gov.in'}, '33': {'name': 'Tamil Nadu', 'id': '33', 'endpoint': 'http://soilhealth9.dac.gov.in'}, '29': {'name': 'Karnataka', 'id': '29', 'endpoint': 'http://soilhealth2.dac.gov.in'}, '23': {'name': 'Madhya Pradesh', 'id': '23', 'endpoint': 'http://soilhealth5.gov.in'}, '27': {'name': 'Maharashtra', 'id': '27', 'endpoint': 'http://soilhealth8.dac.gov.in'}, '21': {'name': 'Odisha', 'id': '21', 'endpoint': 'http://soilhealth8.dac.gov.in'}, '5': {'name': 'Uttarakhand', 'id': '5', 'endpoint': 'http://soilhealth8.dac.gov.in'}, '9': {'name': 'Uttar Pradesh', 'id': '9', 'endpoint': 'http://soilhealth4.dac.gov.in'}}

class ShcDL:
  base_url = 'https://soilhealth.dac.gov.in/HealthCard/HealthCard/state'

  async def setup(self):
    if not os.environ.get('RUN_LOCALLY'):
      self.browser = await pyppeteer.launch({ 'headless': True, 'executablePath': '/usr/bin/chromium', 'autoClose': False, 'args': ['--no-sandbox']},
          handleSIGINT=False,
          handleSIGTERM=False,
          handleSIGHUP=False)
    else:
      self.browser = await pyppeteer.launch({ 'headless': True, 'devTools': False, 'autoClose': False, 'args': ['--no-sandbox'] },
          handleSIGINT=False,
          handleSIGTERM=False,
          handleSIGHUP=False)
    self.page = await self.browser.newPage()
    #self.page.on('console', lambda msg: utils.logText(f'console message {msg.type} {msg.text} {msg.args}'))
    self.states = offlineStates() #{ state['id'] : state for state in await self.getStates() }
    await self.page.setViewport({'width': 0, 'height': 0})

  async def newPage(self):
    self.page = await self.browser.newPage()

  async def close(self):
    await self.page.close()
    await self.browser.close()

  async def getToken(self):
    await self.page.goto(self.base_url)
    return await self.page.Jeval('#forgeryToken', 'el => el.value')

  async def getStates(self):
    await self.page.goto(self.base_url)
    endpoints = await self.page.evaluate('Array.prototype.slice.call(document.getElementById("StateUrl").children).map(ele => { return { state: ele.textContent, endpoint: ele.value}}).filter(ele => ele.state != "--SELECT--")', force_expr=True)
    ids={}
    for endpoint in endpoints:
      
      print(f"Goto {endpoint['endpoint']}\n")
      await self.page.goto(endpoint['endpoint']) 
      
      await self.page.waitForFunction("document.getElementById('State_cd2') != null")
      await self.page.waitForFunction("document.getElementById('State_cd2').length > 1")
      results = await self.page.evaluate('Array.prototype.slice.call(document.getElementById("State_cd2").children).map(ele => { return { state: ele.textContent, id: ele.value}}).filter(ele => ele.state != "--SELECT--")', force_expr=True)
      for ele in results:
        ids[ele['id']] = ele['state']

    result = []
    for state in ids:
        res = {}        
        res['name'] = ids[state]
        res['id'] = state
        for endpoint in endpoints:
            if ids[state] == endpoint['state']:
                endpoint = endpoint['endpoint']
                res['endpoint'] = endpoint[:endpoint.index("/", 8)]
        result.append(res)
    return result

  async def getDistricts(self, state_code):
    token = await self.getToken()
    timestamp = int(time.time()) 
    state_endpoint = self.states[state_code]['endpoint']
    r = requests.get(f'{state_endpoint}/CommonFunction/GetDistrict', params={
       'statecode': state_code,
       'VerificationToken': token,
       '_': timestamp,
    })
    districts = [x for x in r.json() if x['Text'] != '--SELECT--']
    result = []
    for district in districts:
        res = {}        
        res['name'] = district['Text']
        res['id'] = district['Value']
        result.append(res)
    r.close()
    return result

  async def getSubDistricts(self, state_code, district):
    token = await self.getToken()
    timestamp = int(time.time()) 
    state_endpoint = self.states[state_code]['endpoint']
    r = requests.get(f'{state_endpoint}/CommonFunction/GetSubdis', params={
       'Dist_code': district,
       'VerificationToken': token,
       '_': timestamp,
    })
    subdistricts =  [x for x in r.json() if x['Text'] != '--SELECT--']
    result = []
    for subdistrict in subdistricts:
        res = {}        
        res['name'] = subdistrict['Text']
        res['id'] = subdistrict['Value']
        result.append(res)
    r.close()
    return result

  async def getBlock(self, state_code, district):
    token = await self.getToken()
    timestamp = int(time.time()) 
    state_endpoint = self.states[state_code]['endpoint']
    r = requests.get(f'{state_endpoint}/CommonFunction/GetBlock', params={
       'Dist_code': district,
       'VerificationToken': token,
       '_': timestamp,
    })
    subdistricts =  [x for x in r.json() if x['Text'] != '--SELECT--']
    result = []
    for subdistrict in subdistricts:
        res = {}        
        res['name'] = subdistrict['Text']
        res['id'] = subdistrict['Value']
        result.append(res)
    r.close()
    return result

  async def getVillages(self, state, district, subDistrict):
    state_endpoint = self.states[state]['endpoint']

    s = requests.Session()
    retries = Retry(total=5, backoff_factor=1)
    s.mount('http://', HTTPAdapter(max_retries=retries))
    r= s.get(f'{state_endpoint}/CommonFunction/GetVillage', params={
       'Sub_discode': subDistrict
    })
    
    #r = requests.get(f'{state_endpoint}/CommonFunction/GetVillage', params={
    #   'Sub_discode': subDistrict
    #})
    villages =  [x for x in r.json() if x['Text'] != '--SELECT--']
    result = []
    for village in villages:
        res = {}        
        res['name'] = village['Text']
        res['id'] = village['Value']
        res['state_id'] = state
        res['district_id'] = district
        res['mandal_id'] = subDistrict
        result.append(res)
    r.close()
    return result


  async def _selectState(self, state):
    await self.page.select('#State_cd2', state)
    await self.page.waitForFunction("document.getElementById('Dist_cd2').length > 1")

  async def _selectDistrict(self, district):
    await self.page.select('#Dist_cd2', district)
    await self.page.waitForFunction("document.getElementById('Sub_dis2').length > 1")

  async def _selectMandal(self, mandal):
    await self.page.select('#Sub_dis2', mandal)
    await self.page.waitForFunction("document.getElementById('village_cd2').length > 1")

  async def _selectVillage(self, village):
    await self.page.select('#village_cd2', village)

  async def _search(self):
    search_button = await self.page.J('#tb_serch > tr:nth-child(8) > td > a:nth-child(1)')
    await search_button.click()
    await self.page.waitFor(2000)

  async def getCards(self, state, district, subdistrict, village):
    utils.logText(f"retrieving cards for village {village}")
    state_endpoint = self.states[state]['endpoint']

    #await self.page.setRequestInterception(True)
    #self.page.on('request', lambda req: asyncio.ensure_future(req_intercept(req)))
    print(f"Init page {state_endpoint}/HealthCard/HealthCard/HealthCardPNew")
    await self.page.setCacheEnabled(False)
    self.page.setDefaultNavigationTimeout(30000)
    await self.page.goto("https://www.google.com")
    await self.page.goto(f"{state_endpoint}/HealthCard/HealthCard/HealthCardPNew")
    await self._selectState(state)
    await self._selectDistrict(district)
    await self._selectMandal(subdistrict)
    await self._selectVillage(village)
    await self._search()

    token = await self.page.Jeval('#forgeryToken', 'el => el.value')
    sessionCookie = ""
    cookies = await self.page.cookies()
    for cookie in cookies:
      if cookie["name"] == "ASP.NET_SessionId":
        sessionCookie = cookie["value"]
        
    samples = set()
    results = []
    stop = False
    page = 1

    while stop is False:
      timestamp = int(time.time()) 
      url = f'{state_endpoint}/HealthCard/HealthCard/SearchInGridP?S_District_Sample_number=&S_Financial_year=&GetSampleno=&Fname=&Statecode={state}&block=&discode={district}&subdiscode={subdistrict}&village={village}&Source_Type=&Date_Recieve=&VerificationToken={token}&_={timestamp}&page={page}'
      url = url.replace("http://","https://")

      print(f"processing page {page} with {url}")
      retry_strategy = Retry(
          total=10,
          backoff_factor=2
      )
      adapter = HTTPAdapter(max_retries=retry_strategy)
      http = requests.Session()
      http.mount("https://", adapter)
      http.mount("http://", adapter)

      r = http.get(url, headers={
        "x-requested-with": "XMLHttpRequest"
      }, cookies={
        "ASP.NET_SessionId": sessionCookie
      }, timeout=10)
      soup = BeautifulSoup(r.text, 'html.parser')
      rows = soup.find('tbody').find_all("tr")
      if len(rows) >0:
        for row in rows:
          cols = row.find_all('td')
          sample_text = cols[0].text
          village_grid_text = cols[1].text
          srno_text = cols[2].text
          district_text = cols[4].text
          mandal_text = cols[5].text
          district_text = cols[6].text
          village_text = cols[7].text
           
          if sample_text+srno_text in samples:
            stop = True
            break
          samples.add(sample_text+srno_text)
          srno_text = int(srno_text)
                
          results.append({
              'sample': sample_text,
              'village_grid': village_grid_text,
              'sr_no': srno_text,
              'district': district_text,
              'mandal': mandal_text,
              'village': village_text,
              'state_id': state,
              'district_id': district,
              'mandal_id': subdistrict,
              'village_id': village
          })
      else:
        stop = True
        break
      page = page + 1
    return results

  async def _pageHasMoreThanOneRow(self):
    sample_element = await self.page.J(F'#MainTable > tbody > tr:nth-child(2) > td:nth-child(1)')
    if sample_element:
      return True 
    else:
      return False

  async def getCard(self, state, sample_no, village_grid, sr_no):
    utils.logText(f"downloading card {sample_no} {sr_no}")
    Language_Code= "99"
    ShcValidityDateFrom= "NULL"
    ShcValidityDateTo= "NULL"
    shcformate= "NewFormat"
    state_endpoint = self.states[state]['endpoint']
    url = f'{state_endpoint}/HealthCard/HealthCard/HealthCardNewPartialP?Language_Code={Language_Code}&Sample_No={urllib.parse.quote(sample_no,safe="")}&ShcValidityDateFrom={ShcValidityDateFrom}&ShcValidityDateTo={ShcValidityDateTo}&Sr_No={sr_no}&Unit_Code=17&shcformate={shcformate}'
    print(f"Loading sample url {url}")
    counter = 1
    while counter < 5:
        try:
          try:
            report_html = await self._getCardInner(self.page, counter, url)
            if len(report_html) > 60*1024:
              #await page.close()
              return report_html
          except pyppeteer.errors.TimeoutError:
            logging.exception(f"error downloading card {sample_no} {sr_no}")
            #await page.close()
            raise pyppeteer.errors.TimeoutError
        except pyppeteer.errors.TimeoutError:
            counter = counter + 1
     
    raise UnableToDownloadCard

  async def _getCardInner(self, page, counter, url):
    await page.goto(url)
    try:
      await page.waitForFunction("document.querySelector('body > iframe').contentWindow.document.querySelector('#form1 > div:nth-child(3) > div') != null" , { 'timeout': 30000 })
      await page.waitForFunction("document.querySelector('body > iframe').contentWindow.document.querySelector('#form1 > div:nth-child(3) > div').textContent == ' Report server is being updated. Please try later...'" , { 'timeout': 30000 })
      raise ReportServerUpdating()
    except pyppeteer.errors.TimeoutError:
      utils.logText("Report Server is not updating")

    await page.waitForFunction("document.querySelector('body > iframe') != null")   
    await page.waitForFunction("document.querySelector('body > iframe').contentWindow != null")  
    await page.waitForFunction("document.querySelector('body > iframe').contentWindow.document != null")  
    await page.waitForFunction("document.querySelector('body > iframe').contentWindow.document.querySelector('#VisibleReportContentReportViewer1_ctl09') != null")    
    await page.waitForFunction("document.querySelector('body > iframe').contentWindow.document.querySelector('#VisibleReportContentReportViewer1_ctl09').children.length > 0",  {'timeout': 120000 })

    iframe = await (await page.J('body > iframe')).contentFrame()
    return await iframe.content()

async def fetchCard(card, overwrite):
    utils.logText(f"downloading card {card} {overwrite}")
    shc_dl = ShcDL()
    await shc_dl.setup()
    shc = ""
    file_path = storage.getFilePath(card['state_id'], card['district_id'], card['mandal_id'], card['village_id'], card['sample'], card['sr_no'])
    if not storage.isFileDownloaded(file_path) or overwrite == "true":
        counter = 5
        try:
          while shc == "" and counter > 0:
              try:
                  shc = await shc_dl.getCard(card['state_id'], card['sample'], card['village_grid'], card['sr_no'])
              except pyppeteer.errors.TimeoutError:
                  counter = counter - 1
                  if counter == 0:
                    await shc_dl.close()
                    raise UnableToDownloadCard
              except UnableToDownloadCard:
                  counter = counter - 1
                  if counter == 0:
                    await shc_dl.close()
                    raise UnableToDownloadCard
          if len(shc) > 60*1024:
              storage.uploadFile(file_path, shc, {
                  'state': card['state_id'].strip(),
                  'district':  card['district'].strip(),
                  'district_code': card['district_id'].strip(),
                  'mandal': card['mandal'].strip(),
                  'mandal_code': card['mandal_id'].strip(),
                  'village':  card['village'].strip(),
                  'village_code': card['village_id'].strip(),
              })
          else:
              utils.logText(f'failed downloading file {file_path}') 
        except ReportServerUpdating:
          utils.logText(f'File {file_path} cant be downloaded at the moment, the report server is down') 
          await shc_dl.close()
          raise ReportServerUpdating
          
    else:
        utils.logText(f'skipping file {file_path} since its already downloaded') 
    await shc_dl.close()
    return shc

def ingestMetadata():
  shc = ShcDL()
  asyncio.run(shc.setup())
  states = asyncio.run(shc.getStates())
  for state in states:
    districts = asyncio.run(shc.getDistricts(state['id']))
    if len(districts) > 0:
      errors = []
      if errors == []:
          print("New rows have been added.")
      else:
          print("Encountered errors while inserting rows: {}".format(errors))
      for district in districts:
        subdistricts = asyncio.run(shc.getSubDistricts(state['id'],district['id']))
        if len(subdistricts) > 0:
          errors = []
          if errors == []:
              print("New rows have been added.")
          else:
              print("Encountered errors while inserting rows: {}".format(errors))
          for subdistrict in subdistricts:
            villages = asyncio.run(shc.getVillages(state['id'], district['id'], subdistrict['id']))
            if len(villages) > 0:
              errors = []
              if errors == []:
                  print("New rows have been added.")
              else:
                  print("Encountered errors while inserting rows: {}".format(errors))