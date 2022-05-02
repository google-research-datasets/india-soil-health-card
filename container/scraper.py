"""Download soil health cards for the given state.
"""

import logging
import storage
from typing import Sequence
from absl import app
from absl import flags

import base64
import io
import numpy as np
import pandas as pd
import random
import requests
from requests import Request, Session
import re
import time
import os
from pathlib import Path
import itertools
import concurrent.futures
from pyppeteer.network_manager import Request, Response
import urllib.parse

from random import randrange
from requests.exceptions import ConnectionError
from requests.exceptions import SSLError
from requests.exceptions import Timeout
from requests.exceptions import TooManyRedirects
from urllib.parse import urlparse

import nest_asyncio
nest_asyncio.apply()

import asyncio
import pyppeteer
import pyppdf.patch_pyppeteer

class UnableToDownloadCard(Exception):
     pass
async def req_intercept(req: Request):
    req.headers.update({'X-Requested-With': 'XMLHttpRequest'})
    await req.continue_(overrides={'headers': req.headers})

class ShcDL:
  base_url = 'https://soilhealth.dac.gov.in/HealthCard/HealthCard/state'
  state= "tmp"
  district ="tmp"
  mandal="tmp"
  village="tmp"

  async def setup(self):
    self.browser_download_location = os.path.join(Path.home(), 'Downloads')
    #self.browser = await pyppeteer.launch({'headless': False})
    self.browser = await pyppeteer.launch({ 'headless': True, 'args': ['--no-sandbox']},
      handleSIGINT=False,
      handleSIGTERM=False,
      handleSIGHUP=False
    )
    self.page = (await self.browser.pages())[0]
    await self.page.setRequestInterception(True)
    self.page.on('request', lambda req: asyncio.ensure_future(req_intercept(req)))
    self.page.on('console', lambda msg: print(f'console message {msg.type} {msg.text} {msg.args}'))

    await self.page.setViewport({'width': 0, 'height': 0})

  async def getToken(self):
    await self.page.goto(self.base_url)
    return await self.page.Jeval('#forgeryToken', 'el => el.value')

  async def getStates(self):
    await self.page.goto(self.base_url)
    endpoints = await self.page.evaluate('Array.prototype.slice.call(document.getElementById("StateUrl").children).map(ele => { return { state: ele.textContent, endpoint: ele.value}}).filter(ele => ele.state != "--SELECT--")', force_expr=True)
    
    #result = map( lambda el: { 'state': asyncio.run(self.page.evaluate('(element) => element.textContent',el)) , 'endpoint': asyncio.run(self.page.evaluate('(element) => element.value',el)) }, statesDropDown)
    await self.page.goto("https://soilhealth.dac.gov.in/HealthCard/HealthCard/HealthCardPNew?Stname=Assam")
    await self.page.waitForFunction("document.getElementById('State_cd2') != null")
    await self.page.waitForFunction("document.getElementById('State_cd2').length > 1")
    ids = await self.page.evaluate('Array.prototype.slice.call(document.getElementById("State_cd2").children).map(ele => { return { state: ele.textContent, id: ele.value}}).filter(ele => ele.state != "--SELECT--")', force_expr=True)
    
    result = []
    for state in ids:
        res = {}        
        res['name'] = state['state']
        res['id'] = state['id']
        for endpoint in endpoints:
            if state['state'] == endpoint['state']:
                res['endpoint'] = endpoint['endpoint']
        result.append(res)
    return result

  async def getDistricts(self, state_code):
    token = await self.getToken()
    timestamp = int(time.time()) 
    r = requests.get(f'https://soilhealth.dac.gov.in/CommonFunction/GetDistrict', params={
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
    return result

  async def getSubDistricts(self, district):
    token = await self.getToken()
    timestamp = int(time.time()) 
    r = requests.get(f'https://soilhealth.dac.gov.in/CommonFunction/GetSubdis', params={
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
    return result

  async def getBlock(self, district):
    token = await self.getToken()
    timestamp = int(time.time()) 
    r = requests.get(f'https://soilhealth.dac.gov.in/CommonFunction/GetBlock', params={
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
    return result

  async def getVillages(self, subDistrict):
    token = await self.getToken()
    timestamp = int(time.time()) 
    r = requests.get(f'https://soilhealth.dac.gov.in/CommonFunction/GetVillage', params={
       'Sub_discode': subDistrict,
       'VerificationToken': token,
       '_': timestamp,
    })
    villages =  [x for x in r.json() if x['Text'] != '--SELECT--']
    result = []
    for village in villages:
        res = {}        
        res['name'] = village['Text']
        res['id'] = village['Value']
        result.append(res)
    return result

  async def getCards(self, state, district, subdistrict, village):
    print(f"retrieving cards for village {village}")
    token = await self.getToken()
    timestamp = int(time.time()) 
    await self.page.goto("https://soilhealth.dac.gov.in/HealthCard/HealthCard/HealthCardPNew?Stname=Assam")
    
    page = 1
    await self.page.goto(f'https://soilhealth.dac.gov.in/HealthCard/HealthCard/SearchInGridP?S_District_Sample_number=&S_Financial_year=&GetSampleno=&Fname=&Statecode={state}&block=&discode={district}&subdiscode={subdistrict}&village={village}&Source_Type=&Date_Recieve=&VerificationToken={token}&_={timestamp}&page={page}')
    samples = set()
    results = []
    stop = False
    while (await self._pageHasMoreThanOneRow() or page == 1) and stop is False:
        timestamp = int(time.time()) 
        for index in range(1,4):
            sample_element = await self.page.J(F'#MainTable > tbody > tr:nth-child({index}) > td:nth-child(1)')
            village_grid_element = await self.page.J(F'#MainTable > tbody > tr:nth-child({index}) > td:nth-child(2)')
            srno_element = await self.page.J(F'#MainTable > tbody > tr:nth-child({index}) > td:nth-child(3)')

            district_element = await self.page.J(F'#MainTable > tbody > tr:nth-child({index}) > td:nth-child(5)')
            mandal_element = await self.page.J(F'#MainTable > tbody > tr:nth-child({index}) > td:nth-child(6)')
            village_element = await self.page.J(F'#MainTable > tbody > tr:nth-child({index}) > td:nth-child(8)')
            if sample_element and srno_element:
                sample_text = await (await sample_element.getProperty('textContent')).jsonValue()
                village_grid_text = await (await village_grid_element.getProperty('textContent')).jsonValue()

                district_text = await (await district_element.getProperty('textContent')).jsonValue()
                mandal_text = await (await mandal_element.getProperty('textContent')).jsonValue()
                village_text = await (await village_element.getProperty('textContent')).jsonValue()

                srno_text = await (await srno_element.getProperty('textContent')).jsonValue()
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
              await self.page.screenshot({'path': F'error_retrieving_cards_{state}_{district}_{subdistrict}_{village}_{page}.png'})
        page = page + 1
        await self.page.goto(f'https://soilhealth.dac.gov.in/HealthCard/HealthCard/SearchInGridP?S_District_Sample_number=&S_Financial_year=&GetSampleno=&Fname=&block=&Statecode={state}&discode={district}&subdiscode={subdistrict}&village={village}&Source_Type=&Date_Recieve=&VerificationToken={token}&_={timestamp}&page={page}')
        
    return results

  async def _pageHasMoreThanOneRow(self):
    sample_element = await self.page.J(F'#MainTable > tbody > tr:nth-child(2) > td:nth-child(1)')
    if sample_element:
      return True 
    else:
      return False

  async def getCard(self, sample_no, village_grid, sr_no):
    print(f"downloading card {sample_no} {sr_no}")
    Language_Code= "99"
    ShcValidityDateFrom= "NULL"
    ShcValidityDateTo= "NULL"
    shcformate= "NewFormat"
    url = f'https://soilhealth.dac.gov.in/HealthCard/HealthCard/HealthCardNewPartialP?Language_Code={Language_Code}&Sample_No={urllib.parse.quote(sample_no,safe="")}&ShcValidityDateFrom={ShcValidityDateFrom}&ShcValidityDateTo={ShcValidityDateTo}&Sr_No={sr_no}&Unit_Code=17&shcformate={shcformate}'
    counter = 1
    page = await self.browser.newPage()
    while counter < 5:
        try:
          try:
            await page.goto(self.base_url)
            report_html = await self._getCardInner(page, counter, url)
            if len(report_html) > 60*1024:
              #await page.close()
              return report_html
            else:
              sample_no_escaped = sample_no.replace('/','-')
              await page.screenshot({'path': F'error_download_{sample_no_escaped}_{sr_no}_{counter}.png'})
          except pyppeteer.errors.TimeoutError:
            logging.exception(f"error downloading card {sample_no} {sr_no}")
            sample_no_escaped = sample_no.replace('/','-')
            await page.screenshot({'path': F'error_download_{sample_no_escaped}_{sr_no}_{counter}.png'})
            #await page.close()
            raise pyppeteer.errors.TimeoutError
        except pyppeteer.errors.TimeoutError:
            counter = counter + 1
     
    raise UnableToDownloadCard

  async def _getCardInner(self, page, counter, url):
    await page.goto(url)
    #await page.waitFor(15000*counter)
    await page.waitForFunction("document.querySelector('body > iframe') != null")   
    await page.waitForFunction("document.querySelector('body > iframe').contentWindow != null")  
    await page.waitForFunction("document.querySelector('body > iframe').contentWindow.document != null")  
    await page.waitForFunction("document.querySelector('body > iframe').contentWindow.document.querySelector('#VisibleReportContentReportViewer1_ctl09') != null")    
    await page.waitForFunction("document.querySelector('body > iframe').contentWindow.document.querySelector('#VisibleReportContentReportViewer1_ctl09').children.length > 0")
    iframe = await (await page.J('body > iframe')).contentFrame()
    return await iframe.content()

if __name__ == "__main__":
    #shc_dl = ShcDL()
    #asyncio.run(shc_dl.setup())
    #print(asyncio.run(shc_dl.getStates()))
    #print(asyncio.run(shc_dl.getDistricts(18)))
    #print(asyncio.run(shc_dl.getCards(18, 325,2147,304962)))
    #print(asyncio.run(shc_dl.getCard("AS304962/2017-18/137225214", "114", "0")))
    #print(asyncio.run(shc_dl.getCard("AS304962/2017-18/137225214", "114", "0")))
    state = 18
    district = 325
    mandal = 2147
    village = 304962
    overwrite = ""
    shc_dl = ShcDL()
    asyncio.run( shc_dl.setup())
    cards = asyncio.run( shc_dl.getCards(state, district, mandal, village))
    for card in cards:
        file_path = storage.getFilePath(state, district, mandal, village, card['sample'], card['sr_no'])
        if not storage.isFileDownloaded(file_path) or overwrite == "true":
            shc = ""
            counter = 5

            while shc == "" and counter > 0:
                try:
                    shc = asyncio.run( shc_dl.getCard(card['sample'], card['village_grid'], card['sr_no']))
                except pyppeteer.errors.TimeoutError:
                    counter = counter - 1
                except UnableToDownloadCard:
                    counter = counter - 1

            if len(shc) > 60*1024:
                storage.uploadFile(file_path, shc, {
                    'state': state,
                    'district':  card['district'].strip(),
                    'district_code': district,
                    'mandal': card['mandal'].strip(),
                    'mandal_code': mandal,
                    'village':  card['village'].strip(),
                    'village_code': village
                })
            else:
                print(f'failed downloading file {file_path}') 
        else:
           logging.warning(f'skipping file {file_path} since its already downloaded') 