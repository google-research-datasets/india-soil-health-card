"""Download soil health cards for the given state.
"""

import logging
import tempfile
import storage
from typing import Sequence
from absl import app
from absl import flags
import utils

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

from random import getstate, randrange
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


class ReportServerUpdating(Exception):
     pass

class UnableToDownloadCard(Exception):
     pass

async def req_intercept(req: Request):
    req.headers.update({'X-Requested-With': 'XMLHttpRequest'})
    await req.continue_(overrides={'headers': req.headers})

class ShcDL:
  base_url = 'https://soilhealth.dac.gov.in/HealthCard/HealthCard/state'

  async def setup(self):
    self.userDataDir = tempfile.TemporaryDirectory()
    if not os.environ.get('RUN_LOCALLY'):
      self.browser = await pyppeteer.launch({ 'headless': True, 'executablePath': '/usr/bin/chromium','userDataDir':self.userDataDir.name, 'autoClose': False, 'args': ['--no-sandbox']},
          handleSIGINT=False,
          handleSIGTERM=False,
          handleSIGHUP=False
        )
    else:
      self.browser = await pyppeteer.launch({ 'headless': True,'autoClose': False, 'args': ['--no-sandbox'] },
          handleSIGINT=False,
          handleSIGTERM=False,
          handleSIGHUP=False)
    self.page = await self.browser.newPage()
    #self.page.on('console', lambda msg: utils.logText(f'console message {msg.type} {msg.text} {msg.args}'))
    self.states = { state['id'] : state for state in await self.getStates() }
    await self.page.setViewport({'width': 0, 'height': 0})

  async def close(self):
    await self.page.close()
    await self.browser.close()
    self.userDataDir.cleanup()

  async def getToken(self):
    await self.page.goto(self.base_url)
    return await self.page.Jeval('#forgeryToken', 'el => el.value')

  async def getStates(self):
    await self.page.goto(self.base_url)
    endpoints = await self.page.evaluate('Array.prototype.slice.call(document.getElementById("StateUrl").children).map(ele => { return { state: ele.textContent, endpoint: ele.value}}).filter(ele => ele.state != "--SELECT--")', force_expr=True)
    ids={}
    for endpoint in endpoints:
      #result = map( lambda el: { 'state': asyncio.run(self.page.evaluate('(element) => element.textContent',el)) , 'endpoint': asyncio.run(self.page.evaluate('(element) => element.value',el)) }, statesDropDown)
      await self.page.goto(endpoint['endpoint']) #"https://soilhealth.dac.gov.in/HealthCard/HealthCard/HealthCardPNew?Stname=Assam"
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
                res['endpoint'] = endpoint['endpoint'].replace("HealthCardPNew","")
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

  async def getVillages(self, state, district, subDistrict):
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
        res['state_id'] = state
        res['district_id'] = district
        res['mandal_id'] = subDistrict
        result.append(res)
    return result

  async def getCards(self, state, district, subdistrict, village):
    utils.logText(f"retrieving cards for village {village}")
    await self.page.setRequestInterception(True)
    self.page.on('request', lambda req: asyncio.ensure_future(req_intercept(req)))
    token = await self.getToken()
    timestamp = int(time.time()) 
    #await self.page.goto("https://soilhealth.dac.gov.in/HealthCard/HealthCard/HealthCardPNew?Stname=Assam")
    
    page = 1
    state_endpoint = self.states[state]['endpoint']
    url = f'{state_endpoint}/SearchInGridP?S_District_Sample_number=&S_Financial_year=&GetSampleno=&Fname=&Statecode={state}&block=&discode={district}&subdiscode={subdistrict}&village={village}&Source_Type=&Date_Recieve=&VerificationToken={token}&_={timestamp}&page={page}'
    await self.page.goto(url)
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
              await self.screenshot( F'error_retrieving_cards_{state}_{district}_{subdistrict}_{village}_{page}.png')
        page = page + 1
        await self.page.goto(f'{state_endpoint}/SearchInGridP?S_District_Sample_number=&S_Financial_year=&GetSampleno=&Fname=&block=&Statecode={state}&discode={district}&subdiscode={subdistrict}&village={village}&Source_Type=&Date_Recieve=&VerificationToken={token}&_={timestamp}&page={page}')
        
    return results

  async def screenshot(self, path):
      content = await self.page.screenshot()
      storage.uploadFile("screenshots/"+path, content,{})

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
    url = f'{state_endpoint}/HealthCardNewPartialP?Language_Code={Language_Code}&Sample_No={urllib.parse.quote(sample_no,safe="")}&ShcValidityDateFrom={ShcValidityDateFrom}&ShcValidityDateTo={ShcValidityDateTo}&Sr_No={sr_no}&Unit_Code=17&shcformate={shcformate}'
    print(f"Loading sample url {url}")
    counter = 1
    while counter < 5:
        try:
          try:
            report_html = await self._getCardInner(self.page, counter, url)
            if len(report_html) > 60*1024:
              #await page.close()
              return report_html
            else:
              sample_no_escaped = sample_no.replace('/','-')
              await self.screenshot( F'error_download_{sample_no_escaped}_{sr_no}_{counter}.png')
          except pyppeteer.errors.TimeoutError:
            logging.exception(f"error downloading card {sample_no} {sr_no}")
            sample_no_escaped = sample_no.replace('/','-')
            await self.screenshot( F'error_download_{sample_no_escaped}_{sr_no}_{counter}.png')
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
    
    file_path = storage.getFilePath(card['state_id'], card['district_id'], card['mandal_id'], card['village_id'], card['sample'], card['sr_no'])
    if not storage.isFileDownloaded(file_path) or overwrite == "true":
        shc = ""
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

if __name__ == "__main__":
  #overwrite = "true"
  #card = {'district': 'Hnahthial', 'district_id': '1070', 'mandal': 'Hnahthial', 'mandal_id': '1915', 'sample': 'MZ271610/2016-17/10343339', 'sr_no': 1, 'state_id': '15', 'village': 'Darzo', 'village_grid': '7', 'village_id': '271610'}
  #asyncio.run(fetchCard(card, overwrite)).
  shc = ShcDL()
  asyncio.run(shc.setup())
  print(asyncio.run(shc.getStates()))