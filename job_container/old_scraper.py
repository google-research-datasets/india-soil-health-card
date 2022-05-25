"""Download soil health cards for the given state.
"""

import logging
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
from google.cloud import storage
storage_client = storage.Client()

bucket = storage_client.bucket(os.getenv("GCS_BUCKET"))

_STATE = flags.DEFINE_string('state', None, 'Indian state.')

#cns_prefix = '/cns/vk-d/home/aksaw/anthrokrishi/shc/'
#cns_prefix = '/google/src/cloud/grotz/anthrokrishi_scraping/google3/research/climate/anthrokrishi/shc/data/'
cns_prefix = 'shcs/'

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
    await self.page.setViewport({'width': 0, 'height': 0})

  async def navigateToSHCPage(self, state):
    await self.page.goto(self.base_url)
    dropdown = await self.page.J('#StateUrl')
    await dropdown.type(state)
    continue_button = await self.page.J('#skip > div > div:nth-child(2) > fieldset > div.modal-footer > input')
    await asyncio.gather(
      self.page.waitForNavigation(),
      continue_button.click())

  async def getDistrictsForState(self, state):
    df = pd.DataFrame()
    await self.navigateToSHCPage(state)
    self.page.waitFor(2000)
    districts = await self._getDistrictCodes()
    return districts

  async def getMandalsForStateAndDistricts(self, state, district):
    df = pd.DataFrame()
    await self.navigateToSHCPage(state)
    self.page.waitFor(2000)
    await self._selectDistrict(district)
    mandals = await self._getMandalCodes()
    return mandals

  async def getVillagesForMandal(self, state, district, mandal):
    df = pd.DataFrame()
    await self.navigateToSHCPage(state)
    self.page.waitFor(2000)
    await self._selectDistrict(district)
    await self._selectMandal(mandal)
    villages = await self._getVillageCodes()
    return villages

  async def getAllSearchOptionsForState(self, state):
    df = pd.DataFrame()
    await self.navigateToSHCPage(state)
    self.page.waitFor(2000)
    districts = await self._getDistrictCodes()
    for dist in districts:
      try:
        await self._selectDistrict(dist)
        mandals = await self._getMandalCodes()
        for mandal in mandals:
          try:
            await self._selectMandal(mandal)
            villages = await self._getVillageCodes()
            df1 = pd.DataFrame({'village':villages})
            df1['mandal'] = mandal
            df1['district'] = dist
            df = pd.concat([df, df1])
            df.reset_index(drop=True)
          except pyppeteer.errors.TimeoutError:         
            print(f'No Villages for state={state},district={dist},mandal={mandal}')
      except pyppeteer.errors.TimeoutError:         
        print(f'No Mandals for state={state},district={dist}')
    return df

  async def _selectDistrict(self, district):
    await self.page.select('#Dist_cd2', district)
    await self.page.waitForFunction("document.getElementById('Sub_dis2').length > 1")
    await self.page.waitFor(2000)

  async def _selectMandal(self, mandal):
    await self.page.select('#Sub_dis2', mandal)
    await self.page.waitForFunction("document.getElementById('village_cd2').length > 1")
    await self.page.waitFor(2000)

  async def _selectVillage(self, village):
    await self.page.select('#village_cd2', village)
    await self.page.waitFor(2000)

  async def _search(self):
    search_button = await self.page.J('#tb_serch > tr:nth-child(8) > td > a:nth-child(1)')
    await search_button.click()
    # await self.page.waitForFunction("document.getElementById('MainTable')")
    await self.page.waitFor(15000)
    await self.page.evaluate("""{window.scrollBy(0, document.body.scrollHeight);}""")
   #await self.page.screenshot({'path': 'search.png'})

  async def searchWithOptions(self, district, mandal, village):
    await self._selectDistrict(district)
    await self._selectMandal(mandal)
    await self._selectVillage(village)
    await self._search()

  async def waitForOptions(self, selector):
    # TODO add a circuit breaker to assure a maximum runtime and consider an error or something should the select then still only contain --SELECT--
    while len(await self.page.querySelectorAll(selector)) <= 1:
      self.page.waitFor(50)

  async def _getDistrictCodes(self):
    await self.waitForOptions('#Dist_cd2 > option')
    districts = re.findall('value="(\d+)"', (await self.page.Jeval('#Dist_cd2', 'el => el.innerHTML')))
    return districts

  async def _getMandalCodes(self):
    mandals = re.findall('value="(\d+)"', (await self.page.Jeval('#Sub_dis2', 'el => el.innerHTML')))
    return mandals

  async def _getVillageCodes(self):
    villages = re.findall('value="(\d+)"', (await self.page.Jeval('#village_cd2', 'el => el.innerHTML')))
    return villages

  async def _getFileName(self, index):
    sample_element = await self.page.J(F'#MainTable > tbody > tr:nth-child({index}) > td:nth-child(1)')
    sample_text = await (await sample_element.getProperty('textContent')).jsonValue()
    sample_text= sample_text.replace('/','-')

    srno_element = await self.page.J(F'#MainTable > tbody > tr:nth-child({index}) > td:nth-child(3)')
    srno_text = await (await srno_element.getProperty('textContent')).jsonValue()
    srno_text = int(srno_text) - 1

    if srno_text:
      return sample_text + ' (' + str(srno_text) + ').pdf'
    else:
      return sample_text + '.pdf'

  async def _isFileDownloaded(self, index):
    pdf_file_name = await self._getFileName(index)
    html_file_name = pdf_file_name[:-3] + 'html'
    
    file_path = cns_prefix + self.state+ "/"+ self.district + "/"+ self.mandal +"/"+ self.village +"/"+ html_file_name

    #return os.path.exists(os.path.join(self.browser_download_location, file_name))
    return storage.Blob(bucket=bucket, name=file_path).exists(storage_client)

  async def _saveHtmlReport(self, index):
    iframe = await (await self.page.J('#aaa > iframe')).contentFrame()
    report_html = await iframe.content()

    pdf_file_name = await self._getFileName(index)
    html_file_name = pdf_file_name[:-3] + 'html'
    circuit_breaker = 0
    while len(report_html) < 1000 and circuit_breaker < 5:
      await self.page.waitFor(1000)
      iframe = await (await self.page.J('#aaa > iframe')).contentFrame()
      report_html = await iframe.content()
      circuit_breaker = circuit_breaker + 1

    if len(report_html) < 1000:
      logging.warning("Unable to download report {file_name} (state={state},district={district},mandal={mandal},village={village})", file_name=html_file_name, state=self.state, district=self.district,mandal=self.mandal,village=self.village )
      return

    file_path = cns_prefix + self.state +"/"+ self.district +"/"+ self.mandal +"/"+ self.village +"/"+ html_file_name
    print("Writing SHC to "+file_path)
    blob = bucket.blob(file_path)
    metadata = {
      'state': self.state,
      'district': self.district,
      'mandal': self.mandal,
      'village': self.village
    }
    blob.metadata = metadata
    blob.upload_from_string(report_html)
      

  async def _loadSHC(self, index):
    print_target = await self.page.J(F'#MainTable > tbody > tr:nth-child({index}) > td:nth-child(11) > a')
    await print_target.click()
    await self.page.waitForFunction("document.getElementById('aaa') != null")
    await self.page.waitForFunction("document.getElementById('aaa').children.length > 0")
    await self.page.waitForFunction("document.querySelector('#aaa > iframe') != null")
    await self.page.waitForFunction("document.querySelector('#aaa > iframe').contentWindow.document.querySelector('#ReportViewer1_fixedTable > tbody > tr:nth-child(4)') != null")
    await self.page.waitForFunction("document.querySelector('#aaa > iframe').contentWindow.document.querySelector('#ReportViewer1_fixedTable > tbody > tr:nth-child(4)').children.length > 0")
      
    await self.page.waitFor(15000) # Better waiting strategy is important here.
   #await self.page.screenshot({'path': F'load_shc_{index}.png'})

  async def _downloadSHC(self):
    iframe = await (await self.page.J('#aaa > iframe')).contentFrame()

    export_target = await iframe.J('#ReportViewer1_ctl05 > div > div:nth-child(7)')
    await export_target.click()
    await self.page.waitFor(1000)
   #await self.page.screenshot({'path':'export.png'})

    await iframe.waitForSelector('#ReportViewer1_ctl05_ctl04_ctl00_Menu > div:nth-child(4) > a')
    download_target = await iframe.J('#ReportViewer1_ctl05_ctl04_ctl00_Menu > div:nth-child(4) > a')
    # await download_target.click()
    await iframe.evaluate('(btn) => btn.click()', download_target)
    await self.page.waitFor(1000)

  async def _doesShcListTableRowExist(self, index):
    for i in range(1,5):
      sample_element = await self.page.J(F'#MainTable > tbody > tr:nth-child({index}) > td:nth-child(1)')
      if sample_element:
        return True 
      await self.page.waitFor(1000*i)
    return False

  async def _getNewSHCOnPage(self):
    for i in range(1,4):
      if not (await self._doesShcListTableRowExist(i)):
        print('row does not exist')
        return
      if (await self._isFileDownloaded(i)): # skip loading and downloading already fetched files
        print('skipping report download')
        continue

      await self._loadSHC(i)
      await self._saveHtmlReport(i)
      # await self._downloadSHC() # Always downloads loaded SHC

  async def _getNextPage(self):
      next_button_list = await self.page.Jx('//*[@id="MainTable"]/tfoot/tr/td/a[contains(.,"Next >")]')
      if not next_button_list:
        return False
      next_click_target = next_button_list[0]
      if not next_click_target:
        return False
      await next_click_target.click()
      await self.page.waitFor(5000)
      return True

  async def getAllSHCForVillage(self):
    next_page_loaded = True
    while (next_page_loaded):
      await self._getNewSHCOnPage()
      next_page_loaded = await self._getNextPage()

  async def getAllSHCForState(self, state):
    self.state = state
    await self.navigateToSHCPage(state)
    districts = await self._getDistrictCodes()
    for dist in districts:
      self.district = dist
      await self._selectDistrict(dist)
      mandals = await self._getMandalCodes()
      for mandal in mandals:
        self.mandal = mandal
        await self._selectMandal(mandal)
        villages = await self._getVillageCodes()
        for village in villages:
          self.village = village
          await self._selectVillage(village)
          await self.getAllSHCForVillage()
          await self._search()

  async def getAllSHCForStateAndDistrict(self, state, district):
    self.state = state
    self.district = district
    await self.navigateToSHCPage(state)
    await self._selectDistrict(district)
    mandals = await self._getMandalCodes()
    for mandal in mandals:
      self.mandal = mandal
      await self._selectMandal(mandal)
      villages = await self._getVillageCodes()
      for village in villages:
        self.village = village
        await self._selectVillage(village)
        await self.getAllSHCForVillage()
        await self._search()

  async def getAllSHCForMandal(self, state, district, mandal):
    self.state = state
    self.district = district
    self.mandal = mandal
    await self.navigateToSHCPage(state)
    await self._selectDistrict(district)
    await self._selectMandal(mandal)
    villages = await self._getVillageCodes()
    for village in villages:
      self.village = village
      await self._selectVillage(village)
      await self.getAllSHCForVillage()
      await self._search()

  async def loadAllSHCForVillage(self, state, district, mandal, village):
    self.state = state
    self.district = district
    self.mandal = mandal
    self.village = village
    await self.navigateToSHCPage(state)
    await self.page.waitFor(1000)
    await self._selectDistrict(district)
    await self.page.waitFor(1000)
    await self._selectMandal(mandal)
    await self.page.waitFor(1000)
    await self._selectVillage(village)
    await self.page.waitFor(1000)
    await self._search()
    await self.page.waitFor(3000)
    await self.getAllSHCForVillage()

  async def dtor(self):
   #await self.page.screenshot({'path':'dtor.png'})
    await self.browser.close()

  async def getAllSHCForStateAsync(self, state):
    await self.navigateToSHCPage(state)
    districts = await self._getDistrictCodes()
    for dist in districts:
      await self._selectDistrict(dist)
      mandals = await self._getMandalCodes()
      for mandal in mandals:
        await self._selectMandal(mandal)
        villages = await self._getVillageCodes()
        print("downloading shc for",state, dist, mandal, villages)
        #loop = asyncio.get_event_loop()
        #loop.run_until_complete(asyncio.gather(
        #    *(downloadSHC(state, dist, mandal, village) for village in villages)
        #))
        #loop.close()

  async def getSHC(self, state, district, mandal, village):
    await self.navigateToSHCPage(state)
    await self.searchWithOptions(district, mandal, village)
    await self.getAllSHCForVillage()


async def downloadSHC(state, district, mandal, village):
  schDL = ShcDL()
  await schDL.setup()
  await schDL.getSHC(state, district, mandal, village)



