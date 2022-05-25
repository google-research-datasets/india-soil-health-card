import json
import os
import asyncio
import re
import math
import json

import pyppeteer
from sqlalchemy import null
import storage
import scraper
import old_scraper as old_scraper
import database as database
from flask import Flask, jsonify, request,Response
import shc_html_extractor
import base64
import utils
import analytics_storage
import logging
import traceback
import sys
import storage as scraper_storage
from google.cloud import spanner
from google.cloud import storage
from google.cloud import tasks_v2
from google import api_core
storage_client = storage.Client()
tasks_client = tasks_v2.CloudTasksClient()

bucket = storage_client.bucket(os.getenv("GCS_BUCKET"))

file_prefix = 'shcs/'
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

shc_dl = scraper.ShcDL()
asyncio.run(shc_dl.setup())

def insertStates(states):
    with database.batch() as batch:
        batch.insert_or_update(
            table="States",
            columns=("StateId", "Name"),
            values=[(int(state['id']), state['name']) for state in states],
        )

def insertDistricts(stateId, districts):
    with database.batch() as batch:
        batch.insert_or_update(
            table="Districts",
            columns=("DistrictId", "StateId", "Name"),
            values=[(int(district['id']),int(stateId), district['name']) for district in districts],
        )

def insertSubDistricts(districtId, subDistricts):
    with database.batch() as batch:
        batch.insert_or_update(
            table="SubDistricts",
            columns=("SubDistrictId", "DistrictId", "Name"),
            values=[(int(subDistrict['id']),int(districtId), subDistrict['name']) for subDistrict in subDistricts],
        )

def insertVillages(subDistrictId, villages):
    with database.batch() as batch:
        batch.insert_or_update(
            table="Villages",
            columns=("VillageId", "SubDistrictId", "Name"),
            values=[(int(village['id']),int(subDistrictId), village['name']) for village in villages],
        )

def insertCards(villageId, cards):
    if len(cards) > 0:
        with database.batch() as batch:
            batch.insert_or_update(
                table="Cards",
                columns=("VillageId", "Sample", "VillageGrid", "SrNo"),
                values=[(villageId, card['sample'],card['village_grid'],card['sr_no']) for card in cards],
            )

def getCheckpoint(Id):
    with database.snapshot() as snapshot:
        countResults = snapshot.execute_sql(
                '''SELECT StateId, DistrictId, SubDistrictId FROM Checkpoints WHERE Id = @id''',
            params={"id": Id},
            param_types={"id": spanner.param_types.INT64},
            )
        for countResult in countResults:
            StateId = int(countResult[0])
            DistrictId = int(countResult[1])
            SubDistrictId = int(countResult[2])
            return StateId, DistrictId, SubDistrictId
    return -1,-1,-1

def updateCheckpoint(Id, StateId, DistrictId, SubDistrictId):
    with database.batch() as batch:
        batch.insert_or_update(
            table="Checkpoints",
            columns=("Id", "StateId", "DistrictId", "SubDistrictId"),
            values=[
                (Id, StateId, DistrictId, SubDistrictId),
            ],
        )

def markCard(VillageId,Sample, SrNo, Ingested):
    database.run_in_transaction(lambda transaction: transaction.execute_update(
        "UPDATE Cards "
        "SET Ingested = @Ingested "
        "WHERE VillageId = @VillageId AND Sample = @Sample And SrNo = @SrNo",
        params={"VillageId":VillageId, "Sample": Sample, "SrNo": SrNo, "Ingested": Ingested},
        param_types={"VillageId": spanner.param_types.INT64,"Sample": spanner.param_types.STRING,"SrNo": spanner.param_types.INT64,"Ingested": spanner.param_types.BOOL },
    ))

def markVillage(VillageId, CardsLoaded):
    database.run_in_transaction(lambda transaction: transaction.execute_update(
        "UPDATE Villages "
        "SET CardsLoaded = @CardsLoaded "
        "WHERE VillageId = @VillageId ",
        params={"VillageId":VillageId, "CardsLoaded": CardsLoaded},
        param_types={"VillageId": spanner.param_types.INT64,"CardsLoaded": spanner.param_types.BOOL },
    ))

async def ingest():
    StateId, DistrictId, SubDistrictId = getCheckpoint(1)
    print("Loading States")
    states = [state[1] for state in scraper.offlineStates().items()]
    states = sorted(states, key=lambda state: int(state['id']))
    insertStates(states)
    for state in states:
        print(f"Loading and Ingesting Districts for state {state['id']}")
        if (int(state['id']) >= StateId or StateId == -1):
            districts = await shc_dl.getDistricts(state['id'])
            districts = sorted(districts, key=lambda district: int(district['id']))
            if len(districts) >0:
                insertDistricts(state['id'], districts)
                for district in districts:
                    if int(district['id']) >= DistrictId or DistrictId == -1:
                        print(f"Loading and Ingesting SubDistricts for state {state['id']} and district {district['id']}")
                        subDistricts = await shc_dl.getSubDistricts(state['id'], district['id'])
                        subDistricts = sorted(subDistricts, key=lambda subDistrict: int(subDistrict['id']))
                        if len(subDistricts) >0:
                            insertSubDistricts(district['id'], subDistricts)
                            for subDistrict in subDistricts:
                                if int(subDistrict['id']) >= SubDistrictId or SubDistrictId == -1:
                                    print(f"Loading and Ingesting Villages for state {state['id']}, district {district['id']}, subDistrict {subDistrict['id']}")
                                    villages = await shc_dl.getVillages(state['id'], district['id'], subDistrict['id'])
                                    if len(villages) >0:
                                        insertVillages(subDistrict['id'], villages)
                                    updateCheckpoint(1,state['id'], district['id'], subDistrict['id'])
                                else:
                                    print(f"Skipping subDistrict {subDistrict['id']}")
                        updateCheckpoint(1,state['id'], district['id'], -1)
                        SubDistrictId = -1
                    else:
                        print(f"Skipping district {district['id']}")
            updateCheckpoint(1,state['id'], -1, -1)
            DistrictId = -1
        else:
            print(f"Skipping state {state['id']}")
    await shc_dl.close()

async def ingestCards():
    checkpointId = 2+int(TASK_INDEX)
    start, t1, t2 = getCheckpoint(checkpointId)
    print("Loading amount of unchecked villages")
    with database.snapshot() as snapshot:
        results = snapshot.execute_sql(
            '''SELECT count(*) FROM Villages WHERE not CardsLoaded is True''',
        )
        results = list(results)
        count = results[0][0]
        limit = math.ceil(count / int(TASK_COUNT))
        offset = limit*int(TASK_INDEX)
    
    
    if count < 1:
        print("No unchecked villages at the moment")
        return
    else:
        print(f"At the moment at total of {count} villages are pending, scraping from {offset} {limit} villages with checkpoint start {start}")

    with database.snapshot() as snapshot:
        results = snapshot.execute_sql(
            '''SELECT VillageId, SubDistrictId, DistrictId, StateId FROM VILLAGES_VIEW
                WHERE not CardsLoaded is True
                ORDER BY VillageId 
                LIMIT @limit OFFSET @offset''',
            params={ "limit":limit, "offset": offset},
            param_types={"limit": spanner.param_types.INT64,"offset": spanner.param_types.INT64},
        )
        
        for row in results:
            VillageId = row[0]
            SubDistrictId = row[1]
            DistrictId = row[2]
            StateId = row[3]
            print(f"Loading Card for {StateId} {DistrictId} {SubDistrictId} {VillageId}")
            cards = await shc_dl.getCards(str(StateId), str(DistrictId), str(SubDistrictId), str(VillageId))
            insertCards(VillageId,cards )
            markVillage(int(VillageId),True)
            updateCheckpoint(checkpointId, int(VillageId), -1, -1)

async def scrapeCards():
    print("Loading amount of uningested cards")
    with database.snapshot() as snapshot:
        results = snapshot.execute_sql(
            '''SELECT count(*) FROM Cards WHERE not Ingested is True''',
        )
        results = list(results)
        count = results[0][0]
        limit = math.ceil(count / int(TASK_COUNT))
        offset = limit*int(TASK_INDEX)

    if count < 1:
        print("No uningested cards at the moment")
        return
    else:
        print(f"At the moment at total of {count} cards is pending, scraping from {offset} {limit} cards")

    with database.snapshot() as snapshot:
        results = snapshot.execute_sql(
            '''SELECT * FROM Cards WHERE not Ingested is True 
                ORDER BY VillageId 
                LIMIT @limit OFFSET @offset''',
            params={"limit":limit, "offset": offset},
            param_types={"limit": spanner.param_types.INT64,"offset": spanner.param_types.INT64},
        )
        
        for row in results:
            VillageId = str(row[0])
            Sample = row[1]
            VillageGrid = row[2]
            SrNo = str(row[3])
            with database.snapshot() as snapshot:
                results2 = snapshot.execute_sql(
                    '''SELECT VillageName, SubDistrictId, SubDistrictName, DistrictId, DistrictName, StateId, StateName
                     FROM VILLAGES_VIEW WHERE VillageId =  @VillageId''',
                    params={"VillageId":int(VillageId)},
                    param_types={"VillageId": spanner.param_types.INT64},
                )
                for row2 in results2:
                    VillageName = row2[0]
                    SubDistrictId = str(row2[1])
                    SubDistrictName = row2[2]
                    DistrictId = str(row2[3])
                    DistrictName = row2[4]
                    StateId = str(row2[5])
                    StateName = row2[6]

            card = {
                'sample': Sample,
                'village_grid': VillageGrid,
                'sr_no': SrNo,
                'district': DistrictName,
                'mandal': SubDistrictName,
                'village': VillageName,
                'state_id': StateId,
                'state':StateName,
                'district_id': DistrictId,
                'mandal_id': SubDistrictId,
                'village_id': VillageId
            }
            print(f"scraping card {card}")
            content = await scraper.fetchCard(card, False)
            markCard(int(VillageId), Sample, int(SrNo), True)

            try:
                extractor = shc_html_extractor.ShcHtmlExtractor(content)
                extracted = extractor.extract()
                analytics_storage.insertCard(StateId,DistrictId,SubDistrictId, VillageId, Sample, SrNo, extracted)
            except Exception as e:
                logging.error(traceback.format_exc())

def createTasks():
    with database.snapshot() as snapshot:
        results = snapshot.execute_sql(
            '''SELECT * FROM UNIGESTED_CARDS''',
        )
        
        for row in results:
            Sample = row[0]
            VillageGrid = row[1]
            SrNo = row[2]
            VillageId = row[3]
            VillageName = row[4]
            SubDistrictId = row[5]
            SubDistrictName = row[6]
            DistrictId = row[7]
            DistrictName = row[8]
            StateId = row[9]
            StateName = row[10]
            states = scraper.offlineStates()
            stateData = states[str(StateId)]

            PROJECT_ID = "grotz-pso-team"
            LOCATION_ID = "europe-west1"
            QUEUE_ID = "shc-card-queue"

            body = json.dumps({
              'sample': Sample,
              'village_grid': VillageGrid,
              'sr_no': SrNo,
              'district': DistrictName,
              'mandal': SubDistrictName,
              'village': VillageName,
              'state_id': StateId,
              'state':StateName,
              'district_id': DistrictId,
              'mandal_id': SubDistrictId,
              'village_id': VillageId
          }, indent=4, sort_keys=True)
            TASK_ID=f"{Sample.replace('/','-')}_{str(SrNo) }"
            TASK_NAME=f"projects/{PROJECT_ID}/locations/{LOCATION_ID}/queues/{QUEUE_ID}/tasks/{TASK_ID}"
            try:
                task = tasks_client.get_task(tasks_v2.GetTaskRequest(
                    name=TASK_NAME
                ))
                if task:
                    print(f"a task for card {Sample} {SrNo} exists in Queue")
                    continue
            except api_core.exceptions.NotFound as exp:
                print(f"no task for card {Sample} {SrNo} exists in Queue")
            file_path = scraper_storage.getFilePath(StateId, DistrictId, SubDistrictId, VillageId, Sample, str(SrNo))
            if not scraper_storage.isFileDownloaded(file_path):
                response = tasks_client.create_task(
                    request=tasks_v2.CreateTaskRequest(
                        parent=f"projects/{PROJECT_ID}/locations/{LOCATION_ID}/queues/{QUEUE_ID}",
                        task=tasks_v2.Task(
                            name=TASK_NAME,
                            http_request=tasks_v2.HttpRequest(
                                url="https://scraper-pubsub-myfiqpbfbq-ew.a.run.app/download",
                                http_method=tasks_v2.HttpMethod.POST,
                                body=body.encode(),
                                headers={
                                    "Content-Type": "application/json"
                                },
                                oidc_token=tasks_v2.OidcToken(
                                    service_account_email="anthro-workflows@grotz-pso-team.iam.gserviceaccount.com",
                                    audience="https://scraper-pubsub-myfiqpbfbq-ew.a.run.app"
                                )
                            )
                        )
                    )
                )

def mapToDb(item):
    result = re.match('shcs\/.*\/.*\/.*\/(.*)_(.*)\).html', item.name)
    if result:
        sample = result.groups()[0] 
        sr_no = int(result.groups()[1])
        metadata = item.metadata
        village_code = metadata['village_code']
        return (village_code, sample, sr_no, True)

if __name__ == "__main__":
    print(f"Running {MODE} {TASK_INDEX}/{TASK_COUNT}")
    if MODE and MODE == "INGEST":
        print("Ingesting Metadata")
        asyncio.run(shc_dl.newPage())
        asyncio.run(ingest())
    elif MODE and MODE == "CARDS":
        print("Ingesting Cards")
        asyncio.run(ingestCards())
    elif MODE and MODE == "TASKS":
        print("Create Tasks for not Ingested Cards")
        createTasks()
    elif MODE and MODE == "SCRAPE":
        print("Scrape Cards")
        asyncio.run(scrapeCards())
    elif MODE == "CARDS_FROM_GCS":
        objects = list(storage_client.list_blobs(bucket_or_name=os.getenv("GCS_BUCKET"), prefix="shcs"))
        items = [mapToDb(item)for item in objects]
        items = list(filter(None, items))

        for i in range(0, len(items), 2000):
            sublist = items[i:i + 2000]
            with database.batch() as batch:
                batch.insert_or_update(
                    table="Cards",
                    columns=("VillageId", "Sample", "SrNo", "Ingested"),
                    values=sublist,
                )
    else:
        print("Nothing todo")
    