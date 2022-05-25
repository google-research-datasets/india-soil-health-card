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

import os
import asyncio
import re
import math

import scraper

from google.cloud import spanner

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
            await scraper.fetchCard(card, False)
            markCard(int(VillageId), Sample, int(SrNo), True)
            
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
    elif MODE and MODE == "SCRAPE":
        print("Scrape Cards")
        asyncio.run(scrapeCards())
    else:
        print("Nothing todo")
    