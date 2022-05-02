import os
import asyncio

import pyppeteer
import storage
import scraper
import old_scraper as old_scraper
import database as database
from flask import Flask, jsonify, request
import google.cloud.logging
import logging

app = Flask(__name__)

#client = google.cloud.logging.Client()
#client.setup_logging()

pool = database.init_connection_engine()

@app.route("/options")
async def getAllOptions():
    ingested = request.args.get("ingested")
    limit = request.args.get("limit")
    print("getAllOptions ingested=%s", request.args.get("ingested"))
    options = await database.getAllOptions(pool, ingested, limit)
    return jsonify(options)

@app.route("/states/<state>/options/ingest")
async def ingestAllOptionsForState(state):
    print("ingestAllOptionsForState state=%s",state)
    shc_dl = old_scraper.ShcDL()
    await shc_dl.setup()

    options = await shc_dl.getAllSearchOptionsForState(state)
    
    for index, row in options.iterrows():
        mandal = row['mandal']
        district = row['district']
        village = row['village']
        database.insertOptions(pool, state, district, mandal, village)


@app.route("/states/<state>/options")
async def getAllOptionsForState(state):
    ingested = request.args.get("ingested")
    limit = request.args.get("limit")
    print("getAllOptionsForState state=%s, ingested=%s",state, request.args.get("ingested"))
    options = database.getAllOptionsForState(pool, state, ingested, limit)
    return jsonify(options)

@app.route("/states")
async def getStates():
    shc_dl = scraper.ShcDL()
    await shc_dl.setup()
    states = await shc_dl.getStates()
    return jsonify(states)

@app.route("/states/<state>/districts")
async def getDistrictsForState(state):
    shc_dl = scraper.ShcDL()
    await shc_dl.setup()
    districts = await shc_dl.getDistricts(state)
    return jsonify(districts)

@app.route("/states/<state>/districts/<district>/mandals")
async def getMandalsForDistrict(state,district):
    shc_dl = scraper.ShcDL()
    await shc_dl.setup()
    mandals = await shc_dl.getSubDistricts(district)
    return jsonify(mandals)

@app.route("/states/<state>/districts/<district>/mandals/<mandal>/villages")
async def getVillagesForMandal(state,district,mandal):
    shc_dl = scraper.ShcDL()
    await shc_dl.setup()
    villages = await shc_dl.getVillages(mandal)
    return jsonify(villages)

@app.route("/states/<state>/districts/<district>/mandals/<mandal>/villages/<village>/cards")
async def getCards(state,district,mandal,village):
    shc_dl = scraper.ShcDL()
    await shc_dl.setup()
    cards = await shc_dl.getCards(state, district, mandal, village)
    return jsonify(cards)

@app.route("/download", methods = [ 'POST'])
async def downloadCard():
    overwrite = request.args.get("overwrite")
    card = request.get_json()
    print("downloading card", card, overwrite)
    shc_dl = scraper.ShcDL()
    await shc_dl.setup()
    
    file_path = storage.getFilePath(card['state_id'], card['district_id'], card['mandal_id'], card['village_id'], card['sample'], card['sr_no'])
    if not storage.isFileDownloaded(file_path) or overwrite == "true":
        shc = ""
        counter = 5

        while shc == "" and counter > 0:
            try:
                shc = await shc_dl.getCard(card['sample'], card['village_grid'], card['sr_no'])
            except pyppeteer.errors.TimeoutError:
                counter = counter - 1
            except scraper.UnableToDownloadCard:
                counter = counter - 1
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
            print(f'failed downloading file {file_path}') 
    else:
        print(f'skipping file {file_path} since its already downloaded') 
        
    return "success"

@app.route("/states/<state>/districts/<district>/mandals/<mandal>/villages/<village>/load")
async def getAllSHCForVillage(state,district,mandal,village):
    file_counter = 0
    overwrite = request.args.get("overwrite")
    shc_dl = scraper.ShcDL()
    await shc_dl.setup()
    cards = await shc_dl.getCards(state, district, mandal, village)
    for card in cards:
        file_path = storage.getFilePath(state, district, mandal, village, card['sample'], card['sr_no'])
        if not storage.isFileDownloaded(file_path) or overwrite == "true":
            shc = ""
            counter = 5

            while shc == "" and counter > 0:
                try:
                    shc = await shc_dl.getCard(card['sample'], card['village_grid'], card['sr_no'])
                except pyppeteer.errors.TimeoutError:
                    counter = counter - 1
                except scraper.UnableToDownloadCard:
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
                file_counter  = file_counter +1
            else:
                print(f'failed downloading file {file_path}') 

        else:
           print(f'skipping file {file_path} since its already downloaded') 
    print(f"downloaded {file_counter} files, from {len(cards)} cards")
    return f"downloaded {file_counter} files, from {len(cards)} cards"


if __name__ == "__main__":
    app.run(debug=True, host="0.0.0.0", port=int(os.environ.get("PORT", 8080)))