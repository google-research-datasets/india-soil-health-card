import json
import os
import asyncio
import re

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

app = Flask(__name__)

pool = database.init_connection_engine()


@app.route("/extract")
async def extract():
    try:
        file_path = request.args.get("file_path")
        result = re.match('shcs\/.*\/.*\/.*\/(.*)_(.*)\).html', file_path)
        if result:
            sample = result.groups()[0] 
            sr_no = int(result.groups()[1])
            metadata = storage.getMetadata(file_path)
            content = storage.getContent(file_path)
            extractor = shc_html_extractor.ShcHtmlExtractor(content)
            
            analytics_storage.insertCard(metadata['state'], metadata['district_code'], metadata['mandal_code'], metadata['village_code'], sample, sr_no, extractor.extract())
    except Exception as e:
        logging.error(traceback.format_exc())

    return Response("",204)

@app.route("/options")
async def getAllOptions():
    ingested = request.args.get("ingested")
    limit = request.args.get("limit")
    utils.logText(f'getAllOptions ingested={request.args.get("ingested")}')
    options = await database.getAllOptions(pool, ingested, limit)
    return jsonify(options)

@app.route("/states/<state>/options/ingest")
async def ingestAllOptionsForState(state):
    utils.logText(f"ingestAllOptionsForState state={state}")
    shc_dl = old_scraper.ShcDL()
    await shc_dl.setup()

    options = await shc_dl.getAllSearchOptionsForState(state)
    
    for index, row in options.iterrows():
        mandal = row['mandal']
        district = row['district']
        village = row['village']
        database.insertOptions(pool, state, district, mandal, village)
    await shc_dl.close()


@app.route("/states/<state>/options")
async def getAllOptionsForState(state):
    ingested = request.args.get("ingested")
    limit = request.args.get("limit")
    utils.logText(f'getAllOptionsForState state={state}, ingested={request.args.get("ingested")}')
    options = database.getAllOptionsForState(pool, state, ingested, limit)
    return jsonify(options)

@app.route("/states")
async def getStates():
    shc_dl = scraper.ShcDL()
    await shc_dl.setup()
    states = await shc_dl.getStates()
    await shc_dl.close()
    return jsonify(states)

@app.route("/states/<state>/districts")
async def getDistrictsForState(state):
    shc_dl = scraper.ShcDL()
    await shc_dl.setup()
    districts = await shc_dl.getDistricts(state)
    await shc_dl.close()
    return jsonify(districts)

@app.route("/states/<state>/districts/<district>/mandals")
async def getMandalsForDistrict(state,district):
    shc_dl = scraper.ShcDL()
    await shc_dl.setup()
    mandals = await shc_dl.getSubDistricts(state, district)
    await shc_dl.close()
    return jsonify(mandals)

@app.route("/states/<state>/districts/<district>/mandals/<mandal>/villages")
async def getVillagesForMandal(state,district,mandal):
    shc_dl = scraper.ShcDL()
    await shc_dl.setup()
    villages = await shc_dl.getVillages(state, district, mandal)
    await shc_dl.close()
    return jsonify(villages)

@app.route("/states/<state>/districts/<district>/mandals/<mandal>/villages/<village>/cards")
async def getCards(state,district,mandal,village):
    filter_existing = request.args.get("filter_existing")
    publishable = request.args.get("publishable")
    shc_dl = scraper.ShcDL()
    await shc_dl.setup()
    cards = await shc_dl.getCards(state, district, mandal, village)
    utils.logText("Found "+str(len(cards))+" unfiltered")
    if filter_existing == "true":
        filtered = filter(lambda card: not doesCardAlreadyExist(state, district, mandal, village, card['sample'], card['sr_no']), cards)
        cards = list(filtered)
        utils.logText("Found "+str(len(cards))+" filtered")

    await shc_dl.close()
    if not publishable == "true":
        return jsonify(cards)
    else:
        return jsonify( [{ 'data': base64.b64encode(str.encode(json.dumps(card))).decode("utf-8")  } for card in cards ] )

@app.route("/states/<state>/districts/<district>/mandals/<mandal>/villages/<village>/cards/<sample>/<sr_no>/extract")
async def extractCard(state,district,mandal,village,sample, sr_no):
    file_path = storage.getFilePath(state, district, mandal, village, sample, sr_no)
    if not storage.isFileDownloaded(file_path):
        return Response("Card not downloaded yet", status= 404)
    else:
        content = storage.getContent(file_path)
        extractor = shc_html_extractor.ShcHtmlExtractor(content)
        extracted = extractor.extract()
        analytics_storage.insertCard(state,district,mandal, village, sample, sr_no, extracted)
        return jsonify(extracted)

def doesCardAlreadyExist(state, district, mandal, village, sample, sr_no):
    file_path = storage.getFilePath(state, district, mandal, village, sample, sr_no)
    return storage.isFileDownloaded(file_path)

@app.route("/push", methods = [ 'POST'])
async def push():
    utils.logText("Received push")
    envelope = request.get_json()
    if not envelope:
        msg = "no Pub/Sub message received"
        utils.logText(f"error: {msg}")
        return Response(f"Bad Request: {msg}", status=400)

    if not isinstance(envelope, dict) or "message" not in envelope:
        msg = "invalid Pub/Sub message format"
        utils.logText(f"error: {msg}")
        return Response(f"Bad Request: {msg}", status=400)

    pubsub_message = envelope["message"]
    utils.logText(f"Received push message {pubsub_message}")
    if isinstance(pubsub_message, dict) and "data" in pubsub_message:
        pubsub_message = base64.b64decode(pubsub_message["data"]).decode("utf-8").strip()
        card = json.loads(pubsub_message)
        utils.logText(f"Scraping card {card}")
        try:
            await scraper.fetchCard(card, "false")
        except scraper.ReportServerUpdating:
            return Response("Report Server is being update", status=503)

    else:
        utils.logText(f"Will not scrape {pubsub_message}")

    return Response("", status=204)

@app.route("/download", methods = [ 'POST'])
async def downloadCard():
    overwrite = request.args.get("overwrite")
    card = request.get_json()
    card['state_id'] = str(card['state_id'])
    card['district_id'] = str(card['district_id'])
    card['village_id'] = str(card['village_id'])
    card['mandal_id'] = str(card['mandal_id'])
    card['sr_no'] = str(card['sr_no'])
    try:
        content = await scraper.fetchCard(card, overwrite)
        try:
            extractor = shc_html_extractor.ShcHtmlExtractor(content)
            extracted = extractor.extract()
            analytics_storage.insertCard(card['state_id'].strip(),card['district_id'].strip(),card['mandal_id'].strip(), card['village_id'].strip(), card['sample'], card['sr_no'], extracted)
        except Exception as e:
            logging.error(traceback.format_exc())
        return Response("success", status=204)
    except scraper.UnableToDownloadCard:
        return Response("Couldn't download card", status=503)
    except scraper.ReportServerUpdating:
        return Response("Report Server is being update", status=503)
    except pyppeteer.errors.TimeoutError as err:
        print(f"Timeout error {err}")
        return Response("Timed out loading SHC", status=503)

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
                utils.logText(f'failed downloading file {file_path}') 

        else:
           utils.logText(f'skipping file {file_path} since its already downloaded') 
    utils.logText(f"downloaded {file_counter} files, from {len(cards)} cards")
    await shc_dl.close()
    return f"downloaded {file_counter} files, from {len(cards)} cards"


if __name__ == "__main__":
    app.run(debug=True, host="0.0.0.0", port=int(os.environ.get("PORT", 8080)))