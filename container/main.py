import os
import asyncio

import scraper
from flask import Flask, jsonify
import google.cloud.logging
import logging

app = Flask(__name__)

client = google.cloud.logging.Client()
client.setup_logging()

@app.route("/states/<state>/districts")
async def getDistrictsForState(state):
    logging.info("getDistrictsForState state=%s",state)
    shc_dl = scraper.ShcDL()
    await shc_dl.setup()
    districts = await shc_dl.getDistrictsForState(state)
    return jsonify(districts)

@app.route("/states/<state>/districts/<district>/load")
async def getAllSHCForStateAndDistrict(state,district):
    logging.info("getAllSHCForStateAndDistrict state=%s,district=%s",state,district)
    shc_dl = scraper.ShcDL()
    await shc_dl.setup()
    await shc_dl.getAllSHCForStateAndDistrict(state,district)
    return "success"

@app.route("/states/<state>/districts/<district>/mandals")
async def getMandalsForDistrict(state,district):
    logging.info("getMandalsForDistrict state=%s,district=%s",state,district)
    shc_dl = scraper.ShcDL()
    await shc_dl.setup()
    mandals = await shc_dl.getMandalsForStateAndDistricts(state,district)
    return jsonify(mandals)

@app.route("/states/<state>/districts/<district>/mandals/<mandal>/load")
async def getAllSHCForMandal(state,district,mandal):
    logging.info("getAllSHCForMandal state=%s,district=%s,mandal=%s",state,district,mandal)
    shc_dl = scraper.ShcDL()
    await shc_dl.setup()
    await shc_dl.getAllSHCForMandal(state,district,mandal)
    return "success"

@app.route("/states/<state>/districts/<district>/mandals/<mandal>/villages")
async def getVillagesForMandal(state,district,mandal):
    logging.info("getVillagesForMandal state=%s,district=%s,mandal=%s",state,district,mandal)
    shc_dl = scraper.ShcDL()
    await shc_dl.setup()
    villages = await shc_dl.getVillagesForMandal(state,district,mandal)
    return jsonify(villages)

@app.route("/states/<state>/districts/<district>/mandals/<mandal>/villages/<village>/load")
async def getAllSHCForVillage(state,district,mandal,village):
    logging.info("getAllSHCForVillage state=%s,district=%s,mandal=%s,village=%s",state,district,mandal,village)
    shc_dl = scraper.ShcDL()
    await shc_dl.setup()
    await shc_dl.loadAllSHCForVillage(state,district,mandal,village)
    return "success"


if __name__ == "__main__":
    app.run(debug=True, host="0.0.0.0", port=int(os.environ.get("PORT", 8080)))