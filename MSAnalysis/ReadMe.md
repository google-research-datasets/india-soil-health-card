# Introduction
This folder (./MSAnalysis) contain the scripts used to analyze the SHC database. All relevant data files are stored in the "./MSAnalysis/Data" folder

## 1. InitialCheck.R
The Initial check script contains initial checks focused on the spatial and temporal coverage of the database. Specifically, it runs checks on the spatio-temporal coverage of the Soil Health Cards Database.

## 2. DescStats.R
This script generates descriptive statistics for soil parameters and farm size.

## 3. ReliabilityScores.R
This script assigns and analyzes composite reliability scores as a function of location, sample collection date, farm characteristics, physical properties of the soil sample, and macro and micronutrients in the soil sample. Note that the "withindistrict.rds" file used in this script was generated from a separate script (WithinDistrictCheck_S.R), that checks whether the geographical coordinates noted in the soil health card, indeed lies in the district noted in the soil health card (SHC).

## 4. GetSoilGrids_*.txt
These three set of text files contains the javascript code used to download Global SoilGrids data for pH, soil organic carbon (SOC), and Nitrogen, from Google Earth Engine.