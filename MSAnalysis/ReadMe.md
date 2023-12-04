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

## 5. *_OC_N_pH_DataAggregation.R
These two scripts aggregates data from the SHC database and soilgrid database rasters at the State, District, and Agroecological Zones (AEZ) scales, for soil pH, Organic Carbon(OC), and soil Nitrogen (N).

## 6. SHC_SG_OC_N_pH_Comparison.R
This script compares soil pH, Organic Carbon(OC), and soil Nitrogen (N) estimates from the SHC and soilgrid databases at State, District, and Agroecological Zones (AEZ) scales.

## 7. SLUSI_SHC_regionalComparison.R
This script compares multiple soil parameter estiamtes the SHC and SLUSI (bottom-up samples) databases. 