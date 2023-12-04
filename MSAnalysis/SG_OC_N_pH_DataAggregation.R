rm(list=ls())
library(sf)
library(raster)

wd = "D:/GoogleDrive/Workstation/Projects/GoogleSHC"
setwd(wd)
source("./Script/Functions.R")


### STATE-WISE AGGREGATION
shp = read_sf("./Data/SHC_State_OC_N_pH.gpkg")
shp = subset(shp,select=c(StateId,ST_NM))


## Aggregate pH
phfiles = list.files("D:/GoogleDrive/Workstation/Projects/GoogleSHC/Data/SoilGrids",pattern="phh2o",full.names=T)
out1 = applyextractvarState(phfiles,shp)
out1[,3:9] = apply(out1[,3:9],2,function(x)x/10)


## Aggregate Nitrogen
Nfiles = list.files("D:/GoogleDrive/Workstation/Projects/GoogleSHC/Data/SoilGrids",pattern="nitrogen",full.names=T)
out2 = applyextractvarState(Nfiles,shp)
out2[,3:9] = apply(out2[,3:9],2,function(x)x/100)

## Aggregate soc

SOCfiles = list.files("D:/GoogleDrive/Workstation/Projects/GoogleSHC/Data/SoilGrids",pattern="soc",full.names=T)
out3 = applyextractvarState(SOCfiles,shp)

fullout = merge(out1,out2,by=c("StateId","ST_NM"))
fullout = merge(fullout,out3,by=c("StateId","ST_NM"))
write.csv(fullout,"./Data/SG_State.csv")

### District-WISE AGGREGATION

shp = read_sf("./Data/SHC_District_OC_N_pH.gpkg")
shp = subset(shp,select=c(DistrictId,DISTRICT))
shp = subset(shp,!is.na(DistrictId))

## Aggregate pH
phfiles = list.files("D:/GoogleDrive/Workstation/Projects/GoogleSHC/Data/SoilGrids",pattern="phh2o",full.names=T)
out1 = applyextractvarDistrict(phfiles,shp)
out1[,3:9] = apply(out1[,3:9],2,function(x)x/10)


## Aggregate Nitrogen
Nfiles = list.files("D:/GoogleDrive/Workstation/Projects/GoogleSHC/Data/SoilGrids",pattern="nitrogen",full.names=T)
out2 = applyextractvarDistrict(Nfiles,shp)
out2[,3:9] = apply(out2[,3:9],2,function(x)x/100)

## Aggregate soc

SOCfiles = list.files("D:/GoogleDrive/Workstation/Projects/GoogleSHC/Data/SoilGrids",pattern="soc",full.names=T)
out3 = applyextractvarDistrict(SOCfiles,shp)

fullout = merge(out1,out2,by=c("DistrictId","DISTRICT"))
fullout = merge(fullout,out3,by=c("DistrictId","DISTRICT"))
write.csv(fullout,"./Data/SG_District.csv")

shp = read_sf("./Data/aez/aez_dissolved.gpkg")

## Aggregate pH
phfiles = list.files("D:/GoogleDrive/Workstation/Projects/GoogleSHC/Data/SoilGrids",pattern="phh2o",full.names=T)
out1 = applyextractvarAEZ(phfiles,shp)
out1[,2:8] = apply(out1[,2:8],2,function(x)x/10)

## Aggregate Nitrogen
Nfiles = list.files("D:/GoogleDrive/Workstation/Projects/GoogleSHC/Data/SoilGrids",pattern="nitrogen",full.names=T)
out2 = applyextractvarAEZ(Nfiles,shp)
out2[,2:8] = apply(out2[,2:8],2,function(x)x/100)

## Aggregate soc

SOCfiles = list.files("D:/GoogleDrive/Workstation/Projects/GoogleSHC/Data/SoilGrids",pattern="soc",full.names=T)
out3 = applyextractvarAEZ(SOCfiles,shp)

fullout = merge(out1,out2,by=c("Agro_eco_z"))
fullout = merge(fullout,out3,by=c("Agro_eco_z"))
write.csv(fullout,"./Data/SG_AEZ.csv")


