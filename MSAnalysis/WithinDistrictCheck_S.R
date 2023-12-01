rm(list=ls())
gc()

library(sf)
sf_use_s2(FALSE)

setwd("D:/GoogleDrive/Workstation/Projects/GoogleSHC")
source("./Script/Functions.R")

## Load SHC

shc = readSHCs()
shc$IDAllSHCs = 1:dim(shc)[1]

## Check if Points in Districts?

## Retrieve Data that matches District IDs in the SHCs with the IDs in the shapefile.
distid = retrievedistids("D:/GoogleDrive/Workstation/Projects/GoogleSHC")

## All Unique District IDs in the SHC database
uniquest = unique(shc$DistrictId)

## Spatialize the SHC database; convert data.frame to an sf object.
shclatlon = as.data.frame(shc[,c(13,14,5,6,105)])
shclatlon  = shclatlon[complete.cases(shclatlon),]
shclatlon = st_as_sf(shclatlon,coords=c("longitude","latitude"),crs=4326)
shclatlon$ID = 1:dim(shclatlon)[1]

## Read the District shapefile.
shp = read_sf("./Data/maps-master/Districts/2011_Distedited-v2.shp")

## District Location Check Output Dataframe
outdf = data.frame(IDAllSHCs=rep(NA,dim(shc)[1]),Districtlocrel =rep(NA,dim(shc)[1]))

for(i in 1:length(uniquest)){
	shcsub = subset(shclatlon,DistrictId == uniquest[i])
	poly = subset(distid,DistrictId==uniquest[i])
	poly_st = poly$ST_NM_SHP
	poly_dist = poly$DISTRICT_SHP
	poly = subset(shp,(ST_NM == poly_st) & (DISTRICT ==poly_dist))
	shc_shp = st_intersects(shcsub,poly)
	outdf[shcsub$ID,"IDAllSHCs"] = shcsub$IDAllSHCs
	outdf[shcsub$ID,"Districtlocrel"] = as.numeric(lengths(shc_shp)>0)
	print(round(i*100/length(uniquest),2))
	flush.console()
}

## Export District Location Check Output Dataframe
saveRDS(outdf,"./Intfiles/withindistrict1.rds")