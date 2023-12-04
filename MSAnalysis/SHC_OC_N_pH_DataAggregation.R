rm(list=ls())
gc()
library(data.table)
library(rjson)
library(doSNOW)
library(boot)
library(reshape2)
library(scales)
library(ggpubr)
library(sf)
library(tidyverse)
wd = "D:/GoogleDrive/Workstation/Projects/GoogleSHC"
setwd(wd)

source("./Script/Functions.R")
shc = readSHCs()
shc$IDAllSHCs = 1:dim(shc)[1]
rel = fread("./Intfiles/ReliabilityScores_v1.2.csv")

var = "OC_value"
oc = getVariables(shc,var)
oc = merge(oc,rel,by="IDAllSHCs")
oc = subset(oc,(OCinrange==1) & (loccorrect==1))

var = "N_value"
oc2 = getVariables(shc,var)
oc2 = merge(oc2,rel,by="IDAllSHCs")
oc2 = subset(oc2,(Ninrange==1) & (loccorrect==1))

var = "pH_value"
oc3 = getVariables(shc,var)
oc3 = merge(oc3,rel,by="IDAllSHCs")
oc3 = subset(oc3,(pHinrange==1) & (loccorrect==1))


# District-level Aggregation

distR_1 = oc %>% group_by(DistrictId) %>%
summarize(OCmean = mean(OC_value),OCmedian = median(OC_value),
OCsd = sd(OC_value)) %>% as.data.frame(.)

distR_2 = oc2 %>% group_by(DistrictId) %>%
summarize(Nmean = mean(N_value),Nmedian = median(N_value),
Nsd = sd(N_value)) %>% as.data.frame(.)

distR_3 = oc3 %>% group_by(DistrictId) %>%
summarize(pHmean = mean(pH_value),pHmedian = median(pH_value),
pHsd = sd(pH_value)) %>% as.data.frame(.)

distR = merge(distR_1,distR_2,by="DistrictId",all=T)
distR = merge(distR,distR_3,by="DistrictId",all=T)

distid_new = read.csv("./Data/maps-master/districts-00000-of-00001_edited.csv",head=T)[,2:5]
colnames(distid_new) = c("StateId","DistrictId","DISTRICT_SHP","ST_NM_SHP")
shpfile = "./Data/maps-master/Districts/2011_Distedited-v2.shp"
shp = read_sf(shpfile)
shp = merge(shp,distid_new,by.x=c("DISTRICT","ST_NM"),by.y=c("DISTRICT_SHP","ST_NM_SHP"),all.x=T)
distR = merge(shp,distR,by="DistrictId",all.x=T)
write_sf(distR,"./Data/SHC_District_OC_N_pH.gpkg")


# State Aggregation

stR_1 = oc %>% group_by(StateId) %>%
summarize(OCmean = mean(OC_value),OCmedian = median(OC_value),
OCsd = sd(OC_value)) %>% as.data.frame(.)

stR_2 = oc2 %>% group_by(StateId) %>%
summarize(Nmean = mean(N_value),Nmedian = median(N_value),
Nsd = sd(N_value)) %>% as.data.frame(.)

stR_3 = oc3 %>% group_by(StateId) %>%
summarize(pHmean = mean(pH_value),pHmedian = median(pH_value),
pHsd = sd(pH_value)) %>% as.data.frame(.)

stR = merge(stR_1,stR_2,by="StateId",all=T)
stR = merge(stR,stR_3,by="StateId",all=T)

shpfile = "./Data/maps-master/States/Admin2.shp"
statesid = read.csv("./Data/maps-master/states-00000-of-00001_edited.csv",head=F)
colnames(statesid) = c("StateId","ST_NM")
shp = read_sf(shpfile)
shp = merge(shp,statesid,by="ST_NM")

stR1 = merge(shp,stR,by="StateId")
write_sf(stR1,"./Data/SHC_State_OC_N_pH.gpkg")


# AEZ Aggregation

## Read csv file identifying all SHCs with location data, marked to their corresponding Agro-ecological Regions
aez = fread("./Data/SHC_SHPs/df_aez_dis.csv",showProgress=T)
aez = aez[,c("IDAllSHCs","Agro_eco_z")]

oc_aez = merge(oc,aez,by="IDAllSHCs")
oc2_aez = merge(oc2,aez,by="IDAllSHCs")
oc3_aez = merge(oc3,aez,by="IDAllSHCs")

AEZ_1 = oc_aez %>% group_by(Agro_eco_z) %>%
summarize(OCmean = mean(OC_value),OCmedian = median(OC_value),
OCsd = sd(OC_value)) %>% as.data.frame(.)

AEZ_2 = oc2_aez %>% group_by(Agro_eco_z) %>%
summarize(Nmean = mean(N_value),Nmedian = median(N_value),
Nsd = sd(N_value)) %>% as.data.frame(.)

AEZ_3 = oc3_aez %>% group_by(Agro_eco_z) %>%
summarize(pHmean = mean(pH_value),pHmedian = median(pH_value),
pHsd = sd(pH_value)) %>% as.data.frame(.)

AEZ = merge(AEZ_1,AEZ_2,by="Agro_eco_z",all=T)
AEZ = merge(AEZ,AEZ_3,by="Agro_eco_z",all=T)

shp = read_sf("./Data/aez/aez_dissolved.shp")
shp = merge(shp,AEZ,by="Agro_eco_z")
shp$fid = 1:20
write_sf(shp,"./Data/aez/SHC_AEZ_OC_N_pH.gpkg")












