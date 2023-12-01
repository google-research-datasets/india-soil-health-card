rm(list=ls())
gc()
library(ggplot2)
library(sf)
library(tidyverse)
library(viridis)

setwd("D:/GoogleDrive/Workstation/Projects/GoogleSHC")
source("./Script/Functions.R")

shc = readSHCs()
shc$IDAllSHCs = 1:dim(shc)[1]
shc1 = shc

## Location
## Within District

locc_d = readRDS("./Intfiles/withindistrict.rds")
shc1 = merge(shc1,locc_d,by="IDAllSHCs",all.x=T)
shc1$withindistrict = as.numeric(shc1$Districtlocrel)
shc1$withindistrict[is.na(shc1$withindistrict)] = 0
shc1$loccorrect = shc1$withindistrict

## Year

years = as.Date(shc1$sample_collection_date, format = "%Y-%m-%d")
yy = format(years,"%Y")
yy = as.numeric(yy)
yy[is.na(yy)] = 0
yy = yy %in% 2015:2022
shc1$yearsinrange = as.numeric(yy)

iqr.rem = function(data){
	
	quartiles = quantile(data, probs=c(.25, .75), na.rm = T)
	IQR = IQR(data,na.rm=T)
	Lower = quartiles[1] - 1.5*IQR
	Upper = quartiles[2] + 1.5*IQR
	out = data
	out[(out<Lower) | (out > Upper)] = NA
	return(out)	
} 

## pH

pHinrange = shc1$pH_value
pHinrange = iqr.rem(pHinrange)
pHinrange[is.na(pHinrange)]  = -1
shc1$pHinrange = as.numeric((pHinrange >=0) & (pHinrange <=14))

## OC
OCinrange = shc1$OC_value
OCinrange = iqr.rem(OCinrange)
OCinrange[is.na(OCinrange)]  = -1
shc1$OCinrange = as.numeric((OCinrange >=0) & (OCinrange <=100))

##IQRs

ECinrange = shc1$EC_value
ECinrange = iqr.rem(ECinrange)
shc1$ECinrange = as.numeric(!is.na(ECinrange))

Ninrange = shc1$N_value
Ninrange = iqr.rem(Ninrange)
shc1$Ninrange = as.numeric(!is.na(Ninrange))

Pinrange = shc1$P_value
Pinrange = iqr.rem(Pinrange )
shc1$Pinrange = as.numeric(!is.na(Pinrange))

Kinrange = shc1$K_value
Kinrange = iqr.rem(Kinrange)
shc1$Kinrange = as.numeric(!is.na(Kinrange))

Sinrange = shc1$S_value
Sinrange = iqr.rem(Sinrange)
shc1$Sinrange = as.numeric(!is.na(Sinrange))

Zninrange = shc1$Zn_value
Zninrange = iqr.rem(Zninrange )
shc1$Zninrange = as.numeric(!is.na(Zninrange))

Binrange = shc1$B_value
Binrange = iqr.rem(Binrange)
shc1$Binrange = as.numeric(!is.na(Binrange))

Feinrange = shc1$Fe_value
Feinrange = iqr.rem(Feinrange)
shc1$Feinrange = as.numeric(!is.na(Feinrange))

Mninrange = shc1$Mn_value
Mninrange = iqr.rem(Mninrange)
shc1$Mninrange = as.numeric(!is.na(Mninrange))

Cuinrange = shc1$Cu_value
Cuinrange = iqr.rem(Cuinrange)
shc1$Cuinrange = as.numeric(!is.na(Cuinrange))

## Farm Size

fsize = ifelse(shc1$farm_size_unit == "Hectares",shc1$farm_size*2.47105,shc1$farm_size)
fsize[shc1$farm_size_unit == ""] = NA
fsizeinrange = iqr.rem(fsize)
shc1$fsizeinrange = as.numeric(!is.na(fsizeinrange))

## Irrigation Method:

shc1$Irriginrange = ifelse(shc1$irrigation_method %in% c("","-"),0,1)

#Reliability Scores
########### 
shc1$Reliability1 = shc1$loccorrect + shc1$yearsinrange +
(shc1$pHinrange + shc1$OCinrange) + (shc1$ECinrange + shc1$Ninrange +
shc1$Pinrange + shc1$Kinrange + shc1$Sinrange + shc1$Zninrange +
shc1$Binrange + shc1$Feinrange + shc1$Mninrange + shc1$Cuinrange) +
(shc1$fsizeinrange + shc1$Irriginrange)

colnames(shc1)
outcol = colnames(shc1)[c(1:10,107:124)]
print(outcol)
outdf = as.data.frame(shc1[,..outcol])

#write.csv(outdf,"./Intfiles/ReliabilityScores_v1.1.csv",row.names=F)
round(sum(shc1$Reliability1 >=13,na.rm=T) * 100/ dim(shc1)[1],2)
round(sum(shc1$Reliability1 >=14,na.rm=T) * 100/ dim(shc1)[1],2)
round(sum(shc1$Reliability1 >=15,na.rm=T) * 100/ dim(shc1)[1],2)
round(sum(shc1$Reliability1 >=16,na.rm=T) * 100/ dim(shc1)[1],2)

### Individual Variable Reliability:

indaccloc = c("Location","Date","pH","OC","EC","N","P","K","S","Zn","B","Fe","Mn","Cu","Farm Size","Irrigation")

indacc = c(sum(outdf$loccorrect) * 100 / dim(outdf)[1],
sum(outdf$yearsinrange) * 100 / dim(outdf)[1],
sum(outdf$pHinrange) * 100 / dim(outdf)[1],
sum(outdf$OCinrange) * 100 / dim(outdf)[1],
sum(outdf$ECinrange) * 100 / dim(outdf)[1],
sum(outdf$Ninrange) * 100 / dim(outdf)[1],
sum(outdf$Pinrange) * 100 / dim(outdf)[1],
sum(outdf$Kinrange) * 100 / dim(outdf)[1],
sum(outdf$Sinrange) * 100 / dim(outdf)[1],
sum(outdf$Zninrange) * 100 / dim(outdf)[1],
sum(outdf$Binrange) * 100 / dim(outdf)[1],
sum(outdf$Feinrange) * 100 / dim(outdf)[1],
sum(outdf$Mninrange) * 100 / dim(outdf)[1],
sum(outdf$Cuinrange) * 100 / dim(outdf)[1],
sum(outdf$fsizeinrange) * 100 / dim(outdf)[1],
sum(outdf$Irriginrange) * 100 / dim(outdf)[1])

indaccdf = data.frame(Type=indaccloc,Accuracy=round(indacc,2))
indaccdf = indaccdf[order(indaccdf$Accuracy),]
#write.csv(indaccdf,"./Intfiles/IndividualVariableAccuracy.csv",row.names=F)

## Plotting
options(scipen=999)
dev.new(width = 6,height=6)
histplt = ggplot(shc1, aes(x=Reliability1)) + 
geom_histogram(aes(y=..density..), colour="black", fill="gray") + 
theme_bw() + 
xlab("Reliability Score") + ylab("Density") + 
theme(axis.title = element_text(size=16),
axis.text = element_text(size=14))

#ggsave("./Figures/ReliabilityScores/Histogram.jpeg",histplt)

# State-level
stR = shc1 %>% group_by(StateId) %>%
summarize(AvgRel_1 = mean(Reliability1)) %>% as.data.frame(.)
stB = getState()
stR = merge(stR,stB,by="StateId")
stR = stR[order(stR$AvgRel_1),]
stR[stR$StateN=="Jammu And Kashmir","StateN"] = "Jammu & Kashmir"
#write.csv(stR,"./Intfiles/State_AvgRel_v2.csv")

outst = stR[,c("StateN","AvgRel_1")]
colnames(outst) = c("State","Reliability Score")
outst = outst[order(-outst[,2]),]
#write.csv(outst,"./Figures/ReliabilityScores/State_AvgRelv2.csv")

#District-level
distR = shc1 %>% group_by(DistrictId) %>%
summarize(AvgRel_1 = mean(Reliability1)) %>% as.data.frame(.)
distR = distR[order(distR$AvgRel_1),]
#write.csv(stR,"./Intfiles/District_AvgRel_v2.csv")

##Plot Reliability

shpfile = "./Data/maps-master/States/Admin2.shp"
statesid = read.csv("./Data/maps-master/states-00000-of-00001_edited.csv",head=F)
colnames(statesid) = c("StateId","ST_NM")
stR[stR$StateN=="Jammu And Kashmir","StateN"] = "Jammu & Kashmir"
shp = read_sf(shpfile)
shprel = merge(shp,stR,by.x="ST_NM",by.y="StateN",all.x=T)

#jpeg("./Figures/ReliabilityScores/StateR1_v2.jpg")
plot(shprel["AvgRel_1"],main="Average Reliability Score",key.pos = 1,
axes = TRUE, key.width = lcm(1.3), key.length = 1.0,
pal = plasma(8),breaks=seq(7,15,1))
#dev.off()

# District-level
distid_new = read.csv("./Data/maps-master/districts-00000-of-00001_edited.csv",head=T)[,2:5]
colnames(distid_new) = c("StateId","DistrictId","DISTRICT_SHP","ST_NM_SHP")
shpfile = "./Data/maps-master/Districts/2011_Distedited-v2.shp"
shp = read_sf(shpfile)
shp = merge(shp,distid_new,by.x=c("DISTRICT","ST_NM"),by.y=c("DISTRICT_SHP","ST_NM_SHP"),all.x=T)
distR = merge(shp,distR,by="DistrictId",all.x=T)
distR  = distR[order(distR$AvgRel_1),]

#jpeg("./Figures/ReliabilityScores/DistrictR1.jpg")
plot(distR["AvgRel_1"],main="Average Reliability Score",key.pos = 1,
axes = TRUE, key.width = lcm(1.3), key.length = 1.0,
pal = plasma(11),breaks=seq(5,16,1))
#dev.off()

## Reliability by Soil Test Labs
labvar = shc1 %>% group_by(soil_test_lab) %>% summarize(AvgRel = mean(Reliability1,na.rm=T),SDRel = sd(Reliability1,na.rm=T)) %>% as.data.frame(.)
labvar = labvar[order(-labvar$AvgRel),]
#labvar = labvar[complete.cases(labvar),]
labvar$ID = 1:dim(labvar)[1]

labplt = ggplot(labvar, aes(x=ID, y=AvgRel)) + 
geom_pointrange(aes(ymin=AvgRel-SDRel, ymax=AvgRel+SDRel),alpha=0.2,size=0.5) + 
theme_bw() + 
ylab("Average Reliability Score") + xlab("Soil Testing Lab ID") + 
theme(axis.title = element_text(size=16),
axis.text = element_text(size=14))
#ggsave("./Figures/ReliabilityScores/SoilTestLab.jpeg",labplt)

