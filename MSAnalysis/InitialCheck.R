# This scrips reports initial checks regarding spatial and temporal coverage of the
# Soil Health Cards Dataset.

rm(list=ls())
gc()
library(ggplot2)
library(sf)
library(tidyverse)
setwd("D:/GoogleDrive/Workstation/Projects/GoogleSHC")
source("./Script/Functions.R")

shc = readSHCs()
shc$IDAllSHCs = 1:dim(shc)[1]

# Unique Number of States/UTs, Districts, Sub-Districts, and Villages
print(paste("Total Numbers of States/UTs in the Database:",length(unique(shc$StateId))))
print(paste("Total Numbers of Districts in the Database:",length(unique(shc$DistrictId))))
print(paste("Total Numbers of Sub-Districts in the Database:",length(unique(shc$SubDistrictId))))
print(paste("Total Numbers of Villages in the Database:",length(unique(shc$VillageId))))

# Location Check

# 25.66% of the SHCs do not have lat lon values.

round(sum(is.na(shc$longitude))*100/dim(shc)[1],2)
round(sum(is.na(shc$latitude))*100/dim(shc)[1],2)

# Missing GPS Locations by states

shc1 = subset(shc,!is.na(shc$longitude))
shc2 = subset(shc,is.na(shc$longitude))

s1 = table(shc1$StateId)
s1 = data.frame(State=names(s1),SHCs=as.numeric(s1))
s2 = table(shc2$StateId)
s2 = data.frame(State=names(s2),SHCs=as.numeric(s2))
missinglocstate = merge(s1,s2,by="State",all=T)
missinglocstate$Perc = missinglocstate$SHCs.y*100/(missinglocstate$SHCs.y + missinglocstate$SHCs.x)

states = read.csv("./AnData/states-00000-of-00001.csv",head=F)
states$V1 = as.character(states$V1)
colnames(states)= c("State","ST_NM")

missinglocstate = merge(missinglocstate,states,by="State",all=T)
missinglocstate$TotalSHCs = missinglocstate$SHCs.y + missinglocstate$SHCs.x
missinglocstate$MissingSHCs = missinglocstate$SHCs.y
missinglocstate = missinglocstate[,c("ST_NM","TotalSHCs","MissingSHCs","Perc")]
missinglocstate = missinglocstate[order(-missinglocstate$MissingSHCs),]
missinglocstate[order(-missinglocstate$Perc),]

missinglocstate$PercAllSHCs = round(missinglocstate$MissingSHCs *100 / sum(missinglocstate$MissingSHCs,na.rm=T),3)

# 73% of these missing SHCs are located in just three states, i.e., Uttar Pradesh (1,785,549),
# Madhya Pradesh (1,628,549), and Karnataka (460,330)

round(sum(missinglocstate[1:3,3])*100/sum(missinglocstate$MissingSHCs,na.rm=T),2)

#Andaman & Nicobar Islands (71%), Goa (61%), and West Bengal (52%) have more than half of the SHCs
# missing precise location information
percmissing = missinglocstate[order(-missinglocstate$Perc),c(1,4)]
round(percmissing[,2],2)


## Check for Location Information by State, District, Subdistrict, and Village IDs
100* sum(complete.cases(shc[,c("VillageId","SubDistrictId","DistrictId","StateId")]))/
dim(shc)[1]

## Points in India?

shclatlon = as.data.frame(shc[,c("latitude","longitude","IDAllSHCs")])
shclatlon  = shclatlon[complete.cases(shclatlon),]
shclatlon = st_as_sf(shclatlon,coords=c("longitude","latitude"),crs=4326)

# National Boundary
shpfile = "./AnData/StateBound/STATE_11.shp"
shp = read_sf(shpfile)
shp$DID = 1
shp = shp[,"DID"]
shp = shp %>% group_by(DID) %>% dplyr::summarize()

# Check if GPS points are within India

shc_shp = st_intersects(shclatlon,shp)
withinindia_vec = lengths(shc_shp)>0
shclatlon$withinindia = withinindia_vec 
#Of the SHCs containing latitude and longitude information, we noted that 0.11% SHCs have incorrect GPS coordinates
round((1 - 15318900/(sum(missinglocstate$TotalSHCs,na.rm=T) - sum(missinglocstate$MissingSHCs,na.rm=T)))*100,2)

summary(withinindia_vec)
withinindia = unlist(shc_shp) == 1
100 - (sum(withinindia)*100/dim(shc_shp)[1])
sum(withinindia)/dim(shc)[1]
shclatlon = subset(as.data.frame(shclatlon),select=-geometry)

## Temporal Coverage: State-level

years = as.Date(shc$sample_collection_date, format = "%Y-%m-%d")
yy = format(years,"%Y")
shc$Year = yy
shc$validYear = as.numeric(shc$Year)
shc$validYear = (shc$validYear <=2022) & (shc$validYear >=2015)
tempcov = shc %>% group_by(StateId) %>% summarise(CorrectYear = sum(validYear == TRUE, na.rm = TRUE),
    InvalidYear = sum(validYear == FALSE, na.rm = TRUE),
    MissingSCD = sum(is.na(validYear))) %>%  ungroup() %>% as.data.frame()

missingtempstate = merge(tempcov,states,by.x="StateId",by.y="State",all=T)
missingtempstate = missingtempstate[,c("ST_NM","CorrectYear","InvalidYear","MissingSCD")]

## Temporal Coverage: National-level

tabyy = table(yy)
yy_years = names(tabyy)
yy_count = as.numeric(tabyy)
yy_df = data.frame(Year = as.numeric(yy_years),Count = yy_count)

yy_dfrange = subset(yy_df,(Year <=2022) & (Year >=2015))
yy_dforange = subset(yy_df,!((Year <=2022) & (Year >=2015)))

tot = dim(shc)[1]
valid = sum(yy_dfrange$Count)
sum(yy_dforange$Count) + 
sum(yy_dfrange$Count)

yy_dfrange$YearS = as.character(yy_dfrange$Year)
yy_dfrange = rbind(yy_dfrange,data.frame(Year= 0,Count = tot  - sum(yy_dfrange$Count),YearS="Invalid"))

p1 = ggplot(data=yy_dfrange,aes(x=YearS,y=Count)) + geom_bar(stat="identity") +
theme_bw() + xlab("Year") + ylab("No. of Soil Health Cards (SHCs)") + 
theme(axis.title=element_text(size=20),axis.text=element_text(size=18)) + 
scale_y_continuous(labels = scales::comma) + 
annotate(geom="text", x=6.9, y=5000000, label="Total SHCs: 20,629,849",
              color="black",size=5) +
annotate(geom="text", x=7.3, y=4600000, label="SHCs with Invalid Years: 1.51%",
              color="black",size=5)
p1