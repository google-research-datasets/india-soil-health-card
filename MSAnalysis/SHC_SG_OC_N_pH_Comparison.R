rm(list=ls())
library(sf)
library(scales)
library(ggplot2)
library(ggpubr)
library(dplyr)
library(gridExtra)

setwd("D:/GoogleDrive/Workstation/Projects/GoogleSHC/")
source("./Script/Functions.R")

## SHC data

state = read_sf("./Data/SHC_State_OC_N_pH.gpkg") 
district = read_sf("./Data/SHC_District_OC_N_pH.gpkg")
aez = read_sf("./Data/SHC_AEZ_OC_N_pH.gpkg")

## SG data

state1 = read.csv("./Data/SG_State.csv")[,-1]
state1.1 = reshape2::melt(state1,id.vars=c("StateId","ST_NM"))
district1 = read.csv("./Data/SG_District.csv")[,-1]
district1.1 = reshape2::melt(district1,id.vars=c("DistrictId","DISTRICT"))
aez1 = read.csv("./Data/SG_AEZ.csv")[,-1]
aez1.1 = reshape2::melt(aez1,id.vars=c("Agro_eco_z"))

## Add Soil Depths and Soil Attribute as Variables to the SG data objects
depth = unlist(lapply(strsplit(as.character((state1.1[,"variable"])),"_"),function(x)x[2]))
state1.1$depth = paste("Depth: ",gsub("\\.","-",depth),sep="")
state1.1$SoilParameter = unlist(lapply(strsplit(as.character(state1.1[,"variable"]),"_"),function(x)x[1]))

depth = unlist(lapply(strsplit(as.character((district1.1[,"variable"])),"_"),function(x)x[2]))
district1.1$depth = paste("Depth: ",gsub("\\.","-",depth),sep="")
district1.1$SoilParameter = unlist(lapply(strsplit(as.character(district1.1[,"variable"]),"_"),function(x)x[1]))

depth = unlist(lapply(strsplit(as.character((aez1.1[,"variable"])),"_"),function(x)x[2]))
aez1.1$depth = paste("Depth: ",gsub("\\.","-",depth),sep="")
aez1.1$SoilParameter = unlist(lapply(strsplit(as.character(aez1.1[,"variable"]),"_"),function(x)x[1]))

## pH

phdata = getdata4plot(state,state1.1,district,district1.1,aez,aez1.1,shcpara="pHmean",sgpara="phh2o")
shcpara="pHmean"
sgpara="phh2o"

phplot = ggplot(phdata,aes(x=(pHmean),y=(value))) + geom_point() + 
facet_wrap(~scale) +
theme_bw() +
xlab("SHC: Average Soil pH") + 
ylab("SG: Average Soil pH") +
theme(axis.title = element_text(size=13),
axis.text = element_text(size=12)) +
geom_smooth(method="lm") +
  stat_regline_equation(
    aes(label =  paste(..eq.label.., ..adj.rr.label.., sep = "~~~~")),size=4) + 
theme(strip.text.x = element_text(size = 14),title =  element_text(size = 15) ) + 
labs(title="a) Soil pH")

## OC

ocdata = getdata4plot(state,state1.1,district,district1.1,aez,aez1.1,shcpara="OCmean",sgpara="soc")
ocdata$value = ocdata$value*100/10000
occplot = ggplot(ocdata ,aes(x=(OCmean),y=(value))) + geom_point() + 
facet_wrap(~scale) +
theme_bw() +
xlab("SHC: Average Soil Organic Carbon (%)") + 
ylab("SG: Average Soil Organic Carbon (%)") +
theme(axis.title = element_text(size=13),
axis.text = element_text(size=12)) +
geom_smooth(method="lm") +
  stat_regline_equation(
    aes(label =  paste(..eq.label.., ..adj.rr.label.., sep = "~~~~")),size=4) + 
theme(strip.text.x = element_text(size = 14),title =  element_text(size = 15) ) + 
labs(title="b) Soil Organic Carbon")

## N


Ndata = getdata4plot(state,state1.1,district,district1.1,aez,aez1.1,shcpara="Nmean",sgpara="nitrogen")

Nplot = ggplot(Ndata ,aes(x=(Nmean),y=(value))) + geom_point() + 
facet_wrap(~scale) +
theme_bw() +
xlab("SHC: Average Soil Nitrogen (kg/ha)") + 
ylab("SG: Average Soil Nitrogen (g/kg)") +
theme(axis.title = element_text(size=13),
axis.text = element_text(size=12)) +
geom_smooth(method="lm") +
  stat_regline_equation(
    aes(label =  paste(..eq.label.., ..adj.rr.label.., sep = "~~~~")),size=4) + 
theme(strip.text.x = element_text(size = 14),title =  element_text(size = 15) ) + 
labs(title="c) Soil Nitrogen")

outplt = grid.arrange(phplot,occplot,Nplot,nrow=3)

ggsave("./Figures/AllScales_pH_OC_N.jpg",outplt ,
width = 9.67, height=11)


## pooled plot

phplotpool= ggplot(phdata,aes(x=(pHmean),y=(value))) + geom_point() + 
theme_bw() +
xlab("SHC: Average Soil pH") + 
ylab("SG: Average Soil pH") +
theme(axis.title = element_text(size=13),
axis.text = element_text(size=12)) +
geom_smooth(method="lm") +
  stat_regline_equation(
    aes(label =  paste(..eq.label.., ..adj.rr.label.., sep = "~~~~")),size=4) + 
theme(strip.text.x = element_text(size = 14),title =  element_text(size = 15) ) + 
labs(title="a) Soil pH")

ocplotpool = ggplot(ocdata ,aes(x=(OCmean),y=(value))) + geom_point() + 
theme_bw() +
xlab("SHC: Average Soil Organic Carbon (%)") + 
ylab("SG: Average Soil Organic Carbon (%)") +
theme(axis.title = element_text(size=13),
axis.text = element_text(size=12)) +
geom_smooth(method="lm") +
  stat_regline_equation(
    aes(label =  paste(..eq.label.., ..adj.rr.label.., sep = "~~~~")),size=4) + 
theme(strip.text.x = element_text(size = 14),title =  element_text(size = 15) ) + 
labs(title="b) Soil Organic Carbon")

Nplotpool = ggplot(Ndata ,aes(x=(Nmean),y=(value))) + geom_point() + 
theme_bw() +
xlab("SHC: Average Soil Nitrogen (kg/ha)") + 
ylab("SG: Average Soil Nitrogen (g/kg)") +
theme(axis.title = element_text(size=13),
axis.text = element_text(size=12)) +
geom_smooth(method="lm") +
  stat_regline_equation(
    aes(label =  paste(..eq.label.., ..adj.rr.label.., sep = "~~~~")),size=4) + 
theme(strip.text.x = element_text(size = 14),title =  element_text(size = 15)) + 
labs(title="c) Soil Nitrogen")

outplt = grid.arrange(phplotpool,ocplotpool ,Nplotpool ,ncol=3)
ggsave("./Figures/AllScales_pH_OC_N_pooled.jpg",outplt ,
width = 12, height=3.67)

## Boxplots
phdata$Variable = "pH"
colnames(phdata)[1:2] = c("SHC","SG")
ocdata$Variable = "OC"
colnames(ocdata)[1:2] = c("SHC","SG")
bpltdata = rbind(phdata,ocdata)

bpltdata1 = reshape2::melt(bpltdata,id.vars=c("scale","Variable"))
bpltdata1$Variable = factor(bpltdata1$Variable,levels=c("pH","OC"))
# grouped boxplot

boxout = ggplot(bpltdata1 , aes(x=variable, y=value, fill=variable)) + 
    geom_boxplot() +
    facet_wrap(~Variable, scale="free") +
theme_bw() +
xlab("") + 
ylab("Soil Parameter Estimate") +
theme(axis.title = element_text(size=13),
axis.text = element_text(size=12)) +
theme(strip.text.x = element_text(size = 14),title =  element_text(size = 15)) +
labs(fill="")

t.test(value ~ variable, data = subset(bpltdata1,Variable=="pH"),conf.level=.99)
t.test(value ~ variable, data = subset(bpltdata1,Variable=="OC"),conf.level=.99)

ggsave("./Figures/AllScales_pH_OC_pooled_boxplots.jpg",boxout ,
width = 14.25, height=7)




