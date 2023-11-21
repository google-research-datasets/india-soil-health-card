# This script generates descriptive statistics for soil parameters and farm size (quantitative variables)
# without and with IQR-based removal, and checking for overall deficiencies in soil.
  
rm(list=ls())
gc()
library(ggplot2)
library(sf)
library(tidyverse)
library(stargazer)
setwd("D:/GoogleDrive/Workstation/Projects/GoogleSHC")

source("./Script/Functions.R")

shc = readSHCs()
shc$IDAllSHCs = 1:dim(shc)[1]

# Convert Units for Farm Size variable

fsize = ifelse(shc$farm_size_unit == "Hectares",shc$farm_size*2.47105,shc$farm_size)
fsize[shc$farm_size_unit == ""] = NA
shc$FarmSize = fsize

## Descriptive Statistics for all Soil Parameters and Farm Size

shcdf = shc[,c("pH_value","OC_value","EC_value","N_value","P_value","K_value","S_value",
"Zn_value","B_value","Fe_value","Mn_value","Cu_value","FarmSize")]
colnames(shcdf) = c("pH","OC","EC","N","P","K","S","Zn","B","Fe","Mn","Cu","Farm Size")

stargazer(shcdf, type = "text", title="Descriptive statistics", digits=1)

#stargazer(shcdf, type = "html", title="Descriptive statistics", digits=1, 
#out = "./Figures/DStats_nooutlier.doc")

iqr.rem = function(data){
	quartiles = quantile(data, probs=c(.25, .75), na.rm = T)
	IQR = IQR(data,na.rm=T)
	Lower = quartiles[1] - 1.5*IQR
	Upper = quartiles[2] + 1.5*IQR
	out = data
	out[(out < Lower) | (out > Upper)] = NA
	return(out)
}

shcdf_orm = apply(shcdf,2,iqr.rem)
shcdf_orm = as.data.frame(shcdf_orm)

stargazer(shcdf_orm, type = "text", title="Descriptive statistics", digits=1)

#stargazer(shcdf_orm, type = "html", title="Descriptive statistics: Outliers Removed", digits=1, 
#out = "./Figures/DStats_outlier.doc")

## Deficiency?

# Organic Carbon
octab = table(shcdf_orm$OC < 0.75)
octab[2]*100/sum(octab)

# Nitrogen
octab = table(shcdf_orm$N < 560)
octab[2]*100/sum(octab)

# Phosphorus
octab = table(shcdf_orm$P < 25)
octab[2]*100/sum(octab)

# Potassium
octab = table(shcdf_orm$K < 280)
octab[2]*100/sum(octab)

# Boron
octab = table(shcdf_orm$B < 0.5)
octab[2]*100/sum(octab)

# Copper
octab = table(shcdf_orm$Cu < 2)
octab[2]*100/sum(octab)

# Iron
octab = table(shcdf_orm$Fe < 4.5)
round(octab[2]*100/sum(octab),2)

# Manganese
octab = table(shcdf_orm$Mn < 2)
round(octab[2]*100/sum(octab),2)

# Suphur
octab = table(shcdf_orm$S < 10)
round(octab[2]*100/sum(octab),2)

# Zinc
octab = table(shcdf_orm$Zn < 0.6)
round(octab[2]*100/sum(octab),2)





