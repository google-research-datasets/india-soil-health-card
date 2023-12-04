library(data.table)
library(sf)
library(foreach)
library(doSNOW)
readSHCs = function(){
rdsfile = "D:/GoogleDrive/Workstation/Projects/GoogleSHC/SHCAnalysis/Data/Cards_info/Cards_info.rds"

if (!file.exists(rdsfile)){
	file1 = "D:/GoogleDrive/Workstation/Projects/GoogleSHC/SHCAnalysis/Data/Cards_info/20230309_output_cards_info_-00000-of-00002.csv"
	dat = fread(file1)

	file2 = "D:/GoogleDrive/Workstation/Projects/GoogleSHC/SHCAnalysis/Data/Cards_info/20230309_output_cards_info_-00001-of-00002.csv"
	dat1 = fread(file2)
	oc = rbind(dat,dat1)

	hfile = "D:/GoogleDrive/Workstation/Projects/GoogleSHC/SHCAnalysis/Data/Cards_info/20230309_output_cards_info_schema.json"
	hcols = fromJSON(file=hfile)
	colnames(oc) = names(hcols)
	saveRDS(oc,rdsfile)
}else{
	oc = readRDS(rdsfile)
} 

return(oc)
}

getVariables = function(oc,varname){
	cols = c("VillageId","SubDistrictId","DistrictId","StateId","latitude", "longitude","IDAllSHCs",varname)
	oc1 = as.data.frame(oc)[,cols]
	return(oc1)
}

getState = function(){
	state = read.csv("D:/GoogleDrive/Workstation/Projects/GoogleSHC/SHCAnalysis/Data/states-00000-of-00001.csv",header=F)
	colnames(state) = c("StateId","StateN")
	return(state)
}

getDistrict = function(){
	state = read.csv("D:/GoogleDrive/Workstation/Projects/GoogleSHC/SHCAnalysis/Data/states-00000-of-00001.csv",header=F)
	colnames(state) = c("StateId","StateN")
	district = read.csv("D:/GoogleDrive/Workstation/Projects/GoogleSHC/SHCAnalysis/Data/districts-00000-of-00001.csv",header=F)
	colnames(district) = c("StateId","DistrictId","DistrictN")
	district = merge(district,state,by="StateId",all.x=T)
}

retrievedistids = function(wd){
	setwd(wd)
	statesid = read.csv("D:/GoogleDrive/Workstation/Projects/GoogleSHC/Data/maps-master/states-00000-of-00001_edited.csv",head=F)
	colnames(statesid) = c("StateId","ST_NM")
	distid_old = read.csv("D:/GoogleDrive/Workstation/Projects/GoogleSHC/Data/maps-master/districts-00000-of-00001.csv",head=F)#[,2:5]
	distid_old = merge(distid_old,statesid,by.x="V1",by.y="StateId")
	colnames(distid_old) = c("StateId","DistrictId","DISTRICT","ST_NM")
	distid_new = read.csv("D:/GoogleDrive/Workstation/Projects/GoogleSHC/Data/maps-master/districts-00000-of-00001_edited.csv",head=T)[,2:5]
	colnames(distid_new) = c("StateId","DistrictId","DISTRICT_SHP","ST_NM_SHP")
	distid = merge(distid_old,distid_new,by=c("StateId","DistrictId"))
	return(distid)
}

extractvar = function(fnames,aez){
	fname = fnames
	bname = basename(fnames)
	varn = strsplit(bname,"_")[[1]][1]
	depth = strsplit(bname,"_")[[1]][2]
	varname = paste(varn,depth,sep="_")
	ras = raster(fnames)
	aez1 = st_transform(aez,crs(ras))
	aez1[varname] = NA
	aez2 = as.data.frame(aez1)
	aez2 = subset(aez2,select=-c(geom))
	for(i in 1:nrow(aez1)){
		poly_i = as(aez1[i,],"Spatial")
		rascrp = crop(ras,poly_i)
		rassub = mask(rascrp,poly_i)
		rassub[rassub==0] = NA
		aez2[i,varname] = cellStats(rassub,"mean",na.rm=T)
	}
return(aez2)
}

applyextractvarState = function(phfiles,shp){
for(j in 1:length(phfiles)){
	nam = paste("out",j,sep="")
	assign(nam, extractvar(phfiles[j],shp))
	print(j)
	flush.console()
}

out = merge(out1,out2,by=c("StateId","ST_NM"))
out = merge(out,out3,by=c("StateId","ST_NM"))
out = merge(out,out4,by=c("StateId","ST_NM"))
out = merge(out,out5,by=c("StateId","ST_NM"))
out = merge(out,out6,by=c("StateId","ST_NM"))
out = merge(out,out7,by=c("StateId","ST_NM"))
out = out[,c(0,1,3,7,5,6,8,4,2)+1]
return(out)
}


applyextractvarDistrict = function(phfiles,shp){

for(j in 1:length(phfiles)){
	nam = paste("out",j,sep="")
	assign(nam, extractvar(phfiles[j],shp))
	print(j)
	flush.console()
}

out = merge(out1,out2,by=c("DistrictId","DISTRICT"))
out = merge(out,out3,by=c("DistrictId","DISTRICT"))
out = merge(out,out4,by=c("DistrictId","DISTRICT"))
out = merge(out,out5,by=c("DistrictId","DISTRICT"))
out = merge(out,out6,by=c("DistrictId","DISTRICT"))
out = merge(out,out7,by=c("DistrictId","DISTRICT"))
out = out[,c(0,1,3,7,5,6,8,4,2)+1]
return(out)
}

applyextractvarAEZ = function(phfiles,shp){

for(j in 1:length(phfiles)){
	nam = paste("out",j,sep="")
	assign(nam, extractvar(phfiles[j],shp))
	print(j)
	flush.console()
}

out = merge(out1,out2,by=c("Agro_eco_z"))
out = merge(out,out3,by=c("Agro_eco_z"))
out = merge(out,out4,by=c("Agro_eco_z"))
out = merge(out,out5,by=c("Agro_eco_z"))
out = merge(out,out6,by=c("Agro_eco_z"))
out = merge(out,out7,by=c("Agro_eco_z"))
out = out[,c(1,3,7,5,6,8,4,2)]
return(out)
}


getdata4plot = function(state,state1.1,district,district1.1,aez,aez1.1,shcpara,sgpara){
statephshc = state[,c("StateId","ST_NM",shcpara)]
statephsg = subset(state1.1,(depth == "Depth: 0-5cm") & (SoilParameter == sgpara))
stateph = merge(statephshc,statephsg,by=c("StateId","ST_NM"))
stateph$scale = "State"
districtphshc = district[,c("DistrictId","DISTRICT",shcpara)]
districtphsg = subset(district1.1,(depth == "Depth: 0-5cm") & (SoilParameter == sgpara))
districtph = merge(districtphshc ,districtphsg ,by=c("DistrictId","DISTRICT"))
districtph$scale = "District"

aezphshc = aez[,c("Agro_eco_z",shcpara)]
aezphsg = subset(aez1.1,(depth == "Depth: 0-5cm") & (SoilParameter == sgpara))
aezph = merge(aezphshc ,aezphsg ,by=c("Agro_eco_z"))
aezph$scale = "AEZ"

stateph = stateph %>% st_drop_geometry()
stateph = stateph[c(shcpara,"value","scale")]

districtph = districtph  %>% st_drop_geometry()
districtph = districtph [c(shcpara,"value","scale")]

aezph= aezph%>% st_drop_geometry()
aezph= aezph[c(shcpara,"value","scale")]

outdata = rbind(stateph,districtph,aezph)
outdata$scale = factor(outdata$scale,levels = c("State","District","AEZ"))
return(outdata)
}