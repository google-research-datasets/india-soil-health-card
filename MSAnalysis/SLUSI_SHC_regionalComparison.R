rm(list=ls())
gc()

library(sf)
library(doSNOW)
library(ggplot2)
set.seed(439)
setwd("D:/GoogleDrive/Workstation/Projects/GoogleSHC")

shclatlon = read_sf("./Data/BottomupSLUSI/shcforslusifull.shp")
fullshp = read_sf("./Data/BottomupSLUSI/slusifull.shp")
geom = as.data.frame(st_coordinates(fullshp))
shclatlon1  = shclatlon %>% st_transform(.,"EPSG:3857")

## k.means Clustering

getcor = function(numreg,geom,fullshp,shclatlon){
mod = kmeans(geom, numreg, nstart=50,iter.max = 100)
fullshp$region = mod$cluster

regi = unique(fullshp$region)
mediancols = c("pH","EC","OC","N","P","K","S","Zn","Fe","Cu","Mn","B")

slusiavg= data.frame()
shcavg = data.frame()

for(i in 1:length(regi)){

	regi_i = regi[i]
	fullshpsub = subset(fullshp,region==regi_i) %>% 
	st_transform(.,"EPSG:3857") %>% st_union(.)%>% st_convex_hull(.) %>% st_buffer(.,dist=500)
	
	slusiids = st_intersects(fullshpsub,fullshp %>% st_transform(.,"EPSG:3857"))
	slusidata = fullshp[unlist(slusiids),]
	slusidata1 = as.data.frame(st_drop_geometry(slusidata))
	slusidata1 = slusidata1[,mediancols]
	slusidata1 = apply(slusidata1,2,as.numeric)
	outslusi = apply(slusidata1,2,median,na.rm=T)
	outslusi = c(regi_i,outslusi)
	slusiavg = rbind(slusiavg,outslusi)

	shcids = st_intersects(fullshpsub,shclatlon1)
	shcdata = shclatlon1[unlist(shcids),]
	shcdata1 = as.data.frame(st_drop_geometry(shcdata))
	shcdata1 = shcdata1[,mediancols]
	
	outshc = apply(shcdata1,2,median,na.rm=T)
	outshc = c(regi_i,outshc)
	shcavg = rbind(shcavg ,outshc)
}
colnames(slusiavg) = c("Region", mediancols)
colnames(shcavg) = c("Region", mediancols)


pltdat = merge(slusiavg,shcavg,by="Region")

colnames(pltdat)[2:25] = c(paste(mediancols,".x",sep=""),paste(mediancols,".y",sep=""))
slusishc1 = pltdat

corout1 = rep(NA,12)
for(cori in 1:12){
cormod = cor.test(pltdat[,cori+1],pltdat[,cori+12+1])
corout = cormod$estimate
if(cormod$p.value >0.01){corout=0} 
corout1[cori] = corout
}

return(corout1)
}

regcor = getcor(10,geom,fullshp,shclatlon)

for(numreg in 11:40){
regcor1 = getcor(numreg,geom,fullshp,shclatlon)
regcor = rbind(regcor,regcor1)
}


colnames(regcor) = mediancols 
rownames(regcor) = as.character(paste("# of Regions:",10:40))


library(ggplot2)
library(tidyverse)
dat2  = regcor %>% as.data.frame(.)%>%  mutate(Regions=rownames(.)) %>%
  as_tibble() %>%
  pivot_longer(-Regions, names_to = "Var2", values_to = "value") %>%
  mutate(Var2 = factor(Var2,levels=mediancols))

ggplot(dat2, aes(Var2, Regions)) +
  geom_tile(aes(fill = value)) +
  geom_text(aes(label = round(value, 2))) +
 viridis::scale_fill_viridis()+
ylab("") + xlab("") + 
theme_bw() + 
theme(axis.text.x=element_text(size=15),
axis.text.y=element_text(size=13),legend.position="bottom") + labs(fill=expression("Correlation\nCoefficient "*(rho))) +
theme(legend.title=element_text(size=16),legend.key.width = unit(1.5, 'cm'),legend.key.height= unit(0.5, 'cm'))