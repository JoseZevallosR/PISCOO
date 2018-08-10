library(gstat)
library(rgdal)
library(raster)
require(gdalUtils)
require(RCurl)
library(XML)
library(devtools)
library(httr)
library(sp)
library(foreach)
library(doParallel)
library(parallel)




##################################################################################################################
##################################################################################################################
#################################Funcions de Actulizacion de Pisco################################################
##################################################################################################################
##################################################################################################################
##################################################################################################################
#informacion diaria
vozdata=function(){
  ur='http://www.senamhi.gob.pe/site/lvera/reporte_diario.php'  
   tabs = GET(ur)
   tabla=readHTMLTable(rawToChar(tabs$content), stringsAsFactors = F)[[2]]
  
  data=tabla[3:251,4:6]#esto para variando porque cambian el formato
  row.names(data)=NULL
  names(data)=c('y','x','obs')
  data$x=as.numeric(as.vector(data$x))
  data$y=as.numeric(as.vector(data$y))
  data$obs=as.numeric(as.vector(data$obs))
  data=na.omit(data)
  data=data[data$obs>=0,]
  #data$obs=log1p(data$obs)
  data[c('x','y','obs')]
}



genRe=function(grd,puntos,dia=(Sys.Date()-1),path_climatologia='C:\\Users\\joseaugusto\\Documents\\PiscoModerno2018\\proyecto\\'){
    #grd es la grilla del pedu
    #dia anterior a la fecha actual
    #path_climatologia es la ruta a las climatologias del pisco
  	extraccion=function(puntos,ras){
	  #shape : puntos de estaciones
	  #ras  : lista de rasters
	  #extraer los valores de raster a los puntos
	  pp=extract(ras,puntos)

	  resultado=data.frame(puntos@coords,pp)
	  resultado
	}
    #ras=raster('dCPISCOpd_median.nc',band=as.integer(strftime((Sys.Date()-1), format = "%j")))
    ras=raster(paste0(path_climatologia,months(dia),'.tif'))
    coordinates(puntos)=~x+y
    proj4string(puntos)='+proj=longlat +datum=WGS84 +no_defs +ellps=WGS84 +towgs84=0,0,0'
    clima=extraccion(puntos,ras) 
    clima$pp=puntos$obs/clima$pp
    clima=na.omit(clima)
    clima=subset(clima,clima$pp>=0)
    clima$pp[clima$pp>=3]=3
    coordinates(clima) = ~x+y

    proj4string(clima)<- '+proj=longlat +datum=WGS84 +no_defs +ellps=WGS84 +towgs84=0,0,0'
    
    ras*raster(idw(formula =  pp ~ 1, locations = clima, 
           newdata = grd))

}

grd=readRDS('grilla.rds')
puntos=vozdata() #info del dia
mapita=genRe(grd,puntos)