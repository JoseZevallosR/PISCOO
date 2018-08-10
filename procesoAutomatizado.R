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


##################################################################################################################
##################################################################################################################
#################################Funcions de Actulizacion de Prevision ETA########################################
##################################################################################################################
##################################################################################################################
##################################################################################################################

pronostico=function(path_prevision='C:\\Users\\joseaugusto\\Documents\\PiscoModerno2018\\prevision\\'){
    url=paste0('ftp.senamhi.gob.pe/HIDROLOGIA/SMN/',gsub("-","", Sys.Date()),'/')
    filenames <- getURL(url, ftp.use.epsv = FALSE, ftplistonly=TRUE, 
                        crlf=TRUE,userpwd="senamhiddr:servicioddr")
    filePaths <- paste(url, strsplit(filenames, "\r*\n")[[1]], sep="")
    n=which(grepl('peru_eta32', filePaths)==T)[1]
    destino=paste0(path_prevision,strsplit(filenames, "\r*\n")[[1]][n])
    info=paste0('ftp://senamhiddr:servicioddr@',filePaths[n])
    download.file(info, destino, method='curl', quiet = FALSE, mode = "w",
              cacheOK = TRUE)
    destino
}

grilla=function(resX,resY){
  #crea la grilla para la interpolacion
  pixelsx=(86-66)/resX
  pixelsy=(19.25-1.25)/resY
  geog.grd <- expand.grid(x=seq(floor(-86),
                                ceiling(-66),
                                length.out=pixelsx),
                          y=seq(floor(-19.25),
                                ceiling(1.25),
                                length.out=pixelsy))
  grd.pts <- SpatialPixels(SpatialPoints((geog.grd)))
  grd <- as(grd.pts, "SpatialGrid")
  proj4string(grd)='+proj=longlat +datum=WGS84 +no_defs +ellps=WGS84 +towgs84=0,0,0  '
  grd
}

interpolacion=function(datapoints,grd){
  "funcion para interpolar las previciones pluviometricas retorna una lista de interpolaciones"
  names(datapoints)=c('y','x','h24','h48','h72')
  coordinates(datapoints)=~x+y
  proj4string(datapoints)='+proj=longlat +datum=WGS84 +no_defs +ellps=WGS84 +towgs84=0,0,0  '
  
  fecha=Sys.Date()
  
  fechas=seq(fecha, fecha+2, "day")
  
  mapa1=raster(idw(h24~1,locations = datapoints,grd))
  mapa2=raster(idw(h48~1,locations = datapoints,grd))
  mapa3=raster(idw(h72~1,locations = datapoints,grd))

  list(dia1=mapa1,dia2=mapa2,dia3=mapa3)
}


##################################################################################################################
##################################################################################################################
#################################Actualizacion de la Base Historica###############################################
##################################################################################################################
##################################################################################################################
##################################################################################################################
rem=function(x){
  b=paste0(substr(x,7,10),'-',substr(x,4,5),'-',substr(x,1,2))
  b
}
fil=function(a){
  #formato de fechas para el programa minerve
  ano=substr(a,1,4)
  mes=substr(a,6,7)
  dia=substr(a,9,10)
  r=paste(dia,mes,ano,sep='.')
  f=paste0(r,' 00:00:00')
  f
}
extraer=function(puntos,ras){
  "funcion para Extraer los valores del raster a los puntos de la base datos"
  "para 24,48 y 72 horas"
  #shape : puntos de estaciones
  #ras  : lista de rasters
  #extraer los valores de raster a los puntos
  ext=lapply(ras,extract,puntos)
  m=length(ras)
  n=length(puntos)
  datos=matrix(data=0,nrow = n,ncol = m)
  for (i in 1:m){
    datos[,i]=ext[[i]]
  }
  datos=round(datos,2)
  #el shape tiene que tener en su tabla de atributos nombre,x,y,z
  resultado=data.frame(nombre=puntos[[1]],x=puntos[[2]],y=puntos[[3]],z=puntos[[4]])
  resultado=cbind(resultado,datos)
  resultado
}
baseActual=function(path_baseHistorica,prevision,puntos,path_baseDdatos,path0){
  #"Actualiza la base de datos con los pronosticos"
  #baseHistorica:base historica de pluviometria
  #baseSimulacion : base para la simulacion minerve
  #prevision : datos provenientes del ETA en lista
  #puntos: estaciones de la base de datos
  #path_baseDdatos: donde se guardaran las bases historicas unidas con la prevision
  #path0: directorio donse se guarda el pisco corregido del dia de ayer con GPM
  baseHistorica=read.csv(path_baseHistorica)
  fecha=Sys.Date()
  fechas=fil(seq(fecha, fecha+2, "day"))
  #evalua si la ultima fecha de la base de datos historica correponde a la anterior
  eval=as.character(baseHistorica$Station)
  if (eval[length(eval)]!=fil(Sys.Date()-1)){
    #actualiza la base de datos historica con datos del satelite GPM
    yesterday=Sys.Date()-1
    lastDay=as.Date(rem(substr(eval[length(eval)],1,10))) #ultimo dia de la base de datos
    n=yesterday-lastDay
    #DOWNFgpm(inicio=lastDay,fin=yesterday,fileLocation=paste0(path_acumulado_gpm,'/'))
    #interpolacionPisco(path0,path_acumulado_gpm)
    #ruta donde estan los acumulados faltantes a la base de datos
    #setwd(paste0(path_acumulado_gpm,'/'))
    #GPM=list.files(pattern = "\\.tif",recursive = T,include.dirs = T) #lista de tiff acumulados
    #ras1=lapply(GPM[(length(GPM)-n):(length(GPM)-1)],raster) 
    setwd(path0)
    pisco1=list.files(pattern = "\\.tif",recursive = T,include.dirs = T) #lista de pisco del dia anterior
    #ras2=lapply(pisco1[length(pisco1)],raster)
    ras=lapply(pisco1[(length(pisco1)-n+1):(length(pisco1))],raster) 
    #ras=c(ras1,ras2) #lectura de los tiff faltantes
    valores=extraer(puntos,ras)
    fechas1=fil(seq(lastDay+1, yesterday, "day")) #fechas faltantes
    for (j in 5:(n+4)){
      #recorre cada fila de los gpm faltantes
      
      fila_val=data.frame(t(as.matrix(c(fechas1[j-4],valores[[j]]))))
      names(fila_val)=names(baseHistorica)
      baseHistorica=rbind(baseHistorica,fila_val)
    }
    
    write.table(baseHistorica,path_baseHistorica,row.names = F,sep = ',')
  }
  
  for (i in 5:7){
    fila=data.frame(t(as.matrix(c(fechas[i-4],prevision[[i]]))))
    names(fila)=names(baseHistorica)
    baseHistorica=rbind(baseHistorica,fila)
  }
  baseSimulacion=baseHistorica
  write.table(baseSimulacion,paste0(path_baseDdatos,'\\',Sys.Date(),'.csv'),sep = ',',row.names = F)  
}


#corrido del Programa

#Actualizacion del PISCOO
pahtPISCO='C:\\Users\\joseaugusto\\Documents\\PiscoModerno2018\\PISCOO\\PISCO_Operativo'

grd=readRDS('C:\\Users\\joseaugusto\\Documents\\PiscoModerno2018\\proyecto\\grilla.rds')

puntos=vozdata() #info del dia
PISCOO=genRe(grd,puntos)


name=paste0(pahtPISCO,Sys.Date()-1,'.tif')
writeRaster(PISCOO,name,overwrite=T)

#Calculo de la Provision ETA
path_prevision='C:\\Users\\joseaugusto\\Documents\\PiscoModerno2018\\prevision\\'
archivo=pronostico(path_prevision)
ETA=read.table(archivo)
grd=grilla(0.1,0.1)
previETA=interpolacion(ETA,grd)



baseHistorica='C:\\Users\\joseaugusto\\Documents\\PiscoModerno2018\\proyecto\\baseHistorica\\vilcanota.csv'
puntos=shapefile('C:\\Users\\joseaugusto\\Documents\\PiscoModerno2018\\EST_SHAPE\\Estaciones_BD.shp')
path0='C:\\Users\\joseaugusto\\Documents\\PiscoModerno2018\\PISCOO'
path_baseDdatos='C:\\Users\\joseaugusto\\Documents\\PiscoModerno2018\\proyecto\\basePrediccion'

prevision=extraer(puntos,previETA)
baseActual(baseHistorica,prevision,puntos,path_baseDdatos,path0)
