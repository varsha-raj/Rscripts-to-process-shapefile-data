# Required libraries

library(rgdal)
library(dplyr)
library(rgeos)
library(RPostgreSQL)
library(RODBC)

options(stringsAsFactors=FALSE)

# Indicate the database name
database <- "database name"

#Connect to MARS database using RODBC channel
RODBC_con <- odbcConnect(dsn = database)

#Connect using driver name and specify password.
#This is required to append new data to database tables
con <- dbConnect(PostgreSQL(), dbname = database,

		host = "host name", port = 5432,

		user = "postgres",

		password = readline(prompt = 'Enter password: ')) 
#################################################

#Part I

# To update smp_loc table

#Query 'smp location' table from database
smpLoc <- sqlFetch(RODBC_con, "smp_loc", as.is = TRUE)

#alternative and better way of reading data from designated file path

gis_path_SMP <- "GIS Files/smp_data_folder"

smp_shapefile <- readOGR(dsn = paste(gis_path_SMP), layer = "smp_data") #read shapefile

smp_data <- spTransform(smp_shapefile , CRS('+init=epsg:4326')) # spatial trasnformation to lat and lon coordinate system

smp.coords <- as.data.frame(smp_data@coords) %>% # set data type to a data frame object

mutate(smp_id = as.character(smp_data$SMP_ID), # smp_id to character class type
	   lon_wgs84 = round(coords.x1,4),         # limit cooridnates to 4 digit place
	   lat_wgs84 = round(coords.x2,4)) %>% 

na.omit() %>% # delete all missing data

select(smp_id, lon_wgs84, lat_wgs84) %>% # select columns

distinct(smp_id, .keep_all = TRUE) # remove duplicate smp_ids

smp.coords$smp_id <- gsub("\\s","", smp.coords$smp_id) # remove empty white spaces from smp id column 
row.names(smp.coords) <- NULL # row names reset to NULL, to keep row names same as row numbers

if(nrow(smpLoc) == 0){ # if smp loc table is empty
    
    smpLocUpdated <- smp.coords #assign smp coords table to smplocupdated data frame object
    

} else if(nrow(smp.coords) > nrow(smpLoc)) { # if smp coords has more rows (means new smp data)

	smpLocUpdated <- anti_join(smp.coords, smpLoc, by = c("smp_id")) #get only the new data and assign to smplocupdated dataframe
	 

} else {
	
	 print("There is no new data.")
}


if (exists('smpLocUpdated')) { #if data found in smplocupdated table, connect to mars database

	dbWriteTable(con, "smp_loc", smpLocUpdated, append= TRUE, row.names = FALSE) # append the data

} else {print("No update required.")}


#######################################################
# Part II

# To update smp gage table

smp_data <- spTransform(smp_shapefile , CRS('+init=epsg:2272')) # spatial transformation to utm coordinate system

smp.coords <- as.data.frame(smp_data@coords) %>% # set data type to a data frame object

mutate(smp_id = as.character(smp_data$SMP_ID),
	   northing_ft = round(coords.x1,2),
	   easting_ft = round(coords.x2,2)) %>%  # smp_id to character class type

na.omit() %>%    # delete all missing data

distinct(smp_id, .keep_all = TRUE) %>% # remove duplicate smp_ids

select(smp_id, northing_ft, easting_ft)

smp.coords$smp_id <- gsub("\\s","", smp.coords$smp_id) # remove empty white spaces from smp id column 
row.names(smp.coords) <- NULL # row names reset to NULL, to keep row names same as row numbers

smp.coords_temp <- dplyr::select(smp.coords, -smp_id) # get only the UTM coordinates

smp_sptlpts <- SpatialPoints(smp.coords_temp) #create numeric matrix of class spatial points class

# set file path to rain gage shapefiles
gis_path_RG <- "GIS Files/rain_gage_folder"

rg_shapefile <- readOGR(dsn = paste(gis_path_RG), layer = "Storm_Water_Rain_Gauges") # Read shapefile

rg_data <- spTransform(rg_shapefile  , CRS('+init=epsg:2272')) # spatial transformation to utm coordinate system

rg_data$FACILITYNA <- gsub('RG_', '', rg_data$FACILITYNA) # remove names with pattern 'RG_' to keep only numeric value of rain gage

rg.coords <- as.data.frame(rg_data@coords) %>%   # set data type to a data frame object

mutate(rg_id = rg_data$FACILITYNA,
	   northing_ft = round(coords.x1,2),
	   easting_ft = round(coords.x2,2)) %>% 

na.omit() %>%      # delete all missing data

distinct(rg_id, .keep_all = TRUE) %>%# remove duplicate rg_ids, if any

select(rg_id, northing_ft, easting_ft)

rg.coords$rg_id <- gsub("\\s","", rg.coords$rg_id) # remove empty white spaces from smp id column 
row.names(rg.coords) <- NULL # row names reset to NULL, to keep row names same as row numbers

rg.coords_temp <- dplyr::select(rg.coords, -rg_id) # get only the UTM coordinates

rg_sptlpts <- SpatialPoints(rg.coords_temp) #create numeric matrix of class spatial points class

y <- apply(gDistance(rg_sptlpts, smp_sptlpts, byid=TRUE), 1, which.min) # get row index of rain gages located closest to each smp

smp.coords$rg_id <- rg.coords$rg_id[y] # extract gage value using the row indexes

# make changes to smp coords data frame to append to the mars database
smp.coords <- smp.coords %>%

mutate(gage_uid = as.numeric(rg_id)) %>%

select(smp_id, gage_uid)

#Query 'smp gage' table from database
smpgage <- sqlFetch(RODBC_con, "smp_gage", as.is = TRUE)

if(nrow(smp.coords) > nrow(smpgage)) { # coonect to mars database if new data

	new_smp <- anti_join(smp.coords, smpgage, by="smp_id")

	dbWriteTable(con, "smp_gage", new_smp, append= TRUE, row.names = FALSE) # append the data

} else {print("There is no new data.")}

#######################################################

# Part III

# To update smp radarcell table

radar_loc <- sqlFetch(RODBC_con, "radarcell_loc", as.is = TRUE) # Query radarcell_loc from mars database

smp_radar <- sqlFetch(RODBC_con, "smp_radarcell", as.is = TRUE) # Queryy smp id referencing radarcell table

smpLoc <- sqlFetch(RODBC_con, "smp_loc", as.is = TRUE) #Query smp loc table (make sure this table reflects the new data aw well)
                                                 # smp loc table is updated uaing part 1 of code
odbcClose(RODBC_con) #close RODBC connection

radar.coords <- select(radar_loc, lon_wgs84, lat_wgs84) %>% #select lat and lon columns

mutate_if(is.character, as.numeric) #change to numeric class type if in character type

coordinates(radar.coords) <- c('lon_wgs84','lat_wgs84') # set spatial coordinates 

proj4string(radar.coords) <- CRS("+proj=longlat") #this is the notation used to describe CRS

CRS.new <- CRS('+init=epsg:2272') # set required CRS (utm coordinate system)

radar.coords_sp <- spTransform(radar.coords, CRS.new) # convert lat lon to utm coordinate system

radar.coords_ft <- as.data.frame(radar.coords_sp@coords) %>% # convert to dataframe object

mutate(northing_ft = round(lon_wgs84,2),
       easting_ft=round(lat_wgs84,2)) %>%

select(northing_ft, easting_ft)

radar_loc <- cbind(radar_loc, radar.coords_ft) #column bind utm coordinates with radarloc table

smp_new <- anti_join(smpLoc, smp_radar, by='smp_id') # get only the new smp ids that does not exist in smp radar

smp_new.coords <- select(smp_new, lon_wgs84, lat_wgs84) %>% # select lat and lon columns

mutate_if(is.character, as.numeric) #change to numeric class type if in character type

coordinates(smp_new.coords) <- c('lon_wgs84','lat_wgs84') # set spatial coordinates 

proj4string(smp_new.coords) <- CRS("+proj=longlat") #this is the notation used to describe CRS

smp_new.coords_sp <- spTransform(smp_new.coords, CRS.new) # convert lat lon to utm coordinate system

y <- apply(gDistance(radar.coords_sp, smp_new.coords_sp, byid=TRUE), 1, which.min) # get row index of radar located closest to each smp

smp_new$radarcell_uid <- radar_loc$radarcell_uid[y]

smp_radarNew <- smp_new %>%

select(smp_id, radarcell_uid) %>% #select columns

distinct(smp_id, .keep_all = TRUE) #remove any duplicated smp ids

if(nrow(smp_radarNew) > nrow(smp_radar)) { # coonect to mars database if new data

	dbWriteTable(con, "smp_radarcell", smp_radarNew, append= TRUE, row.names = FALSE) # append the data

} else {print("There is no new data.")}

dbDisconnect(con)


