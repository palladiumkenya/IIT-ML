
# Get Geocoded List of Facilities ---------------------
setwd("~/Kenya/IIT")

library(dplyr)
library(raster)
library(sp)
library(rgdal)
library(dplyr)
library(rgeos)
library(leaflet)

# raw_demographics <- read.csv("Demographics.csv")
gis_coords <- read.csv("../Data/KenyaHMIS Facility geocodes Jan 2022 Clean file.csv") %>%
  rename("FacilityCode" = MFL_Code,
         "Latitude" = latitude)

facilities <- gis_coords %>%
  # merge(., gis_coords, by.x = "FacilityCode", by.y = "MFL_Code") %>%
  dplyr::select(FacilityCode, Longitude, Latitude) %>%
  unique() %>%
  filter(!(Longitude %in% c("", "Null"))) %>%
  filter(!(Latitude %in% c("", "Null"))) %>%
  mutate(Longitude = as.numeric(Longitude),
         Latitude = as.numeric(Latitude)) 


# Add facilities that do not appear in this inventory of facilities
facilities_to_add <- data.frame(FacilityCode = c("Sindo DICE", "DICE IRDO - Mbita",
                                                  "Litare Community Health Centre", "Nyawawa Dispensary"),
                                Longitude = c(34.45310, 34.20607, 34.20599, 34.58398),
                                Latitude = c(-0.53481, -0.43650, -0.43659, -0.59986),
                                stringsAsFactors = FALSE)

# Stack facilities together
facilities <- rbind(facilities, facilities_to_add)

# Remove facilities with incorrect Longitudes
facilities <- filter(facilities, Longitude > 20)

leaflet() %>%
  addMarkers(lng = facilities$Longitude,
             lat = facilities$Latitude,
             label = facilities$FacilityCode) %>%
  addTiles()

# Convert latitudes and longitudes to spatial points
fac_mat <- as.matrix(facilities[, c("Longitude", "Latitude")])
points <- SpatialPoints(fac_mat, 
                        proj4string=CRS('+proj=longlat +datum=WGS84'))

# Get buffer of 3 km around each facility
pb <- buffer(points, width = 3000, dissolve = FALSE)

# Get extent of county to crop subsequent geospatial files
long <- range(facilities$Longitude)
lat <- range(facilities$Latitude)
e <- extent(long[1]-1, long[2]+1, lat[1]-1, lat[2]+1) 

# Births and Pregnancies ---------------------------------
# Births: https://www.worldpop.org/project/categories?id=5
births <- raster("../NDHW Data/TestData/HTS_App/FHW App/HomaBay/WorldPop/Kenya_1km_births/KEN_births_pp_v2_2015.tif")
births <- crop(births, e)
list_polygons <- extract(births, pb)
sum_polygons <- lapply(list_polygons, function(x){sum(x, na.rm = TRUE)})
births <- do.call("rbind", sum_polygons)

# Pregnancies: https://www.worldpop.org/project/categories?id=6
# Estimated number of pregnancies
pregnancies <- raster("../NDHW Data/TestData/HTS_App/FHW App/HomaBay/WorldPop/Kenya_1km_pregnancies/KEN_pregs_pp_v2_2015.tif")
pregnancies <- crop(pregnancies, e)
list_polygons <- extract(pregnancies, pb)
sum_polygons <- lapply(list_polygons, function(x){sum(x, na.rm = TRUE)})
pregnancies <- do.call("rbind", sum_polygons)

# Literacy: https://www.worldpop.org/geodata/summary?id=1261
# predicted proportion of female literacy ages 15-49
literacy <- raster("../NDHW Data/TestData/HTS_App/FHW App/HomaBay/WorldPop/Literacy/KEN_literacy_F.tif")
literacy <- crop(literacy, e)
list_polygons <- extract(literacy, pb)
sum_polygons <- lapply(list_polygons, function(x){mean(x, na.rm = TRUE)})
literacy <- do.call("rbind", sum_polygons)

# Poverty: https://www.worldpop.org/geodata/summary?id=1262
#  estimates of proportion of people per grid square living in poverty,
# as defined by the Multidimensional Poverty Index
# (http://www.ophi.org.uk/policy/multidimensional-poverty-index/)
poverty <- raster("../NDHW Data/TestData/HTS_App/FHW App/HomaBay/WorldPop/Poverty/ken08povmpi.tif")
poverty <- crop(poverty, e)
list_polygons <- extract(poverty, pb)
sum_polygons <- lapply(list_polygons, function(x){mean(x, na.rm = TRUE)})
poverty <- do.call("rbind", sum_polygons)

# Maternal Health: https://www.worldpop.org/geodata/summary?id=1263
# estimates represent the probability of
# a) receiving four or more antenatal care (ANC) visits at time of delivery,
# b) skilled birth attendance (SBA) during delivery, and 
# c) postnatal care (PNC) received within 48 hours of delivery.
anc <- raster("../NDHW Data/TestData/HTS_App/FHW App/HomaBay/WorldPop/Maternal_Newborn_Health/KEN_MNH_ANC.tif")
anc <- crop(anc, e)
list_polygons <- extract(anc, pb)
sum_polygons <- lapply(list_polygons, function(x){mean(x, na.rm = TRUE)})
anc <- do.call("rbind", sum_polygons)

pnc <- raster("../NDHW Data/TestData/HTS_App/FHW App/HomaBay/WorldPop/Maternal_Newborn_Health/KEN_MNH_PNC.tif")
pnc <- crop(pnc, e)
list_polygons <- extract(pnc, pb)
sum_polygons <- lapply(list_polygons, function(x){mean(x, na.rm = TRUE)})
pnc <- do.call("rbind", sum_polygons)

sba <- raster("../NDHW Data/TestData/HTS_App/FHW App/HomaBay/WorldPop/Maternal_Newborn_Health/KEN_MNH_SBA.tif")
sba <- crop(sba, e)
list_polygons <- extract(sba, pb)
sum_polygons <- lapply(list_polygons, function(x){mean(x, na.rm = TRUE)})
sba <- do.call("rbind", sum_polygons)

# IHME HIV -----------------------------------------
# http://ghdx.healthdata.org/record/ihme-data/africa-hiv-prevalence-geospatial-estimates-2000-2017

# Estimated HIV prevalence
hiv_prev <- raster("../NDHW Data/TestData/HTS_App/FHW App/HomaBay/IHME/IHME_AFRICA_HIV_2000_2017_CONDOM_LAST_TIME_PREV_MEAN_2017_Y2019M03D15.TIF")
hiv_prev <- crop(hiv_prev, e)
list_polygons <- extract(hiv_prev, pb)
sum_polygons <- lapply(list_polygons, function(x){mean(x, na.rm = TRUE)})
hiv_prev <- do.call("rbind", sum_polygons)

# Estimated PLHIV
hiv_count <- raster("../NDHW Data/TestData/HTS_App/FHW App/HomaBay/IHME/IHME_AFRICA_HIV_2000_2017_HIV_COUNT_MEAN_2017_Y2019M03D15.TIF")
hiv_count <- crop(hiv_count, e)
list_polygons <- extract(hiv_count, pb)
sum_polygons <- lapply(list_polygons, function(x){mean(x, na.rm = TRUE)})
hiv_count <- do.call("rbind", sum_polygons)

# Estimated prevalence of condom use at last sexual encounter
condom <- raster("../NDHW Data/TestData/HTS_App/FHW App/HomaBay/IHME/IHME_AFRICA_HIV_2000_2017_CONDOM_LAST_TIME_PREV_MEAN_2017_Y2019M03D15.TIF")
condom <- crop(condom, e)
list_polygons <- extract(condom, pb)
sum_polygons <- lapply(list_polygons, function(x){mean(x, na.rm = TRUE)})
condom <- do.call("rbind", sum_polygons)

# Prevalence of reporting ever had intercourse among young adults
intercourse <- raster("../NDHW Data/TestData/HTS_App/FHW App/HomaBay/IHME/IHME_AFRICA_HIV_2000_2017_HAD_INTERCOURSE_PREV_MEAN_2017_Y2019M03D15.TIF")
intercourse <- crop(intercourse, e)
list_polygons <- extract(intercourse, pb)
sum_polygons <- lapply(list_polygons, function(x){mean(x, na.rm = TRUE)})
intercourse <- do.call("rbind", sum_polygons)

# Prevalence of married or living with a partner as married
in_union <- raster("../NDHW Data/TestData/HTS_App/FHW App/HomaBay/IHME/IHME_AFRICA_HIV_2000_2017_IN_UNION_PREV_MEAN_2017_Y2019M03D15.TIF")
in_union <- crop(in_union, e)
list_polygons <- extract(in_union, pb)
sum_polygons <- lapply(list_polygons, function(x){mean(x, na.rm = TRUE)})
in_union <- do.call("rbind", sum_polygons)

# Prevalence of male circumcision
circumcision <- raster("../NDHW Data/TestData/HTS_App/FHW App/HomaBay/IHME/IHME_AFRICA_HIV_2000_2017_MALE_CIRCUMCISION_PREV_MEAN_2017_Y2019M03D15.TIF")
circumcision <- crop(circumcision, e)
list_polygons <- extract(circumcision, pb)
sum_polygons <- lapply(list_polygons, function(x){mean(x, na.rm = TRUE)})
circumcision <- do.call("rbind", sum_polygons)

# Prevalence of one's current partner living away from home
partner_away <- raster("../NDHW Data/TestData/HTS_App/FHW App/HomaBay/IHME/IHME_AFRICA_HIV_2000_2017_PARTNER_AWAY_PREV_MEAN_2017_Y2019M03D15.TIF")
partner_away <- crop(partner_away, e)
list_polygons <- extract(partner_away, pb)
sum_polygons <- lapply(list_polygons, function(x){mean(x, na.rm = TRUE)})
partner_away <- do.call("rbind", sum_polygons)

# Prevalence of men with multiple partners in past year
partner_men <- raster("../NDHW Data/TestData/HTS_App/FHW App/HomaBay/IHME/IHME_AFRICA_HIV_2000_2017_PARTNERS_YEAR_MN_PREV_MEAN_2017_Y2019M03D15.TIF")
partner_men <- crop(partner_men, e)
list_polygons <- extract(partner_men, pb)
sum_polygons <- lapply(list_polygons, function(x){mean(x, na.rm = TRUE)})
partner_men <- do.call("rbind", sum_polygons)

# Prevalence of women with multiple partners in past year
partner_women <- raster("../NDHW Data/TestData/HTS_App/FHW App/HomaBay/IHME/IHME_AFRICA_HIV_2000_2017_PARTNERS_YEAR_WN_PREV_MEAN_2017_Y2019M03D15.TIF")
partner_women <- crop(partner_women, e)
list_polygons <- extract(partner_women, pb)
sum_polygons <- lapply(list_polygons, function(x){mean(x, na.rm = TRUE)})
partner_women <- do.call("rbind", sum_polygons)

# Prevalence of self-reported STI symptoms
sti <- raster("../NDHW Data/TestData/HTS_App/FHW App/HomaBay/IHME/IHME_AFRICA_HIV_2000_2017_STI_SYMPTOMS_PREV_MEAN_2017_Y2019M03D15.TIF")
sti <- crop(sti, e)
list_polygons <- extract(sti, pb)
sum_polygons <- lapply(list_polygons, function(x){mean(x, na.rm = TRUE)})
sti <- do.call("rbind", sum_polygons)

facilities <- cbind(facilities, births, pregnancies, literacy, poverty, anc, pnc, sba, hiv_prev, hiv_count, condom, intercourse, in_union, 
                    circumcision, partner_away, partner_men, partner_women, sti)
names(facilities)[4:20] <- c("births", "pregnancies", "literacy", "poverty", "anc", "pnc", "sba",
                             "hiv_prev", "hiv_count", "condom", "intercourse", "in_union", 
                             "circumcision", "partner_away", "partner_men", "partner_women", "sti")

# Facebook Population Density  ---------------------------------------------
# Read in population density data from Facebook Data 4 Good
# https://data.humdata.org/dataset/highresolutionpopulationdensitymaps-ken
fb <- raster("~/Kenya/NDHW Data/TestData/HTS_App/FHW App/HomaBay/Facebook/ken_general_2020.tif")
fb <- crop(fb, e)
list_polygons <- extract(fb, pb)
sum_polygons <- lapply(list_polygons, function(x){mean(x, na.rm = TRUE)})
fb <- do.call("rbind", sum_polygons)

facilities <- cbind(facilities, fb)
names(facilities)[21] <- "pop_density"

fb <- raster("~/Kenya/NDHW Data/TestData/HTS_App/FHW App/HomaBay/Facebook/ken_women_of_reproductive_age_15_49_2020.tif")
fb <- crop(fb, e)
list_polygons <- extract(fb, pb)
sum_polygons <- lapply(list_polygons, function(x){mean(x, na.rm = TRUE)})
fb <- do.call("rbind", sum_polygons)

facilities <- cbind(facilities, fb)
names(facilities)[22] <- "women_reproductive_age"

fb <- raster("~/Kenya/NDHW Data/TestData/HTS_App/FHW App/HomaBay/Facebook/ken_youth_15_24_2020.tif")
fb <- crop(fb, e)
list_polygons <- extract(fb, pb)
sum_polygons <- lapply(list_polygons, function(x){mean(x, na.rm = TRUE)})
fb <- do.call("rbind", sum_polygons)

facilities <- cbind(facilities, fb)
names(facilities)[23] <- "young_adults"
  
saveRDS(facilities, "gis_features_hts.rds")

facilities <- facilities[, 1:21]
names(facilities)[21] <- "fb"
saveRDS(facilities, "gis_features_iit.rds")





