library(dplyr)
library(tidyr)
library(lubridate)
library(caret)
library(xgboost)
library(foreach)
library(doParallel)
library(rjson)
numCores <- detectCores()
registerDoParallel(numCores)

setwd("~/Kenya/IIT")

source("utils.R")

xgb <- readRDS("iit_xgb_0725.rds")

actives <- readxl::read_xlsx("ARTPatients_details.xlsx")

dem <- read.csv('OneDrive_1_7-25-2022/Patients_slim.csv', stringsAsFactors = FALSE)
dem <- dem[dem$PatientCCCNumber %in% unique(actives$PatientID), ]
dem$DOB <- ymd(dem$DOB)
dem$Age <- floor((Sys.Date() - dem$DOB) / 365)
art <- read.csv('OneDrive_1_7-25-2022/ART_slim.csv', stringsAsFactors = FALSE)
visits <- read.csv('visits_data_active_Aug_2022_20220923/visits_data_active_Aug_2022_20220923.csv', stringsAsFactors = FALSE)
lab <- read.csv('OneDrive_1_7-25-2022/labs.slim.csv', stringsAsFactors = FALSE)
pharmacy <- read.csv('OneDrive_1_7-25-2022/pharmacy_slim.csv', stringsAsFactors = FALSE)
gis <- readRDS('gis_features_iit.rds') %>% dplyr::select(-Latitude, -Longitude)

PATIENTS_IDS <- unique(dem$PatientID)

vec <- floor(seq(1, length(PATIENTS_IDS), length.out = 100))

for(a in 1:(length(vec)-1)){
  print(Sys.time())
  PATIENTS <- PATIENTS_IDS[vec[a]:vec[a+1]]

  dem_features <- genDemographicInputs(dem = dem, art = art)
  lateness_features <- genLatenessMetrics(visits_data = visits, pharmacy_data = pharmacy)
  regimen_features <- genRegimenInputs(pharmacy_data = pharmacy)
  visits_inputs_features <- genVisitInputs(visits_data = visits)
  vl_features <- genVLInputs(lab = lab)

  all <- merge(dem_features, regimen_features, by = "PatientID", all.x = TRUE) %>%
    merge(., visits_inputs_features, by = "PatientID", all.x = TRUE) %>%
    merge(., lateness_features, by = "PatientID", all.x = TRUE) %>%
    merge(., vl_features, by = "PatientID", all.x = TRUE) %>%
    merge(., gis, by.x = "SiteCode", by.y = "FacilityCode", all.x = TRUE) %>%
    data.frame(.)

  all_wide <- encodeXGBoost(all[, !names(all) %in% c("PatientID", "SiteCode")])

  all_wide <- all_wide %>%
    select(xgb$feature_names)

  val_predict <- predict(xgb,newdata = data.matrix(all_wide))
  payload <- data.frame(
    all[, 1:56],
    "Prediction" = val_predict
  )

  write.csv(payload, paste0("PredsSep2022/iit_payload_", a, ".csv"), row.names = FALSE)
  print(paste0("Batch ", a, " completed"))
}

filenames <- list.files("~/Kenya/IIT", pattern="*.csv", full.names=TRUE)
filenames <- filenames[grepl("payload", filenames)]
ldf <- lapply(filenames, read.csv)
preds <- data.table::rbindlist(ldf, use.names = TRUE)

dem <- dem[, c("PatientID", "DOB", "PatientCCCNumber", "Pkv")]
dem <- unique(dem)
# Keep only patients that have some visits recorded
dem <- filter(dem, PatientID %in% visits$PatientID)
rm(visits); gc()
# preds <- preds %>% select(PatientID, SiteCode, Prediction)
preds <- merge(preds, dem, by = "PatientID")

gis_coords <- read.csv("../Data/KenyaHMIS Facility geocodes Jan 2022 Clean file.csv") %>%
  rename("SiteCode" = MFL_Code) %>%
  select(SiteCode, Facility.Name)

preds <- merge(preds, gis_coords, by = "SiteCode")
preds$EvaluationDate <- as.character(Sys.Date())
preds$Description <- ifelse(preds$Prediction >= 0.1458253, "High Risk",
                            ifelse(preds$Prediction >= 0.04587388, "Medium Risk", "Low Risk"))

preds_out <- preds %>%
  select(SiteCode, Facility.Name, Pkv, PatientID, PatientCCCNumber,
         Gender, DOB, Prediction, Description, EvaluationDate) %>%
  rename("SiteName" = Facility.Name,
         "PatientGUID" = PatientID,
         "PatientPID" = Pkv,
         "PatientCccNumber" = PatientCCCNumber,
         "risk_score" = Prediction)

# Let's get the biggest risk factors for each patient
# Let's go in order of risk factors from Shap analysis:
# Time on ART < 12; Recent IIT > 0, Unscheduled Visits > 0, Optimized HIV Regimen = No
# Number of HIV regimens > 1, Age over 35, Previously Not Stable
risks <- list()
for(i in 1:nrow(preds)){

  if(i%%10000==0){print(i)}

  pred <- preds[i, ]
  pat_list <- list()
  if(pred$timeOnArt %in% c(0:12)){pat_list[["Time On Art"]] <- pred$timeOnArt}
  if(pred$missed30_last5 %in% c(1:5)){pat_list[["Recently IIT"]] <- pred$missed30_last5}
  if(pred$n_unscheduled_lastfive %in% c(1:5)){pat_list[["Unscheduled Visits"]] <- pred$n_unscheduled_lastfive}
  if(pred$OptimizedHIVRegimen %in% "No"){pat_list[["HIV Regimen"]] <- "Not Optimized"}
  if(pred$num_hiv_regimens %in% c(2:10)){pat_list[["Number HIV Regimens"]] <- pred$num_hiv_regimens}
  if(pred$StabilityAssessment %in% "Unstable"){pat_list[["Recent Stability"]] <- "Unstable"}
  risks[[i]] <- pat_list

}

risks_out <- list()


for(i in 1:length(risks)){

  if(i%%10000==0){print(i)}
  # risks_out[[i]] <- list("patient" = c(preds_out[i, ], "risks" = risks[[i]]))
  risks_out[[i]] <- list("patient" = list(
    SiteCode = as.character(preds_out[i, 1]),
    SiteName = as.character(preds_out[i, 2]),
    PatientPID = as.character(preds_out[i, 3]),
    PatientGUID = as.character(preds_out[i, 4]),
    PatientCccNumber = as.character(preds_out[i, 5]),
    Gender = as.character(preds_out[i, 6]),
    DOB = as.character(preds_out[i, 7]),
    risk_score = as.character(preds_out[i, 8]),
    Description = as.character(preds_out[i, 9]),
    EvaluationDate = as.character(preds_out[i, 10]),
    risks = risks[[i]]))
  # risks_out[[i]] <- c(preds_out[i, ], risks[[i]])
}

jsonData <- toJSON(risks_out)
write(jsonData, "iit_predictions_0925.json")
# write.csv(preds, paste0("iit_preds_", Sys.Date(), ".csv"), row.names = FALSE)

