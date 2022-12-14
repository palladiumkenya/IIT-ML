library(dplyr)
library(tidyr)
library(lubridate)
library(foreach)
library(doParallel)
numCores <- detectCores()
registerDoParallel(numCores)

# read in Demographics -----------------------------------------------------
dem <- read.csv('Demographics.csv', stringsAsFactors = FALSE) # 100,055
names(dem)[1] <- 'SiteCode'
samp <- sample(unique(dem$PatientID), 1000, replace = FALSE)

# read in and clean up ART -----------------------------------------------------
art <- read.csv('ART.csv', stringsAsFactors = FALSE) # 2,047,715
names(art)[1] <- 'SiteCode'
art <- art[art$PatientID %in% samp, ]

# read in and clean up visits --------------------------------------------------
visits <- read.csv('Visits.csv', stringsAsFactors = FALSE) # 2,850,852
names(visits)[1] <- 'SiteCode'
visits <- visits[visits$PatientID %in% samp, ]

# read in and clean up labs ----------------------------------------------------
lab <- read.csv('Labs.csv', stringsAsFactors = FALSE) # 594,523
names(lab)[1] <- 'SiteCode'
lab <- lab[lab$PatientID %in% samp, ]

# read in and clean up pharmacy ------------------------------------------------
pharmacy <- read.csv('Pharmacy.csv', stringsAsFactors = FALSE) # 1,110,283
names(pharmacy)[1] <- 'SiteCode'
pharmacy <- pharmacy[pharmacy$PatientId %in% samp, ]

print(Sys.time())

# read in most prediction date -------------------
# pred_dates <- readRDS("Dropbox/prediction_dates.rds")

# 1. features for prior missingness pharmacy and visits (one row for patient) ----
# integrate pharmacy and visits and pivot wide (one integrated time series)

# first, let's process visits data
# keep only variables needed to assess missingness (patient id, visit date, and next appointment dates)
visits_missing <- visits %>% select(PatientID, VisitDate, NextAppointmentDate)
# convert date variables to date types
visits_missing$NextAppointmentDate <- ymd(visits_missing$NextAppointmentDate)
visits_missing$VisitDate <- ymd(visits_missing$VisitDate)

visits_missing <- visits_missing[visits_missing$VisitDate >= "2015-01-01", ]
visits_missing <- visits_missing[visits_missing$VisitDate <= "2022-07-01", ]

# Drop entries where next appointment date is missing (inlcuding where next appointment is Jan 1, 1900)
visits_missing <- visits_missing[visits_missing$NextAppointmentDate != "1900-01-01", ]
# 2,502,294 remain
visits_missing <- visits_missing[!is.na(visits_missing$NextAppointmentDate), ]
# 2,325,966 remain
# Drop entries where NAD is before Visit Date
visits_missing$gap <- visits_missing$NextAppointmentDate - visits_missing$VisitDate
visits_missing <- visits_missing[visits_missing$gap > 0, ]
visits_missing <- visits_missing %>% select(-gap)
# 2, 315,816 remain

# 2,850,852 number of rows initially
# some visits are recorded more than once. for each of this, keep the visits with the 
# next appointment date that is furthest in the future, and drop other records of this visit
visits_missing <- visits_missing %>%
  group_by(PatientID, VisitDate) %>%
  arrange(desc(NextAppointmentDate)) %>%
  mutate(rownum = row_number()) %>%
  filter(rownum == 1) %>%
  select(-rownum)
#2,623,587 remain


# Now, let's process pharmacy data in a similar way
# Keep only rows necessary to assess missingness, and convert date columns to date type
pharmacy <- pharmacy %>% select(PatientId, DispenseDate, ExpectedReturn)
pharmacy$DispenseDate <- ymd(pharmacy$DispenseDate)
pharmacy$ExpectedReturn <- ymd(pharmacy$ExpectedReturn)

# Set any incorrect expected return dates to NA
pharmacy$ExpectedReturn[pharmacy$ExpectedReturn == "1900-01-01"] <- NA
pharmacy$ExpectedReturn[pharmacy$ExpectedReturn >= "2023-07-01"] <- NA
pharmacy$ExpectedReturn[pharmacy$ExpectedReturn - pharmacy$DispenseDate <= 0] <- NA
pharmacy$ExpectedReturn[pharmacy$ExpectedReturn - pharmacy$DispenseDate > 365] <- NA

# some pharmacy pickups are recorded more than once. for each of this, keep the pickup with the 
# expected return date that is furthest in the future, and drop other records of this pickup
pharmacy <- pharmacy %>%
  group_by(PatientId, DispenseDate) %>%
  arrange(desc(ExpectedReturn)) %>%
  mutate(rownum = row_number()) %>%
  filter(rownum == 1) %>%
  select(-rownum)

# Loop through each patient
# if there is a missing expected return date, then get gap between two prior return dates
# and impute
# Also flag if visit was imputed, so that we know not to use it as a target
patients <- unique(pharmacy$PatientId)
patient_list <- list()

# patient_list <- foreach(i = 1:length(patients),
#         .packages = c("dplyr", "lubridate"),
#         .errorhandling = 'remove')%dopar%{
for(i in 1:length(patients)){
  
  if(i%%100==0){print(i)}
  
  patient <- pharmacy[pharmacy$PatientId == patients[i], ]
  patient <- patient[order(patient$DispenseDate, decreasing = TRUE), ]
  
  
  # If a patient has only one visit, then they cannot have been IIT, so skip
  if(nrow(patient) == 1){next}
  
  # Calculate the gap between the prior two dispense dates
  for(j in 1:(nrow(patient)-1)){
    if(as.numeric(patient[j, "DispenseDate"] - patient[j+1, "DispenseDate"]) > 365){
      patient[j, "dispensediff"] <- 30
    } else{
      patient[j, "dispensediff"] <- as.numeric(patient[j, "DispenseDate"] - patient[j+1, "DispenseDate"])
    }
  }
  # Now, if expected return is missing, create flag
  patient$MissingExpectedReturn <- if_else(is.na(patient$ExpectedReturn), 1, 0)
  # Now, if expected return is missing, calculate it as the dispense date plus prior dispensediff
  patient$ExpectedReturn <- if_else(is.na(patient$ExpectedReturn),
                                    patient$DispenseDate + patient$dispensediff,
                                    patient$ExpectedReturn)
  # If the earliest pickup is the one missing an expected return, then ER will still be NA
  # So, let's clean those up by just adding 30
  patient$ExpectedReturn <- if_else(is.na(patient$ExpectedReturn),
                                    patient$DispenseDate + 30,
                                    patient$ExpectedReturn)
  
  # Add to outlist
  # return(patient)
  patient_list[[i]] <- patient
  
}

pharmacy_complete <- data.table::rbindlist(patient_list)

# in order to create a single integrated longitudinal record for each patient, we must combine
# visit and pharmacy pickup data. To facilitate this, rename variables in pharmacy dataframe to
# match names of variables in visits dataframe, and then rowbind them
pharmacy_complete <- pharmacy_complete %>%
  rename("VisitDate" = DispenseDate,
         "NextAppointmentDate" = ExpectedReturn,
         "PatientID" = PatientId)
# Now, rowbind them
allvisits <- bind_rows(visits_missing, pharmacy_complete)
allvisits$MissingExpectedReturn <- ifelse(is.na(allvisits$MissingExpectedReturn),
                                          0,
                                          allvisits$MissingExpectedReturn)
#3,138,845 - number of rows in combined dataframe

# As before, if visits and pickups occurred on the same date, keep the record for the visit/pickup
# keep the record with the next appointment date that is furthest in the future, and drop others
allvisits <- allvisits %>%
  group_by(PatientID, VisitDate) %>%
  dplyr::arrange(desc(NextAppointmentDate), .by_group = TRUE) %>%
  mutate(rownum = row_number()) %>%
  filter(rownum == 1) %>%
  select(-rownum)
#2,552,683 remain

# drop the few rows with visit dates that are erroneous
allvisits <- allvisits[allvisits$VisitDate >= "2000-01-01", ]
allvisits <- allvisits[allvisits$VisitDate <= "2022-07-01", ]
#2,686,407

# Loop through each patient and each visit from Jan 1 and on
patients <- unique(allvisits$PatientID)
patient_list <- list()
target_list <- list()

# target <- foreach(i = 1:length(patients),
#         .packages = c("dplyr", "lubridate"),
#         .errorhandling = 'remove')%dopar%{
for(i in 1:length(patients)){
  
  if(i%%1000==0){print(i)}
  
  patient <- allvisits[allvisits$PatientID == patients[i], ]
  
  # Arrange chronologically
  patient <- patient %>%
    arrange(desc(VisitDate))
  
  # Get number of NADs since Jan 1
  num_targets <- nrow(filter(patient, VisitDate > "2022-01-01"))
  
  if(num_targets == 0){next}
  
  for(k in 1:num_targets){
    
    # Arrange chronologically and select most recent 101 variables
    patient_tmp <- patient %>%
      ungroup() %>%
      arrange(desc(VisitDate)) %>%
      mutate(rownum = row_number()) %>%
      filter(rownum <= 101) %>%
      filter(rownum >= k) %>%
      select(-rownum)
    
    if(nrow(patient_tmp)<=1){next}
    
    # Calculate the gap between the visit date and the previous NAD
    for(j in 1:(nrow(patient_tmp)-1)){
      patient_tmp[j, "visitdiff"] <- patient_tmp[j, "VisitDate"] - patient_tmp[j+1, "NextAppointmentDate"]
    }
    
    # Get target variable - if visitdiff > 30, then IIT, otherwise not IIT
    target_tmp <- data.frame(PatientID = patient_tmp$PatientID[1],
                             Target = ifelse(patient_tmp[1, "visitdiff"] > 28, 1, 0),
                             PredictionDate = patient_tmp[2, "VisitDate"],
                             MissingNAD = patient_tmp[2, "MissingExpectedReturn"])
    target_list[[length(target_list) + 1]] <- target_tmp
    
    # Drop the target variable
    patient_tmp <- patient_tmp[2:nrow(patient_tmp), ]
    
    # Now get how many times they were late
    patient_lateness <- patient_tmp %>%
      group_by(PatientID) %>%
      summarize(n_appts = n(),
                missed1 = sum(visitdiff > 0, na.rm = T),
                missed5 = sum(visitdiff > 5, na.rm = T),
                missed30 = sum(visitdiff > 30, na.rm = T))
    
    patient_lateness_five <- patient_tmp %>%
      mutate(rownum = row_number()) %>%
      filter(rownum <= 5) %>%
      group_by(PatientID) %>%
      summarize(missed1_last5 = sum(visitdiff > 0, na.rm = T),
                missed5_last5 = sum(visitdiff > 5, na.rm = T),
                missed30_last5 = sum(visitdiff > 30, na.rm = T))
    
    patient_tmp <- merge(patient_lateness, 
                     patient_lateness_five,
                     by = "PatientID")
    
    # Add to outlist
    patient_list[[length(patient_list) + 1]] <- patient_tmp
    # return(c(target_tmp, patient_tmp))
    
  }
  
}

target <- data.table::rbindlist(target_list)
lateness <- data.table::rbindlist(patient_list)
target_lateness <- cbind(target, lateness[, -"PatientID"]) %>%
  filter(MissingExpectedReturn %in% c(0, NA)) %>%
  rename("target" = visitdiff,
         "PredictionDate" = VisitDate) %>%
  select(-MissingExpectedReturn)
target_lateness$key <- 1:nrow(target_lateness)
saveRDS(target_lateness, "target_lateness_1206.rds")

# 2. most recent pharmacy pickup features ---------------------

# Read in original pharmacy data
pharmacy <- read.csv('Pharmacy.csv', stringsAsFactors = FALSE) # 1,110,283
names(pharmacy)[1] <- 'SiteCode'
pharmacy <- pharmacy[pharmacy$PatientId %in% samp, ]

# Similar processing as above - convert to date format
pharmacy$DispenseDate <- ymd(pharmacy$DispenseDate)

hiv <- pharmacy %>%
  filter(!TreatmentType %in% c("NULL", "Prophylaxis"))
other <- pharmacy %>%
  filter(TreatmentType %in% c("NULL", "Prophylaxis"))

# Loop through each record from target_lateness and get most recent regimen data
hiv_regimen_list <- list()
other_regimen_list <- list()

# foreach(i = 1:nrow(target_lateness))%dopar%{
for(i in 1:nrow(target_lateness)){
  
  if(i%%1000==0){print(i)}
  
  tmp <- target_lateness[i, ]
  
  # Join on prediction dates and limit to pharmacy pickups at or before the prediction date,
  # to avoid leakage
  hiv_tmp <- hiv %>%
    filter(PatientId == tmp$PatientID)
  hiv_tmp <- hiv_tmp %>%
    filter(DispenseDate <= tmp$PredictionDate)
  
  # Now, let's filter for each patient to visits within the previous year (using 400 days as a buffer)
  hiv_tmp <- hiv_tmp %>%
    filter(as.numeric(tmp$PredictionDate - DispenseDate) < 400)
  # 151,199
  
  # Get rid of Null drugs
  hiv_tmp <- hiv_tmp %>%
    filter(Drug != "NULL") %>%
    filter(!is.na(Drug))
  # 216,205 remain
  
  # Now, get the number of HIV regimens per patient
  hiv_tmp <- hiv_tmp %>%
    group_by(PatientId) %>%
    mutate(num_hiv_regimens = n_distinct(Drug)) %>%
    arrange(desc(DispenseDate)) %>%
    mutate(rownum = row_number()) %>%
    filter(rownum == 1) %>%
    select(-rownum) %>%
    rename("PatientID" = PatientId) %>%
    mutate(OptimizedHIVRegimen = ifelse(grepl("DTG", Drug), "Yes", "No"))
  
  # Group ART Treatment Types
  hiv_tmp$TreatmentType[hiv_tmp$TreatmentType %in% c("ART", "ARV", "HIV Treatment")] <- "ART"
  
  hiv_regimen_list[[i]] <- hiv_tmp %>%
    select(num_hiv_regimens,
           TreatmentType,
           OptimizedHIVRegimen) %>%
    mutate(key = tmp$key)
  
  # Join on prediction dates and limit to pharmacy pickups at or before the prediction date,
  # to avoid leakage
  other_tmp <- other %>%
    filter(PatientId == tmp$PatientID)
  other_tmp <- other_tmp %>%
    filter(DispenseDate <= tmp$PredictionDate)
  
  # Now, let's filter for each patient to visits within the previous year (using 400 days as a buffer)
  other_tmp <- other_tmp %>%
    filter(as.numeric(tmp$PredictionDate - DispenseDate) < 400)
  # 151,199
  
  # Get rid of Null drugs
  other_tmp <- other_tmp %>%
    filter(Drug != "NULL") %>%
    filter(!is.na(Drug))
  # 8,103 remain
  
  other_regimen_list[[i]] <- data.frame(Other_Regimen = ifelse(nrow(other_tmp) > 0, "Yes", "No"),
                                        key = tmp$key)
}

hiv_regimen <- data.table::rbindlist(hiv_regimen_list)
other_regimen <- data.table::rbindlist(other_regimen_list)
tlr <- merge(target_lateness, hiv_regimen, by = "key", all.x = TRUE) %>%
  merge(., other_regimen, by = "key", all.x = TRUE)


# 3. most recent visit pickup features ------------------------

visits_list <- list()

# foreach(i = 1:nrow(target_lateness))%dopar%{
for(i in 1:nrow(target_lateness)){

  if(i%%1000==0){print(i)}
  
  tmp <- target_lateness[i, ]
  
  # Join on prediction dates and limit to pharmacy pickups at or before the prediction date,
  # to avoid leakage
  visits_tmp <- visits %>%
    filter(PatientID == tmp$PatientID)
  
  
# Join on prediction dates and limit visits to those that took place before or on the predictiond date
visits_tmp$VisitDate <- ymd(visits_tmp$VisitDate)
visits_tmp <- visits_tmp %>%
  filter(VisitDate <= tmp$PredictionDate)

# Let's get most recent 5 visits and count how many are unscheduled
visits_last5 <- visits_tmp %>%
  arrange(desc(VisitDate)) %>%
  mutate(rownum = row_number()) %>%
  filter(rownum <= 5)
visits_last5$visitType <- tolower(visits_last5$visitType)
visits_last5$unscheduled <- ifelse(grepl("unscheduled", visits_last5$visitType), 1, 0)
visits_last5 <- visits_last5 %>%
  group_by(PatientID) %>%
  summarize(n_visits_lastfive = n(),
            n_unscheduled_lastfive = sum(unscheduled))

# Order visits in reverse chronological order for each patient
visits_recent <- visits_tmp %>%
  arrange(desc(VisitDate)) %>%
  mutate(rownum = row_number()) %>%
  filter(rownum == 1)

# Calculate BMI for each patient and take most recent
# Check height and weight for additional cleaning

visits_bmi <- visits_tmp %>%
  merge(., dem[, c("PatientID", "Age")], by = "PatientID") %>%
  filter(Age >= 15) %>%
  filter(Weight != "NULL",
         Height != "NULL") %>%
  filter(between(as.numeric(Height), 100, 250)) %>%
  filter(between(as.numeric(Weight), 30, 200)) %>%
  mutate(BMI = as.numeric(Weight) / ((as.numeric(Height)/100)**2)) %>%
  mutate(BMI = ifelse(between(BMI, 10, 50), BMI, NA)) %>%
  filter(!is.na(BMI)) %>%
  arrange(desc(VisitDate)) %>%
  mutate(rownum = row_number())

# Get most recently recorded BMI
bmi_recent <- visits_bmi %>%
  filter(rownum == 1) %>%
  select(PatientID, BMI, Weight)

# Get BMI from six months ago
visits_bmi$timediff <- tmp$PredictionDate - visits_bmi$VisitDate
visits_bmi$difffrom60 <- abs(60 -visits_bmi$timediff)
bmi_sixmonths <- visits_bmi %>%
  group_by(PatientID) %>%
  arrange(difffrom60) %>%
  mutate(rownum = row_number()) %>%
  filter(rownum == 1) %>%
  select(PatientID, BMI, Weight)

# Join, and calculate percent change in BMI
bmi <- merge(bmi_recent, bmi_sixmonths, by = "PatientID", all.x = T)
bmi$changeInBMI <- (bmi$BMI.x - bmi$BMI.y) / bmi$BMI.y
bmi$changeInWeight <- (as.numeric(bmi$Weight.x) - as.numeric(bmi$Weight.y)) / as.numeric(bmi$Weight.y)
bmi$BMI <- bmi$BMI.x
bmi$Weight <- as.numeric(bmi$Weight.x)

# Keep patients' most recent BMI and change in BMI
bmi <- bmi %>%
  select(PatientID, BMI, changeInBMI, Weight, changeInWeight)

# Pregnant 
# Join with demographics data and we see this requires some cleaning (men and older/younger women show yes)
# So, join, and then fix men, women under 10 or over 49 as not pregnant
pregnant <- visits_recent %>%
  select(PatientID, Pregnant) %>%
  merge(., dem[, c("PatientID", "Gender", "Age")], by = "PatientID") %>%
  unique()
pregnant$Pregnant[pregnant$Gender %in% c("Male", "M")] <- "NR"
pregnant$Pregnant[!between(pregnant$Age, 10, 49)] <- "NR"
pregnant$Pregnant[pregnant$Pregnant == "NO"] <- "No"
pregnant$Pregnant[pregnant$Pregnant == "NULL"] <- NA
pregnant <- pregnant %>%
  select(PatientID, Pregnant)

# Differentiated Care
visits_join <- visits_tmp
diff_care <- visits_join %>%
  filter(!DifferentiatedCare %in% c("NULL", "")) %>%
  mutate(recent = ifelse(tmp$PredictionDate - VisitDate < 400, 1, 0)) %>%
  filter(recent == 1) %>%
  arrange(desc(VisitDate)) %>%
  mutate(rownum = row_number()) %>%
  filter(rownum == 1) %>%
  select(-rownum)
diff_care$DifferentiatedCare <- gsub("[^[:alnum:][:space:]]","",diff_care$DifferentiatedCare)
diff_care$DifferentiatedCare <- gsub(" ", "", diff_care$DifferentiatedCare)
diff_care <- diff_care %>% select(PatientID, DifferentiatedCare)

# Adherence
# There are up to two adherences (for ART and CTX) recorded for each patient
# These are recorded in the same cell, so next lines parse these into new variables
# Adherence and Adherence Category, when there are two, are always separated by a |,
# so we are splitting on that symbol
visits_join$adherence_symbol <- ifelse(grepl("\\|", visits_join$Adherence), 1, 0)
visits_join$adherence1 <- ifelse(visits_join$adherence_symbol == 1,
                                 gsub("\\|.*", "", visits_join$Adherence),
                                 visits_join$Adherence) 
visits_join$adherence2 <- ifelse(visits_join$adherence_symbol == 1,
                                 gsub(".*\\|", "", visits_join$Adherence),
                                 NA) 
visits_join$adherencecat_symbol <- ifelse(grepl("\\|", visits_join$AdherenceCategory), 1, 0)
visits_join$adherencecat1 <- ifelse(visits_join$adherencecat_symbol == 1,
                                    gsub("\\|.*", "", visits_join$AdherenceCategory),
                                    visits_join$AdherenceCategory) 
visits_join$adherencecat2 <- ifelse(visits_join$adherencecat_symbol == 1,
                                    gsub(".*\\|", "", visits_join$AdherenceCategory),
                                    NA) 

adherence1 <- visits_join %>%
  select(PatientID, VisitDate, adherence1, adherencecat1) %>%
  filter(adherencecat1 %in% c("ART", "ARV", "ARVAdherence", "CTX")) %>%
  mutate(adherencecat1 = ifelse(adherencecat1 == "CTX", "CTX", "ART")) %>%
  rename("adherence_category" = adherencecat1,
         "adherence" = adherence1)
adherence2 <- visits_join %>%
  select(PatientID, VisitDate, adherence2, adherencecat2) %>%
  filter(adherencecat2 %in% c("ART", "ARV", "ARVAdherence", "CTX")) %>%
  mutate(adherencecat2 = ifelse(adherencecat2 == "CTX", "CTX", "ART")) %>%
  rename("adherence_category" = adherencecat2,
         "adherence" = adherence2)
adherence <- rbind(adherence1, adherence2)
adherence$adherence <- tolower(adherence$adherence)
adherence <- adherence %>%
  mutate(adherence = ifelse(adherence == "g=good", "good", adherence)) %>%
  filter(adherence %in% c("fair", "good", "poor"))

# Get the most recent adherence for each adherence category for each patient
adherence_recent <- adherence %>%
  group_by(PatientID, adherence_category) %>%
  arrange(desc(VisitDate)) %>%
  mutate(rownum = row_number()) %>%
  filter(rownum == 1) %>%
  select(-rownum) %>%
  ungroup()

# for each patient, identify if their adherence has ever been poor or fair among the 
# five most recent visits
adherence_lastfive <- adherence %>%
  group_by(PatientID, adherence_category) %>%
  arrange(desc(VisitDate)) %>%
  mutate(rownum = row_number()) %>%
  filter(rownum <= 5) %>%
  select(-rownum) %>% 
  summarize(num_adherence = n(),
            num_poor = sum(adherence == "poor"),
            num_fair = sum(adherence == "fair"))

adherence_five_wide <- pivot_wider(adherence_lastfive,
                                   id_cols="PatientID",
                                   names_from = "adherence_category",
                                   values_from = c("num_adherence",
                                                   "num_poor",
                                                   "num_fair"))
adherence_recent_wide <- pivot_wider(adherence_recent,
                                     id_cols="PatientID",
                                     names_from = "adherence_category",
                                     values_from = c("adherence"))

# Now, combine 
adherence_all <- merge(adherence_five_wide, 
                       adherence_recent_wide,
                       by = "PatientID",
                       all = TRUE)

# Stability
stab <- visits_join %>%
  filter(!StabilityAssessment %in% c("NULL", "")) %>%
  mutate(recent = ifelse(tmp$PredictionDate - VisitDate < 400, 1, 0)) %>%
  filter(recent == 1) %>%
  group_by(PatientID) %>%
  arrange(desc(VisitDate)) %>%
  mutate(rownum = row_number()) %>%
  filter(rownum == 1) %>%
  select(-rownum) %>%
  select(PatientID, StabilityAssessment)

# Combine all features 
visits_out <- merge(visits_last5, bmi, by = "PatientID", all = TRUE) %>%
  merge(., pregnant, by = "PatientID", all = TRUE) %>%
  merge(., diff_care, by = "PatientID", all = TRUE) %>%
  merge(., adherence_all, by = "PatientID", all = TRUE) %>%
  merge(., stab, by = "PatientID", all = TRUE) %>%
  mutate(key = tmp$key) %>%
  select(-PatientID)

visits_list[[i]] <- visits_out

}

visits_df <- data.table::rbindlist(visits_list, fill = TRUE)
tlrv <- merge(tlr, visits_df, by = "key", all.x = TRUE) 


# 4. features for lab (one row for patient) ------------------
# how many viral loads were taken
# how many viral loads were high
# binary variable for if most recent viral load was high
# binary variable for if there ever was a high viral load

lab_list <- list()

# foreach(i = 1:nrow(target_lateness))%dopar%{
for(i in 1:nrow(target_lateness)){
  
  if(i%%100==0){print(i)}
  
  tmp <- target_lateness[i, ]
  
  # Need to join lab and get most recent touchpoint and filter to labs prior to that date
  lab_tmp <- lab[lab$PatientID == tmp$PatientID, ]
  lab_tmp$OrderedbyDate <- ymd(lab_tmp$OrderedbyDate)
  lab_tmp <- lab_tmp %>%
    filter(OrderedbyDate <= tmp$PredictionDate)
  
  if(nrow(lab_tmp)==0){next}
  
  ## Let's get TestResult in shape
  # First, let's move to lowercase
  lab_tmp$TestResult <- tolower(lab_tmp$TestResult)
  
  # First, let's get the content before the period (following Kennedy's script)
  first.word <- function(my.string){
    unlist(strsplit(my.string, ".", fixed = T))[1]
  }
  
  lab_tmp$TestResult <- sapply(lab_tmp$TestResult, first.word)
  
  # Next, generate a new variable which is the numeric of the string
  lab_tmp$TR_num <- as.numeric(lab_tmp$TestResult)
  sum(is.na(lab_tmp$TR_num)) # about 200K NA
  
  # Next, create new variable vs and update
  lab_tmp$vs <- 0
  lab_tmp$vs <- ifelse(lab_tmp$TR_num < 400, 1, 0)
  
  # For the character strings, first, what are the character strings that we have?
  unique(filter(lab_tmp, is.na(TR_num))$TestResult)
  
  # If the string contains "detect", which here means not detected, then make it 1 (which will become suppressed); if >100, then keep at 0, otherwise Null
  lab_tmp <- lab_tmp %>%
    mutate(vs = ifelse(grepl("detect", lab_tmp$TestResult), 1,
                       ifelse(grepl(">", lab_tmp$TestResult), 0,
                              ifelse(!is.na(lab_tmp$TR_num), vs, NA))))
  
  lab_tmp <- lab_tmp %>%
    mutate(vs = ifelse(lab_tmp$TR_num >= 400 & lab_tmp$TR_num < 1000 & !is.na(lab_tmp$TR_num), 2, vs))
  
  lab_tmp <- lab_tmp %>%
    mutate(vs = ifelse(lab_tmp$vs == 0, 'HVL',
                       ifelse(lab_tmp$vs == 1, 'Suppressed',
                              ifelse(lab_tmp$vs == 2, 'LVL', 'Missing'))))
  
  lab_tmp <- lab_tmp %>% select(-c('TestResult', 'TR_num'))
  
  # Let's drop observations with NA results - around 300
  lab_tmp <- lab_tmp %>% filter(vs != 'Missing')
  
  lab_patient <- lab_tmp %>%
    group_by(PatientID) %>%
    summarize(n_tests_all = n(),
              n_hvl_all = sum(vs == "HVL"))
  
  lab_tmp$ReportedByDate <- ymd(lab_tmp$ReportedByDate)
  
  # Lab – look at n_tests and n_hvl for most recent three years
  lab_lastthreeyears <- lab_tmp %>%
    filter(tmp$PredictionDate - ReportedByDate < 1000) %>%
    group_by(PatientID) %>%
    summarize(n_tests_threeyears = n(),
              n_hvl_threeyears = sum(vs == "HVL"))
  
  lab_recent <- lab_tmp %>%
    group_by(PatientID) %>%
    arrange(desc(ReportedByDate)) %>%
    mutate(rownum = row_number()) %>%
    filter(rownum == 1) %>%
    rename("most_recent" = vs) %>%
    select(PatientID, most_recent)
  
  lab_out <- merge(lab_patient, lab_recent, by = "PatientID", all = TRUE) %>%
    merge(., lab_lastthreeyears, by = "PatientID", all = TRUE) %>%
    mutate(key = tmp$key)
  
  lab_list[[i]] <- lab_out

}

lab_df <- data.table::rbindlist(lab_list, fill = TRUE)
tlrvl <- merge(tlrv, lab_df, by = "key", all.x = TRUE) 

# 5. features for dem (one row for patient) ---------------
# basically just cleaning this up
# filter art to id's in dem and join on dem
# Filter to PatientID from dem

# Filter to active and LTFU
art$ExitReason <- toupper(art$ExitReason)
art <- art %>%
  mutate(ExitReason = toupper(ExitReason)) %>%
  mutate(ExitStatus = case_when(
    grepl("DEAD", ExitReason) ~ "DEAD",
    grepl("TRANSFER", ExitReason) ~ "TRANSFER",
    TRUE ~ "OTHER"
  ))

art$StartARTDate <- ymd(art$StartARTDate)
art$StartARTDate[art$StartARTDate <= "1990-01-01"] <- NA

dem <- unique(dem) # 15 duplicates
art <- art[, c("PatientID", "StartARTDate")] # all we need is art start date
art <- unique(art) # from 93,648 to 93,462

dem <- merge(dem, art, by = "PatientID") # should be 93,462

# Gender - about 2-1 female
dem$Gender[dem$Gender == "F"] <- "Female"
dem$Gender[dem$Gender == "M"] <- "Male"

# Age
# Two patients have DOB from 1900 and age of 122 - assume these are incorrect - drop
dem$Age[dem$Age > 100] <- NA

# Time to start ART - this will StartARTDate - DateConfirmedHIVPositive
dem$DateConfirmedHIVPositive <- ymd(dem$DateConfirmedHIVPositive)
dem$StartARTDate <- ymd(dem$StartARTDate)
dem$timeToStartArt <- dem$StartARTDate - dem$DateConfirmedHIVPositive

# Time on ART - this will be second most recent visit - ART Start Date (need to join on that)
dem <- merge(dem, tlrvl, by.x = "PatientID", all.x = T)
dem$timeOnArt <- dem$PredictionDate - dem$StartARTDate

# Age at which tested positive (age - (how many years ago was art start date))
dem$artstart_yearsago <- floor((Sys.Date() - dem$StartARTDate)/365)
dem$AgeARTStart <- as.numeric(dem$Age - dem$artstart_yearsago)

# Patient Source
table(dem$PatientSource, useNA = "always")
dem$PatientSource[dem$PatientSource %in% c("VCT")] <- "VCT"
dem$PatientSource[dem$PatientSource %in% c("OPD")] <- "OPD"
dem$PatientSource[dem$PatientSource %in% c("MCH")] <- "MCH"
dem$PatientSource[dem$PatientSource %in% c("VMMC")] <- "VMMC"
dem$PatientSource[dem$PatientSource %in% c("PITC")] <- "Other"
dem$PatientSource[dem$PatientSource %in% c("OTHER")] <- "Other"
dem$PatientSource[dem$PatientSource %in% c("")] <- "Other"
dem$PatientSource[dem$PatientSource %in% c("NULL")] <- "Other"
dem$PatientSource[dem$PatientSource %in% c("OTHERS")] <- "Other"
dem$PatientSource[dem$PatientSource %in% c("HEI")] <- "Other"
dem$PatientSource[dem$PatientSource %in% c("CCC")] <- "CCC"
dem$PatientSource[dem$PatientSource %in% c("IPD - Adult")] <- "IPDAdult"
dem$PatientSource[dem$PatientSource %in% c("TB Clinic")] <- "TBClinic"
dem$PatientSource[dem$PatientSource %in% c("Transfer In")] <- "OPD"
dem$PatientSource[dem$PatientSource %in% c("CWC")] <- "CWC"
# Group rare categories
dem$PatientSource[dem$PatientSource %in% c("VMMC", "CWC")] <- "Other"

# Marital Status
table(dem$MaritalStatus, useNA = "always")
dem$MaritalStatus <- tolower(dem$MaritalStatus)
dem$MaritalStatus[dem$MaritalStatus %in% c("married monogamous", "cohabiting")] <- "Married"
dem$MaritalStatus[dem$MaritalStatus %in% c("divorced", "separated", "seperated/divorced")] <- "Divorced"
dem$MaritalStatus[dem$MaritalStatus %in% c("widow", "widowed")] <- "Widow"
dem$MaritalStatus[dem$MaritalStatus %in% c("married polygamous")] <- "Polygamous"
dem$MaritalStatus[dem$MaritalStatus %in% c("single", "not married")] <- "Single"
dem$MaritalStatus[dem$MaritalStatus %in% c("null", "unknown", "other")] <- "Other"
dem$MaritalStatus[dem$Age <= 15] <- "Single"

# Population Type
dem$PopulationType <- gsub(" ", "", dem$PopulationType)
dem$PopulationType[dem$PopulationType != "KeyPopulation"] <- "GeneralPopulation"

dem <- dem %>%
  select("PatientID", "SiteCode", "Gender", "PatientSource", "MaritalStatus",
         "EducationLevel", "PopulationType", "PatientResidentWard", "Age", "target",                    
         "n_appts", "missed1",  "missed5", "missed30", "missed1_last5", "missed5_last5",
         "missed30_last5", "num_hiv_regimens", "TreatmentType", "OptimizedHIVRegimen",
         "Other_Regimen", "n_visits_lastfive", "n_unscheduled_lastfive", "BMI",
         "changeInBMI", "Weight", "changeInWeight", "Pregnant", "DifferentiatedCare",
         "num_adherence_ART", "num_adherence_CTX", "num_poor_ART",  "num_poor_CTX",
         "num_fair_ART", "num_fair_CTX", "ART", "CTX" , "StabilityAssessment",      
         "n_tests_all","n_hvl_all","most_recent","n_tests_threeyears",
         "n_hvl_threeyears","timeOnArt", "AgeARTStart") 

saveRDS(dem, "iit_1206.rds")

print(Sys.time())