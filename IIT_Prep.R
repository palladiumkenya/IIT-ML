library(dplyr)
library(tidyr)
library(lubridate)

# read in Demographics -----------------------------------------------------
dem <- read.csv('Demographics.csv', stringsAsFactors = FALSE) # 100,055
names(dem)[1] <- 'SiteCode'
# samp <- sample(unique(dem$PatientID), 5000, replace = FALSE)

# read in and clean up ART -----------------------------------------------------
art <- read.csv('ART.csv', stringsAsFactors = FALSE) # 2,047,715
names(art)[1] <- 'SiteCode'


# read in and clean up visits --------------------------------------------------
visits <- read.csv('Visits.csv', stringsAsFactors = FALSE) # 2,850,852
names(visits)[1] <- 'SiteCode'
# visits <- visits[visits$PatientID %in% samp, ]

# read in and clean up labs ----------------------------------------------------
lab <- read.csv('Labs.csv', stringsAsFactors = FALSE) # 594,523
names(lab)[1] <- 'SiteCode'
# lab <- lab[lab$PatientID %in% samp, ]

# read in and clean up pharmacy ------------------------------------------------
pharmacy <- read.csv('Pharmacy.csv', stringsAsFactors = FALSE) # 1,110,283
names(pharmacy)[1] <- 'SiteCode'
# pharmacy <- pharmacy[pharmacy$PatientId %in% samp, ]

# read in most prediction date -------------------
pred_dates <- readRDS("Dropbox/prediction_dates.rds")

# 1. features for prior missingness pharmacy and visits (one row for patient) ----
# integrate pharmacy and visits and pivot wide (one integrated time series)

# first, let's process visits data
# keep only variables needed to assess missingness (patient id, visit date, and next appointment dates)
visits_missing <- visits %>% select(PatientID, VisitDate, NextAppointmentDate)
# convert date variables to date types
visits_missing$NextAppointmentDate <- ymd(visits_missing$NextAppointmentDate)
visits_missing$VisitDate <- ymd(visits_missing$VisitDate)


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

# Now, let's process pharmacy data in a similar way
# Keep only rows necessary to assess missingness, and convert date columns to date type
pharmacy_missing <- pharmacy %>% select(PatientId, DispenseDate, ExpectedReturn)
pharmacy_missing$DispenseDate <- ymd(pharmacy_missing$DispenseDate)
pharmacy_missing$ExpectedReturn <- ymd(pharmacy_missing$ExpectedReturn)

# some pharmacy pickups are recorded more than once. for each of this, keep the pickup with the 
# expected return date that is furthest in the future, and drop other records of this pickup
pharmacy_missing <- pharmacy_missing %>%
  group_by(PatientId, DispenseDate) %>%
  arrange(desc(ExpectedReturn)) %>%
  mutate(rownum = row_number()) %>%
  filter(rownum == 1) %>%
  select(-rownum)

# Set any incorrect expected return dates to NA
pharmacy_missing$ExpectedReturn[pharmacy_missing$ExpectedReturn == "1900-01-01"] <- NA
pharmacy_missing$ExpectedReturn[pharmacy_missing$ExpectedReturn >= "2023-07-01"] <- NA
pharmacy_missing$ExpectedReturn[pharmacy_missing$ExpectedReturn - pharmacy_missing$DispenseDate <= 0] <- NA
pharmacy_missing$ExpectedReturn[pharmacy_missing$ExpectedReturn - pharmacy_missing$DispenseDate > 365] <- NA

# Loop through each patient
# if there is a missing expected return date, then get gap between two prior return dates
# and impute
patients <- unique(pharmacy_missing$PatientId)
patient_list <- list()

for(i in 1:length(patients)){
  
  if(i%%100==0){print(i)}
  
  patient <- pharmacy_missing[pharmacy_missing$PatientId == patients[i], ]
  patient <- patient %>%
    arrange(desc(DispenseDate))
  
  # If a patient has only one visit, then they cannot have been IIT, so skip
  if(nrow(patient) == 1){next}
  
  # Calculate the gap between the prior two dispense dates
  for(j in 1:(nrow(patient)-1)){
    patient[j, "dispensediff"] <- patient[j, "DispenseDate"] - patient[j+1, "DispenseDate"]
  }
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
  patient_list[[i]] <- patient 
  
}

pharmacy_missing <- data.table::rbindlist(patient_list)

# in order to create a single integrated longitudinal record for each patient, we must combine
# visit and pharmacy pickup data. To facilitate this, rename variables in pharmacy dataframe to
# match names of variables in visits dataframe, and then rowbind them
pharmacy_missing <- pharmacy_missing %>%
  rename("VisitDate" = DispenseDate,
         "NextAppointmentDate" = ExpectedReturn,
         "PatientID" = PatientId)
# Now, rowbind them
missing <- rbind(visits_missing, pharmacy_missing)
#3,138,845 - number of rows in combined dataframe

#12.5%
sum(is.na(missing$NextAppointmentDate)) / nrow(missing)

# As before, if visits and pickups occurred on the same date, keep the record for the visit/pickup
# keep the record with the next appointment date that is furthest in the future, and drop others
missing <- missing %>%
  group_by(PatientID, VisitDate) %>%
  arrange(desc(NextAppointmentDate)) %>%
  mutate(rownum = row_number()) %>%
  filter(rownum == 1) %>%
  select(-rownum)
#2,552,683 remain

#5.1%
sum(is.na(missing$NextAppointmentDate)) / nrow(missing)

# drop the few rows with visit dates that are erroneous
missing <- missing[missing$VisitDate >= "2000-01-01", ]
missing <- missing[missing$VisitDate <= "2022-07-01", ]
#2,686,407


# We want temporal variation in target dates. Given the date of the extract,
# if we took attendance at the most recent visit as the target, the dates of our target
# would be concentrated in June. This could introduce bias. So, let's randomly drop 2-5 visits
# for patients with >20 visits, so that their target date shifts through the year.

# Loop through each patient and if they have more than
# 20 visits, delete 2-5 of their most recent visits, otherwise keep as is
patients <- unique(missing$PatientID)
patient_low <- c()
patient_high <- c()
prediction_low <- c()
prediction_high <- c()
df_low <- list()
df_high <- list()

set.seed(2231)

for(i in 1:length(patients)){

  if(i%%100==0){print(i)}

  patient <- missing[which(missing$PatientID == patients[i]), ]
  patient <- patient %>%
    arrange(desc(VisitDate))

  if(nrow(patient)<20){
    patient_low <- c(patient_low, patients[i]) # get patient ID
    prediction_low <- c(prediction_low, patient[2, "VisitDate"]) # get second most recent visit date 
    df_low[[i]] <- patient # get subset of dataframe for this patient, unamended
  }


  if(nrow(patient)>= 20){
    n_skip <- sample(2:5, 1) # randomly select number between 2 and 5
    patient <- patient[n_skip:nrow(patient), ] # delete the most recent 2-5 visits
    patient_high <- c(patient_high, patients[i]) # get patient id
    prediction_high <- c(prediction_high, patient[2, "VisitDate"]) # get second most recent visit date
    df_high[[i]] <- patient # get reduced subset of dataframe for this patient
  }

}

# combine remaining patient records
df_low <- data.table::rbindlist(df_low)
df_high <- data.table::rbindlist(df_high)
df_all <- rbind(df_low, df_high)

# combine patient IDs
patients <- c(patient_low, patient_high)

# Combine prediction dates - these are dates at which we'd make a prediction for next visit.
# This is the date that is the cutoff for including input data from other tables
# Including any data beyond this date would be model leakage
prediction_date <- c(do.call("c", prediction_low), do.call("c", prediction_high))

# Get the prediction dates as a dataframe for later us, cutting off data from other tables
pred_dates <- data.frame("PatientID" = patients,
                         "Dates" = prediction_date)

saveRDS(pred_dates, "Dropbox/prediction_dates.rds")
saveRDS(df_all, "Dropbox/df_all.rds")

# Let's look at Next Appointment Dates more closely, specifically those that are missing
# All missing NADs at this point come from pharmacy pickups, and this is to be expected 
# since setting an expected return date is an optional field

# First, there are some NADs that are Jan 1, 1900 or in the future - let's make these missing
missing <- df_all
rm(df_all)

# Loop through each patient
# if there is a missing expected return date, then get gap between two prior return dates
# and impute
patients <- unique(missing$PatientID)
patient_list <- list()
target_list <- list()

for(i in 1:length(patients)){
  
  if(i%%100==0){print(i)}
  
  patient <- missing[missing$PatientID == patients[i], ]
  # Arrange chronologically and select most recent 101 variables
  patient <- patient %>%
    arrange(desc(VisitDate)) %>%
    mutate(rownum = row_number()) %>%
    filter(rownum <= 101) %>%
    select(-rownum)
  # Calculate the gap between the visit date and the prevous NAD
  for(j in 1:(nrow(patient)-1)){
    patient[j, "visitdiff"] <- patient[j, "VisitDate"] - patient[j+1, "NextAppointmentDate"]
  }
  
  # if NAD from two times ago is NA, then skip
  if(is.na(patient[1, "visitdiff"])){next}
  
  # Get target variable - if visitdiff > 30, then IIT, otherwise not IIT
  target_list[[i]] <- data.frame("PatientID" = patient$PatientID[1],
                                 "Target" = ifelse(patient[1, "visitdiff"] > 30, 1, 0))
  
  # Drop the target variable
  patient <- patient[2:nrow(patient), ]
  
  # Now get how many times they were late
  patient_lateness <- patient %>%
    group_by(PatientID) %>%
    summarize(n_appts = n(),
              missed1 = sum(visitdiff > 0, na.rm = T),
              missed5 = sum(visitdiff > 5, na.rm = T),
              missed30 = sum(visitdiff > 30, na.rm = T))
  
  patient_lateness_five <- patient %>%
    mutate(rownum = row_number()) %>%
    filter(rownum <= 5) %>%
    group_by(PatientID) %>%
    summarize(missed1_last5 = sum(visitdiff > 0, na.rm = T),
              missed5_last5 = sum(visitdiff > 5, na.rm = T),
              missed30_last5 = sum(visitdiff > 30, na.rm = T))
  
  patient <- merge(patient_lateness, 
                   patient_lateness_five,
                   by = "PatientID")

  # Add to outlist
  patient_list[[i]] <- patient 
  
}

target <- data.table::rbindlist(target_list)
lateness <- data.table::rbindlist(patient_list)

# # Now, 4.2% of NADs are missing. We'll need to impute these
# sum(is.na(missing$NextAppointmentDate))/nrow(missing)
# 
# # The process we'll follow is this: When a next appointment date is missing, 
# # we'll look at the gap between the prior two appointments, and group them as 
# # 30/60/90/180 gaps. Then, to impute, we'll take the prior visit date and add
# # either 30/60/90/180 and that date will be the imputed NAD
# missing$TCA <- as.numeric(missing$NextAppointmentDate - missing$VisitDate)
# missing$TCA[missing$TCA <= 0 | missing$TCA > 365] <- NA
# missing$TCA_Grouped <- NA
# missing$TCA_Grouped[missing$TCA <= 45] <- 30
# missing$TCA_Grouped[missing$TCA > 45 & missing$TCA <= 75] <- 60
# missing$TCA_Grouped[missing$TCA > 75 & missing$TCA <= 135] <- 90
# missing$TCA_Grouped[missing$TCA > 135] <- 180
# 
# # Now let's get into wide format
# missing <- missing %>%
#   group_by(PatientID) %>%
#   arrange(desc(VisitDate)) %>%
#   mutate(rownum = row_number()) %>%
#   ungroup()
# 
# # Let's limit to the past 100 visits.
# # We'll take 101 for now, but in the next step, we'll use the most recent visit to generate our 
# # target variable, and then drop it from the input dataset, leaving us with 100 visits per patient.
# missing <- missing %>% filter(rownum <= 101) %>% mutate(rownum = paste0('visit_', rownum)) %>% select(-TCA)
# 
# missing_wide <- pivot_wider(missing,
#                             id_cols = c('PatientID'),
#                             names_from = c('rownum'),
#                             values_from = c('VisitDate', 'NextAppointmentDate', 'TCA_Grouped'))
# 
# # get target variable (if visit_1 - nextappointmentdate_2 > 30, 1, 0)
# target <- data.frame(PatientID = missing_wide$PatientID,
#                      Visit1 = missing_wide$VisitDate_visit_1,
#                      NAD2 = missing_wide$NextAppointmentDate_visit_2) %>%
#   mutate(target = ifelse(Visit1 - NAD2 > 30, 1, 0))  %>%
#   filter(!is.na(target)) %>%
#   select(PatientID, target)
# # then, drop visit_1 and next_appointmentdate_1 and TCA_Grouped_1
# missing_wide <- missing_wide %>%
#   select(-VisitDate_visit_1, -NextAppointmentDate_visit_1, -TCA_Grouped_visit_1)
# missing_wide <- missing_wide[missing_wide$PatientID %in% target$PatientID, ]
# 
# # # if next appointment date is missing, use previous gap + visit date
# # # if previous gap is missing, use 30 days for previous gap
# # 
# # # loop
# # 
# # # for each cell in the columns associated with Next appointments
# # #   if it's not NA, or if it is NA but associated visit date is NA too, skip
# # #   otherwise, get the gap from the previous visits - next appointment date
# # #   if that gap is NA, then set it to 28
# # #   update the next appointment date to corresponding visit date + gap
# # 
# # gaps <- c()
# # 
# # for (i in 1:nrow(missing_wide)){
# # 
# #   if(i%%100==0){print(i)}
# # 
# #   for (j in 201:103){
# # 
# #     if(!is.na(missing_wide[i,j]) | (is.na(missing_wide[i,j]) & is.na(missing_wide[i, j-99]))){next}
# # 
# #     gap <- as.numeric(missing_wide[i,j+99])
# #     gap <- ifelse(is.na(gap), 30, gap)
# # 
# #     missing_wide[i,j] <- missing_wide[i,j-100] + gap
# #     gaps <- c(gaps, gap)
# #   }
# # }
# # 
# # # what's the distribution of gaps we added?
# # table(gaps)
# 
# # Now, loop through again and generate for each Next Appointment Date, when was the next visit
# # maybe to summarize, for each patient get the number of missed visits according to this approach
# 
# tbv <- data.frame(matrix(data = NA, nrow = nrow(missing_wide), ncol = 99))
# 
# for (i in 1:nrow(missing_wide)){
# 
#   if(i%%100==0){print(i)}
# 
#   for (j in 2:100){
# 
#     # if visit date is NA, then next
#     if(is.na(missing_wide[i,j])){next}
# 
#     # get time between visit date and previous next appointment date (as.numeric)
#     tbv[i, j-1] <- missing_wide[i, j] - missing_wide[i, j+101]
# 
#   }
# }
# 
# missed30 <- apply(tbv, 1, function(x) sum(x>30, na.rm = T))
# missed5 <- apply(tbv, 1, function(x) sum(x>5, na.rm = T))
# missed1 <- apply(tbv, 1, function(x) sum(x>0, na.rm = T))
# missed30_last5 <- apply(tbv[, 1:5], 1, function(x) sum(x>30, na.rm = T))
# missed5_last5 <- apply(tbv[, 1:5], 1, function(x) sum(x>5, na.rm = T))
# missed1_last5 <- apply(tbv[, 1:5], 1, function(x) sum(x>0, na.rm = T))
# n_appts <- apply(tbv, 1, function(x) sum(!is.na(x)))
# 
# missing_out <- data.frame("PatientID" = missing_wide$PatientID,
#                           "Cadence" = missing_wide$TCA_Grouped_visit_2,
#                           "n_appts_history" = n_appts,
#                           "late1day" = missed1,
#                           "late5day" = missed5,
#                           "late30day" = missed30,
#                           "late1day_5recentvisits" = missed1_last5,
#                           "late5day_5recentvisits" = missed5_last5,
#                           "late30day_5recentvisits" = missed30_last5)

saveRDS(target, "Dropbox/target.rds")
saveRDS(lateness, "Dropbox/lateness.rds")

# 2. most recent pharmacy pickup features ---------------------

# Read in original pharmacy data
pharmacy <- read.csv('Pharmacy.csv', stringsAsFactors = FALSE) # 1,110,283
names(pharmacy)[1] <- 'SiteCode'
# pharmacy <- pharmacy[pharmacy$PatientId %in% samp, ]

# Similar processing as above - convert to date format
pharmacy$DispenseDate <- ymd(pharmacy$DispenseDate)

hiv <- pharmacy %>%
  filter(!TreatmentType %in% c("NULL", "Prophylaxis"))

# Join on prediction dates and limit to pharmacy pickups at or before the prediction date,
# to avoid leakage
hiv_join <- merge(hiv, pred_dates, by.x = "PatientId", by.y = "PatientID")
hiv_join <- hiv_join %>%
  filter(DispenseDate <= Dates)

# Now, let's filter for each patient to visits within the previous year (using 400 days as a buffer)
hiv_join <- hiv_join %>%
  filter(as.numeric(Dates - DispenseDate) < 400)
# 151,199

# Get rid of Null drugs
hiv_join <- hiv_join %>%
  filter(Drug != "NULL") %>%
  filter(!is.na(Drug))
# 216,205 remain

# Now, get the number of HIV regimens per patient
hiv_per_patient <- hiv_join %>%
  group_by(PatientId) %>%
  mutate(num_hiv_regimens = n_distinct(Drug)) %>%
  arrange(desc(DispenseDate)) %>%
  mutate(rownum = row_number()) %>%
  filter(rownum == 1) %>%
  select(-rownum) %>%
  rename("PatientID" = PatientId) %>%
  mutate(OptimizedHIVRegimen = ifelse(grepl("DTG", Drug), "Yes", "No"))

# Group ART Treatment Types
hiv_per_patient$TreatmentType[hiv_per_patient$TreatmentType %in% c("ART", "ARV", "HIV Treatment")] <- "ART"


other <- pharmacy %>%
  filter(TreatmentType %in% c("NULL", "Prophylaxis"))

# Join on prediction dates and limit to pharmacy pickups at or before the prediction date,
# to avoid leakage
other_join <- merge(other, pred_dates, by.x = "PatientId", by.y = "PatientID")
other_join <- other_join %>%
  filter(DispenseDate <= Dates)

# Now, let's filter for each patient to visits within the previous year (using 400 days as a buffer)
other_join <- other_join %>%
  filter(as.numeric(Dates - DispenseDate) < 400)
# 151,199

# Get rid of Null drugs
other_join <- other_join %>%
  filter(Drug != "NULL") %>%
  filter(!is.na(Drug))
# 8,103 remain

other_reg <- data.frame(PatientID = unique(other_join$PatientId),
                        Other_Regimen = "Yes")

reg_per_patient <- merge(hiv_per_patient, other_reg, by = "PatientID", all = TRUE)


# Save out number of regimens, treatment type, and most recent drug
reg_out <- reg_per_patient %>%
  select(PatientID, num_hiv_regimens, TreatmentType, OptimizedHIVRegimen, Other_Regimen) %>%
  mutate(Other_Regimen = ifelse(is.na(Other_Regimen), "No", Other_Regimen))
saveRDS(reg_out, "Dropbox/regimen.rds")



# 3. most recent visit pickup features ------------------------

# Join on prediction dates and limit visits to those that took place before or on the predictiond date
visits_join <- merge(visits, pred_dates, by = "PatientID")
visits_join$VisitDate <- ymd(visits_join$VisitDate)
visits_join <- visits_join %>%
  filter(VisitDate <= Dates)

# Let's get most recent 5 visits and count how many are unscheduled
visits_last5 <- visits_join %>%
  group_by(PatientID) %>%
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
visits_recent <- visits_join %>%
  group_by(PatientID) %>%
  arrange(desc(VisitDate)) %>%
  mutate(rownum = row_number()) %>%
  filter(rownum == 1)

# WHO Stage, BMI, Pregnant, OI, Differentiated Care, Adherence, Stability

# Get most recent WHO Stage
# Only 7,330 have this recorded ever
visits_who <- visits_join %>%
  filter(WhoStage != "NULL") %>%
  group_by(PatientID) %>%
  arrange(desc(VisitDate)) %>%
  mutate(rownum = row_number()) %>%
  filter(rownum == 1) %>% 
  select(PatientID, WhoStage)

# Calculate BMI for each patient and take most recent

# Check height and weight for additional cleaning

visits_bmi <- visits_join %>%
  merge(., dem[, c("PatientID", "Age")], by = "PatientID") %>%
  filter(Age >= 15) %>%
  filter(Weight != "NULL",
         Height != "NULL") %>%
  filter(between(as.numeric(Height), 100, 250)) %>%
  filter(between(as.numeric(Weight), 30, 200)) %>%
  mutate(BMI = as.numeric(Weight) / ((as.numeric(Height)/100)**2)) %>%
  mutate(BMI = ifelse(between(BMI, 10, 50), BMI, NA)) %>%
  filter(!is.na(BMI)) %>%
  group_by(PatientID) %>%
  arrange(desc(VisitDate)) %>%
  mutate(rownum = row_number())

# Get most recently recorded BMI
bmi_recent <- visits_bmi %>%
  filter(rownum == 1) %>%
  select(PatientID, BMI, Weight)

# Get BMI from six months ago
visits_bmi$timediff <- visits_bmi$Dates - visits_bmi$VisitDate
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
pregnant$Pregnant[pregnant$Gender %in% c("Male", "M")] <- "No"
pregnant$Pregnant[!between(pregnant$Age, 10, 49)] <- "No"
pregnant$Pregnant[pregnant$Pregnant == "NO"] <- "No"
pregnant$Pregnant[pregnant$Pregnant == "NULL"] <- NA
pregnant <- pregnant %>%
  select(PatientID, Pregnant)

# OI - *put aside as we wait for more documentation*
# Got OI from most recent visit - very few patients with Yes
oi_recent <- visits_recent %>%
  select(PatientID, OI) %>%
  mutate(OI = ifelse(OI %in% c("", "NULL"), "No", "Yes"))

# Get if patient had an OI at any point in the past year - more, but still very few
oi_lastyear <- visits_join %>%
  mutate(recent = ifelse(Dates - VisitDate < 400, 1, 0)) %>%
  filter(recent == 1)
oi_lastyear <- oi_lastyear %>%
  select(PatientID, OI) %>%
  mutate(OI = ifelse(OI %in% c("", "NULL"), 0, 1)) %>%
  group_by(PatientID) %>%
  summarize(OI_sum = sum(OI)) %>%
  mutate(OI_lastyear = ifelse(OI_sum >= 1, "Yes", "No")) %>%
  select(PatientID, OI_lastyear)

oi <- merge(oi_recent, oi_lastyear, by = "PatientID")


# Differentiated Care
table(visits_join$DifferentiatedCare)
diff_care <- visits_join %>%
  filter(!DifferentiatedCare %in% c("NULL", "")) %>%
  mutate(recent = ifelse(Dates - VisitDate < 400, 1, 0)) %>%
  filter(recent == 1) %>%
  group_by(PatientID) %>%
  arrange(desc(VisitDate)) %>%
  mutate(rownum = row_number()) %>%
  filter(rownum == 1) %>%
  select(-rownum)
diff_care$DifferentiatedCare <- gsub("[^[:alnum:][:space:]]","",diff_care$DifferentiatedCare)
table(diff_care$DifferentiatedCare)
diff_care$DifferentiatedCare <- gsub(" ", "", diff_care$DifferentiatedCare)
diff_care <- diff_care %>% select(PatientID, DifferentiatedCare)

# Adherence
table(visits_join$Adherence)
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
table(visits_join$StabilityAssessment)
stab <- visits_join %>%
  filter(!StabilityAssessment %in% c("NULL", "")) %>%
  mutate(recent = ifelse(Dates - VisitDate < 400, 1, 0)) %>%
  filter(recent == 1) %>%
  group_by(PatientID) %>%
  arrange(desc(VisitDate)) %>%
  mutate(rownum = row_number()) %>%
  filter(rownum == 1) %>%
  select(-rownum) %>%
  select(PatientID, StabilityAssessment)

# Combine all features 
visits_out <- merge(visits_who, bmi, by = "PatientID", all = TRUE) %>%
  merge(., oi, by = "PatientID", all = TRUE) %>%
  merge(., pregnant, by = "PatientID", all = TRUE) %>%
  merge(., diff_care, by = "PatientID", all = TRUE) %>%
  merge(., adherence_all, by = "PatientID", all = TRUE) %>%
  merge(., stab, by = "PatientID", all = TRUE) %>%
  merge(., visits_last5, by = "PatientID", all = TRUE)

saveRDS(visits_out, "Dropbox/visit_features.rds")

# 4. features for lab (one row for patient) ------------------
# how many viral loads were taken
# how many viral loads were high
# binary variable for if most recent viral load was high
# binary variable for if there ever was a high viral load

# Need to join lab and get most recent touchpoint and filter to labs prior to that date
lab <- merge(lab, pred_dates, by.x = "PatientID", by.y = "PatientID")
lab$OrderedbyDate <- ymd(lab$OrderedbyDate)
lab <- lab %>%
  filter(OrderedbyDate <= Dates)

## Let's get TestResult in shape
# First, let's move to lowercase
lab$TestResult <- tolower(lab$TestResult)

# First, let's get the content before the period (following Kennedy's script)
first.word <- function(my.string){
  unlist(strsplit(my.string, ".", fixed = T))[1]
}

lab$TestResult <- sapply(lab$TestResult, first.word)

# Next, generate a new variable which is the numeric of the string
lab$TR_num <- as.numeric(lab$TestResult)
sum(is.na(lab$TR_num)) # about 200K NA

# Next, create new variable vs and update
lab$vs <- 0
lab$vs <- ifelse(lab$TR_num < 400, 1, 0)

# For the character strings, first, what are the character strings that we have?
unique(filter(lab, is.na(TR_num))$TestResult)

# If the string contains "detect", which here means not detected, then make it 1 (which will become suppressed); if >100, then keep at 0, otherwise Null
lab <- lab %>%
  mutate(vs = ifelse(grepl("detect", lab$TestResult), 1,
                     ifelse(grepl(">", lab$TestResult), 0,
                            ifelse(!is.na(lab$TR_num), vs, NA))))

lab <- lab %>%
  mutate(vs = ifelse(lab$TR_num >= 400 & lab$TR_num < 1000 & !is.na(lab$TR_num), 2, vs))

table(lab$vs, useNA='always')

lab <- lab %>%
  mutate(vs = ifelse(lab$vs == 0, 'HVL',
                     ifelse(lab$vs == 1, 'Suppressed',
                            ifelse(lab$vs == 2, 'LVL', 'Missing'))))

lab <- lab %>% select(-c('TestResult', 'TR_num'))

# Let's drop observations with NA results - around 300
lab <- lab %>% filter(vs != 'Missing')

lab_patient <- lab %>%
  group_by(PatientID) %>%
  summarize(n_tests_all = n(),
            n_hvl_all = sum(vs == "HVL"))

lab$ReportedByDate <- ymd(lab$ReportedByDate)

# Lab – look at n_tests and n_hvl for most recent three years
lab_lastthreeyears <- lab %>%
  filter(Dates - ReportedByDate < 1000) %>%
  group_by(PatientID) %>%
  summarize(n_tests_threeyears = n(),
            n_hvl_threeyears = sum(vs == "HVL"))

lab_recent <- lab %>%
  group_by(PatientID) %>%
  arrange(desc(ReportedByDate)) %>%
  mutate(rownum = row_number()) %>%
  filter(rownum == 1) %>%
  rename("most_recent" = vs) %>%
  select(PatientID, most_recent)

lab_out <- merge(lab_patient, lab_recent, by = "PatientID", all = TRUE) %>%
  merge(., lab_lastthreeyears, by = "PatientID", all = TRUE)
saveRDS(lab_out, "Dropbox/lab.rds")

# 5. features for dem (one row for patient) ---------------
# basically just cleaning this up
# filter art to id's in dem and join on dem
# Filter to PatientID from dem
art <- art[art$PatientID %in% dem$PatientID, ]

# Filter to active and LTFU
art$ExitReason <- tolower(art$ExitReason)
table(art$ExitReason, useNA = "always")
art <- art[!art$ExitReason %in% c("completed", "dead", "died", "discontinue",
                                  "toxicity, drug", "transfer out", "transfer_out",
                                  "transferred out", "treatment complete", "unknown"), ]

art$StartARTDate <- ymd(art$StartARTDate)
art$StartARTDate[art$StartARTDate <= "1990-01-01"] <- NA

dem <- unique(dem) # 15 duplicates
art <- art[, c("PatientID", "StartARTDate")] # all we need is art start date
art <- unique(art) # from 93,648 to 93,462

dem <- merge(dem, art, by = "PatientID") # should be 93,462

# Gender - about 2-1 female
table(dem$Gender, useNA = "always")
dem$Gender[dem$Gender == "F"] <- "Female"
dem$Gender[dem$Gender == "M"] <- "Male"

# Age
hist(dem$Age)
range(dem$Age)
# Two patients have DOB from 1900 and age of 122 - assume these are incorrect - drop
dem$Age[dem$Age == 122] <- NA

# Time to start ART - this will StartARTDate - DateConfirmedHIVPositive
dem$DateConfirmedHIVPositive <- ymd(dem$DateConfirmedHIVPositive)
dem$StartARTDate <- ymd(dem$StartARTDate)
dem$timeToStartArt <- dem$StartARTDate - dem$DateConfirmedHIVPositive

# Time on ART - this will be second most recent visit - ART Start Date (need to join on that)
dem <- merge(dem, pred_dates, by.x = "PatientID", all.x = T)
dem$timeOnArt <- dem$Dates - dem$StartARTDate

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
table(dem$PopulationType, useNA = "always")
dem$PopulationType <- gsub(" ", "", dem$PopulationType)
dem$PopulationType[dem$PopulationType != "KeyPopulation"] <- "GeneralPopulation"

dem <- dem %>%
  select(PatientID, SiteCode, Gender, PatientSource, MaritalStatus, PopulationType,
         Age, timeToStartArt, StartARTDate, timeOnArt, AgeARTStart)

saveRDS(dem, "Dropbox/demographics.rds")
