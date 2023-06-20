# First, load necessary packages
library(aws.s3)
library(dplyr)
library(lubridate)
library(foreach)
library(doParallel)
library(data.table)
library(tidyr)
numCores <- detectCores()
registerDoParallel(numCores)

# # get the keys from the .Renviron file and connect to the server
# Sys.setenv("AWS_ACCESS_KEY_ID" = "AKIAXRAVXGNNKTISKPEH",
#            "AWS_SECRET_ACCESS_KEY" = "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",
#            "AWS_DEFAULT_REGION" = "us-east-1")
# # name the bucket
# aws_bucket <- "kehmishtstest"
# 
# # get the bucket
# get_bucket(aws_bucket)

print("starting")

dem <- readRDS("patients_samp250k.rds")
pharmacy <- readRDS("pharmacy_samp250k.rds")
visits <- readRDS("visits_samp250k.rds")
labs <- readRDS("labs_samp250k.rds")

# dem <- s3read_using(FUN = readRDS, bucket = aws_bucket, object = "March2023Dump/patients_samp.rds")
# pharmacy <- s3read_using(FUN = readRDS, bucket = aws_bucket, object = "March2023Dump/pharmacy_samp.rds")
# visits <- s3read_using(FUN = readRDS, bucket = aws_bucket, object = "March2023Dump/visits_samp.rds")
# labs <- s3read_using(FUN = readRDS, bucket = aws_bucket, object = "March2023Dump/labs_samp.rds")


# Create Age variable for dem ----------------------
dem$DOB <- ymd(substr(dem$DOB, 1, 10))
dem <- dem[!duplicated(dem$key),]

# 1. features for prior missingness pharmacy and visits (one row for appointment) ----
# integrate pharmacy and visits and pivot wide (one integrated time series)

# first, let's process visits data
# keep only variables needed to assess missingness (patient id, visit date, and next appointment dates)
visits_missing <- visits %>% select(key, VisitDate, NextAppointmentDate)
# convert date variables to date types
visits_missing$NextAppointmentDate <- ymd(visits_missing$NextAppointmentDate)
visits_missing$VisitDate <- ymd(visits_missing$VisitDate)

# Visits are all between 2019 and present so nothing drops
range(visits_missing$VisitDate)
visits_missing <- visits_missing[visits_missing$VisitDate <= "2023-03-23", ]

# Drop entries where next appointment date is missing (inlcuding where next appointment is before January 1, 2019
# and beyond april 2024)
range(visits_missing$NextAppointmentDate, na.rm = TRUE)
visits_missing <- visits_missing[!is.na(visits_missing$NextAppointmentDate), ]
visits_missing <- visits_missing[visits_missing$NextAppointmentDate >= "2019-01-01", ]
visits_missing <- visits_missing[visits_missing$NextAppointmentDate <= "2024-04-01", ]

# 2,325,966 remain
# Drop entries where NAD is before Visit Date or more than 365 days in the future
visits_missing$gap <- visits_missing$NextAppointmentDate - visits_missing$VisitDate
visits_missing <- visits_missing[visits_missing$gap > 0, ]
visits_missing <- visits_missing[visits_missing$gap < 400, ]
visits_missing <- visits_missing %>% select(-gap)
# 2, 315,816 remain

# some visits are recorded more than once. for each of this, keep the visits with the 
# next appointment date that is furthest in the future, and drop other records of this visit
visits_missing <- visits_missing %>%
  group_by(key, VisitDate) %>%
  arrange(desc(NextAppointmentDate)) %>%
  mutate(rownum = row_number()) %>%
  filter(rownum == 1) %>%
  select(-rownum)


# Now, let's process pharmacy data in a similar way
# Keep only rows necessary to assess missingness, and convert date columns to date type
pharmacy <- pharmacy %>% select(key, DispenseDate, ExpectedReturn)
pharmacy$DispenseDate <- ymd(pharmacy$DispenseDate)
pharmacy$ExpectedReturn <- ymd(pharmacy$ExpectedReturn)

# Set any incorrect expected return dates to NA
range(pharmacy$DispenseDate)
range(pharmacy$ExpectedReturn, na.rm = TRUE)
pharmacy$ExpectedReturn[pharmacy$ExpectedReturn < "2019-01-01"] <- NA
pharmacy$ExpectedReturn[pharmacy$ExpectedReturn >= "2024-04-01"] <- NA
pharmacy$ExpectedReturn[pharmacy$ExpectedReturn - pharmacy$DispenseDate < 0] <- NA
pharmacy$ExpectedReturn[pharmacy$ExpectedReturn - pharmacy$DispenseDate > 365] <- NA

# some pharmacy pickups are recorded more than once. for each of this, keep the pickup with the 
# expected return date that is furthest in the future, and drop other records of this pickup
pharmacy <- pharmacy %>%
  group_by(key, DispenseDate) %>%
  arrange(desc(ExpectedReturn)) %>%
  mutate(rownum = row_number()) %>%
  filter(rownum == 1) %>%
  select(-rownum)

# Loop through each patient
# if there is a missing expected return date, then get gap between two prior return dates
# and impute
# Also flag if visit was imputed, so that we know not to use it as a target
patients <- unique(pharmacy$key)

# patient_list <- foreach(i = 1:length(patients),
#         .packages = c("dplyr", "lubridate"),
#         .errorhandling = 'remove')%dopar%{
# start <- Sys.time()
pharmacy_complete <- foreach(i = 1:length(patients),
        .packages = c("dplyr", "lubridate"),
        .combine = "rbind",
        .errorhandling = 'remove')%dopar%{
# for(i in 1:length(patients)){
  
  # if(i%%100==0){print(i)}
  
  patient <- pharmacy[pharmacy$key == patients[i], ]
  patient <- patient[order(patient$DispenseDate, decreasing = TRUE), ]
  
  
  # If a patient has only one pickup and the expected return date is NA, then skip
  if(nrow(patient) == 1 & is.na(patient$ExpectedReturn[1])){next}
  # If a patient has only one pickup and the expected return date is NA, then skip
  if(nrow(patient) == 1 & !is.na(patient$ExpectedReturn[1])){
    # patient_list[[i]] <- patient
    patient
  } else{
  
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
  # patient_list[[i]] <- patient
  patient
  
  }
  
        }

print("pharmacy expected return imputed")

# end <- Sys.time()
# print(end - start)

# 2.14 minutes down to 40 seconds

# pharmacy_complete <- data.table::rbindlist(patient_list, fill = TRUE)

# in order to create a single integrated longitudinal record for each patient, we must combine
# visit and pharmacy pickup data. To facilitate this, rename variables in pharmacy dataframe to
# match names of variables in visits dataframe, and then rowbind them
pharmacy_complete <- pharmacy_complete %>%
  rename("VisitDate" = DispenseDate,
         "NextAppointmentDate" = ExpectedReturn)
# Now, rowbind them
allvisits <- bind_rows(visits_missing, pharmacy_complete)
allvisits$MissingExpectedReturn <- ifelse(is.na(allvisits$MissingExpectedReturn),
                                          0,
                                          allvisits$MissingExpectedReturn)
#3,138,845 - number of rows in combined dataframe

## Drop any appointment whose next appointment date is more than 28 days before
# the most recent upload. Since we're working with a March upload now, assuming visits are captured
# through February 28, the latest an NAD we can have and know the person is IIT is January 31st
# So, let's filter to NAD on or before January 31st
# allvisits <- allvisits[allvisits$NextAppointmentDate <= "2023-01-31", ]

# As before, if visits and pickups occurred on the same date, keep the record for the visit/pickup
# keep the record with the next appointment date that is furthest in the future, and drop others
# start <- Sys.time()
allvisits <- allvisits %>%
  group_by(key, VisitDate) %>%
  dplyr::arrange(desc(NextAppointmentDate), .by_group = TRUE) %>%
  mutate(rownum = row_number()) %>%
  filter(rownum == 1) %>%
  select(-rownum)
# end <- Sys.time()
# print(end - start)
#2,552,683 remain

# Loop through each patient and each visit from Jan 1 and on
patients <- unique(allvisits$key)
# patient_list <- list()

# from 4.5 minutes to 3.5 minutes
# start <- Sys.time()

outlist_first <- foreach(i = 1:length(patients),
                         .packages = c("dplyr", "lubridate"),
                         # .combine = "rbind",
                         .errorhandling = 'remove')%dopar%{
                           
                           patient <- allvisits[allvisits$key == patients[i], ]
                           
                           # Arrange chronologically
                           patient <- patient %>%
                             arrange(desc(VisitDate))
                           
                           if(patient[1, "NextAppointmentDate"]$NextAppointmentDate > "2023-01-31"){next}
                           if(patient[1, "VisitDate"]$VisitDate < "2021-01-01"){next}
                             
                             # Arrange chronologically and select most recent 51 variables
                             patient_tmp <- patient %>%
                               ungroup() %>%
                               arrange(desc(VisitDate)) %>%
                               mutate(rownum = row_number()) %>%
                               filter(rownum <= 26)
                             
                             # Calculate the gap between the visit date and the previous NAD
                             for(j in 1:(nrow(patient_tmp)-1)){
                               patient_tmp[j, "visitdiff"] <- patient_tmp[j, "VisitDate"] - patient_tmp[j+1, "NextAppointmentDate"]
                             }
                             
                             
                             # if next appointment date of the most recent visit is before janaury 31st, then include it as a IIT
                             target_tmp <- data.frame(key = patient_tmp$key[1],
                                                      Target = 1,
                                                      PredictionDate = patient_tmp[1, "VisitDate"]$VisitDate,
                                                      MissingNAD = patient_tmp[1, "MissingExpectedReturn"]$MissingExpectedReturn,
                                                      stringsAsFactors = FALSE)
                             
                             # generate features
                             # Drop the target variable (first row) & the earliest visit since cannot have been late (last row)
                             patient_tmp <- patient_tmp[2:nrow(patient_tmp), ]
                             
                             
                             # Now get how many times they were late
                             patient_lateness_all <- patient_tmp %>%
                               group_by(key) %>%
                               mutate(visitdiff = as.numeric(visitdiff)) %>%
                               mutate(visitdiff = ifelse(visitdiff < 0, 0,visitdiff)) %>%
                               summarize(n_appts = n(),
                                         late = sum(visitdiff > 0, na.rm = T),
                                         late28 = sum(visitdiff > 28, na.rm = T),
                                         averagelateness = min(100, mean(visitdiff, na.rm = T)),
                                         late_rate = late / n_appts,
                                         late28_rate = late28 / n_appts)
                             
                             patient_lateness_two <- patient_tmp %>%
                               mutate(rownum = row_number()) %>%
                               filter(rownum <= 2) %>%
                               mutate(visitdiff = as.numeric(visitdiff)) %>%
                               mutate(visitdiff = ifelse(visitdiff < 0, 0, visitdiff)) %>%
                               group_by(key) %>%
                               summarize(late_last2 = sum(visitdiff > 0, na.rm = T),
                                         late28_last2 = sum(visitdiff > 28, na.rm = T),
                                         averagelateness_last2 = min(100, mean(visitdiff, na.rm = T)),
                                         latelast2_rate = late_last2 / n(),
                                         late28last2_rate = late28_last2 / n())  
                             
                             patient_lateness_five <- patient_tmp %>%
                               mutate(rownum = row_number()) %>%
                               filter(rownum <= 5) %>%
                               mutate(visitdiff = as.numeric(visitdiff)) %>%
                               mutate(visitdiff = ifelse(visitdiff < 0, 0, visitdiff)) %>%
                               group_by(key) %>%
                               summarize(late_last5 = sum(visitdiff > 0, na.rm = T),
                                         late28_last5 = sum(visitdiff > 28, na.rm = T),
                                         averagelateness_last5 = min(100, mean(visitdiff, na.rm = T)),
                                         latelast5_rate = late_last5 / n(),
                                         late28last5_rate = late28_last5 / n())
                             
                             patient_lateness_ten <- patient_tmp %>%
                               mutate(rownum = row_number()) %>%
                               filter(rownum <= 10) %>%
                               mutate(visitdiff = as.numeric(visitdiff)) %>%
                               mutate(visitdiff = ifelse(visitdiff < 0, 0, visitdiff)) %>%
                               group_by(key) %>%
                               summarize(late_last10 = sum(visitdiff > 0, na.rm = T),
                                         late28_last10 = sum(visitdiff > 28, na.rm = T),
                                         averagelateness_last10 = min(100, mean(visitdiff, na.rm = T)),
                                         latelast10_rate = late_last10 / n(),
                                         late28last10_rate = late28_last10 / n())
                             
                             
                             patient_tmp <- merge(patient_lateness_all, 
                                                  patient_lateness_five,
                                                  by = "key") %>%
                               merge(., patient_lateness_two, by = "key") %>%
                               merge(., patient_lateness_ten, by = "key") %>%
                               merge(., target_tmp, by = "key")
                             
                             patient_tmp
                             
                           }

outlist_other <- foreach(i = 1:length(patients),
        .packages = c("dplyr", "lubridate"),
        # .combine = "rbind",
        .errorhandling = 'remove')%dopar%{
# for(i in 1:length(patients)){
  
  patient <- allvisits[allvisits$key == patients[i], ]
  
  # Arrange chronologically
  patient <- patient %>%
    arrange(desc(VisitDate))

  # Get number of NADs since Jan 1
  num_targets <- nrow(filter(patient, VisitDate > "2021-01-01"))
  # num_targets <- nrow(patient) - 1
  
  if(num_targets == 0){next}
  
  target_lateness <- foreach(k = 1:num_targets,
                             .packages = c("dplyr", "lubridate"))%dopar%{

  # Arrange chronologically and select most recent 51 variables
  patient_tmp <- patient %>%
    ungroup() %>%
    arrange(desc(VisitDate)) %>%
    mutate(rownum = row_number()) %>%
    filter(rownum <= 26) %>%
    filter(rownum >= k)
  
    # # If only one visit and NAD is 
    if(nrow(patient_tmp)<=1){next}
    
    # Calculate the gap between the visit date and the previous NAD
    for(j in 1:(nrow(patient_tmp)-1)){
      patient_tmp[j, "visitdiff"] <- patient_tmp[j, "VisitDate"] - patient_tmp[j+1, "NextAppointmentDate"]
    }
    
    # Get target variable - if visitdiff > 30, then IIT, otherwise not IIT
    target_tmp <- data.frame(key = patient_tmp$key[1],
                             Target = ifelse(as.numeric(patient_tmp[1, "visitdiff"]) > 28, 1, 0),
                             PredictionDate = patient_tmp[2, "VisitDate"]$VisitDate,
                             MissingNAD = patient_tmp[2, "MissingExpectedReturn"]$MissingExpectedReturn,
                             stringsAsFactors = FALSE)
  
    
    # Drop the target variable (first row) & the earliest visit since cannot have been late (last row)
    patient_tmp <- patient_tmp[2:nrow(patient_tmp), ]
  
    
    # Now get how many times they were late
    patient_lateness_all <- patient_tmp %>%
      group_by(key) %>%
      mutate(visitdiff = as.numeric(visitdiff)) %>%
      mutate(visitdiff = ifelse(visitdiff < 0, 0,visitdiff)) %>%
      summarize(n_appts = n(),
                late = sum(visitdiff > 0, na.rm = T),
                late28 = sum(visitdiff > 28, na.rm = T),
                averagelateness = min(100, mean(visitdiff, na.rm = T)),
                late_rate = late / n_appts,
                late28_rate = late28 / n_appts)
      
    patient_lateness_two <- patient_tmp %>%
      mutate(rownum = row_number()) %>%
      filter(rownum <= 2) %>%
      mutate(visitdiff = as.numeric(visitdiff)) %>%
      mutate(visitdiff = ifelse(visitdiff < 0, 0, visitdiff)) %>%
      group_by(key) %>%
      summarize(late_last2 = sum(visitdiff > 0, na.rm = T),
                late28_last2 = sum(visitdiff > 28, na.rm = T),
                averagelateness_last2 = min(100, mean(visitdiff, na.rm = T)),
                latelast2_rate = late_last2 / n(),
                late28last2_rate = late28_last2 / n())  
    
      patient_lateness_five <- patient_tmp %>%
        mutate(rownum = row_number()) %>%
        filter(rownum <= 5) %>%
        mutate(visitdiff = as.numeric(visitdiff)) %>%
        mutate(visitdiff = ifelse(visitdiff < 0, 0, visitdiff)) %>%
        group_by(key) %>%
        summarize(late_last5 = sum(visitdiff > 0, na.rm = T),
                  late28_last5 = sum(visitdiff > 28, na.rm = T),
                  averagelateness_last5 = min(100, mean(visitdiff, na.rm = T)),
                  latelast5_rate = late_last5 / n(),
                  late28last5_rate = late28_last5 / n())

      patient_lateness_ten <- patient_tmp %>%
        mutate(rownum = row_number()) %>%
        filter(rownum <= 10) %>%
        mutate(visitdiff = as.numeric(visitdiff)) %>%
        mutate(visitdiff = ifelse(visitdiff < 0, 0, visitdiff)) %>%
        group_by(key) %>%
        summarize(late_last10 = sum(visitdiff > 0, na.rm = T),
                  late28_last10 = sum(visitdiff > 28, na.rm = T),
                  averagelateness_last10 = min(100, mean(visitdiff, na.rm = T)),
                  latelast10_rate = late_last10 / n(),
                  late28last10_rate = late28_last10 / n())

    
    patient_tmp <- merge(patient_lateness_all, 
                         patient_lateness_five,
                         by = "key") %>%
      merge(., patient_lateness_two, by = "key") %>%
      merge(., patient_lateness_ten, by = "key") %>%
      merge(., target_tmp, by = "key")
    
    patient_tmp
                             }
  }
outlist_first <- rbindlist(outlist_first, fill = T)
outlist <- unlist(outlist_other, recursive = FALSE)
target_lateness <- rbindlist(outlist, fill = T)
target_lateness <- bind_rows(target_lateness, outlist_first)
# Filter out any observations where the target variable was imputed
target_lateness <- target_lateness[target_lateness$MissingNAD == 0, ]
saveRDS(target_lateness, "target_latenessv2.rds")
print("lateness metrics calculated")

# 2. most recent pharmacy pickup features ---------------------

# target_lateness <- readRDS("target_lateness.rds")
# # Let's chunk this into five
# keys <- unique(target_lateness$key)
# keys_df <- data.frame(key = keys,
#                       group = c(rep(1:10, each = length(keys)/10), rep(10, 6)))

# target_lateness <- target_lateness[target_lateness$key %in% keys_df$key[keys_df$group %in% 6:10],  ]
# Read in original pharmacy data
# pharmacy <- s3read_using(FUN = readRDS, bucket = aws_bucket, object = "March2023Dump/pharmacy_samp.rds")
# pharmacy <- readRDS("pharmacy_samp250k.rds") %>%
#   filter(key %in% target_lateness$key)
pharmacy <- readRDS("pharmacy_samp250k.rds")
pharmacy$DispenseDate <- ymd(pharmacy$DispenseDate)
pharmacy$ExpectedReturn <- ymd(pharmacy$ExpectedReturn)


# some pharmacy pickups are recorded more than once. for each of this, keep the pickup with the 
# expected return date that is furthest in the future, and drop other records of this pickup
pharmacy <- pharmacy %>%
  group_by(key, DispenseDate) %>%
  arrange(desc(ExpectedReturn)) %>%
  mutate(rownum = row_number()) %>%
  filter(rownum == 1) %>%
  select(-rownum)


hiv <- pharmacy %>%
  filter(!TreatmentType %in% c("NULL", "Prophylaxis")) %>%
  ungroup()
other <- pharmacy %>%
  filter(TreatmentType %in% c("NULL", "Prophylaxis")) %>%
  ungroup()

# Loop through each record from target_lateness and get most recent regimen data
hiv_regimen_list <- list()
other_regimen_list <- list()

# start <- Sys.time()
hiv_regimen_list <- foreach(i = 1:nrow(target_lateness),
        .packages = c("dplyr", "lubridate"))%dopar%{
# for(i in 1:nrow(target_lateness)){
  
  tmp <- target_lateness[i, ]
  
  # Join on prediction dates and limit to pharmacy pickups at or before the prediction date,
  # to avoid leakage
  hiv_tmp <- hiv[hiv$key == tmp$key, ]
  hiv_tmp <- hiv_tmp[hiv_tmp$DispenseDate <= tmp$PredictionDate, ]
  hiv_tmp <- hiv_tmp[as.numeric(tmp$PredictionDate - hiv_tmp$DispenseDate) < 400, ]
  hiv_tmp <- hiv_tmp[hiv_tmp$Drug != "NULL", ]
  hiv_tmp <- hiv_tmp[!is.na(hiv_tmp$Drug), ]
  
  # Now, get the number of HIV regimens per patient
  hiv_tmp <- hiv_tmp %>%
    group_by(key) %>%
    mutate(num_hiv_regimens = n_distinct(Drug)) %>%
    arrange(desc(DispenseDate)) %>%
    mutate(rownum = row_number()) %>%
    filter(rownum == 1) %>%
    select(-rownum) %>%
    mutate(OptimizedHIVRegimen = ifelse(grepl("DTG", Drug), "Yes", "No")) %>%
    ungroup()
  
  # Group ART Treatment Types
  hiv_tmp$TreatmentType[hiv_tmp$TreatmentType %in% c("ART", "ARV", "HIV Treatment")] <- "ART"
  
  hiv_tmp <- hiv_tmp %>%
    select(num_hiv_regimens,
           TreatmentType,
           OptimizedHIVRegimen) %>%
    mutate(key = tmp$key,
           PredictionDate = tmp$PredictionDate)
  
  hiv_tmp
  
        }

other_regimen_list <- foreach(i = 1:nrow(target_lateness),
                              .packages = c("dplyr", "lubridate"))%dopar%{
                              # for(i in 1:nrow(target_lateness)){
                              
                              tmp <- target_lateness[i, ]
                                
                              # Join on prediction dates and limit to pharmacy pickups at or before the prediction date,
                              # to avoid leakage
                              other_tmp <- other[other$key == tmp$key, ]
                              other_tmp <- other_tmp[other_tmp$DispenseDate <= tmp$PredictionDate, ]
                              other_tmp <- other_tmp[as.numeric(tmp$PredictionDate - other_tmp$DispenseDate) < 400, ]
                              other_tmp <- other_tmp[other_tmp$Drug != "NULL", ]
                              other_tmp <- other_tmp[!is.na(other_tmp$Drug), ]
                              
                              other_tmp <- data.frame(Other_Regimen = ifelse(nrow(other_tmp) > 0, "Yes",
                                                                             ifelse(tmp$key %in% pharmacy$key, "No", NA)),
                                                      key = tmp$key,
                                                      PredictionDate = tmp$PredictionDate)
                              
                              other_tmp
                              
                            }
# end <- Sys.time()
# print(start - end)

hiv_regimen <- data.table::rbindlist(hiv_regimen_list)
other_regimen <- data.table::rbindlist(other_regimen_list)
tlr <- merge(target_lateness, hiv_regimen, by = c("key", "PredictionDate"), all.x = TRUE) %>%
  merge(., other_regimen, by = c("key", "PredictionDate"), all.x = TRUE)

print("pharmacy metrics calculated")

# 3. most recent visit pickup features ------------------------
# visits <- readRDS("visits_samp250k.rds")
# dem <- readRDS("patients_samp250k.rds")
# dem <- dem[dem$key %in% target_lateness$key, ]
# dem$DOB <- ymd(substr(dem$DOB, 1, 10))
# dem <- dem[!duplicated(dem$key),]
visits <- visits[visits$key %in% target_lateness$key, ]
visits$NextAppointmentDate <- ymd(visits$NextAppointmentDate)
visits$VisitDate <- ymd(visits$VisitDate)

# Visits are all between 2019 and present so nothing drops
visits <- visits[visits$VisitDate <= "2023-03-23", ]

# Drop entries where next appointment date is missing (inlcuding where next appointment is before January 1, 2019
# and beyond april 2024)
range(visits$NextAppointmentDate, na.rm = TRUE)
visits <- visits[!is.na(visits$NextAppointmentDate), ]
visits <- visits[visits$NextAppointmentDate >= "2019-01-01", ]
visits <- visits[visits$NextAppointmentDate <= "2024-04-01", ]

# 2,325,966 remain
# Drop entries where NAD is before Visit Date or more than 365 days in the future
visits$gap <- visits$NextAppointmentDate - visits$VisitDate
visits <- visits[visits$gap > 0, ]
visits <- visits[visits$gap < 400, ]
visits <- visits %>% select(-gap)
# 2, 315,816 remain

# some visits are recorded more than once. for each of this, keep the visits with the 
# next appointment date that is furthest in the future, and drop other records of this visit
visits <- visits %>%
  group_by(key, VisitDate) %>%
  arrange(desc(NextAppointmentDate)) %>%
  mutate(rownum = row_number()) %>%
  filter(rownum == 1) %>%
  ungroup()

# start <- Sys.time()
visit_features <- foreach(i = 1:nrow(target_lateness),
        .packages = c("dplyr", "lubridate", "tidyr"),
        .errorhandling = "remove")%dopar%{
# for(i in 1:nrow(target_lateness)){
  
  # if(i%%1000==0){print(i)}
  
  tmp <- target_lateness[i, ]
  
  # Join on prediction dates and limit to pharmacy pickups at or before the prediction date,
  # to avoid leakage
  visits_tmp <- visits[visits$key == tmp$key, ]

  # Join on prediction dates and limit visits to those that took place before or on the prediction date
  visits_tmp$VisitDate <- ymd(visits_tmp$VisitDate)
  visits_tmp <- visits_tmp %>%
    filter(VisitDate <= tmp$PredictionDate)
  
  # Let's get most recent 5 visits and count how many are unscheduled
  visits_last5 <- visits_tmp %>%
    arrange(desc(VisitDate)) %>%
    mutate(rownum = row_number()) %>%
    filter(rownum <= 5)
  
  visits_last5$visitType <- tolower(visits_last5$VisitType)
  visits_last5$unscheduled <- ifelse(grepl("unscheduled", visits_last5$visitType), 1, 0)
  visits_last5$mmd<-ifelse(visits_last5$NextAppointmentDate -visits_last5$VisitDate >=84, 1, 0)
  visits_last5 <- visits_last5 %>%
    group_by(key) %>%
    summarize(n_visits_lastfive = n(),
              n_unscheduled_lastfive = sum(unscheduled),
              unscheduled_rate = n_unscheduled_lastfive / n_visits_lastfive,
              n_mmd_lastfive = sum(mmd),
              mmd_rate = n_mmd_lastfive / n_visits_lastfive,
              PredictionDate = tmp$PredictionDate)
  
  # Order visits in reverse chronological order for each patient
  visits_recent <- visits_tmp %>%
    arrange(desc(VisitDate)) %>%
    mutate(rownum = row_number()) %>%
    filter(rownum == 1)
  
  # Calculate BMI for each patient and take most recent
  # Check height and weight for additional cleaning
  
  visits_bmi <- visits_tmp %>%
    merge(., dem[, c("key", "DOB")], by = "key") %>%
    mutate(Age = as.numeric((tmp$PredictionDate - DOB) / 365)) %>%
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
    select(key, BMI, Weight) 
  
  # Get BMI from six months ago
  visits_bmi$timediff <- tmp$PredictionDate - visits_bmi$VisitDate
  visits_bmi$difffrom60 <- abs(60 -visits_bmi$timediff)
  bmi_sixmonths <- visits_bmi %>%
    group_by(key) %>%
    arrange(difffrom60) %>%
    mutate(rownum = row_number()) %>%
    filter(rownum == 1) %>%
    select(key, BMI, Weight)
  
  # Join, and calculate percent change in BMI
  bmi <- merge(bmi_recent, bmi_sixmonths, by = "key", all.x = T)
  bmi$changeInBMI <- (bmi$BMI.x - bmi$BMI.y) / bmi$BMI.y
  bmi$changeInWeight <- (as.numeric(bmi$Weight.x) - as.numeric(bmi$Weight.y)) / as.numeric(bmi$Weight.y)
  bmi$BMI <- bmi$BMI.x
  bmi$Weight <- as.numeric(bmi$Weight.x)
  
  # Keep patients' most recent BMI and change in BMI
  bmi <- bmi %>%
    select(key, BMI, changeInBMI, Weight, changeInWeight) %>%
    mutate(PredictionDate =tmp$PredictionDate)
  
  # Pregnant 
  # Join with demographics data and we see this requires some cleaning (men and older/younger women show yes)
  # So, join, and then fix men, women under 10 or over 49 as not pregnant
  pregnant <- visits_recent %>%
    select(key, Pregnant, Breastfeeding) %>%
    mutate(Pregnant = tolower(Pregnant),
           Breastfeeding = tolower(Breastfeeding)) %>%
    mutate(Pregnant = ifelse(grepl("yes", Pregnant), "yes", "no"),
           Breastfeeding = ifelse(grepl("yes", Breastfeeding), "yes", "no")) %>%
    merge(., dem[, c("key", "Gender", "DOB")], by = "key") %>%
    mutate(Age = as.numeric((tmp$PredictionDate - DOB) / 365)) %>%
    unique()
  pregnant <- pregnant %>%
    mutate(Pregnant = case_when(
      Gender %in% c("Male", "M") | !between(pregnant$Age, 10, 49) ~ "NR",
      .default = Pregnant
    )) %>%
    mutate(Breastfeeding = case_when(
      Gender %in% c("Male", "M") | !between(pregnant$Age, 10, 49) ~ "NR",
      .default = Breastfeeding
    )) %>%
    select(key, Pregnant, Breastfeeding) %>%
    mutate(PredictionDate = tmp$PredictionDate)
  
  # Differentiated Care
  diff_care <- visits_tmp %>%
    ungroup() %>%
    filter(!DifferentiatedCare %in% c("NULL", "")) %>%
    mutate(recent = ifelse(tmp$PredictionDate - VisitDate < 400, 1, 0)) %>%
    filter(recent == 1) %>%
    arrange(desc(VisitDate)) %>%
    mutate(rownum = row_number()) %>%
    filter(rownum == 1) %>%
    select(-rownum)
  diff_care$DifferentiatedCare <- tolower(diff_care$DifferentiatedCare)
  diff_care$DifferentiatedCare <- gsub("[^[:alnum:][:space:]]","",diff_care$DifferentiatedCare)
  diff_care$DifferentiatedCare <- gsub(" ", "", diff_care$DifferentiatedCare)
  diff_care <- diff_care %>%
    select(key, DifferentiatedCare) %>%
    mutate(PredictionDate = tmp$PredictionDate)
  
  # Adherence
  # There are up to two adherences (for ART and CTX) recorded for each patient
  # These are recorded in the same cell, so next lines parse these into new variables
  # Adherence and Adherence Category, when there are two, are always separated by a |,
  # so we are splitting on that symbol
  
  ctx_adherence <- visits_tmp[, c("key", "VisitDate", "CTXAdherence")] %>%
    mutate(CTXAdherence = tolower(CTXAdherence)) %>%
    mutate(CTXAdherence = case_when(
      grepl("good", CTXAdherence) ~ "good",
      grepl("fair", CTXAdherence) ~ "fair",
      grepl("bad", CTXAdherence) ~ "bad",
      .default = NA
    ))
  
  ctx_adherence_recent <- ctx_adherence %>%
    ungroup() %>%
    arrange(desc(VisitDate)) %>%
    filter(!is.na(CTXAdherence)) %>%
    mutate(rownum = row_number()) %>%
    filter(rownum == 1) %>%
    rename(most_recent_ctx_adherence = CTXAdherence) %>%
    select(key, most_recent_ctx_adherence) %>%
    mutate(PredictionDate = tmp$PredictionDate) 
  
  ctx_adherence_recent5 <- ctx_adherence %>%
    ungroup() %>%
    arrange(desc(VisitDate)) %>%
    mutate(rownum = row_number()) %>%
    filter(rownum <= 5) %>%
    filter(!is.na(CTXAdherence)) %>%
    group_by(key) %>%
    summarize(num_adherence_CTX = n(),
              num_bad_CTX = sum(CTXAdherence == "bad"),
              num_fair_CTX = sum(CTXAdherence == "fair"),
              ctx_bad_adherence_rate = num_bad_CTX / num_adherence_CTX)%>%
    mutate(PredictionDate = tmp$PredictionDate)
  
  art_adherence <- visits_tmp[, c("key", "VisitDate", "Adherence")] %>%
    mutate(ARTAdherence = tolower(gsub("\\|.*", "", Adherence))) %>%
    mutate(ARTAdherence = case_when(
      grepl("good", ARTAdherence) ~ "good",
      grepl("fair", ARTAdherence) ~ "fair",
      grepl("poor", ARTAdherence) ~ "poor",
      .default = NA
    ))

  art_adherence_recent <- art_adherence %>%
    ungroup() %>%
    arrange(desc(VisitDate)) %>%
    filter(!is.na(ARTAdherence)) %>%
    mutate(rownum = row_number()) %>%
    filter(rownum <= 1) %>%
    rename(most_recent_art_adherence = ARTAdherence) %>%
    select("key", "most_recent_art_adherence")%>%
    mutate(PredictionDate = tmp$PredictionDate)
  
  art_adherence_recent5 <- art_adherence %>%
    ungroup() %>%
    arrange(desc(VisitDate)) %>%
    mutate(rownum = row_number()) %>%
    filter(rownum <= 5) %>%
    filter(!is.na(ARTAdherence)) %>%
    group_by(key) %>%
    summarize(num_adherence_ART = n(),
              num_poor_ART = sum(ARTAdherence == "poor"),
              num_fair_ART = sum(ARTAdherence == "fair"),
              art_poor_adherence_rate = num_poor_ART / num_adherence_ART) %>%
    mutate(PredictionDate = tmp$PredictionDate)

  
  # Stability
  stab <- visits_tmp %>%
    filter(!StabilityAssessment %in% c("NULL", "")) %>%
    mutate(recent = ifelse(tmp$PredictionDate - VisitDate < 400, 1, 0)) %>%
    filter(recent == 1) %>%
    group_by(key) %>%
    arrange(desc(VisitDate)) %>%
    mutate(rownum = row_number()) %>%
    filter(rownum == 1) %>%
    select(-rownum) %>%
    select(key, StabilityAssessment) %>%
    mutate(PredictionDate = tmp$PredictionDate)
  
  # WHO Stage - if 3 or 4, then AHD
  who <- data.frame(key = tmp$key,
                    PredictionDate = tmp$PredictionDate,
                    AHD_who = "No")
  
  if(3 %in% visits_tmp$WHOStage | 4 %in% visits_tmp$WHOStage){
    who$AHD_who <- "Yes"
  }
  
  # Combine all features 
  visits_out <- merge(visits_last5, bmi, by = c("key", "PredictionDate"), all = TRUE) %>%
    merge(., pregnant, by = c("key", "PredictionDate"), all = TRUE) %>%
    merge(., diff_care, by = c("key", "PredictionDate"), all = TRUE) %>%
    merge(., ctx_adherence_recent, by = c("key", "PredictionDate"), all = TRUE) %>%
    merge(., ctx_adherence_recent5, by = c("key", "PredictionDate"), all = TRUE) %>%
    merge(., art_adherence_recent, by = c("key", "PredictionDate"), all = TRUE) %>%
    merge(., art_adherence_recent5, by = c("key", "PredictionDate"), all = TRUE) %>%
    merge(., who, by = c("key", "PredictionDate"), all = TRUE) %>%
    merge(., stab, by = c("key", "PredictionDate"), all = TRUE) 
  
  visits_out
  
}

# end <- Sys.time()
# print(end - start)

visits_df <- rbindlist(visit_features, fill = TRUE)
tlrv <- merge(tlr, visits_df, by = c("key", "PredictionDate"), all.x = TRUE) 

print("visits metrics calculated")

# 4. features for lab (one row for patient) ------------------
# how many viral loads were taken
# how many viral loads were high
# binary variable for if most recent viral load was high
# binary variable for if there ever was a high viral load
# labs <- readRDS("labs_samp250k.rds")
# labs <- labs[labs$key %in% target_lateness$key, ]
labs_dedup <- labs %>%
  select(key, OrderedbyDate, TestName, TestResult, ReportedbyDate) %>%
  unique() %>%
  mutate(ReportedbyDate = ymd(ReportedbyDate),
         OrderedbyDate = ymd(OrderedbyDate)) %>%
  filter(ReportedbyDate >= OrderedbyDate) %>%
  group_by(key, OrderedbyDate) %>%
  arrange(desc(ReportedbyDate)) %>%
  mutate(rownum = row_number()) %>%
  filter(rownum == 1) %>%
  select(-rownum) %>%
  ungroup()

# Filter to VL and CD4
labs2 <- labs_dedup %>%
  mutate(TestName = tolower(TestName)) %>%
  mutate(TestName = case_when(
    grepl("cd4", TestName) & !grepl("%|percent", TestName) ~ "CD4",
    grepl("viral|vl", TestName) ~ "VL",
    .default = "Other"
  )) %>%
  filter(TestName %in% c("CD4", "VL"))

# Categorize VL
vl_nums <- labs2 %>%
  filter(TestName == "VL") %>%
  mutate(LabResult = as.numeric(TestResult),
         LabNum = ifelse(is.na(LabResult), 0, 1)) %>%
  filter(LabNum == 1) %>%
  mutate(VL = ifelse(LabResult < 200, "suppressed", "unsuppressed"))

vl_char <- labs2 %>%
  filter(TestName == "VL") %>%
  mutate(LabResult = as.numeric(TestResult),
         LabNum = ifelse(is.na(LabResult), 0, 1)) %>%
  filter(LabNum == 0) %>%
  mutate(TestResult = tolower(TestResult)) %>%
  filter(grepl("hvl|lvl|detect", TestResult)) %>%
  mutate(VL = ifelse(TestResult == "hvl", "unsuppressed", "suppressed"))

vl <- bind_rows(vl_nums, vl_char)

cd4 <- labs2 %>%
  filter(TestName == "CD4") %>%
  mutate(TestResult = as.numeric(TestResult)) %>%
  mutate(AHD = ifelse(TestResult < 200, "Yes", "No"))

# start <- Sys.time()
lab_list <- foreach(i = 1:nrow(tlrv),
                   .packages = c("dplyr", "lubridate"),
                   .errorhandling = "remove")%dopar%{
# for(i in 1:nrow(target_lateness)){
  
  tmp <- tlrv[i, ]

    # Need to join lab and get most recent touchpoint and filter to labs prior to that date
    lab_tmp <- vl[vl$key == tmp$key, ]
    lab_tmp <- lab_tmp %>%
      filter(OrderedbyDate <= tmp$PredictionDate)
    
    if(nrow(lab_tmp)==0){next}
    
    lab_patient <- lab_tmp %>%
      group_by(key) %>%
      summarize(n_tests_all = n(),
                n_hvl_all = sum(VL == "unsuppressed"),
                total_hvl_rate = n_hvl_all / n_tests_all)
    
    # Lab – look at n_tests and n_hvl for most recent three years
    lab_lastthreeyears <- lab_tmp %>%
      filter(tmp$PredictionDate - ReportedbyDate < 1000) %>%
      group_by(key) %>%
      summarize(n_tests_threeyears = n(),
                n_hvl_threeyears = sum(VL == "unsuppressed"),
                n_lvl_threeyears = sum(VL == "suppressed"),
                recent_hvl_rate = n_hvl_threeyears / n_tests_threeyears)
    
    lab_recent <- lab_tmp %>%
      group_by(key) %>%
      arrange(desc(ReportedbyDate)) %>%
      mutate(rownum = row_number()) %>%
      filter(rownum == 1) %>%
      rename("most_recent_vl" = VL) %>%
      select(key, most_recent_vl)
    
    # Duration between tho viralload
    
    lab_duration <- lab_tmp %>%
      group_by(key) %>%
      arrange(desc(OrderedbyDate)) %>%
      mutate(duration= as.numeric(lag(OrderedbyDate)-OrderedbyDate ))%>%
      mutate(above12= ifelse(duration>400, 1,0))
    
    duration_btwn_twolabs<-lab_duration%>%
      group_by(key) %>%
      summarize(n_duration_above12 =sum(above12, na.rm = TRUE))
    
    # Two unsuppressed viral load (Treatment failure)
    # identify groups with two consecutive HVL values
    consecutive_HVL <- lab_tmp %>%
      group_by(key)%>%
      arrange(OrderedbyDate)%>%
      mutate(count = n()) %>%
      mutate(treatment_failure = ifelse(VL == "unsuppressed"& lag(VL) == "unsuppressed", 1,0))
    
    
    #retaining distinct entries among those who experienced treatment failuresupp
    n_consecutive_HVL <- consecutive_HVL %>%
      group_by(key) %>%
      summarize(n_consecutive_HVL=sum(treatment_failure)) 
    
    
    # now cd4 to get AHD
    cd4_tmp <- cd4[cd4$key == tmp$key, ]
    cd4_tmp <- cd4_tmp %>%
      filter(OrderedbyDate <= tmp$PredictionDate) %>%
      ungroup() %>%
      arrange(desc(OrderedbyDate)) %>%
      mutate(rownum = row_number()) %>%
      filter(rownum <= 5) 
    
    cddf <- data.frame(key = tmp$key,
                       AHD = "No")
    
    if(any(cd4_tmp$AHD == "Yes")){
      cddf$AHD = "Yes"
    }
    
    
    lab_out <- merge(lab_patient, lab_recent, by = "key", all = TRUE) %>%
      merge(., lab_lastthreeyears, by = "key", all = TRUE) %>%
      merge(., duration_btwn_twolabs, by = "key", all = TRUE) %>%
      merge(., n_consecutive_HVL, by = "key", all = TRUE) %>%
      merge(., cddf, by = "key", all = TRUE) %>%
      mutate(PredictionDate = tmp$PredictionDate)

    lab_out
}

# end <- Sys.time()
# print(end - start)

lab_df <- rbindlist(lab_list, fill = TRUE)
tlrvl <- merge(tlrv, lab_df, by = c("key", "PredictionDate"), all.x = TRUE) 

print("lab metrics calculated")

# 5. features for dem (one row for patient) ---------------
# basically just cleaning this up
# filter art to id's in dem and join on dem
# Filter to PatientID from dem
# Time to start ART - this will StartARTDate - DateConfirmedHIVPositive
dem$StartARTDate <- ymd(dem$StartARTDate)
dem$StartARTDate[dem$StartARTDate < "1985-01-01"] <- NA

# Time on ART - this will be second most recent visit - ART Start Date (need to join on that)
dem <- merge(dem, tlrvl, by = "key")
dem$timeOnArt <- as.numeric(floor((dem$PredictionDate - dem$StartARTDate) / 30))
dem$AgeatTest <- as.numeric(floor((dem$PredictionDate - dem$DOB)/365))
dem$Age[dem$Age > 100] <- NA
dem$Age[dem$age < 0] <- NA
dem$timeOnArt[dem$timeOnArt < 0] <- NA

# Patient Source
dem$PatientSource <- tolower(dem$PatientSource)
table(dem$PatientSource, useNA = "always")
dem <- dem %>%
  mutate(PatientSource = case_when(
    grepl("vct", PatientSource) ~ "VCT",
    grepl("opd|transfer", PatientSource) ~ "OPD",
    grepl("mch", PatientSource) ~ "MCH",
    grepl("vmmc", PatientSource) ~ "VMMC",
    grepl("ccc", PatientSource) ~ "CCC",
    grepl("tb", PatientSource) ~ "TBClinic",
    grepl("cwc", PatientSource) ~ "CWC",
    grepl("ipd", PatientSource) ~ "IPD",
    .default = "Other"
  ))
rare_cats <- names(which(prop.table(table(dem$PatientSource)) < .01))
dem$PatientSource[dem$PatientSource %in% rare_cats] <- "Other"

# Marital Status
table(dem$MaritalStatus, useNA = "always")
dem$MaritalStatus <- tolower(dem$MaritalStatus)
dem <- dem %>%
  mutate(MaritalStatus = case_when(
    Age <= 15 | grepl("never|single",MaritalStatus) ~ "Single",
    grepl("monogamous|cohabit|living|married", MaritalStatus) ~ "Married",
    grepl("divorced|separated", MaritalStatus) ~ "Divorced",
    grepl("widow", MaritalStatus) ~ "Widow",
    grepl("poly", MaritalStatus) ~ "Polygamous",
    .default = "Other"
  ))


# Population Type
dem$PopulationType <- tolower(dem$PopulationType)
dem <- dem %>%
  mutate(PopulationType = case_when(
    grepl("key", PopulationType) ~ "KP",
    .default = "GP"
  ))

dem <- dem %>%
  mutate(AHD = ifelse(AHD == "Yes" | AHD_who == "Yes", "Yes", "No"))



dem <- dem %>%
  select("key", "Gender", "PatientSource", "MaritalStatus", "PopulationType", "PredictionDate", "AgeatTest", "Target",
         "late", "late28", "averagelateness", "late_rate","late28_rate", "late_last5", "late28_last5", "averagelateness_last5", "late_last2", "late28_last2",
         "averagelateness_last2", "late_last10","late28_last10", "averagelateness_last10",  "num_hiv_regimens", "mmd_rate",
         "latelast5_rate", "late28last5_rate", "latelast2_rate", "late28last2_rate", "latelast10_rate", "late28last10_rate",
         "OptimizedHIVRegimen", "Other_Regimen", "n_visits_lastfive", "n_unscheduled_lastfive","n_mmd_lastfive", "BMI",
         "changeInBMI", "Weight", "changeInWeight","Pregnant", "DifferentiatedCare", "num_adherence_ART", "num_adherence_CTX",
         "art_poor_adherence_rate",  "ctx_bad_adherence_rate", "SiteCode", "AHD", "Breastfeeding",
         "num_poor_ART", "num_bad_CTX", "num_fair_ART", "num_fair_CTX", "StabilityAssessment", "n_tests_all", "n_hvl_all",
         "most_recent_art_adherence",  "most_recent_ctx_adherence", "most_recent_vl", "n_tests_threeyears","n_hvl_threeyears",
         "n_lvl_threeyears", "recent_hvl_rate", "total_hvl_rate", "unscheduled_rate", "n_duration_above12", "n_consecutive_HVL", "timeOnArt")

saveRDS(dem, "iit_samp_250k_v2.rds")
# s3write_using(dem, FUN = saveRDS, bucket = aws_bucket, object = "March2023Dump/iit_samp.rds")

print("end")