# First, load necessary packages
# library(aws.s3)
library(dplyr)
library(lubridate)
library(foreach)
library(doParallel)
library(data.table)
library(tidyr)
numCores <- detectCores()
registerDoParallel(numCores)


print("starting")

dem_all <- readRDS("MarchData/patients_samp250k.rds")
pharmacy_all <- readRDS("MarchData/pharmacy_samp250k.rds")
visits_all <- readRDS("MarchData/visits_samp250k.rds")
labs_all <- readRDS("MarchData/labs_samp250k.rds")

keys <- unique(dem_all$key)
cuts <- seq(1, length(keys), length.out = 200)

for (a in 1:length(cuts)) {

  dem <- dem_all[dem_all$key %in% keys[cuts[a]:cuts[a + 1]],]
  pharmacy <-
    pharmacy_all[pharmacy_all$key %in% keys[cuts[a]:cuts[a + 1]],]
  phar <- pharmacy
  visits <-
    visits_all[visits_all$key %in% keys[cuts[a]:cuts[a + 1]],]
  labs <- labs_all[labs_all$key %in% keys[cuts[a]:cuts[a + 1]],]
  
  #
  # set.seed(2231)
  # samp <- sample(dem$key, 10000, replace = FALSE)
  # dem10000 <- dem[dem$key %in% samp, ]
  # pharmacy10000 <- pharmacy[pharmacy$key %in% samp, ]
  # visits10000 <- visits[visits$key %in% samp, ]
  # labs10000 <- labs[labs$key %in% samp, ]
  # saveRDS(dem10000, "dem10000.rds")
  # saveRDS(pharmacy10000, "pharmacy10000.rds")
  # saveRDS(visits10000, "visits10000.rds")
# saveRDS(labs10000, "labs10000.rds")

# dem <- readRDS("dem10000.rds")
# pharmacy <- readRDS("pharmacy10000.rds")
# visits <- readRDS("visits10000.rds")
# labs <- readRDS("labs10000.rds")

# dem <- s3read_using(FUN = readRDS, bucket = aws_bucket, object = "March2023Dump/patients_samp.rds")
# pharmacy <- s3read_using(FUN = readRDS, bucket = aws_bucket, object = "March2023Dump/pharmacy_samp.rds")
# visits <- s3read_using(FUN = readRDS, bucket = aws_bucket, object = "March2023Dump/visits_samp.rds")
# labs <- s3read_using(FUN = readRDS, bucket = aws_bucket, object = "March2023Dump/labs_samp.rds")


# Create Age variable for dem ----------------------
dem$DOB <- ymd(substr(dem$DOB, 1, 10))
dem <- dem[!duplicated(dem$key), ]

# 1. features for prior missingness pharmacy and visits (one row for appointment) ----
# integrate pharmacy and visits and pivot wide (one integrated time series)

# first, let's process visits data
# keep only variables needed to assess missingness (patient id, visit date, and next appointment dates)
visits_missing <-
  visits %>% dplyr::select(key, VisitDate, NextAppointmentDate)
# convert date variables to date types
visits_missing$NextAppointmentDate <-
  ymd(visits_missing$NextAppointmentDate)
visits_missing$VisitDate <- ymd(visits_missing$VisitDate)

# Visits are all between 2019 and present so nothing drops
range(visits_missing$VisitDate)
visits_missing <-
  visits_missing[visits_missing$VisitDate <= "2023-03-23",]

# Drop entries where next appointment date is missing (inlcuding where next appointment is before January 1, 2019
# and beyond april 2024)
range(visits_missing$NextAppointmentDate, na.rm = TRUE)
visits_missing <-
  visits_missing[!is.na(visits_missing$NextAppointmentDate),]
visits_missing <-
  visits_missing[visits_missing$NextAppointmentDate >= "2019-01-01",]
visits_missing <-
  visits_missing[visits_missing$NextAppointmentDate <= "2024-04-01",]

# 2,325,966 remain
# Drop entries where NAD is before Visit Date or more than 365 days in the future
visits_missing$gap <-
  visits_missing$NextAppointmentDate - visits_missing$VisitDate
visits_missing <- visits_missing[visits_missing$gap > 0,]
visits_missing <- visits_missing[visits_missing$gap < 400,]
visits_missing <- visits_missing %>% dplyr::select(-gap)
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
pharmacy$ExpectedReturn[pharmacy$ExpectedReturn < "2019-01-01"] <-
  NA
pharmacy$ExpectedReturn[pharmacy$ExpectedReturn >= "2024-04-01"] <-
  NA
pharmacy$ExpectedReturn[pharmacy$ExpectedReturn - pharmacy$DispenseDate < 0] <-
  NA
pharmacy$ExpectedReturn[pharmacy$ExpectedReturn - pharmacy$DispenseDate > 365] <-
  NA

# some pharmacy pickups are recorded more than once. for each of this, keep the pickup with the
# expected return date that is furthest in the future, and drop other records of this pickup
pharmacy <- pharmacy %>%
  group_by(key, DispenseDate) %>%
  arrange(desc(ExpectedReturn)) %>%
  mutate(rownum = row_number()) %>%
  filter(rownum == 1) %>%
  select(-rownum)

# Looping is too complicated to deploy - instead, if an expectedreturn date is NA,
# then impute as 30 days following the dispense date

pharmacy_complete <- pharmacy %>%
  mutate(MissingExpectedReturn = ifelse(is.na(ExpectedReturn), 1, 0)) %>%
  mutate(ExpectedReturn = if_else(is.na(ExpectedReturn), DispenseDate + 30, ExpectedReturn))

# in order to create a single integrated longitudinal record for each patient, we must combine
# visit and pharmacy pickup data. To facilitate this, rename variables in pharmacy dataframe to
# match names of variables in visits dataframe, and then rowbind them
pharmacy_complete <- pharmacy_complete %>%
  rename("VisitDate" = DispenseDate,
         "NextAppointmentDate" = ExpectedReturn)
# Now, rowbind them
allvisits <- bind_rows(visits_missing, pharmacy_complete)
allvisits$MissingExpectedReturn <-
  ifelse(is.na(allvisits$MissingExpectedReturn),
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

outlist_first <- foreach(
  i = 1:length(patients),
  .packages = c("dplyr", "lubridate", "tidyr"),
  # .combine = "rbind",
  .errorhandling = 'remove'
) %dopar% {
  patient <- allvisits[allvisits$key == patients[i],]
  
  # Arrange chronologically
  patient <- patient %>%
    arrange(desc(VisitDate))
  
  if (patient[1, "NextAppointmentDate"]$NextAppointmentDate > "2023-01-31") {
    next
  }
  if (patient[1, "VisitDate"]$VisitDate < "2021-01-01") {
    next
  }
  
  # Arrange chronologically and select most recent 51 variables
  patient_tmp <- patient %>%
    ungroup() %>%
    arrange(desc(VisitDate)) %>%
    mutate(rownum = row_number()) %>%
    filter(rownum <= 26)
  
  patient_tca <- patient_tmp %>%
    filter(rownum == 1) %>%
    mutate(NextAppointmentDate = as.numeric(NextAppointmentDate - VisitDate)) %>%
    select(key, NextAppointmentDate)
  
  # Calculate the gap between the visit date and the previous NAD
  for (j in 1:(nrow(patient_tmp) - 1)) {
    patient_tmp[j, "visitdiff"] <-
      patient_tmp[j, "VisitDate"] - patient_tmp[j + 1, "NextAppointmentDate"]
  }
  
  
  # if next appointment date of the most recent visit is before janaury 31st, then include it as a IIT
  target_tmp <-
    data.frame(
      key = patient_tmp$key[1],
      Target = 1,
      PredictionDate = patient_tmp[1, "VisitDate"]$VisitDate,
      MissingNAD = patient_tmp[1, "MissingExpectedReturn"]$MissingExpectedReturn,
      stringsAsFactors = FALSE
    )
  
  # generate features
  # Drop the target variable (first row) & the earliest visit since cannot have been late (last row)
  patient_tmp <-
    patient_tmp[2:nrow(patient_tmp),]
  
  patient_tca_last5 <- patient_tmp %>%
    filter(rownum <= 5) %>%
    mutate(NextAppointmentDate = as.numeric(NextAppointmentDate - VisitDate)) %>%
    group_by(key) %>%
    summarize(average_tca_last5 = mean(NextAppointmentDate))
  
  # Now get how many times they were late
  patient_lateness_all <- patient_tmp %>%
    group_by(key) %>%
    mutate(visitdiff = as.numeric(visitdiff)) %>%
    mutate(visitdiff = ifelse(visitdiff < 0, 0, visitdiff)) %>%
    summarize(
      n_appts = n(),
      late = sum(visitdiff > 0, na.rm = T),
      late28 = sum(visitdiff > 28, na.rm = T),
      averagelateness = min(100, mean(visitdiff, na.rm = T)),
      late_rate = late / n_appts,
      late28_rate = late28 / n_appts
    )
  
  patient_lateness_three <- patient_tmp %>%
    mutate(rownum = row_number()) %>%
    filter(rownum <= 3) %>%
    mutate(visitdiff = as.numeric(visitdiff)) %>%
    mutate(visitdiff = ifelse(visitdiff < 0, 0, visitdiff)) %>%
    group_by(key) %>%
    summarize(
      late_last3 = sum(visitdiff > 0, na.rm = T),
      averagelateness_last3 = min(100, mean(visitdiff, na.rm = T))
    )
  
  patient_lateness_five <- patient_tmp %>%
    mutate(rownum = row_number()) %>%
    filter(rownum <= 5) %>%
    mutate(visitdiff = as.numeric(visitdiff)) %>%
    mutate(visitdiff = ifelse(visitdiff < 0, 0, visitdiff)) %>%
    group_by(key) %>%
    summarize(
      late_last5 = sum(visitdiff > 0, na.rm = T),
      averagelateness_last5 = min(100, mean(visitdiff, na.rm = T))
    )
  
  patient_lateness_ten <- patient_tmp %>%
    mutate(rownum = row_number()) %>%
    filter(rownum <= 10) %>%
    mutate(visitdiff = as.numeric(visitdiff)) %>%
    mutate(visitdiff = ifelse(visitdiff < 0, 0, visitdiff)) %>%
    group_by(key) %>%
    summarize(
      late_last10 = sum(visitdiff > 0, na.rm = T),
      averagelateness_last10 = min(100, mean(visitdiff, na.rm = T))
    )
  
  patient_last5 <- patient_tmp %>%
    mutate(rownum = row_number()) %>%
    filter(rownum <= 5) %>%
    mutate(visitdiff = as.numeric(visitdiff)) %>%
    mutate(visitdiff = ifelse(visitdiff >= 0, visitdiff, 0)) %>%
    pivot_wider(
      id_cols = key,
      names_from = rownum,
      values_from = visitdiff,
      names_prefix = "visit_"
    )
  
  patient_tmp <-
    merge(patient_lateness_all,
          patient_lateness_five,
          by = "key") %>%
    merge(., patient_lateness_three, by = "key") %>%
    merge(., patient_lateness_ten, by = "key") %>%
    merge(., patient_last5, by = "key") %>%
    merge(., patient_tca, by = "key") %>%
    merge(., patient_tca_last5, by = "key") %>%
    merge(., target_tmp, by = "key")
  
  patient_tmp
  
}



outlist_other <- foreach(
  i = 1:length(patients),
  .packages = c("dplyr", "lubridate", "tidyr", "doParallel", "foreach")
) %dopar% {
  # for(i in 1:length(patients)){
  
  patient <-
    allvisits[allvisits$key == patients[i],]
  
  # Arrange chronologically
  patient <- patient %>%
    arrange(desc(VisitDate))
  
  # Get number of NADs since Jan 1
  num_targets <-
    nrow(filter(patient, VisitDate > "2021-01-01")) - 1
  # num_targets <- nrow(patient) - 1
  
  if (num_targets <= 0) {
    return(NULL)
  }
  
  target_lateness <-
    foreach(
      k = 1:num_targets,
      .packages = c("dplyr", "lubridate", "tidyr"),
      .errorhandling = 'remove'
    ) %do% {
      # Arrange chronologically and select most recent 51 variables
      patient_tmp <-
        patient %>%
        ungroup() %>%
        arrange(desc(VisitDate)) %>%
        mutate(rownum = row_number()) %>%
        # filter(rownum <= k+26) %>%
        filter(rownum >= k)
      
      # # If only one visit and NAD is
      if (nrow(patient_tmp) <=
          1) {
        next
      }
      
      patient_tca <-
        patient_tmp %>%
        filter(row_number() ==
                 1) %>%
        mutate(
          Month = lubridate::month(NextAppointmentDate, label = TRUE),
          Day = lubridate::wday(NextAppointmentDate, label = TRUE)
        ) %>%
        mutate(NextAppointmentDate = as.numeric(NextAppointmentDate - VisitDate)) %>%
        select(key, NextAppointmentDate, Month, Day)
      
      # Calculate the gap between the visit date and the previous NAD
      for (j in 1:(nrow(patient_tmp) -
                   1)) {
        patient_tmp[j, "visitdiff"] <-
          patient_tmp[j, "VisitDate"] - patient_tmp[j + 1, "NextAppointmentDate"]
      }
      
      # Get target variable - if visitdiff > 30, then IIT, otherwise not IIT
      target_tmp <-
        data.frame(
          key = patient_tmp$key[1],
          Target = ifelse(as.numeric(patient_tmp[1, "visitdiff"]) > 28, 1, 0),
          PredictionDate = patient_tmp[2, "VisitDate"]$VisitDate,
          MissingNAD = patient_tmp[2, "MissingExpectedReturn"]$MissingExpectedReturn,
          stringsAsFactors = FALSE
        )
      
      
      # Drop the target variable (first row) & the earliest visit since cannot have been late (last row)
      patient_tmp <-
        patient_tmp[2:nrow(patient_tmp),]
      
      patient_tca_last5 <-
        patient_tmp %>%
        mutate(rownum = row_number()) %>%
        filter(rownum <= 5) %>%
        mutate(NextAppointmentDate = as.numeric(NextAppointmentDate - VisitDate)) %>%
        group_by(key) %>%
        summarize(average_tca_last5 = mean(NextAppointmentDate))
      
      # Now get how many times they were late
      patient_lateness_all <-
        patient_tmp %>%
        group_by(key) %>%
        mutate(visitdiff = as.numeric(visitdiff)) %>%
        mutate(visitdiff = ifelse(visitdiff < 0, 0, visitdiff)) %>%
        summarize(
          n_appts = n(),
          late = sum(visitdiff > 0, na.rm = T),
          late28 = sum(visitdiff > 28, na.rm = T),
          averagelateness = min(100, mean(visitdiff, na.rm = T)),
          late_rate = late / n_appts,
          late28_rate = late28 / n_appts
        )
      
      patient_lateness_three <-
        patient_tmp %>%
        mutate(rownum = row_number()) %>%
        filter(rownum <= 3) %>%
        mutate(visitdiff = as.numeric(visitdiff)) %>%
        mutate(visitdiff = ifelse(visitdiff < 0, 0, visitdiff)) %>%
        group_by(key) %>%
        summarize(
          late_last3 = sum(visitdiff > 0, na.rm = T),
          averagelateness_last3 = min(100, mean(visitdiff, na.rm = T))
        )
      
      patient_lateness_five <-
        patient_tmp %>%
        mutate(rownum = row_number()) %>%
        filter(rownum <= 5) %>%
        mutate(visitdiff = as.numeric(visitdiff)) %>%
        mutate(visitdiff = ifelse(visitdiff < 0, 0, visitdiff)) %>%
        group_by(key) %>%
        summarize(
          late_last5 = sum(visitdiff > 0, na.rm = T),
          averagelateness_last5 = min(100, mean(visitdiff, na.rm = T))
        )
      
      patient_lateness_ten <-
        patient_tmp %>%
        mutate(rownum = row_number()) %>%
        filter(rownum <= 10) %>%
        mutate(visitdiff = as.numeric(visitdiff)) %>%
        mutate(visitdiff = ifelse(visitdiff < 0, 0, visitdiff)) %>%
        group_by(key) %>%
        summarize(
          late_last10 = sum(visitdiff > 0, na.rm = T),
          averagelateness_last10 = min(100, mean(visitdiff, na.rm = T))
        )
      
      patient_last5 <-
        patient_tmp %>%
        mutate(rownum = row_number()) %>%
        filter(rownum <= 5) %>%
        mutate(visitdiff = as.numeric(visitdiff)) %>%
        mutate(visitdiff = ifelse(visitdiff >= 0, visitdiff, 0)) %>%
        pivot_wider(
          id_cols = key,
          names_from = rownum,
          values_from = visitdiff,
          names_prefix = "visit_"
        )
      
      patient_tmp <-
        merge(patient_lateness_all,
              patient_lateness_five,
              by = "key") %>%
        merge(., patient_lateness_three, by = "key") %>%
        merge(., patient_lateness_ten, by = "key") %>%
        merge(., patient_last5, by = "key") %>%
        merge(., patient_tca, by = "key") %>%
        merge(., patient_tca_last5, by = "key") %>%
        merge(., target_tmp, by = "key")
      
      patient_tmp
      
    }
  
}

outlist_first <- rbindlist(outlist_first, fill = T)
outlist <- unlist(outlist_other, recursive = FALSE)
target_lateness <- rbindlist(outlist, fill = T)
target_lateness <- bind_rows(target_lateness, outlist_first)
# Filter out any observations where the target variable was imputed
target_lateness <-
  target_lateness[target_lateness$MissingNAD == 0,]
# saveRDS(target_lateness, "target_latenessv2.rds")
print("lateness metrics calculated")

# 2. most recent pharmacy pickup features ---------------------

# target_lateness <- target_lateness[target_lateness$key %in% keys_df$key[keys_df$group %in% 6:10],  ]
# Read in original pharmacy data
# pharmacy <- s3read_using(FUN = readRDS, bucket = aws_bucket, object = "March2023Dump/pharmacy_samp.rds")
# pharmacy <- readRDS("pharmacy_samp250k.rds")
pharmacy <- phar
# pharmacy <- readRDS("pharmacy1000.rds")
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

# start <- Sys.time()
hiv_regimen_list <- foreach(i = 1:nrow(target_lateness),
                            .packages = c("dplyr", "lubridate")) %dopar% {
                              # for(i in 1:nrow(target_lateness)){
                              
                              tmp <- target_lateness[i,]
                              
                              # Join on prediction dates and limit to pharmacy pickups at or before the prediction date,
                              # to avoid leakage
                              hiv_tmp <- hiv[hiv$key == tmp$key,]
                              hiv_tmp <-
                                hiv_tmp[hiv_tmp$DispenseDate <= tmp$PredictionDate,]
                              hiv_tmp <-
                                hiv_tmp[as.numeric(tmp$PredictionDate - hiv_tmp$DispenseDate) < 400,]
                              hiv_tmp <-
                                hiv_tmp[hiv_tmp$Drug != "NULL",]
                              hiv_tmp <-
                                hiv_tmp[!is.na(hiv_tmp$Drug),]
                              
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
                              hiv_tmp$TreatmentType[hiv_tmp$TreatmentType %in% c("ART", "ARV", "HIV Treatment")] <-
                                "ART"
                              
                              hiv_tmp <- hiv_tmp %>%
                                select(num_hiv_regimens,
                                       TreatmentType,
                                       OptimizedHIVRegimen) %>%
                                mutate(key = tmp$key,
                                       PredictionDate = tmp$PredictionDate)
                              
                              hiv_tmp
                              
                            }

hiv_regimen <- data.table::rbindlist(hiv_regimen_list)
tlr <-
  merge(
    target_lateness,
    hiv_regimen,
    by = c("key", "PredictionDate"),
    all.x = TRUE
  )

print("pharmacy metrics calculated")

# 3. most recent visit pickup features ------------------------
# visits <- readRDS("visits_samp250k.rds")
# dem <- readRDS("patients_samp250k.rds")
# dem <- dem[dem$key %in% target_lateness$key, ]
# dem$DOB <- ymd(substr(dem$DOB, 1, 10))
# dem <- dem[!duplicated(dem$key),]
visits <- visits[visits$key %in% target_lateness$key,]
visits$NextAppointmentDate <- ymd(visits$NextAppointmentDate)
visits$VisitDate <- ymd(visits$VisitDate)

# Visits are all between 2019 and present so nothing drops
visits <- visits[visits$VisitDate <= "2023-03-23",]

# Drop entries where next appointment date is missing (inlcuding where next appointment is before January 1, 2019
# and beyond april 2024)
range(visits$NextAppointmentDate, na.rm = TRUE)
visits <- visits[!is.na(visits$NextAppointmentDate),]
visits <- visits[visits$NextAppointmentDate >= "2019-01-01",]
visits <- visits[visits$NextAppointmentDate <= "2024-04-01",]

# 2,325,966 remain
# Drop entries where NAD is before Visit Date or more than 365 days in the future
visits$gap <- visits$NextAppointmentDate - visits$VisitDate
visits <- visits[visits$gap > 0,]
visits <- visits[visits$gap < 400,]
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
visit_features <- foreach(
  i = 1:nrow(target_lateness),
  .packages = c("dplyr", "lubridate", "tidyr"),
  .errorhandling = "remove"
) %dopar% {
  # for(i in 1:nrow(target_lateness)){
  
  # if(i%%1000==0){print(i)}
  
  tmp <- target_lateness[i,]
  
  # Join on prediction dates and limit to pharmacy pickups at or before the prediction date,
  # to avoid leakage
  visits_tmp <-
    visits[visits$key == tmp$key,]
  
  # Join on prediction dates and limit visits to those that took place before or on the prediction date
  visits_tmp$VisitDate <-
    ymd(visits_tmp$VisitDate)
  visits_tmp <- visits_tmp %>%
    filter(VisitDate <= tmp$PredictionDate)
  
  # Let's get most recent 5 visits and count how many are unscheduled
  visits_last5 <- visits_tmp %>%
    arrange(desc(VisitDate)) %>%
    mutate(rownum = row_number()) %>%
    filter(rownum <= 5)
  
  visits_last5$visitType <-
    tolower(visits_last5$VisitType)
  visits_last5$unscheduled <-
    ifelse(grepl("unscheduled", visits_last5$visitType), 1, 0)
  visits_last5 <- visits_last5 %>%
    group_by(key) %>%
    summarize(
      n_visits_lastfive = n(),
      n_unscheduled_lastfive = sum(unscheduled),
      unscheduled_rate = n_unscheduled_lastfive / n_visits_lastfive,
      PredictionDate = tmp$PredictionDate
    ) %>%
    select(key, unscheduled_rate, PredictionDate)
  
  # Order visits in reverse chronological order for each patient
  visits_recent <- visits_tmp %>%
    arrange(desc(VisitDate)) %>%
    mutate(rownum = row_number()) %>%
    filter(rownum == 1)
  
  # Calculate BMI for each patient and take most recent
  # Check height and weight for additional cleaning
  
  bmi <- visits_tmp %>%
    merge(., dem[, c("key", "DOB")], by = "key") %>%
    mutate(Age = as.numeric((tmp$PredictionDate - DOB) / 365)) %>%
    filter(Age >= 15) %>%
    filter(Weight != "NULL",
           Height != "NULL") %>%
    filter(between(as.numeric(Height), 100, 250)) %>%
    filter(between(as.numeric(Weight), 30, 200)) %>%
    mutate(BMI = as.numeric(Weight) / ((as.numeric(Height) /
                                          100) ** 2)) %>%
    mutate(BMI = ifelse(between(BMI, 10, 50), BMI, NA)) %>%
    filter(!is.na(BMI)) %>%
    arrange(desc(VisitDate)) %>%
    filter(row_number() == 1) %>%
    select(key, BMI, Weight) %>%
    mutate(PredictionDate = tmp$PredictionDate)
  
  # Pregnant
  # Join with demographics data and we see this requires some cleaning (men and older/younger women show yes)
  # So, join, and then fix men, women under 10 or over 49 as not pregnant
  pregnant <- visits_recent %>%
    select(key, Pregnant, Breastfeeding) %>%
    mutate(Pregnant = tolower(Pregnant),
           Breastfeeding = tolower(Breastfeeding)) %>%
    mutate(
      Pregnant = ifelse(grepl("yes", Pregnant), "yes", "no"),
      Breastfeeding = ifelse(grepl("yes", Breastfeeding), "yes", "no")
    ) %>%
    merge(., dem[, c("key", "Gender", "DOB")], by = "key") %>%
    mutate(Age = as.numeric((tmp$PredictionDate - DOB) / 365)) %>%
    unique()
  pregnant <- pregnant %>%
    mutate(Pregnant = ifelse(
      Gender %in% c("Male", "M") |
        !between(pregnant$Age, 10, 49),
      "NR",
      Pregnant
    )) %>%
    mutate(Breastfeeding = ifelse(
      Gender %in% c("Male", "M") |
        !between(pregnant$Age, 10, 49),
      "NR",
      Breastfeeding
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
  diff_care$DifferentiatedCare <-
    tolower(diff_care$DifferentiatedCare)
  diff_care$DifferentiatedCare <-
    gsub("[^[:alnum:][:space:]]", "", diff_care$DifferentiatedCare)
  diff_care$DifferentiatedCare <-
    gsub(" ", "", diff_care$DifferentiatedCare)
  diff_care <- diff_care %>%
    select(key, DifferentiatedCare) %>%
    mutate(PredictionDate = tmp$PredictionDate)
  
  # Adherence
  # There are up to two adherences (for ART and CTX) recorded for each patient
  # These are recorded in the same cell, so next lines parse these into new variables
  # Adherence and Adherence Category, when there are two, are always separated by a |,
  # so we are splitting on that symbol
  
  art_adherence <-
    visits_tmp[, c("key", "VisitDate", "Adherence")] %>%
    mutate(ARTAdherence = tolower(gsub("\\|.*", "", Adherence))) %>%
    mutate(ARTAdherence = ifelse(
      grepl("good", ARTAdherence),
      "good",
      ifelse(
        grepl("fair", ARTAdherence),
        "fair",
        ifelse(grepl("poor", ARTAdherence), "poor", NA)
      )
    ))
  
  art_adherence_recent <-
    art_adherence %>%
    ungroup() %>%
    arrange(desc(VisitDate)) %>%
    filter(!is.na(ARTAdherence)) %>%
    mutate(rownum = row_number()) %>%
    filter(rownum <= 1) %>%
    rename(most_recent_art_adherence = ARTAdherence) %>%
    select("key", "most_recent_art_adherence") %>%
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
  who <- data.frame(
    key = tmp$key,
    PredictionDate = tmp$PredictionDate,
    AHD_who = "No"
  )
  
  if (3 %in% visits_tmp$WHOStage |
      4 %in% visits_tmp$WHOStage) {
    who$AHD_who <- "Yes"
  }
  
  # Combine all features
  visits_out <-
    merge(visits_last5,
          bmi,
          by = c("key", "PredictionDate"),
          all = TRUE) %>%
    merge(.,
          pregnant,
          by = c("key", "PredictionDate"),
          all = TRUE) %>%
    merge(.,
          diff_care,
          by = c("key", "PredictionDate"),
          all = TRUE) %>%
    merge(.,
          art_adherence_recent,
          by = c("key", "PredictionDate"),
          all = TRUE) %>%
    merge(.,
          who,
          by = c("key", "PredictionDate"),
          all = TRUE) %>%
    merge(.,
          stab,
          by = c("key", "PredictionDate"),
          all = TRUE)
  
  visits_out
  
}

# end <- Sys.time()
# print(end - start)

visits_df <- rbindlist(visit_features, fill = TRUE)
tlrv <-
  merge(tlr,
        visits_df,
        by = c("key", "PredictionDate"),
        all.x = TRUE)

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
  mutate(
    ReportedbyDate = ymd(ReportedbyDate),
    OrderedbyDate = ymd(OrderedbyDate)
  ) %>%
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
  mutate(TestName = ifelse(
    grepl("cd4", TestName) & !grepl("%|percent", TestName),
    "CD4",
    ifelse(grepl("viral|vl", TestName), "VL", "Other")
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
  filter(grepl("detect|ldl", TestResult)) %>%
  mutate(VL = "suppressed")

vl <- bind_rows(vl_nums, vl_char)

cd4 <- labs2 %>%
  filter(TestName == "CD4") %>%
  mutate(TestResult = as.numeric(TestResult)) %>%
  mutate(AHD = ifelse(TestResult < 200, "Yes", "No"))

# start <- Sys.time()
lab_list <- foreach(
  i = 1:nrow(tlr),
  .packages = c("dplyr", "lubridate"),
  .errorhandling = "remove"
) %dopar% {
  # for(i in 1:nrow(target_lateness)){
  
  tmp <- tlr[i,]
  
  # Need to join lab and get most recent touchpoint and filter to labs prior to that date
  lab_tmp <- vl[vl$key == tmp$key,]
  lab_tmp <- lab_tmp %>%
    filter(OrderedbyDate <= tmp$PredictionDate)
  
  if (nrow(lab_tmp) == 0) {
    next
  }
  
  # Lab – look at n_tests and n_hvl for most recent three years
  lab_lastthreeyears <- lab_tmp %>%
    filter(tmp$PredictionDate - ReportedbyDate < 1000) %>%
    group_by(key) %>%
    summarize(
      n_tests_threeyears = n(),
      n_hvl_threeyears = sum(VL == "unsuppressed"),
      n_lvl_threeyears = sum(VL == "suppressed"),
      recent_hvl_rate = n_hvl_threeyears / n_tests_threeyears
    )
  
  lab_recent <- lab_tmp %>%
    group_by(key) %>%
    arrange(desc(ReportedbyDate)) %>%
    mutate(rownum = row_number()) %>%
    filter(rownum == 1) %>%
    rename("most_recent_vl" = VL) %>%
    select(key, most_recent_vl)
  
  # now cd4 to get AHD
  cd4_tmp <- cd4[cd4$key == tmp$key,]
  cd4_tmp <- cd4_tmp %>%
    filter(OrderedbyDate <= tmp$PredictionDate) %>%
    ungroup() %>%
    arrange(desc(OrderedbyDate)) %>%
    mutate(rownum = row_number()) %>%
    filter(rownum <= 5)
  
  cddf <- data.frame(key = tmp$key,
                     AHD = "No")
  
  if (any(cd4_tmp$AHD == "Yes")) {
    cddf$AHD = "Yes"
  }
  
  
  lab_out <-
    merge(lab_recent,
          lab_lastthreeyears,
          by = "key",
          all = TRUE) %>%
    merge(., cddf, by = "key", all = TRUE) %>%
    mutate(PredictionDate = tmp$PredictionDate)
  
  lab_out
}

lab_df <- rbindlist(lab_list, fill = TRUE)
tlrvl <-
  merge(tlrv,
        lab_df,
        by = c("key", "PredictionDate"),
        all.x = TRUE)

print("lab metrics calculated")

# 5. features for dem (one row for patient) ---------------
# basically just cleaning this up
# filter art to id's in dem and join on dem
# Filter to PatientID from dem
# Time to start ART - this will StartARTDate - DateConfirmedHIVPositive
dem$StartARTDate <- ymd(dem$StartARTDate)
dem$StartARTDate[dem$StartARTDate < "1985-01-01"] <- NA

dem <- dem %>%
  select(
    key,
    Gender,
    DOB,
    PatientSource,
    MaritalStatus,
    PopulationType,
    StartARTDate,
    SiteCode
  )

# Time on ART - this will be second most recent visit - ART Start Date (need to join on that)
dem <- merge(dem, tlrvl, by = "key")
dem$timeOnArt <-
  as.numeric(floor((dem$PredictionDate - dem$StartARTDate) / 30))
dem$Age <- as.numeric(floor((dem$PredictionDate - dem$DOB) / 365))
dem$Age[dem$Age > 100] <- NA
dem$Age[dem$age < 0] <- NA
dem$timeOnArt[dem$timeOnArt < 0] <- NA

# Patient Source
dem$PatientSource <- tolower(dem$PatientSource)
table(dem$PatientSource, useNA = "always")
dem <- dem %>%
  mutate(PatientSource = ifelse(
    grepl("vct", PatientSource),
    "VCT",
    ifelse(
      grepl("opd|transfer", PatientSource),
      "OPD",
      ifelse(
        grepl("mch", PatientSource),
        "MCH",
        ifelse(
          grepl("vmmc", PatientSource),
          "VMMC",
          ifelse(
            grepl("ccc", PatientSource),
            "CCC",
            ifelse(
              grepl("tb", PatientSource),
              "TBClinic",
              ifelse(
                grepl("cwc", PatientSource),
                "CWC",
                ifelse(grepl("ipd", PatientSource), "IPD",
                       "Other")
              )
            )
          )
        )
      )
    )
  ))

rare_cats <-
  names(which(prop.table(table(dem$PatientSource)) < .01))
dem$PatientSource[dem$PatientSource %in% rare_cats] <- "Other"

# Marital Status
table(dem$MaritalStatus, useNA = "always")
dem$MaritalStatus <- tolower(dem$MaritalStatus)
dem <- dem %>%
  mutate(MaritalStatus = ifelse(Age <= 15, "Minor",
                                ifelse(
                                  grepl("never|single", MaritalStatus),
                                  "Single",
                                  ifelse(
                                    grepl("monogamous|cohabit|living|married", MaritalStatus),
                                    "Married",
                                    ifelse(
                                      grepl("divorced|separated", MaritalStatus),
                                      "Divorced",
                                      ifelse(
                                        grepl("widow", MaritalStatus),
                                        "Widow",
                                        ifelse(grepl("poly", MaritalStatus), "Polygamous",
                                               "Other")
                                      )
                                    )
                                  )
                                )))



# Population Type
dem$PopulationType <- tolower(dem$PopulationType)
dem <- dem %>%
  mutate(PopulationType = ifelse(grepl("key", PopulationType), "KP", "GP"))

dem <- dem %>%
  mutate(AHD = ifelse(AHD == "Yes" |
                        AHD_who == "Yes" | Age <= 5, "Yes", "No"))



dem <- dem %>%
  select(-DOB,-StartARTDate,-MissingNAD,-AHD_who,-TreatmentType)

saveRDS(dem, paste0("DecemberRefresh/iit_prep_12142023_", a, ".rds"))

}