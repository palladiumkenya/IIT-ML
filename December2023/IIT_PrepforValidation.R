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


# Read in datasets
dem <- read.csv("December Investigation/PatientDemographics.csv")
dem$key <- paste0(dem$PatientID, dem$SiteCode)
pharmacy <- read.csv("December Investigation/PatientPharmacy.csv")
pharmacy$key <- paste0(pharmacy$PatientID, pharmacy$siteCode)
visits <- read.csv("December Investigation/VisitsData.csv")
visits$key <- paste0(visits$PatientID, visits$SiteCode)
labs <- read.csv("December Investigation/PatientLab.csv")
labs$key <- paste0(labs$PatientID, labs$SiteCode)
art <- read.csv("December Investigation/PatientARTExtract.csv")
art$key <- paste0(art$PatientID, art$SiteCode)

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
  mdy(visits_missing$NextAppointmentDate)
visits_missing$VisitDate <- mdy(visits_missing$VisitDate)

# Filter Visit Dates from 2019 to present
range(visits_missing$VisitDate)
visits_missing <-
  visits_missing[visits_missing$VisitDate >= "2019-01-01",]
visits_missing <-
  visits_missing[visits_missing$VisitDate <= Sys.Date(),]

# Drop entries where next appointment date is missing (inlcuding where next appointment is before January 1, 2019
# and beyond 365 days from today)
range(visits_missing$NextAppointmentDate, na.rm = TRUE)
visits_missing <-
  visits_missing[!is.na(visits_missing$NextAppointmentDate),]
visits_missing <-
  visits_missing[visits_missing$NextAppointmentDate >= "2019-01-01",]
visits_missing <-
  visits_missing[visits_missing$NextAppointmentDate <= Sys.Date() + 365,]

# Drop entries where NAD is before Visit Date or more than 400 days after visit date
visits_missing$gap <-
  visits_missing$NextAppointmentDate - visits_missing$VisitDate
visits_missing <- visits_missing[visits_missing$gap > 0,]
visits_missing <- visits_missing[visits_missing$gap < 400,]
visits_missing <- visits_missing %>% dplyr::select(-gap)


# some visits are recorded more than once. for each of this, keep the visits with the
# next appointment date that is furthest in the future, and drop other records of this visit
visits_missing <- visits_missing %>%
  group_by(key, VisitDate) %>%
  arrange(desc(NextAppointmentDate)) %>%
  mutate(rownum = row_number()) %>%
  filter(rownum == 1) %>%
  select(-rownum) %>%
  ungroup()


# Now, let's process pharmacy data in a similar way
# Keep only rows necessary to assess missingness, and convert date columns to date type
pharmacy <- pharmacy %>% select(key, DispenseDate, ExpectedReturn)
pharmacy$DispenseDate <- ymd(pharmacy$DispenseDate)
pharmacy$ExpectedReturn <- ymd(pharmacy$ExpectedReturn)

# Set any incorrect expected return dates to NA
range(pharmacy$DispenseDate)
range(pharmacy$ExpectedReturn, na.rm = TRUE)
pharmacy <- pharmacy[pharmacy$DispenseDate >= "2019-01-01", ]
pharmacy$ExpectedReturn[pharmacy$ExpectedReturn < "2019-01-01"] <-
  NA
pharmacy$ExpectedReturn[pharmacy$ExpectedReturn >= Sys.Date() + 365] <-
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
  filter(row_number() == 1)

# Looping is too complicated to deploy - instead, if an expectedreturn date is NA,
# then impute as 30 days following the dispense date
pharmacy_complete <- pharmacy %>%
  mutate(ExpectedReturn = if_else(is.na(ExpectedReturn), DispenseDate + 30, ExpectedReturn))

# in order to create a single integrated longitudinal record for each patient, we must combine
# visit and pharmacy pickup data. To facilitate this, rename variables in pharmacy dataframe to
# match names of variables in visits dataframe, and then rowbind them
pharmacy_complete <- pharmacy_complete %>%
  rename("VisitDate" = DispenseDate,
         "NextAppointmentDate" = ExpectedReturn)

# Now, rowbind them
allvisits <- bind_rows(visits_missing, pharmacy_complete)

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
  filter(row_number() == 1)

# Loop through each patient and each visit from Jan 1 and on
patients <- unique(allvisits$key)

lateness_outlist <- list()

lateness <- foreach(
  i = 1:length(patients),
  .packages = c("dplyr", "lubridate", "tidyr"),
  # .combine = "rbind",
  .errorhandling = 'remove'
) %dopar% {
# for(i in 1:length(patients)){
  
  patient <-
    allvisits[allvisits$key == patients[i],]
  
  # Arrange chronologically
  patient <- patient %>%
    arrange(desc(VisitDate))
  
  
  # Arrange chronologically and select most recent 51 variables
  patient_tmp <- patient %>%
    ungroup() %>%
    arrange(desc(VisitDate)) 
  
  # # If only one visit and NAD is
  if (nrow(patient_tmp) <= 1) {
    return(NULL)
  }
  
  patient_tca <- patient_tmp %>%
    filter(row_number() == 1) %>%
    mutate(
      Month = lubridate::month(NextAppointmentDate, label = TRUE),
      Day = lubridate::wday(NextAppointmentDate, label = TRUE)
    ) %>%
    mutate(NextAppointmentDate = as.numeric(NextAppointmentDate - VisitDate)) %>%
    select(key, NextAppointmentDate, Month, Day)
  
  # Calculate the gap between the visit date and the previous NAD
  for (j in 1:(nrow(patient_tmp) - 1)) {
    patient_tmp[j, "visitdiff"] <-
      patient_tmp[j, "VisitDate"] - patient_tmp[j + 1, "NextAppointmentDate"]
  }
  
  patient_tca_last5 <- patient_tmp %>%
    mutate(rownum = row_number()) %>%
    filter(rownum >= 2 & rownum <= 6) %>%
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
  
  patient_tmp <- merge(patient_lateness_all,
                       patient_lateness_five,
                       by = "key") %>%
    merge(., patient_lateness_three, by = "key") %>%
    merge(., patient_lateness_ten, by = "key") %>%
    merge(., patient_last5, by = "key") %>%
    merge(., patient_tca, by = "key") %>%
    merge(., patient_tca_last5, by = "key") 
  
  patient_tmp
  
}

lateness <- rbindlist(lateness, fill = T)

print("lateness metrics calculated")

# 2. most recent pharmacy pickup features ---------------------

# Read in original pharmacy data
pharmacy <- read.csv("December Investigation/PatientPharmacy.csv")
pharmacy$key <- paste0(pharmacy$PatientID, pharmacy$siteCode)
pharmacy$DispenseDate <- ymd(pharmacy$DispenseDate)
pharmacy$ExpectedReturn <- ymd(pharmacy$ExpectedReturn)


# some pharmacy pickups are recorded more than once. for each of this, keep the pickup with the
# expected return date that is furthest in the future, and drop other records of this pickup
pharmacy <- pharmacy %>%
  group_by(key, DispenseDate) %>%
  arrange(desc(ExpectedReturn)) %>%
  filter(row_number()==1)

hiv <- pharmacy %>%
  filter(!TreatmentType %in% c("NULL", "Prophylaxis")) %>%
  ungroup()

# start <- Sys.time()
hiv_regimen_list <- foreach(i = 1:nrow(lateness),
                            .packages = c("dplyr", "lubridate"),
                            .errorhandling = 'remove') %dopar% {
                              # for(i in 1:nrow(target_lateness)){
                              
                              tmp <- lateness[i,]
                              
                              # Join on prediction dates and limit to pharmacy pickups at or before the prediction date,
                              # to avoid leakage
                              hiv_tmp <- hiv[hiv$key == tmp$key,]
                              hiv_tmp <-
                                hiv_tmp[hiv_tmp$DispenseDate <= Sys.Date(),]
                              hiv_tmp <-
                                hiv_tmp[as.numeric(Sys.Date() - hiv_tmp$DispenseDate) < 400,]
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
                              
                              hiv_tmp <- hiv_tmp %>%
                                select(num_hiv_regimens,
                                       OptimizedHIVRegimen) %>%
                                mutate(key = tmp$key,
                                       PredictionDate = tmp$PredictionDate)
                              
                              hiv_tmp
                              
                            }

hiv_regimen <- data.table::rbindlist(hiv_regimen_list)
tlr <-
  merge(
    lateness,
    hiv_regimen,
    by = c("key"),
    all.x = TRUE
  )

print("pharmacy metrics calculated")

# 3. most recent visit pickup features ------------------------

visits <- visits[visits$key %in% lateness$key,]
visits$NextAppointmentDate <- mdy(visits$NextAppointmentDate)
visits$VisitDate <- mdy(visits$VisitDate)

# Visits are all between 2019 and present so nothing drops
visits <- visits[visits$VisitDate <= Sys.Date(),]
visits <- visits[visits$VisitDate >= "2019-01-01", ]

# Drop entries where next appointment date is missing (inlcuding where next appointment is before January 1, 2019
# and beyond april 2024)
range(visits$NextAppointmentDate, na.rm = TRUE)
visits <- visits[!is.na(visits$NextAppointmentDate),]
visits <- visits[visits$NextAppointmentDate >= "2019-01-01",]
visits <- visits[visits$NextAppointmentDate <= Sys.Date() + 365,]

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
  filter(row_number()==1)

# start <- Sys.time()
visit_features <- foreach(
  i = 1:nrow(tlr),
  .packages = c("dplyr", "lubridate", "tidyr"),
  .errorhandling = "remove"
) %dopar% {
  
  tmp <- tlr[i,]
  
  # Join on prediction dates and limit to pharmacy pickups at or before the prediction date,
  # to avoid leakage
  visits_tmp <-
    visits[visits$key == tmp$key,]
  
  # Join on prediction dates and limit visits to those that took place before or on the prediction date
  visits_tmp$VisitDate <-
    ymd(visits_tmp$VisitDate)
  visits_tmp <- visits_tmp %>%
    filter(VisitDate <= Sys.Date())
  
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
      unscheduled_rate = n_unscheduled_lastfive / n_visits_lastfive
    ) %>%
    select(key, unscheduled_rate)
  
  # Order visits in reverse chronological order for each patient
  visits_recent <- visits_tmp %>%
    ungroup() %>%
    arrange(desc(VisitDate)) %>%
    mutate(rownum = row_number()) %>%
    filter(rownum == 1)
  
  # Calculate BMI for each patient and take most recent
  # Check height and weight for additional cleaning
  
  bmi <- visits_tmp %>%
    merge(., dem[, c("key", "DOB")], by = "key") %>%
    mutate(Age = as.numeric((Sys.Date() - DOB) / 365)) %>%
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
    select(key, BMI, Weight) 
  
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
    mutate(Age = as.numeric((Sys.Date() - DOB) / 365)) %>%
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
    select(key, Pregnant, Breastfeeding) 
  
  # Differentiated Care
  diff_care <- visits_tmp %>%
    ungroup() %>%
    filter(!DifferentiatedCare %in% c("NULL", "")) %>%
    mutate(recent = ifelse(Sys.Date() - VisitDate < 400, 1, 0)) %>%
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
    select(key, DifferentiatedCare) 
  
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
    select("key", "most_recent_art_adherence") 
  
  # Stability
  stab <- visits_tmp %>%
    filter(!StabilityAssessment %in% c("NULL", "")) %>%
    mutate(StabilityAssessment = tolower(StabilityAssessment),
           StabilityAssessment = ifelse(grepl("un|not", StabilityAssessment), "Unstable", "Stable")) %>%
    mutate(recent = ifelse(Sys.Date() - VisitDate < 400, 1, 0)) %>%
    filter(recent == 1) %>%
    group_by(key) %>%
    arrange(desc(VisitDate)) %>%
    mutate(rownum = row_number()) %>%
    filter(rownum == 1) %>%
    select(-rownum) %>%
    select(key, StabilityAssessment)
  
  # WHO Stage - if 3 or 4, then AHD
  who <- data.frame(
    key = tmp$key,
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
          by = c("key"),
          all = TRUE) %>%
    merge(.,
          pregnant,
          by = c("key"),
          all = TRUE) %>%
    merge(.,
          diff_care,
          by = c("key"),
          all = TRUE) %>%
    merge(.,
          art_adherence_recent,
          by = c("key"),
          all = TRUE) %>%
    merge(.,
          who,
          by = c("key"),
          all = TRUE) %>%
    merge(.,
          stab,
          by = c("key"),
          all = TRUE)
  
  visits_out
  
}

# end <- Sys.time()
# print(end - start)

visits_df <- rbindlist(visit_features, fill = TRUE)
tlrv <-
  merge(tlr,
        visits_df,
        by = c("key"),
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
  select(key, OrderedByDate, TestName, TestResult, ReportedByDate) %>%
  unique() %>%
  mutate(
    ReportedbyDate = ymd(ReportedByDate),
    OrderedbyDate = ymd(OrderedByDate)
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
  
  tmp <- tlrv[i,]
  
  # Need to join lab and get most recent touchpoint and filter to labs prior to that date
  lab_tmp <- vl[vl$key == tmp$key,]
  lab_tmp <- lab_tmp %>%
    filter(OrderedbyDate <= Sys.Date())
  
  if (nrow(lab_tmp) == 0) {
    return(NULL)
  }
  
  # Lab – look at n_tests and n_hvl for most recent three years
  lab_lastthreeyears <- lab_tmp %>%
    filter(Sys.Date() - ReportedbyDate < 1000) %>%
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
    filter(OrderedbyDate <= Sys.Date()) %>%
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
    merge(., cddf, by = "key", all = TRUE)
  
  lab_out
}

lab_df <- rbindlist(lab_list, fill = TRUE)
tlrvl <-
  merge(tlrv,
        lab_df,
        by = c("key"),
        all.x = TRUE)

print("lab metrics calculated")

# 5. features for dem (one row for patient) ---------------
# basically just cleaning this up
# filter art to id's in dem and join on dem
# Filter to PatientID from dem
# Time to start ART - this will StartARTDate - DateConfirmedHIVPositive
dem <- merge(dem, art[, c("key", "StartARTDate")], by = "key", all.x = TRUE)
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
  as.numeric(floor((Sys.Date() - dem$StartARTDate) / 30))
dem$Age <- as.numeric(floor((Sys.Date() - dem$DOB) / 365))
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
      "OPD", "Other"
      )
    )
  )


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
  select(-DOB,-StartARTDate,-AHD_who)

# Now join with locational data
gis <- read.csv('gis_features_dec2023.csv') %>% dplyr::select(-Latitude, -Longitude)
dem$SiteCode <- as.character(dem$SiteCode)
iit <- merge(dem, gis,by.x = "SiteCode", by.y = "FacilityCode", all.x = TRUE) %>%
  dplyr::select(-SiteCode)

iit$Weight <- as.numeric(iit$Weight)
iit$Month <- as.character(iit$Month)
iit$Day <- as.character(iit$Day)

saveRDS(iit, "iit_temp_12142023.rds")

# Cheat - rowbind with the 10k
# Better - fix the category levels - will that work?
iit <- readRDS("iit_prep_10K_12142023.rds")

iit <- iit %>%
  mutate(StabilityAssessment = tolower(StabilityAssessment),
         StabilityAssessment = ifelse(grepl("un|not", StabilityAssessment), "Unstable", "Stable"))

gis <- read.csv('gis_features_dec2023.csv') %>% dplyr::select(-Latitude, -Longitude)
iit$SiteCode <- as.character(iit$SiteCode)
iit <- merge(iit, gis,by.x = "SiteCode", by.y = "FacilityCode", all.x = TRUE) %>%
  dplyr::select(-SiteCode)

iit$Weight <- as.numeric(iit$Weight)
iit$Month <- as.character(iit$Month)
iit$Day <- as.character(iit$Day)
iit10K <- iit

iit <- readRDS("iit_temp_12142023.rds")
numpreds <- nrow(iit)
iit <- bind_rows(iit, iit10K)


encodeXGBoost <- function(dataset){
  # Need to one-hot encode all the factor variables
  ohe_features <- names(dataset)[ sapply(dataset, is.factor) | sapply(dataset, is.character) ]
  
  dmy <- dummyVars("~ Month + Gender+ PatientSource + MaritalStatus + PopulationType + Day+
                   OptimizedHIVRegimen +  Pregnant + DifferentiatedCare + owner_type +
                   StabilityAssessment + most_recent_art_adherence + keph_level_name +
                   most_recent_vl + AHD + Breastfeeding",
                   data = dataset)
  ohe <- data.frame(predict(dmy, newdata = dataset))
  dataset <- cbind(dataset, ohe)
  
  dataset[, !(names(dataset) %in% ohe_features)]
  
}
# pregnant and breastfeeding didn't come through with NRs for males - 
# check main dataset and fix if necessary

test_data <- iit %>% 
  select(- key, -PredictionDate, -Target) %>%
  encodeXGBoost() %>%
  mutate(rownum = row_number()) %>%
  filter(rownum <= numpreds) %>%
  select(-rownum)

# Load model
xgb <- readRDS("iit_xgb_1214_dummy.rds")

# reorder column names so they match what's in xgb feature names
test_data <- test_data %>% select(xgb$feature_names)
val_predict <- predict(xgb,newdata = data.matrix(test_data))
