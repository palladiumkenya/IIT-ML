

genLatenessMetrics <- function(visits_data, pharmacy_data){
  
  if(!"PatientID" %in% names(visits_data)){
    stop("Visits table is lacking PatientID")
  }
  
  if(!"VisitDate" %in% names(visits_data)){
    stop("Visits table is lacking VisitDate")
  }
  
  if(!"NextAppointmentDate" %in% names(visits_data)){
    stop("Visits table is lacking NextAppointmentDate")
  }
 
  if(!"PatientId" %in% names(pharmacy_data)){
    stop("Pharmacy table is lacking PatientId")
  }
  
  if(!"DispenseDate" %in% names(pharmacy_data)){
    stop("Pharmacy table is lacking DispenseDate")
  }
  
  if(!"ExpectedReturn" %in% names(pharmacy_data)){
    stop("Pharmacy table is lacking ExpectedReturn")
  }
  
  visits_data <- visits_data[visits_data$PatientID %in% PATIENTS, ]
  pharmacy_data <- pharmacy_data[pharmacy_data$PatientId %in% PATIENTS, ]
  
  # first, let's process visits data
  # keep only variables needed to assess missingness (patient id, visit date, and next appointment dates)
  visits_missing <- visits_data %>% select(PatientID, VisitDate, NextAppointmentDate)
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
  pharmacy_missing <- pharmacy_data %>% select(PatientId, DispenseDate, ExpectedReturn)
  pharmacy_missing$DispenseDate <- ymd(pharmacy_missing$DispenseDate)
  pharmacy_missing$ExpectedReturn <- ymd(pharmacy_missing$ExpectedReturn)
  
  # some pharmacy pickups are recorded more than once. for each of this, keep the pickup with the 
  # expected return date that is furthest in the future, and drop other records of this pickup
  pharmacy_missing <- pharmacy_missing %>%
    group_by(PatientId, DispenseDate) %>%
    arrange(desc(ExpectedReturn)) %>%
    mutate(rownum = row_number()) %>%
    filter(rownum == 1) %>%
    select(-rownum) %>%
    ungroup()
  
  pharmacy_missing <- pharmacy_missing %>%
    group_by(PatientId) %>%
    mutate(count = n()) %>%
    filter(count > 1) %>%
    select(-count) %>%
    ungroup()
  
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
  
  pharmacy_out <- foreach(i = 1:length(patients),
                          .combine=rbind,
                          .packages='dplyr')%dopar%{
    
    # if(i%%100==0){print(i)}
    
    patient <- pharmacy_missing[pharmacy_missing$PatientId == patients[i], ]
    patient <- patient %>%
      arrange(desc(DispenseDate))
    
    # If a patient has only one visit, then they cannot have been IIT, so skip
    # if(nrow(patient) == 1){next}
    
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
  
  # pharmacy_missing <- data.table::rbindlist(patient_list)
  
  # in order to create a single integrated longitudinal record for each patient, we must combine
  # visit and pharmacy pickup data. To facilitate this, rename variables in pharmacy dataframe to
  # match names of variables in visits dataframe, and then rowbind them
  pharmacy_missing <- pharmacy_out %>%
    rename("VisitDate" = DispenseDate,
           "NextAppointmentDate" = ExpectedReturn,
           "PatientID" = PatientId)
  # Now, rowbind them
  missing <- rbind(visits_missing, pharmacy_missing)
  #3,138,845 - number of rows in combined dataframe
  
  # As before, if visits and pickups occurred on the same date, keep the record for the visit/pickup
  # keep the record with the next appointment date that is furthest in the future, and drop others
  missing <- missing %>%
    group_by(PatientID, VisitDate) %>%
    arrange(desc(NextAppointmentDate)) %>%
    mutate(rownum = row_number()) %>%
    filter(rownum == 1) %>%
    select(-rownum)
  #2,552,683 remain
  
  # drop the few rows with visit dates that are erroneous
  missing <- missing[missing$VisitDate >= "2000-01-01", ]
  missing <- missing[missing$VisitDate <= "2022-07-01", ]
  #2,686,407
  
  missing <- missing %>%
    group_by(PatientID) %>%
    mutate(count = n()) %>%
    filter(count > 1) %>%
    select(-count) %>%
    ungroup()
  
  # Loop through each patient
  # if there is a missing expected return date, then get gap between two prior return dates
  # and impute
  patients <- unique(missing$PatientID)
  patient_list <- list()
  
  patients_out <- foreach(i = 1:length(patients),
                          .combine=rbind,
                          .packages='dplyr')%dopar%{
    
    # if(i%%100==0){print(i)}
    
    patient <- missing[missing$PatientID == patients[i], ]
    
    # if(nrow(patient)==1){next}
    
    # Arrange chronologically and select most recent 101 variables
    patient <- patient %>%
      arrange(desc(VisitDate)) %>%
      mutate(rownum = row_number()) %>%
      filter(rownum <= 50) %>%
      select(-rownum)
    # Calculate the gap between the visit date and the prevous NAD
    for(j in 1:(nrow(patient)-1)){
      patient[j, "visitdiff"] <- patient[j, "VisitDate"] - patient[j+1, "NextAppointmentDate"]
    }
    
    # Now get how many times they were late
    patient_lateness <- patient %>%
      group_by(PatientID) %>%
      summarize(n_appts = n(),
                missed1 = sum(visitdiff > 0, na.rm = T),
                missed5 = sum(visitdiff > 5, na.rm = T),
                missed30 = sum(visitdiff > 30, na.rm = T))
    
    patient_lateness_five <- patient %>%
      ungroup() %>%
      mutate(rownum = row_number()) %>%
      filter(rownum <= 5) %>%
      group_by(PatientID) %>%
      summarize(n_appts_last5 = n(),
                missed1_last5 = sum(visitdiff > 0, na.rm = T),
                missed5_last5 = sum(visitdiff > 5, na.rm = T),
                missed30_last5 = sum(visitdiff > 30, na.rm = T))
    
    patient <- merge(patient_lateness, 
                     patient_lateness_five,
                     by = "PatientID")
    
    # Add to outlist
    patient_list[[i]] <- patient 
    
  }
  
  # lateness <- data.table::rbindlist(patient_list)
  lateness <- patients_out
  
  # Get recent and overall lateness rates
  # lateness$unscheduled_rate <- lateness$n_unscheduled_lastfive / lateness$n_visits_lastfive
  lateness$all_late30_rate <- lateness$missed30 / lateness$n_appts
  lateness$all_late5_rate <- lateness$missed5 / lateness$n_appts
  lateness$all_late1_rate <- lateness$missed1 / lateness$n_appts
  lateness$recent_late30_rate <- lateness$missed30_last5 / lateness$n_appts_last5
  lateness$recent_late5_rate <- lateness$missed5_last5 / lateness$n_appts_last5
  lateness$recent_late1_rate <- lateness$missed1_last5 / lateness$n_appts_last5
  
  lateness
  
}

genRegimenInputs <- function(pharmacy_data){
  
  pharmacy_data <- pharmacy_data[pharmacy_data$PatientId %in% PATIENTS, ]
  
  # Similar processing as above - convert to date format
  pharmacy_data$DispenseDate <- ymd(pharmacy_data$DispenseDate)
  
  hiv <- pharmacy_data %>%
    filter(!TreatmentType %in% c("NULL", "Prophylaxis"))
  
  # Get rid of Null drugs
  hiv <- hiv %>%
    filter(Drug != "NULL") %>%
    filter(!is.na(Drug))
  # 216,205 remain
  
  # Now, get the number of HIV regimens per patient
  hiv_per_patient <- hiv %>%
    ungroup() %>%
    mutate(recent = ifelse(Sys.Date() - DispenseDate < 400, 1, 0)) %>%
    filter(recent == 1) %>%
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
  
  
  other <- pharmacy_data %>%
    filter(TreatmentType %in% c("NULL", "Prophylaxis")) %>%
    mutate(recent = ifelse(Sys.Date() - DispenseDate < 400, 1, 0)) %>%
    filter(recent == 1)
  
  # Get rid of Null drugs
  other <- other %>%
    filter(Drug != "NULL") %>%
    filter(!is.na(Drug))
  # 8,103 remain
  
  if(nrow(other)>0){
    other_reg <- data.frame(PatientID = unique(other$PatientId),
                            Other_Regimen = "Yes")
    
    reg_per_patient <- merge(hiv_per_patient, other_reg, by = "PatientID", all = TRUE)
  } else{
    
    reg_per_patient <- hiv_per_patient %>%
      mutate("Other_Regimen" = "No")
    
  }
  
  # Save out number of regimens, treatment type, and most recent drug
  reg_out <- reg_per_patient %>%
    select(PatientID, num_hiv_regimens, TreatmentType, OptimizedHIVRegimen, Other_Regimen) %>%
    mutate(Other_Regimen = ifelse(is.na(Other_Regimen), "No", Other_Regimen))
  
  reg_out
}

genVisitInputs <- function(visits_data){
  
  visits_data <- visits_data[visits_data$PatientID %in% PATIENTS, ]
  
  visits_data$VisitDate <- ymd(visits_data$VisitDate)
  
  # Let's get most recent 5 visits and count how many are unscheduled
  visits_last5 <- visits_data %>%
    ungroup() %>%
    group_by(PatientID) %>%
    arrange(desc(VisitDate)) %>%
    mutate(rownum = row_number()) %>%
    filter(rownum <= 5)
  visits_last5$visitType <- tolower(visits_last5$visitType)
  visits_last5$unscheduled <- ifelse(grepl("unscheduled", visits_last5$visitType), 1, 0)
  visits_last5 <- visits_last5 %>%
    ungroup() %>%
    group_by(PatientID) %>%
    summarize(n_visits_lastfive = n(),
              n_unscheduled_lastfive = sum(unscheduled))
  
  # Order visits in reverse chronological order for each patient
  visits_recent <- visits_data %>%
    ungroup() %>%
    group_by(PatientID) %>%
    arrange(desc(VisitDate)) %>%
    mutate(rownum = row_number()) %>%
    filter(rownum == 1)
  
  # Calculate BMI for each patient and take most recent
  
  # Check height and weight for additional cleaning
  
  visits_bmi <- visits_data %>%
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
  visits_bmi$timediff <- Sys.Date() - visits_bmi$VisitDate
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
  
  
  # Differentiated Care
  diff_care <- visits_data %>%
    filter(!DifferentiatedCare %in% c("NULL", "")) %>%
    mutate(recent = ifelse(Sys.Date() - VisitDate < 400, 1, 0)) %>%
    filter(recent == 1) %>%
    group_by(PatientID) %>%
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
  visits_data$adherence_symbol <- ifelse(grepl("\\|", visits_data$Adherence), 1, 0)
  visits_data$adherence1 <- ifelse(visits_data$adherence_symbol == 1,
                                   gsub("\\|.*", "", visits_data$Adherence),
                                   visits_data$Adherence) 
  visits_data$adherence2 <- ifelse(visits_data$adherence_symbol == 1,
                                   gsub(".*\\|", "", visits_data$Adherence),
                                   NA) 
  visits_data$adherencecat_symbol <- ifelse(grepl("\\|", visits_data$AdherenceCategory), 1, 0)
  visits_data$adherencecat1 <- ifelse(visits_data$adherencecat_symbol == 1,
                                      gsub("\\|.*", "", visits_data$AdherenceCategory),
                                      visits_data$AdherenceCategory) 
  visits_data$adherencecat2 <- ifelse(visits_data$adherencecat_symbol == 1,
                                      gsub(".*\\|", "", visits_data$AdherenceCategory),
                                      NA) 
  
  adherence1 <- visits_data %>%
    select(PatientID, VisitDate, adherence1, adherencecat1) %>%
    filter(adherencecat1 %in% c("ART", "ARV", "ARVAdherence", "CTX")) %>%
    mutate(adherencecat1 = ifelse(adherencecat1 == "CTX", "CTX", "ART")) %>%
    rename("adherence_category" = adherencecat1,
           "adherence" = adherence1)
  adherence2 <- visits_data %>%
    select(PatientID, VisitDate, adherence2, adherencecat2) %>%
    filter(adherencecat2 %in% c("ART", "ARV", "ARVAdherence", "CTX")) %>%
    mutate(adherencecat2 = ifelse(adherencecat2 == "CTX", "CTX", "ART")) %>%
    rename("adherence_category" = adherencecat2,
           "adherence" = adherence2)
  
  ## Temporary Fix ##
  adherence2$adherence <- "good"
  
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
                         all = TRUE) %>%
    rename("most_recent_art_adherence" = ART,
           "most_recent_ctx_adherence" = CTX)
  
  # Stability
  stab <- visits_data %>%
    filter(!StabilityAssessment %in% c("NULL", "")) %>%
    mutate(recent = ifelse(Sys.Date() - VisitDate < 400, 1, 0)) %>%
    filter(recent == 1) %>%
    group_by(PatientID) %>%
    arrange(desc(VisitDate)) %>%
    mutate(rownum = row_number()) %>%
    filter(rownum == 1) %>%
    select(-rownum) %>%
    select(PatientID, StabilityAssessment)
  
  # Combine all features 
  visits_out <- merge(pregnant, bmi, by = "PatientID", all = TRUE) %>%
    merge(., diff_care, by = "PatientID", all = TRUE) %>%
    merge(., adherence_all, by = "PatientID", all = TRUE) %>%
    merge(., stab, by = "PatientID", all = TRUE) %>%
    merge(., visits_last5, by = "PatientID", all = TRUE)
  
  visits_out$art_poor_adherence_rate <- visits_out$num_poor_ART / visits_out$num_adherence_ART
  visits_out$art_fair_adherence_rate <- visits_out$num_fair_ART / visits_out$num_adherence_ART
  visits_out$ctx_poor_adherence_rate <- visits_out$num_poor_CTX / visits_out$num_adherence_CTX
  visits_out$ctx_fair_adherence_rate <- visits_out$num_fair_CTX / visits_out$num_adherence_CTX
  visits_out$unscheduled_rate <- visits_out$n_unscheduled_lastfive / visits_out$n_visits_lastfive
  
  visits_out
  
}

genVLInputs <- function(lab){
  
  lab <- lab[lab$PatientID %in% PATIENTS, ]
  
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
    filter(Sys.Date() - ReportedByDate < 1000) %>%
    group_by(PatientID) %>%
    summarize(n_tests_threeyears = n(),
              n_hvl_threeyears = sum(vs == "HVL"))
  
  lab_recent <- lab %>%
    group_by(PatientID) %>%
    arrange(desc(ReportedByDate)) %>%
    mutate(rownum = row_number()) %>%
    filter(rownum == 1) %>%
    rename("most_recent_vl" = vs) %>%
    select(PatientID, most_recent_vl)
  
  lab_out <- merge(lab_patient, lab_recent, by = "PatientID", all = TRUE) %>%
    merge(., lab_lastthreeyears, by = "PatientID", all = TRUE)
  
  lab_out$recent_hvl_rate <- lab_out$n_hvl_threeyears / lab_out$n_tests_threeyears
  lab_out$total_hvl_rate <- lab_out$n_hvl_all / lab_out$n_tests_all
  
  lab_out
  
}

genDemographicInputs <- function(dem, art){
  
  names(dem)[1] <- 'SiteCode'
  dem$SiteCode <- as.character(dem$SiteCode)
  
  # basically just cleaning this up
  # filter art to id's in dem and join on dem
  # Filter to PatientID from dem
  art <- art[art$PatientID %in% PATIENTS, ]
  dem <- dem[dem$PatientID %in% PATIENTS, ]
  
  # Filter to active and LTFU
  # art$ExitReason <- tolower(art$ExitReason)
  # art <- art[!art$ExitReason %in% c("completed", "dead", "died", "discontinue",
  #                                   "toxicity, drug", "transfer out", "transfer_out",
  #                                   "transferred out", "treatment complete", "unknown"), ]
  
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
  dem$Age <- as.numeric(dem$Age)
  dem$Age[dem$Age > 100] <- NA
  
  # Time to start ART - this will StartARTDate - DateConfirmedHIVPositive
  # dem$DateConfirmedHIVPositive <- ymd(dem$DateConfirmedHIVPositive)
  dem$StartARTDate <- ymd(dem$StartARTDate)
  # dem$timeToStartArt <- dem$StartARTDate - dem$DateConfirmedHIVPositive
  
  # Time on ART - this will be second most recent visit - ART Start Date (need to join on that)
  dem$timeOnArt <- Sys.Date() - dem$StartARTDate
  
  # Age at which tested positive (age - (how many years ago was art start date))
  dem$artstart_yearsago <- floor((Sys.Date() - dem$StartARTDate)/365)
  dem$AgeARTStart <- as.numeric(dem$Age - dem$artstart_yearsago)
  
  # Patient Source
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
  
  # Convert timeonart to months
  dem$timeOnArt <- as.numeric(floor(dem$timeOnArt / 30))
  
  # Final cleaning
  dem$Age[dem$Age < 0] <- NA
  dem$AgeARTStart[dem$AgeARTStart < 0] <- NA
  dem$timeOnArt[dem$timeOnArt < 0] <- NA
  
  
  dem <- dem %>%
    select(PatientID, SiteCode, Gender, PatientSource, MaritalStatus, PopulationType,
           Age, timeOnArt, AgeARTStart)
  
  dem
  
}



encodeXGBoost <- function(dataset){
  # Need to one-hot encode all the factor variables
  ohe_features <- names(dataset)[ sapply(dataset, is.character) ]
  
  # Gender
  dataset$GenderFemale <- ifelse(dataset$Gender == "Female", 1,
                                 ifelse(is.na(dataset$Gender), NA, 0))
  dataset$GenderMale <- ifelse(dataset$Gender == "Male", 1,
                                 ifelse(is.na(dataset$Gender), NA, 0))
  
  # PatientSource
  dataset$PatientSourceCCC <- ifelse(dataset$PatientSource == "CCC", 1,
                               ifelse(is.na(dataset$PatientSource), NA, 0))
  dataset$PatientSourceIPDAdult <- ifelse(dataset$PatientSource == "IPDAdult", 1,
                                     ifelse(is.na(dataset$PatientSource), NA, 0))
  dataset$PatientSourceMCH <- ifelse(dataset$PatientSource == "MCH", 1,
                                     ifelse(is.na(dataset$PatientSource), NA, 0))
  dataset$PatientSourceOPD <- ifelse(dataset$PatientSource == "OPD", 1,
                                     ifelse(is.na(dataset$PatientSource), NA, 0))
  dataset$PatientSourceOther <- ifelse(dataset$PatientSource == "Other", 1,
                                     ifelse(is.na(dataset$PatientSource), NA, 0))
  dataset$PatientSourceTBClinic <- ifelse(dataset$PatientSource == "TBClinic", 1,
                                     ifelse(is.na(dataset$PatientSource), NA, 0))
  dataset$PatientSourceVCT <- ifelse(dataset$PatientSource == "VCT", 1,
                                     ifelse(is.na(dataset$PatientSource), NA, 0))
 
  # Marital Status
  dataset$MaritalStatusDivorced <- ifelse(dataset$MaritalStatus == "Divorced", 1,
                                     ifelse(is.na(dataset$MaritalStatus), NA, 0))
  dataset$MaritalStatusMarried <- ifelse(dataset$MaritalStatus == "Married", 1,
                                          ifelse(is.na(dataset$MaritalStatus), NA, 0))
  dataset$MaritalStatusOther <- ifelse(dataset$MaritalStatus == "Other", 1,
                                          ifelse(is.na(dataset$MaritalStatus), NA, 0))
  dataset$MaritalStatusPolygamous <- ifelse(dataset$MaritalStatus == "Polygamous", 1,
                                          ifelse(is.na(dataset$MaritalStatus), NA, 0))
  dataset$MaritalStatusSingle <- ifelse(dataset$MaritalStatus == "Single", 1,
                                          ifelse(is.na(dataset$MaritalStatus), NA, 0))
  dataset$MaritalStatusWidow <- ifelse(dataset$MaritalStatus == "Widow", 1,
                                          ifelse(is.na(dataset$MaritalStatus), NA, 0))
  
  # PopulationType
  dataset$PopulationTypeGeneralPopulation <- ifelse(dataset$PopulationType == "GeneralPopulation", 1,
                                       ifelse(is.na(dataset$PopulationType), NA, 0))
  dataset$PopulationTypeKeyPopulation <- ifelse(dataset$PopulationType == "KeyPopulation", 1,
                                                    ifelse(is.na(dataset$PopulationType), NA, 0))
  
  # TreatmentType
  dataset$TreatmentTypeART <- ifelse(dataset$TreatmentType == "ART", 1,
                                                    ifelse(is.na(dataset$TreatmentType), NA, 0))
  dataset$TreatmentTypePMTCT <- ifelse(dataset$TreatmentType == "PMTCT", 1,
                                     ifelse(is.na(dataset$TreatmentType), NA, 0))
  
  #Optimized HIV Regimen
  dataset$OptimizedHIVRegimenYes <- ifelse(dataset$OptimizedHIVRegimen == "Yes", 1,
                                       ifelse(is.na(dataset$OptimizedHIVRegimen), NA, 0))
  dataset$OptimizedHIVRegimenNo <- ifelse(dataset$OptimizedHIVRegimen == "No", 1,
                                           ifelse(is.na(dataset$OptimizedHIVRegimen), NA, 0))

  #Other Regimen
  dataset$Other_RegimenYes <- ifelse(dataset$Other_Regimen == "Yes", 1,
                                           ifelse(is.na(dataset$Other_Regimen), NA, 0))
  dataset$Other_RegimenNo <- ifelse(dataset$Other_Regimen == "No", 1,
                                     ifelse(is.na(dataset$Other_Regimen), NA, 0))
  
  #Pregnant
  dataset$PregnantYes <- ifelse(dataset$Pregnant == "Yes", 1,
                                     ifelse(is.na(dataset$Pregnant), NA, 0))
  dataset$PregnantNo <- ifelse(dataset$Pregnant == "No", 1,
                                    ifelse(is.na(dataset$Pregnant), NA, 0))
  
  # Differentiated Care
  dataset$DifferentiatedCareStandardCare <- ifelse(dataset$DifferentiatedCare == "StandardCare", 1,
                                                   ifelse(is.na(dataset$DifferentiatedCare), NA, 0))
  dataset$DifferentiatedCareFastTrack <- ifelse(dataset$DifferentiatedCare == "FastTrack", 1,
                                                   ifelse(is.na(dataset$DifferentiatedCare), NA, 0))
  dataset$DifferentiatedCareFacilityARTdistributionGroup <- ifelse(dataset$DifferentiatedCare == "FacilityARTdistributionGroup", 1,
                                                   ifelse(is.na(dataset$DifferentiatedCare), NA, 0))
  dataset$DifferentiatedCareCommunityARTDistributionpeerled <- ifelse(dataset$DifferentiatedCare == "CommunityARTDistributionpeerled", 1,
                                                   ifelse(is.na(dataset$DifferentiatedCare), NA, 0))
  dataset$DifferentiatedCareCommunityARTDistributionHCWLed <- ifelse(dataset$DifferentiatedCare == "CommunityARTDistributionHCWLed", 1,
                                                   ifelse(is.na(dataset$DifferentiatedCare), NA, 0))
  
  # ART Adherence
  dataset$most_recent_art_adherencegood <-  ifelse(dataset$most_recent_art_adherence == "good", 1,
                                                ifelse(is.na(dataset$most_recent_art_adherence), NA, 0))
  dataset$most_recent_art_adherencefair <-  ifelse(dataset$most_recent_art_adherence == "fair", 1,
                                                   ifelse(is.na(dataset$most_recent_art_adherence), NA, 0))
  dataset$most_recent_art_adherencepoor <-  ifelse(dataset$most_recent_art_adherence == "poor", 1,
                                                   ifelse(is.na(dataset$most_recent_art_adherence), NA, 0))
  
  # CTX Adherence
  dataset$most_recent_ctx_adherencegood <-  ifelse(dataset$most_recent_ctx_adherence == "good", 1,
                                                   ifelse(is.na(dataset$most_recent_ctx_adherence), NA, 0))
  dataset$most_recent_ctx_adherencefair <-  ifelse(dataset$most_recent_ctx_adherence == "fair", 1,
                                                   ifelse(is.na(dataset$most_recent_ctx_adherence), NA, 0))
  dataset$most_recent_ctx_adherencepoor <-  ifelse(dataset$most_recent_ctx_adherence == "poor", 1,
                                                   ifelse(is.na(dataset$most_recent_ctx_adherence), NA, 0))
  
  # Stability Assessment
  dataset$StabilityAssessmentStable <- ifelse(dataset$StabilityAssessment == "Stable", 1,
                                              ifelse(is.na(dataset$StabilityAssessment), NA, 0))
  dataset$StabilityAssessmentUnstable <- ifelse(dataset$StabilityAssessment == "Unstable", 1,
                                              ifelse(is.na(dataset$StabilityAssessment), NA, 0))
 
  # VL
  dataset$most_recent_vlHVL <- ifelse(dataset$most_recent_vl == "HVL", 1,
                                      ifelse(is.na(dataset$most_recent_vl), NA, 0))
  dataset$most_recent_vlLVL <- ifelse(dataset$most_recent_vl == "LVL", 1,
                                      ifelse(is.na(dataset$most_recent_vl), NA, 0))
  dataset$most_recent_vlSuppressed <- ifelse(dataset$most_recent_vl == "Suppressed", 1,
                                      ifelse(is.na(dataset$most_recent_vl), NA, 0))
  
  
  dataset[, !(names(dataset) %in% ohe_features)]
  
}


