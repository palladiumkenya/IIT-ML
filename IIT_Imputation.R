setwd("~/Kenya/IIT")

library(dplyr)
library(caret)
library(mice)
library(testthat)

# First, let's read in and combine input tables ----------------
demographics <- readRDS("demographics.rds")
regimen <- readRDS("regimen.rds")
visits <- readRDS("visit_features.rds")
lateness <- readRDS("lateness.rds")
lab <- readRDS("lab.rds")
target <- readRDS("target.rds")

all <- merge(target, demographics, by = "PatientID") %>%
  merge(., regimen, by = "PatientID", all.x = TRUE) %>%
  merge(., visits, by = "PatientID", all.x = TRUE) %>%
  merge(., lateness, by = "PatientID", all.x = TRUE) %>%
  merge(., lab, by = "PatientID", all.x = TRUE) %>%
  data.frame(.)

# Rename a few variables for clarity target variable
all <- all %>% rename("target" = visitdiff,
                      "most_recent_vl" = most_recent,
                      "most_recent_art_adherence" = ART,
                      "most_recent_ctx_adherence" = CTX)


# Some final prep ------------
# drop some columns we won't use
all <- all %>% 
  dplyr::select(-timeToStartArt, - StartARTDate, -WhoStage, -OI, -OI_lastyear, -PatientID)

## convert and/or add some column combinations to ratios

# Get recent and total viral suppression rate
all$recent_hvl_rate <- all$n_hvl_threeyears / all$n_tests_threeyears
all$total_hvl_rate <- all$n_hvl_all / all$n_tests_all

# Get poor and fair adherence rates
all$art_poor_adherence_rate <- all$num_poor_ART / all$num_adherence_ART
all$art_fair_adherence_rate <- all$num_fair_ART / all$num_adherence_ART
all$ctx_poor_adherence_rate <- all$num_poor_CTX / all$num_adherence_CTX
all$ctx_fair_adherence_rate <- all$num_fair_CTX / all$num_adherence_CTX

# Get recent and overall lateness rates
all$unscheduled_rate <- all$n_unscheduled_lastfive / all$n_visits_lastfive
all$all_late30_rate <- all$missed30 / all$n_appts
all$all_late5_rate <- all$missed5 / all$n_appts
all$all_late1_rate <- all$missed1 / all$n_appts
all$recent_late30_rate <- all$missed30_last5 / all$n_appts_last5
all$recent_late5_rate <- all$missed5_last5 / all$n_appts_last5
all$recent_late1_rate <- all$missed1_last5 / all$n_appts_last5

# Convert timeonart to months
all$timeOnArt <- as.numeric(floor(all$timeOnArt / 30))

# Final cleaning
all$Age[all$Age < 0] <- NA
all$AgeARTStart[all$AgeARTStart < 0] <- NA
all$timeOnArt[all$timeOnArt < 0] <- NA

# Add in GIS features -------------------------
gis <- readRDS('gis_features.rds') %>% dplyr::select(-Latitude, -Longitude)
all$SiteCode <- as.character(all$SiteCode)
all <- merge(all, gis,by.x = "SiteCode", by.y = "FacilityCode", all.x = TRUE) %>%
  dplyr::select(-SiteCode)

# all$Gender <- as.factor(all$Gender)
# all$PatientSource <- as.factor(all$PatientSource)
# all$MaritalStatus <- as.factor(all$MaritalStatus)
# all$PopulationType <- as.factor(all$PopulationType)
# all$TreatmentType <- as.factor(all$TreatmentType)
# all$OptimizedHIVRegimen <- as.factor(all$OptimizedHIVRegimen)
# all$Other_Regimen <- as.factor(all$Other_Regimen)
# all$Pregnant <- as.factor(all$Pregnant)
# all$DifferentiatedCare <- as.factor(all$DifferentiatedCare)
# all$most_recent_art_adherence <- as.factor(all$most_recent_art_adherence)
# all$most_recent_ctx_adherence <- as.factor(all$most_recent_ctx_adherence)
# all$StabilityAssessment <- as.factor(all$StabilityAssessment)
# all$most_recent_vl <- as.factor(all$most_recent_vl)

# Save out sparse dataset --------
# saveRDS(all, "iit_sparse.rds")

# Simple Imputation -------------

set.seed(2231)

#partition and create training, testing data
split <- createDataPartition(y = all$target,p = 0.8,list = FALSE)

train_all <- all[split, ] 
test <- all[-split, ]

split_val <- createDataPartition(y = train_all$target,p = 0.75,list = FALSE)

train <- train_all[split_val, ]
val <- train_all[-split_val, ]

sparse <- list("sparse_train" = train,
               "sparse_val" = val,
               "sparse_test" = test)

Mode <- function(x) {
  ux <- unique(x)
  ux[which.max(tabulate(match(x, ux)))]
}
replaceWithMode <- function(dataset_calc, dataset_impute, position){
  dataset_impute[, position] <- ifelse(is.na(dataset_impute[, position]),
                                Mode(dataset_calc[, position][!is.na(dataset_calc[,position])]),
                                dataset_impute[, position])
}
replaceWithMean <- function(dataset_calc, dataset_impute, position){
  dataset_impute[, position] <- ifelse(is.na(dataset_impute[, position]),
                                mean(dataset_calc[, position], na.rm = TRUE),
                                dataset_impute[, position])
}

train <- data.frame(train)
val <- data.frame(val)
test <- data.frame(test)

for(i in c(2:5, 10:12, 17:18, 25:27, 40)){
  train[, i] <- replaceWithMode(train, train, i)
  val[, i] <- replaceWithMode(train, val, i)
  test[, i] <- replaceWithMode(train, test, i)
}

for(i in c(6:9, 13:16, 19:24, 28:39, 41:73)){
  train[, i] <- replaceWithMean(train, train, i)
  val[, i] <- replaceWithMean(train, val, i)
  test[, i] <- replaceWithMean(train, test, i)
}

simple <- list("simple_train" = train,
               "simple_val" = val,
               "simple_test" = test)

# MICE Imputation -----------------

mice.reuse <- function(mids, newdata, maxit = 5, printFlag = TRUE, seed = NA){


  if(is.na(seed)){
    assign(".Random.seed", mids$lastSeedValue, pos = 1)
  } else {
    set.seed(seed)
  }

  # Check that the newdata is the same as the old data
  rows <- nrow(newdata)
  cols <- ncol(newdata)
  expect_equal(cols, ncol(mids$data))

  nm <- names(newdata)
  expect_equal(nm, names(mids$data))

  # Set up a mids object for the newdata, but set all variables to missing
  all_miss <- matrix(TRUE, rows, cols, dimnames = list(seq_len(rows), nm))
  mids.new <- mice(newdata, mids$m, where = all_miss, maxit = 0,
                   remove.collinear = FALSE, remove.constant = FALSE)

  # Combine the old (trained) and the new mids objects
  mids.comb <- mids.append(mids, mids.new)
  new_idx <- mids.comb$app_idx
  mids.comb <- mids.comb$mids

  mids.comb$lastSeedValue <- .Random.seed # set the seed to the current value

  # Set the newdata to missing (so it doesn't influence the imputation)
  # but remember the actual values
  actual_data <- mids.comb$data
  mids.comb$data[mids.comb$where] <- NA

  # Also make sure all observed variables in the newdata are set to their
  # true values in the imputations
  for(j in names(mids.comb$imp)){
    for(i in seq_len(mids.comb$m)){
      mids.comb$imp[[j]][, i] <-
        replace_overimputes(actual_data, mids.comb$imp, j, i)
    }
  }

  cond_imp <- "imp[[j]][, i] <- replace_overimputes(fetch_data(), imp, j, i)"
  mids.comb$post <- sapply(mids.comb$post,
                           function(x) if(x != "") paste0(x, "; ", cond_imp)
                           else cond_imp)

  # Run the procedure for a few times
  mids.comb <- mice.mids(mids.comb, maxit = maxit, printFlag = printFlag)

  # Return the imputed test dataset
  res <- lapply(complete(mids.comb, "all"), function(x) x[new_idx, ])
  class(res) <- c("mild", "list")
  res
}


mids.append <- function(x, y){

  expect_equal(names(x$data), names(y$data))
  app <- x

  miss_xy <- intersect(names(x$nmis), names(y$nmis))
  expect_true(all(names(y$nmis) %in% miss_xy))

  # Append `data`
  app$data <- rbind(x$data, y$data)
  x_idx <- rownames(x$data)
  y_idx <- base::setdiff(rownames(app$data), x_idx)
  names(y_idx) <- rownames(y$data)

  # Append `imp` and `nmis`
  for(i in names(x$imp)){
    if(i %in% miss_xy){
      # Imputations
      app_imp <- y$imp[[i]]
      rownames(app_imp) <- y_idx[rownames(app_imp)]
      app$imp[[i]] <- rbind(x$imp[[i]], app_imp)

      # nmis
      app$nmis[[i]] <- x$nmis[[i]] + y$nmis[[i]]
    }
  }

  # Append `where`
  app$where <- rbind(x$where, y$where)
  rownames(app$where) <- rownames(app$data)

  list(mids = app, app_idx = setNames(y_idx, NULL))
}


replace_overimputes <- function(data, imp, j, i){

  # Find those values that weren't missing but imputed
  overlap <- base::intersect(
    rownames(data[!is.na(data[, j]), j, drop = FALSE]),
    rownames(imp[[j]])
  )

  # Replace them with the true values and return
  imp[[j]][overlap, i] <- data[overlap, j]
  imp[[j]][, i]
}


fetch_data <- function(){

  get('actual_data', pos = parent.frame(5))
}

# Select variables for imputation (no target variable and no variables that are perfectly collinear)
cols_to_not_impute <- c("target", "condom", "pnc", "pregnancies")
all_to_impute <- all[, !names(all) %in% cols_to_not_impute]

iit_impute_mice <- mice(all_to_impute,
                        m = 5,
                        maxit = 5,
                        seed = 2231)
iit_impute_mice_train <- complete(iit_impute_mice, action = "broad") # get all imputations

# Get variables with missing fields
vars_to_impute <- names(all_to_impute)[which(apply(all_to_impute, 2, function(x) any(is.na(x))))]

# loop through vars_to_impute and average imputations
for(i in vars_to_impute){
  if(is.character(all[, i])){
    # for factors, take mode
    all_to_impute[,i] <- apply(iit_impute_mice_train[, c(paste0(i, ".1"), paste0(i, ".2"),
                                                                  paste0(i, ".3"),paste0(i, ".4"), paste0(i, ".5"))],
                                        1, function(x) unique(x)[which.max(tabulate(match(x, unique(x))))])
  } else if(!is.character(all[, i])){
    # for numerics, take mean
    all_to_impute[,i] <- apply(iit_impute_mice_train[, c(paste0(i, ".1"), paste0(i, ".2"),
                                                           paste0(i, ".3"), paste0(i, ".4"), paste0(i, ".5"))],
                                                           1, function(x) mean(x))
  }
}

train_all <- all_to_impute[split, ]
test <- all_to_impute[-split, ]
train <- train_all[split_val, ]
val <- train_all[-split_val, ]

micelist <- list("mice_train" = train,
                 "mice_val" = val,
                 "mice_test" = test)

outlist <- c(sparse, simple, micelist)
saveRDS(outlist, "iit_outlist.rds")
