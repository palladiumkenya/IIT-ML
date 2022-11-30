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


# Simple Imputation -------------

#partition and create training, testing data
set.seed(2231)

#partition and create training, testing data
split <- createDataPartition(y = all$target,p = 0.6,list = FALSE)
train <- all[split, ]
test <- all[-split, ]

split_val <- createDataPartition(y = test$target,p = 0.5,list = FALSE)
val <- test[split_val, ]
test <- test[-split_val, ]

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
# MICE
# Select variables for imputation (no target variable and no variables that are perfectly collinear)
cols_to_not_impute <- c("target", "condom", "pnc", "pregnancies")

# get character columns
char_cols <- names(all[, sapply(all, class) == 'character'])

# convert character to factor?
for(i in char_cols){
  all[, i] <- as.factor(all[, i])
}

train_to_impute <- all[split, !names(all) %in% cols_to_not_impute]
val_to_impute_total <- all[-split, !names(all) %in% cols_to_not_impute]



all_impute_mice <- mice(train_to_impute,
                        pred = quickpred(train_to_impute, minpuc = 0.1),
                        m = 5,
                        maxit = 5,
                        seed = 2231)

all_impute_mice_train <- complete(all_impute_mice, action = "broad") # get all imputations

# Get variables with missing fields
vars_to_impute <- names(train_to_impute)[which(apply(train_to_impute, 2, function(x) any(is.na(x))))]

# loop through vars_to_impute and average imputations
for(i in vars_to_impute){
  for(j in 1:nrow(train_to_impute)){
    if(is.na(train_to_impute[j,i]) & is.factor(train_to_impute[, i])){
      x <- c(all_impute_mice_train[j, paste0(i, ".1")],
             all_impute_mice_train[j, paste0(i, ".2")],
             all_impute_mice_train[j, paste0(i, ".3")],
             all_impute_mice_train[j, paste0(i, ".4")],
             all_impute_mice_train[j, paste0(i, ".5")])
      ux <- unique(x)
      train_to_impute[j,i] <- ux[which.max(tabulate(match(x, ux)))]
    }
    if(is.na(train_to_impute[j,i]) & !is.numeric(train_to_impute[, i])){
      x <- c(all_impute_mice_train[j, paste0(i, ".1")],
             all_impute_mice_train[j, paste0(i, ".2")],
             all_impute_mice_train[j, paste0(i, ".3")],
             all_impute_mice_train[j, paste0(i, ".4")],
             all_impute_mice_train[j, paste0(i, ".5")])
      train_to_impute[j,i] <- mean(x)
    }
  }
}

train_to_impute$target <- all[split, "target"]

## Val set
# convert character to factor?
char_cols <- char_cols[char_cols != "target"]
for(i in char_cols){
  val_to_impute_total[, i] <- as.factor(val_to_impute_total[, i])
}

vals_mice_wide <- mice.mids(all_impute_mice, newdata = val_to_impute_total)
vals_mice_wide <- complete(vals_mice_wide, action = "broad") # get all imputations

# loop through vars_to_impute and average imputations
for(i in vars_to_impute){
  for(j in 1:nrow(val_to_impute_total)){
    if(is.na(val_to_impute_total[j,i]) & is.factor(val_to_impute_total[, i])){
      x <- c(vals_mice_wide[j, paste0(i, ".1")],
             vals_mice_wide[j, paste0(i, ".2")],
             vals_mice_wide[j, paste0(i, ".3")],
             vals_mice_wide[j, paste0(i, ".4")],
             vals_mice_wide[j, paste0(i, ".5")])
      ux <- unique(x)
      val_to_impute_total[j,i] <- ux[which.max(tabulate(match(x, ux)))]
    }
    if(is.na(val_to_impute_total[j,i]) & is.numeric(val_to_impute_total[, i])){
      x <- c(vals_mice_wide[j, paste0(i, ".1")],
             vals_mice_wide[j, paste0(i, ".2")],
             vals_mice_wide[j, paste0(i, ".3")],
             vals_mice_wide[j, paste0(i, ".4")],
             vals_mice_wide[j, paste0(i, ".5")])
      val_to_impute_total[j,i] <- mean(x)
    }
  }
}
val_to_impute_total$target <- all[-split, "target"]
val_mice <- val_to_impute_total[split_val, ]
test_mice <- val_to_impute_total[-split_val, ]

micelist <- list("mice_train" = train_to_impute,
                 "mice_val" = val_mice,
                 "mice_test" = test_mice)

outlist <- c(sparse, simple, micelist)

# get columns with missing values
train_sparse <- outlist$sparse_train
val_sparse <- outlist$sparse_val
test_sparse <- outlist$sparse_test

# get columns with missing values from sparse dataset
sparse_cols <- c()
for(i in 1:ncol(train_sparse)){
  vec <- train_sparse[, i]
  if(any(is.na(vec))){
    sparse_cols <- c(sparse_cols, names(train_sparse)[i])
  }
}

# take just sparse columns
sparse_sub <- train_sparse[, names(train_sparse) %in% sparse_cols]
# change names of subset dataframe to indicate that these are binary variables
names(sparse_sub) <- paste0(names(sparse_sub), "_bin")
# Convert present values to 1, missing values to 0
sparse_sub[!is.na(sparse_sub)] <- 1
sparse_sub[is.na(sparse_sub)] <- 0
sparse_sub <- sparse_sub %>% mutate_all(as.numeric)
# Column bind this to simple and mice train sets
outlist$simple_train <- cbind(outlist$simple_train, sparse_sub)
outlist$mice_train <- cbind(outlist$mice_train, sparse_sub)

# Repeat process for validation set
sparse_sub <- val_sparse[, names(val_sparse) %in% sparse_cols]
names(sparse_sub) <- paste0(names(sparse_sub), "_bin")
sparse_sub[!is.na(sparse_sub)] <- 1
sparse_sub[is.na(sparse_sub)] <- 0
sparse_sub <- sparse_sub %>% mutate_all(as.numeric)
outlist$simple_val <- cbind(outlist$simple_val, sparse_sub)
outlist$mice_val <- cbind(outlist$mice_val, sparse_sub)

# Repeat process for test set.
sparse_sub <- test_sparse[, names(test_sparse) %in% sparse_cols]
names(sparse_sub) <- paste0(names(sparse_sub), "_bin")
sparse_sub[!is.na(sparse_sub)] <- 1
sparse_sub[is.na(sparse_sub)] <- 0
sparse_sub <- sparse_sub %>% mutate_all(as.numeric)
outlist$simple_test <- cbind(outlist$simple_test, sparse_sub)
outlist$mice_test <- cbind(outlist$mice_test, sparse_sub)


saveRDS(outlist, "iit_outlist_1126.rds")
