## This script explores the best performing model using Shapley values and uses the best performing model
## to generate an AUC-PR on the test set and to generate test set predictions for review.
##
## Author: Yoni Friedman
## Last edited: May 20, 2021


library(caret)
library(dplyr)
library(lime)
library(xgboost)
library(PRROC)
library(ggplot2)
library(tidyr)

setwd("~/Kenya/NDHW Data/TestData/HTS_App/FHW App/HomaBay")
hts <- readRDS('./hts_homabay_imputed_0520.rds')


# XGBoost requires only numeric inputs, so one-hot encode all categorical variables
encodeXGBoost <- function(dataset){
  dataset$FinalTestResult <- if_else(dataset$FinalTestResult == "Positive", 1, 0)
  # Need to one-hot encode all the factor variables
  ohe_features <- names(dataset)[ sapply(dataset, is.factor) ]
  
  dmy <- dummyVars("~ Gender + KeyPopulationType + MaritalStatus + PatientDisabled + EverTestedForHIV +
                   ClientTestedAs + EntryPoint + TestingStrategy + TBScreening + ClientSelfTested +
                   month_of_test + dayofweek",
                   data = dataset)
  ohe <- data.frame(predict(dmy, newdata = dataset))
  dataset <- cbind(dataset, ohe)
  
  dataset[, !(names(dataset) %in% ohe_features)]
  
}

hts_sparse_train <- encodeXGBoost(hts$sparse$train) %>%
  select(-Facility.Name, -Longitude, -Latitude, -Sitecode)
hts_sparse_val <- encodeXGBoost(hts$sparse$val) %>%
  select(-Facility.Name, -Longitude, -Latitude, -Sitecode)
hts_sparse_test <- encodeXGBoost(hts$sparse$test) %>%
  select(-Facility.Name, -Longitude, -Latitude, -Sitecode)

# Train best-performing model --------------
set.seed(2231)
xgb <- xgboost::xgboost(data = data.matrix(hts_sparse_train[,which(names(hts_sparse_train) != "FinalTestResult")]), 
                        label = hts_sparse_train$FinalTestResult, 
                        eta = .1,
                        max_depth = 12, 
                        colsample_bytree = .3,
                        nrounds = 100,
                        objective = "binary:logistic",
                        metric = 'auc',
                        verbose = 0
)

val_predict <- predict(xgb,newdata = data.matrix(hts_sparse_val[, which(names(hts_sparse_val) != "FinalTestResult")]))
fg <- val_predict[hts_sparse_val$FinalTestResult == 1]
bg <- val_predict[hts_sparse_val$FinalTestResult == 0]
prc <- pr.curve(scores.class0 = fg, scores.class1 = bg, curve = T)
plot(prc)

# Generate table by risk category
hts_sparse_val$risk <- "Low"
hts_sparse_val[val_predict >= 0.002625179 & val_predict < 0.0106387809, "risk"] <- "Medium"
hts_sparse_val[val_predict >= 0.0106387809 & val_predict < 0.0289241020, "risk"] <- "High"
hts_sparse_val[val_predict >= 0.0289241020, "risk"] <- "Highest"

hts_sparse_val %>%
  group_by(risk) %>%
  summarize(count = n(),
            positives = sum(FinalTestResult == 1),
            negatives = sum(FinalTestResult == 0))

# saveRDS(xgb,'homabay_model_20210713.rds')

# Shapley Values -----------------
source('./shap.R')
## Calculate shap values
shap_result = shap.score.rank(xgb_model = xgb, 
                              X_train = data.matrix(hts_sparse_train[,which(names(hts_sparse_train) != "FinalTestResult")]),
                              shap_approx = F
)

## Prepare data for top N variables
shap_long = shap.prep(shap = shap_result,
                      X_train = data.matrix(hts_sparse_train[,which(names(hts_sparse_train) != "FinalTestResult")]), 
                      top_n = 25
)

## Plot shap overall metrics
plot.shap.summary(data_long = shap_long)


## 
xgb.plot.shap(data = data.matrix(hts_sparse_train[,which(names(hts_sparse_train) != "FinalTestResult")]), # input data
              model = xgb, # xgboost model
              features = names(shap_result$mean_shap_score[1:25]), # only top 10 var
              n_col = 5, # layout option
              plot_loess = T # add red line to plot
)

# LIME Analysis --------------------------------
xgb_explained <- lime(hts_sparse_train[, !(names(hts_sparse_train) %in% 'FinalTestResult')],
                      bin_continuous = TRUE,
                      n_bins = 5,
                      quantile_bins = TRUE,
                      # use_density = TRUE,
                      model = xgb)

explanations <- lime::explain(x=hts_sparse_val[1:1000, !(names(hts_sparse_val) %in% 'FinalTestResult')],
                              explainer=xgb_explained,
                              label = "1",
                              n_features = 10,
                              n_permutations= 1000)

explanations_overall <- explanations %>%
  filter(!feature %in% c("PC1", "PC2", "PC3", "PC4", "PC5", "PC6", "PC7", "PC8", "PC9", "PC10")) %>%
  group_by(feature) %>%
  mutate(count = n()) %>%
  filter(count >= 100) %>%
  select(-count)
# Overall, at the individual level, weather just doesn't matter as much as individual characteristics
plot_explanations(explanations_overall)

# Explain Positive Predictions
top_20_positives <- explanations %>%
  arrange(desc(label_prob), case)

plot_features(top_20_positives[1:20, ], ncol = 2)
plot_features(top_20_positives[21:40, ], ncol = 2)
plot_features(top_20_positives[41:60, ], ncol = 2)
plot_features(top_20_positives[61:80, ], ncol = 2)
plot_features(top_20_positives[81:100, ], ncol = 2)
plot_features(top_20_positives[101:120, ], ncol = 2)
plot_features(top_20_positives[121:140, ], ncol = 2)
plot_features(top_20_positives[141:160, ], ncol = 2)
plot_features(top_20_positives[161:180, ], ncol = 2)
plot_features(top_20_positives[181:200, ], ncol = 2)

explanations <- lime::explain(x=hts_sparse_val[1:1000, !(names(hts_sparse_val) %in% 'FinalTestResult')],
                              explainer=xgb_explained,
                              label = "0",
                              n_features = 10,
                              n_permutations= 1000)

# Explain Positive Predictions
top_20_negatives <- explanations %>%
  arrange(label_prob, case)

plot_features(top_20_negatives[1:20, ], ncol = 2)
plot_features(top_20_negatives[21:40, ], ncol = 2)
plot_features(top_20_negatives[41:60, ], ncol = 2)
plot_features(top_20_negatives[61:80, ], ncol = 2)
plot_features(top_20_negatives[81:100, ], ncol = 2)
plot_features(top_20_negatives[101:120, ], ncol = 2)
plot_features(top_20_negatives[121:140, ], ncol = 2)
plot_features(top_20_negatives[141:160, ], ncol = 2)
plot_features(top_20_negatives[161:180, ], ncol = 2)
plot_features(top_20_negatives[181:200, ], ncol = 2)


# Generate cutoffs for each facility ----------------------
hts_for_cutoffs <- encodeXGBoost(hts$sparse$test) %>%
  select(-Longitude, -Latitude, -Sitecode)
facilities_for_cutoff <- hts_for_cutoffs %>%
  .$Facility.Name %>%
  unique()

cutoff_list <- list()

for(i in facilities_for_cutoff){
  
  hts_tmp <- hts_for_cutoffs[hts_for_cutoffs$Facility.Name==i, ]
  if(nrow(hts_tmp)<200){next}
  hts_mod <- hts_tmp %>% select(xgb$feature_names)
  preds_tmp <- predict(xgb, newdata = data.matrix(hts_mod))
  fg <- preds_tmp[hts_tmp[, "FinalTestResult"] == 1]
  bg <- preds_tmp[hts_tmp[, "FinalTestResult"] == 0]
  prc <- pr.curve(scores.class0 = fg, scores.class1 = bg, curve = T)
  pr_df <- data.frame(prc$curve)
  names(pr_df) <- c("Recall", "Precision", "Threshold")
  thresh_75 <- pr_df %>%
    filter(Precision > .75) %>%
    filter(row_number() == 1)
  
  thresh_50 <- pr_df %>%
    filter(Precision > .5) %>%
    filter(row_number() == 1)
  
  thresh_25 <- pr_df %>%
    filter(Precision > .25) %>%
    filter(row_number() == 1)
  
  thresh_all <- rbind(thresh_75, thresh_50, thresh_25)
  
  cutoff_list[[i]] <- thresh_all 
  
}

# For facilities with fewer than 100 observations, use overall cutoffs
test_predict <- predict(xgb,newdata = data.matrix(hts_sparse_test[, which(names(hts_sparse_test) != "FinalTestResult")]))
fg <- test_predict[hts_sparse_test$FinalTestResult == 1]
bg <- test_predict[hts_sparse_test$FinalTestResult == 0]
prc <- pr.curve(scores.class0 = fg, scores.class1 = bg, curve = T)
pr_df <- data.frame(prc$curve)
names(pr_df) <- c("Recall", "Precision", "Threshold")
thresh_75 <- pr_df %>%
  filter(Precision > .75) %>%
  filter(row_number() == 1)

thresh_50 <- pr_df %>%
  filter(Precision > .5) %>%
  filter(row_number() == 1)

thresh_25 <- pr_df %>%
  filter(Precision > .25) %>%
  filter(row_number() == 1)

thresh_all <- rbind(thresh_75, thresh_50, thresh_25)

cutoff_list[["Overall"]] <- thresh_all

saveRDS(cutoff_list, paste0("cutoffs_", Sys.Date(), ".rds"))

# Generate predictions for test set -------------------------

# Get AUC-PR for test set
test_predict <- predict(xgb,newdata = data.matrix(hts_sparse_test[, which(names(hts_sparse_test) != "FinalTestResult")]))
fg <- test_predict[hts_sparse_test$FinalTestResult == 1]
bg <- test_predict[hts_sparse_test$FinalTestResult == 0]
prc <- pr.curve(scores.class0 = fg, scores.class1 = bg, curve = T)
plot(prc, curve = TRUE)
roc <- roc.curve(scores.class0 = fg, scores.class1 = bg, curve = T)
plot(roc, curve = TRUE)
# Break out for male/female and for over/under 15
# Male
test_predict_male <- predict(xgb,
                             newdata = data.matrix(hts_sparse_test[hts_sparse_test$Gender.Male==1, which(names(hts_sparse_test) != "FinalTestResult")]))
fg_male <- test_predict_male[hts_sparse_test[hts_sparse_test$Gender.Male==1, "FinalTestResult"] == 1]
bg_male <- test_predict_male[hts_sparse_test[hts_sparse_test$Gender.Male==1, "FinalTestResult"] == 0]
prc_male <- pr.curve(scores.class0 = fg_male, scores.class1 = bg_male, curve = T)
plot(prc_male, curve = TRUE)
# Baseline positive for Males - .4 is roughly 13X positivity rate
prop.table(table(filter(hts_sparse_test, Gender.Male==1)$FinalTestResult))

# Female
test_predict_female <- predict(xgb,
                             newdata = data.matrix(hts_sparse_test[hts_sparse_test$Gender.Female==1, which(names(hts_sparse_test) != "FinalTestResult")]))
fg_female <- test_predict_female[hts_sparse_test[hts_sparse_test$Gender.Female==1, "FinalTestResult"] == 1]
bg_female <- test_predict_female[hts_sparse_test[hts_sparse_test$Gender.Female==1, "FinalTestResult"] == 0]
prc_female <- pr.curve(scores.class0 = fg_female, scores.class1 = bg_female, curve = T)
plot(prc_female, curve = TRUE)
# Baseline positive for Females - .57 is roughly 9X positivity rate
prop.table(table(filter(hts_sparse_test, Gender.Female==1)$FinalTestResult))

# Over 15
test_predict_adult <- predict(xgb,
                             newdata = data.matrix(hts_sparse_test[hts_sparse_test$AgeAtTest>15, which(names(hts_sparse_test) != "FinalTestResult")]))
fg_adult <- test_predict_adult[hts_sparse_test[hts_sparse_test$AgeAtTest>15, "FinalTestResult"] == 1]
bg_adult <- test_predict_adult[hts_sparse_test[hts_sparse_test$AgeAtTest>15, "FinalTestResult"] == 0]
prc_adult <- pr.curve(scores.class0 = fg_adult, scores.class1 = bg_adult, curve = T)
plot(prc_adult, curve = TRUE)
# Baseline positive for over 15 - .55 is roughly 10X positivity rate
prop.table(table(filter(hts_sparse_test, AgeAtTest > 15)$FinalTestResult))

# Under 15
test_predict_child <- predict(xgb,
                              newdata = data.matrix(hts_sparse_test[hts_sparse_test$AgeAtTest<=15, which(names(hts_sparse_test) != "FinalTestResult")]))
fg_child <- test_predict_child[hts_sparse_test[hts_sparse_test$AgeAtTest<=15, "FinalTestResult"] == 1]
bg_child <- test_predict_child[hts_sparse_test[hts_sparse_test$AgeAtTest<=15, "FinalTestResult"] == 0]
prc_child <- pr.curve(scores.class0 = fg_child, scores.class1 = bg_child, curve = T)
plot(prc_child, curve = TRUE)
# Baseline positive for 15 and under - .22 is roughly 11X positivity rate
prop.table(table(filter(hts_sparse_test, AgeAtTest <= 15)$FinalTestResult))

# Append predictions to predictors and save out
hts_sparse_test <- encodeXGBoost(hts$sparse$test)
test_out <- cbind(test_predict, hts_sparse_test)
write.csv(test_out, './homabay_test_predictions.csv')

# Generate Thresholds for App --------------------------
pr_df <- data.frame(prc$curve)
names(pr_df) <- c("Recall", "Precision", "Threshold")

# Let's set thresholds at precision levels
# 75% of clients at this level or higher tested positive.
# Patient with this risk score of higher account for X% of all positive tests

# July 13: .529
thresh_75 <- pr_df %>%
  filter(Precision > .75) %>%
  filter(row_number() == 1)

# July 13: .197
thresh_50 <- pr_df %>%
  filter(Precision > .5) %>%
  filter(row_number() == 1)

#July 13: .049
thresh_25 <- pr_df %>%
  filter(Precision > .25) %>%
  filter(row_number() == 1)

# Generate plot of precision/recall by % of patients ---------------------
# Sort patients by probability of testing positive
test_sorted <- test_out %>% arrange(desc(test_predict))
num_pos <- sum(test_sorted$FinalTestResult)

vals <- c(round(nrow(test_sorted)*.01),
          round(nrow(test_sorted)*.03),
          round(seq(nrow(test_sorted)*.05, nrow(test_sorted), length.out = 20)))
pr_by_share <- test_sorted%>%
  mutate(tot_pos = cumsum(FinalTestResult),
         tot_patients = row_number()) %>%
  mutate(Precision = tot_pos / tot_patients,
         Recall = tot_pos / num_pos) %>%
  mutate(rownum = row_number()) %>%
  filter(rownum %in% vals) %>%
  mutate(Percent_Patients = round(rownum * 100 / max(vals))) %>%
  pivot_longer(cols = c("Recall", "Precision"), names_to = "Metric")

ggplot() +
  geom_line(data = pr_by_share, aes(x = Percent_Patients, y = value, color = Metric)) +
  xlab("Percent of Patients Tested") +
  ylab("") +
  scale_x_continuous(breaks = seq(5,100,length.out = 20)) +
  scale_color_manual(labels = c("Precision (% Positive)", "Recall (% of all Positives)"), values = c("blue", "red")) +
  theme(legend.position = c(.8, .5)) +
  ggtitle("Precision and Recall by Percent of Patients Tested")

# Who is high risk / low risk -------------
# Use risk categories
test_risk <- test_sorted %>%
  mutate(risk = ifelse(test_predict >= as.numeric(thresh_75[3]), "Highest",
                       ifelse(test_predict >= as.numeric(thresh_50[3]) & test_predict < as.numeric(thresh_75[3]), "High",
                              ifelse(test_predict >= as.numeric(thresh_25[3]) & test_predict < as.numeric(thresh_50[3]), "Medium", "Low"))))

# Summarize patient data for each of these groups
overall_features_adults <- test_risk %>%
  filter(AgeAtTest > 15) %>%
  summarize(TestingStrategy.HB = mean(TestingStrategy.HB, na.rm = TRUE),
            TestingStrategy.MOBILE = mean(TestingStrategy.MOBILE, na.rm = TRUE),
            TestingStrategy.VCT = mean(TestingStrategy.VCT, na.rm = TRUE),
            Gender_Male = mean(Gender.Male),
            MaritalStatus.Widowed = mean(MaritalStatus.Widowed, na.rm = TRUE),
            AgeAtTest = mean(AgeAtTest),
            TBScreening.Presumed.TB = mean(TBScreening.Presumed.TB, na.rm = TRUE),
            MonthsSinceLastTest = mean(MonthsSinceLastTest, na.rm = TRUE),
            count = n()
  )

highest_features_adults <- test_risk %>%
  filter(AgeAtTest > 15 & risk == "Highest") %>%
  summarize(TestingStrategy.HB = mean(TestingStrategy.HB, na.rm = TRUE),
            TestingStrategy.MOBILE = mean(TestingStrategy.MOBILE, na.rm = TRUE),
            TestingStrategy.VCT = mean(TestingStrategy.VCT, na.rm = TRUE),
            Gender_Male = mean(Gender.Male),
            MaritalStatus.Widowed = mean(MaritalStatus.Widowed, na.rm = TRUE),
            AgeAtTest = mean(AgeAtTest),
            TBScreening.Presumed.TB = mean(TBScreening.Presumed.TB, na.rm = TRUE),
            MonthsSinceLastTest = mean(MonthsSinceLastTest, na.rm = TRUE),
            count = n()
  )

high_features_adults <- test_risk %>%
  filter(AgeAtTest > 15 & risk == "High") %>%
  summarize(TestingStrategy.HB = mean(TestingStrategy.HB, na.rm = TRUE),
            TestingStrategy.MOBILE = mean(TestingStrategy.MOBILE, na.rm = TRUE),
            TestingStrategy.VCT = mean(TestingStrategy.VCT, na.rm = TRUE),
            Gender_Male = mean(Gender.Male),
            MaritalStatus.Widowed = mean(MaritalStatus.Widowed, na.rm = TRUE),
            AgeAtTest = mean(AgeAtTest),
            TBScreening.Presumed.TB = mean(TBScreening.Presumed.TB, na.rm = TRUE),
            MonthsSinceLastTest = mean(MonthsSinceLastTest, na.rm = TRUE),
            count = n()
  )

medium_features_adults <- test_risk %>%
  filter(AgeAtTest > 15 & risk == "Medium") %>%
  summarize(TestingStrategy.HB = mean(TestingStrategy.HB, na.rm = TRUE),
            TestingStrategy.MOBILE = mean(TestingStrategy.MOBILE, na.rm = TRUE),
            TestingStrategy.VCT = mean(TestingStrategy.VCT, na.rm = TRUE),
            Gender_Male = mean(Gender.Male),
            MaritalStatus.Widowed = mean(MaritalStatus.Widowed, na.rm = TRUE),
            AgeAtTest = mean(AgeAtTest),
            TBScreening.Presumed.TB = mean(TBScreening.Presumed.TB, na.rm = TRUE),
            MonthsSinceLastTest = mean(MonthsSinceLastTest, na.rm = TRUE),
            count = n()
  )


low_features_adults <- test_risk %>%
  filter(AgeAtTest > 15 & risk == "Low") %>%
  summarize(TestingStrategy.HB = mean(TestingStrategy.HB, na.rm = TRUE),
            TestingStrategy.MOBILE = mean(TestingStrategy.MOBILE, na.rm = TRUE),
            TestingStrategy.VCT = mean(TestingStrategy.VCT, na.rm = TRUE),
            Gender_Male = mean(Gender.Male),
            MaritalStatus.Widowed = mean(MaritalStatus.Widowed, na.rm = TRUE),
            AgeAtTest = mean(AgeAtTest),
            TBScreening.Presumed.TB = mean(TBScreening.Presumed.TB, na.rm = TRUE),
            MonthsSinceLastTest = mean(MonthsSinceLastTest, na.rm = TRUE),
            count = n()
  )

features_adults <- rbind(overall_features_adults,
                         highest_features_adults,
                         high_features_adults,
                         medium_features_adults,
                         low_features_adults)
row.names(features_adults) <- c("Overall","Highest Risk Patients", "High Risk Patients", "Medium Risk Patients", "Low Risk Patients")
features_adults


# Summarize patient data for each of these groups
overall_features_children <- test_risk %>%
  filter(AgeAtTest <= 15) %>%
  summarize(TestingStrategy.HB = mean(TestingStrategy.HB, na.rm = TRUE),
            TestingStrategy.MOBILE = mean(TestingStrategy.MOBILE, na.rm = TRUE),
            TestingStrategy.VCT = mean(TestingStrategy.VCT, na.rm = TRUE),
            Gender_Male = mean(Gender.Male),
            MaritalStatus.Widowed = mean(MaritalStatus.Widowed, na.rm = TRUE),
            AgeAtTest = mean(AgeAtTest),
            TBScreening.Presumed.TB = mean(TBScreening.Presumed.TB, na.rm = TRUE),
            MonthsSinceLastTest = mean(MonthsSinceLastTest, na.rm = TRUE),
            count = n()
  )

highest_features_children <- test_risk %>%
  filter(AgeAtTest <= 15 & risk == "Highest") %>%
  summarize(TestingStrategy.HB = mean(TestingStrategy.HB, na.rm = TRUE),
            TestingStrategy.MOBILE = mean(TestingStrategy.MOBILE, na.rm = TRUE),
            TestingStrategy.VCT = mean(TestingStrategy.VCT, na.rm = TRUE),
            Gender_Male = mean(Gender.Male),
            MaritalStatus.Widowed = mean(MaritalStatus.Widowed, na.rm = TRUE),
            AgeAtTest = mean(AgeAtTest),
            TBScreening.Presumed.TB = mean(TBScreening.Presumed.TB, na.rm = TRUE),
            MonthsSinceLastTest = mean(MonthsSinceLastTest, na.rm = TRUE),
            count = n()
  )

high_features_children <- test_risk %>%
  filter(AgeAtTest <= 15 & risk == "High") %>%
  summarize(TestingStrategy.HB = mean(TestingStrategy.HB, na.rm = TRUE),
            TestingStrategy.MOBILE = mean(TestingStrategy.MOBILE, na.rm = TRUE),
            TestingStrategy.VCT = mean(TestingStrategy.VCT, na.rm = TRUE),
            Gender_Male = mean(Gender.Male),
            MaritalStatus.Widowed = mean(MaritalStatus.Widowed, na.rm = TRUE),
            AgeAtTest = mean(AgeAtTest),
            TBScreening.Presumed.TB = mean(TBScreening.Presumed.TB, na.rm = TRUE),
            MonthsSinceLastTest = mean(MonthsSinceLastTest, na.rm = TRUE),
            count = n()
  )

medium_features_children <- test_risk %>%
  filter(AgeAtTest <= 15 & risk == "Medium") %>%
  summarize(TestingStrategy.HB = mean(TestingStrategy.HB, na.rm = TRUE),
            TestingStrategy.MOBILE = mean(TestingStrategy.MOBILE, na.rm = TRUE),
            TestingStrategy.VCT = mean(TestingStrategy.VCT, na.rm = TRUE),
            Gender_Male = mean(Gender.Male),
            MaritalStatus.Widowed = mean(MaritalStatus.Widowed, na.rm = TRUE),
            AgeAtTest = mean(AgeAtTest),
            TBScreening.Presumed.TB = mean(TBScreening.Presumed.TB, na.rm = TRUE),
            MonthsSinceLastTest = mean(MonthsSinceLastTest, na.rm = TRUE),
            count = n()
  )


low_features_children <- test_risk %>%
  filter(AgeAtTest <= 15 & risk == "Low") %>%
  summarize(TestingStrategy.HB = mean(TestingStrategy.HB, na.rm = TRUE),
            TestingStrategy.MOBILE = mean(TestingStrategy.MOBILE, na.rm = TRUE),
            TestingStrategy.VCT = mean(TestingStrategy.VCT, na.rm = TRUE),
            Gender_Male = mean(Gender.Male),
            MaritalStatus.Widowed = mean(MaritalStatus.Widowed, na.rm = TRUE),
            AgeAtTest = mean(AgeAtTest),
            TBScreening.Presumed.TB = mean(TBScreening.Presumed.TB, na.rm = TRUE),
            MonthsSinceLastTest = mean(MonthsSinceLastTest, na.rm = TRUE),
            count = n()
  )

features_children <- rbind(overall_features_children,
                         highest_features_children,
                         high_features_children,
                         medium_features_children,
                         low_features_children)
row.names(features_children) <- c("Overall","Highest Risk Patients", "High Risk Patients", "Medium Risk Patients", "Low Risk Patients")
features_children
