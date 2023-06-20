setwd("~/Kenya/IIT/Abstract")

library(dplyr)
library(caret)
library(xgboost)
library(PRROC)
library(JOUSBoost)

# First, let's read in and combine input tables ----------------
iit <- readRDS("../MarchData/iit_samp_250k_v2.rds")

# Last items ------------------
iit <- iit %>%
  mutate(DifferentiatedCare = case_when(
    grepl("fasttrack", DifferentiatedCare) ~ "fasttrack",
    .default = DifferentiatedCare
  ))

# # Add in GIS features -------------------------
gis <- readRDS('../gis_features.rds') %>% dplyr::select(-Latitude, -Longitude)
iit$SiteCode <- as.character(iit$SiteCode)
iit <- merge(iit, gis,by.x = "SiteCode", by.y = "FacilityCode", all.x = TRUE) %>%
  dplyr::select(-SiteCode)

set.seed(2231)
iit <- iit %>% 
  group_by(key) %>% 
  mutate(patient_group = sample(1:2, 1, prob = c(0.8, 0.2)))

# helper functions -----------------------------
encodeXGBoost <- function(dataset){
  # Need to one-hot encode all the factor variables
  ohe_features <- names(dataset)[ sapply(dataset, is.factor) | sapply(dataset, is.character) ]
  
  dmy <- dummyVars("~ Gender + PatientSource + MaritalStatus + PopulationType +
                   OptimizedHIVRegimen + Other_Regimen + Pregnant + DifferentiatedCare +
                   StabilityAssessment + most_recent_art_adherence + most_recent_ctx_adherence +
                   most_recent_vl + AHD + Breastfeeding",
                   data = dataset)
  ohe <- data.frame(predict(dmy, newdata = dataset))
  dataset <- cbind(dataset, ohe)
  
  dataset[, !(names(dataset) %in% ohe_features)]
  
}

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

# Train best performing XGBoost ---------------
train_data <- iit %>% 
  ungroup() %>%
  filter(patient_group == 1) %>% 
  select(-patient_group) %>%
  arrange(PredictionDate) %>%
  filter(PredictionDate >= "2021-01-01") %>%
  filter(PredictionDate <= "2022-05-31") %>%
  select(-PredictionDate) %>%
  select(- key) %>%
  encodeXGBoost()

test_data <- iit %>% 
  ungroup() %>%
  filter(patient_group == 2) %>% 
  select(-patient_group) %>%
  arrange(PredictionDate) %>%
  filter(PredictionDate > "2022-05-31") %>%
  select(-PredictionDate, - key) %>%
  encodeXGBoost()

dtrain <- xgb.DMatrix(data = data.matrix(train_data[,which(names(train_data) != "Target")]),
                      label = train_data$Target)

set.seed(2231)
xgb <- xgboost::xgb.train(data = dtrain,
                          eta = 0.1,
                          max_depth = 6,
                          colsample_bytree = 0.3,
                          nround = 150,
                          objective = "binary:logistic",
                          metric = 'aucpr',
                          eval.metric = "aucpr",
                          verbose = 0
)

val_predict = predict(xgb, newdata=data.matrix(test_data[,which(names(test_data) != "Target")]))
fg <- val_predict[test_data$Target == 1]
bg <- val_predict[test_data$Target == 0]
prc <- pr.curve(scores.class0 = fg, scores.class1 = bg, curve = T)
roc <- roc.curve(scores.class0 = fg, scores.class1 = bg, curve = T)

vals <- cbind(test_data, val_predict)

vals <- vals %>%
  arrange(desc(val_predict)) %>%
  mutate(total_pos = cumsum(Target == 1)) %>%
  mutate(sensitivity = total_pos / sum(test_data$Target == 1)) %>%
  mutate(rownum = row_number()) %>%
  mutate(share_of_patients = rownum / n()) %>%
  mutate(ppv = total_pos / rownum) %>%
  select(AgeatTest, GenderFemale, Target, val_predict, total_pos, sensitivity,
         share_of_patients, ppv)

saveRDS(vals, "xgb_efficiency_graph.rds")
plot(vals$share_of_patients, vals$sensitivity)

# Random Forest Best Performing Model ---------------------

train_data <- iit %>%
  ungroup() %>%
  filter(patient_group == 1) %>%
  select(-patient_group) %>%
  arrange(PredictionDate) %>%
  filter(PredictionDate >= "2021-01-01") %>%
  filter(PredictionDate <= "2022-05-31") %>%
  select(-PredictionDate, - key)

test_data <- iit %>%
  ungroup() %>%
  filter(patient_group == 2) %>%
  select(-patient_group) %>%
  arrange(PredictionDate) %>%
  filter(PredictionDate > "2022-05-31") %>%
  select(-PredictionDate, - key)

  # Impute
  train_data <- data.frame(train_data)
  test_data <- data.frame(test_data)
  
  for(k in which(sapply(train_data, class) %in% c('character', 'factor'))){
    train_data[, k] <- replaceWithMode(train_data, train_data, k)
    test_data[, k] <- replaceWithMode(train_data, test_data, k)
  }
  
  for(k in which(sapply(train_data, class) %in% c('numeric', 'integer'))){
    train_data[, k] <- replaceWithMean(train_data, train_data, k)
    test_data[, k] <- replaceWithMean(train_data, test_data, k)
  }
  
  set.seed(2231)
  rf <- randomForest(
    as.factor(Target) ~ .,
    data = train_data,
    mtry = 6,
    nodesize = 5
  )

pred_val = predict(rf, newdata=test_data, type = "prob")
fg <- pred_val[test_data$Target == 1, 2]
bg <- pred_val[test_data$Target == 0, 2]
prc <- pr.curve(scores.class0 = fg, scores.class1 = bg, curve = T)
roc <- roc.curve(scores.class0 = fg, scores.class1 = bg, curve = T)

vals <- cbind(test_data, val_predict = pred_val[, 2])

vals <- vals %>%
  arrange(desc(val_predict)) %>%
  mutate(total_pos = cumsum(Target == 1)) %>%
  mutate(sensitivity = total_pos / sum(test_data$Target == 1)) %>%
  mutate(rownum = row_number()) %>%
  mutate(share_of_patients = rownum / n()) %>%
  mutate(ppv = total_pos / rownum) %>%
  select(AgeatTest, Gender, Target, val_predict, total_pos, sensitivity,
         share_of_patients, ppv)

saveRDS(vals, "rf_efficiency_graph.rds")



library(ggplot2)

xgb <- readRDS("./xgb_efficiency_graph.rds") %>%
  mutate(decile = c(rep(1:10, each = 3350), 10)) %>%
  group_by(decile) %>%
  summarize(pos = sum(FinalTestResult == 1)) %>%
  ungroup() %>%
  mutate(tot_pos = cumsum(pos),
         gain = tot_pos / sum(pos)) %>%
  mutate(Model = "XGBoost") %>%
  select(decile, gain, Model)
xgbzero <- data.frame(decile = 0, gain = 0, Model = "XGBoost")
xgb <- bind_rows(xgb, xgbzero)

rf <- readRDS("./rf_efficiency_graph.rds")%>%
  mutate(decile = c(rep(1:10, each = 3350), 10)) %>%
  group_by(decile) %>%
  summarize(pos = sum(FinalTestResult == "Positive")) %>%
  ungroup() %>%
  mutate(tot_pos = cumsum(pos),
         gain = tot_pos / sum(pos)) %>%
  mutate(Model = "Random Forest")%>%
  select(decile, gain, Model)
rfzero <- data.frame(decile = 0, gain = 0, Model = "Random Forest")
rf <- bind_rows(rf, rfzero)

ada <- readRDS("./ada_efficiency_graph.rds")%>%
  mutate(decile = c(rep(1:10, each = 3350), 10)) %>%
  group_by(decile) %>%
  summarize(pos = sum(FinalTestResult == 1)) %>%
  ungroup() %>%
  mutate(tot_pos = cumsum(pos),
         gain = tot_pos / sum(pos)) %>%
  mutate(Model = "Adaboost")%>%
  select(decile, gain, Model)
adazero <- data.frame(decile = 0, gain = 0, Model = "Adaboost")
ada <- bind_rows(ada, adazero)

lr <- readRDS("./lr_efficiency_graph.rds")%>%
  mutate(decile = c(rep(1:10, each = 3350), 10)) %>%
  group_by(decile) %>%
  summarize(pos = sum(FinalTestResult == "Positive")) %>%
  ungroup() %>%
  mutate(tot_pos = cumsum(pos),
         gain = tot_pos / sum(pos)) %>%
  mutate(Model = "Logistic Regression")%>%
  select(decile, gain, Model)
lrzero <- data.frame(decile = 0, gain = 0, Model = "Logistic Regression")
lr <- bind_rows(lr, lrzero)

baseline <- data.frame(decile = 0:10,
                       gain = seq(0, 1, by = 0.1),
                       Model = "Baseline")


all <- bind_rows(xgb, rf, ada, lr, baseline)
all$Model <- factor(all$Model, levels = c("Baseline", "Logistic Regression",
                                          "Adaboost", "Random Forest", "XGBoost"))
p <- ggplot(data = all, aes(x = decile, y = gain, group = Model, color = Model)) +
  geom_line() +
  xlab("Decile of Patients Tested") +
  ylab("% Gain") +
  ggtitle("Gain Chart by Model Type") +
  scale_y_continuous(labels = scales::percent, limits = c(0, 1),  expand = c(0, 0)) +
  scale_x_continuous(limits = c(0, 10), breaks = c(0,1,2,3,4,5,6,7,8,9,10), expand = c(0, 0)) +
  theme(legend.position = c(0.8, 0.3)) +
  theme(plot.title = element_text(color = "#0099f9", size = 12, face = "bold", hjust = 0.5))
p



