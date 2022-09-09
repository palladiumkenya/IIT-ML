library(caret)
library(dplyr)
library(PRROC)
library(xgboost)

setwd("~/Kenya/IIT")
iit_list <- readRDS("iit_outlist.rds")


# XGBoost requires only numeric inputs, so one-hot encode all categorical variables
encodeXGBoost <- function(dataset){
  # Need to one-hot encode all the factor variables
  ohe_features <- names(dataset)[ sapply(dataset, is.character) ]
  
  dmy <- dummyVars("~ Gender + PatientSource + MaritalStatus + PopulationType + TreatmentType +
                   OptimizedHIVRegimen + Other_Regimen + Pregnant + DifferentiatedCare +
                   most_recent_art_adherence + most_recent_ctx_adherence + StabilityAssessment +
                   most_recent_vl",
                   data = dataset)
  ohe <- data.frame(predict(dmy, newdata = dataset))
  dataset <- cbind(dataset, ohe)
  
  dataset[, !(names(dataset) %in% ohe_features)]
  
}

iit_sparse_train <- encodeXGBoost(iit_list$sparse_train) 
iit_sparse_val <- encodeXGBoost(iit_list$sparse_val) 
iit_simple_train <- encodeXGBoost(iit_list$simple_train) 
iit_simple_val <- encodeXGBoost(iit_list$simple_val) 
iit_mice_train <- encodeXGBoost(iit_list$mice_train) 
iit_mice_val <- encodeXGBoost(iit_list$mice_val) 
iit_mice_train <- cbind(iit_mice_train, "target" =  iit_sparse_train$target)
iit_mice_val <- cbind(iit_mice_val, "target" =  iit_sparse_val$target)

# Sparse Model -------------------------------------------------

grid_sparse <- expand.grid(model = "xgboost",
                           sparsity = "sparse",
                           eta = c(0.01, 0.1),
                           max_depth = c(6, 8, 10, 12, 14),
                           cs = c(.2, .3, .4, .5),
                           nrounds = c(50,100, 150))

for (i in 1:nrow(grid_sparse)){
  
  set.seed(2231)
  xgb <- xgboost::xgboost(data = data.matrix(iit_sparse_train[,which(names(iit_sparse_train) != "target")]), 
                          label = iit_sparse_train$target, 
                          eta = grid_sparse[i, 3],
                          max_depth = grid_sparse[i, 4], 
                          colsample_bytree = grid_sparse[i, 5],
                          nrounds = grid_sparse[i, 6],
                          objective = "binary:logistic",
                          metric = 'aucpr',
                          verbose = 1
  )
  
  val_predict <- predict(xgb,newdata = data.matrix(iit_sparse_val[, which(names(iit_sparse_val) != "target")]))
  fg <- val_predict[iit_sparse_val$target == 1]
  bg <- val_predict[iit_sparse_val$target == 0]
  prc <- pr.curve(scores.class0 = fg, scores.class1 = bg, curve = T)
  grid_sparse$val_pr_auc[i] <- prc$auc.integral
  
}

# Simple Imputation ------------------------------------------------

grid_simple <- expand.grid(model = "xgboost",
                           sparsity = "simple",
                           eta = c(0.01, 0.1),
                           max_depth = c(6, 8, 10, 12, 14),
                           cs = c(.2, .3, .4, .5),
                           nrounds = c(50,100, 150))

for (i in 1:nrow(grid_simple)){
  
  set.seed(2231)
  xgb <- xgboost::xgboost(data = data.matrix(iit_simple_train[,which(names(iit_simple_train) != "target")]), 
                          label = iit_simple_train$target, 
                          eta = grid_simple[i, 3],
                          max_depth = grid_simple[i, 4], 
                          colsample_bytree = grid_simple[i, 5],
                          nrounds = grid_simple[i, 6],
                          objective = "binary:logistic",
                          metric = 'aucpr',
                          verbose = 1
  )
  
  val_predict <- predict(xgb,newdata = data.matrix(iit_simple_val[, which(names(iit_simple_val) != "target")]))
  fg <- val_predict[iit_simple_val$target == 1]
  bg <- val_predict[iit_simple_val$target == 0]
  prc <- pr.curve(scores.class0 = fg, scores.class1 = bg, curve = T)
  grid_simple$val_pr_auc[i] <- prc$auc.integral
  
}



# MICE Imputation ------------------------------------------------

grid_mice <- expand.grid(model = "xgboost",
                         sparsity = "mice",
                         eta = c(0.01, 0.1),
                         max_depth = c(6, 8, 10, 12, 14),
                         cs = c(.2, .3, .4, .5),
                         nrounds = c(50,100, 150))

for (i in 1:nrow(grid_mice)){
  
  set.seed(2231)
  xgb <- xgboost::xgboost(data = data.matrix(iit_mice_train[,which(names(iit_mice_train) != "target")]), 
                          label = iit_mice_train$target, 
                          eta = grid_mice[i, 3],
                          max_depth = grid_mice[i, 4], 
                          colsample_bytree = grid_mice[i, 5],
                          nrounds = grid_mice[i, 6],
                          objective = "binary:logistic",
                          metric = 'aucpr',
                          verbose = 0
  )
  
  val_predict <- predict(xgb,newdata = data.matrix(iit_mice_val[, which(names(iit_mice_val) != "target")]))
  fg <- val_predict[iit_mice_val$target == 1]
  bg <- val_predict[iit_mice_val$target == 0]
  prc <- pr.curve(scores.class0 = fg, scores.class1 = bg, curve = T)
  grid_mice$val_pr_auc[i] <- prc$auc.integral
  
}

grid_all <- rbind(grid_sparse, grid_simple, grid_mice)
saveRDS(grid_all, "xgb_grid.rds")

# Investigate -----------------
# Train best-performing model 
set.seed(2231)
xgb <- xgboost::xgboost(data = data.matrix(iit_sparse_train[,which(names(iit_sparse_train) != "target")]), 
                        label = iit_sparse_train$target, 
                        eta = 0.1,
                        max_depth = 6, 
                        colsample_bytree = 0.5,
                        nrounds = 150,
                        objective = "binary:logistic",
                        metric = 'aucpr',
                        verbose = 1, 
)

saveRDS(xgb, "iit_xgb_0725.rds")

val_predict <- predict(xgb,newdata = data.matrix(iit_sparse_val[, which(names(iit_sparse_val) != "target")]))
fg <- val_predict[iit_sparse_val$target == 1]
bg <- val_predict[iit_sparse_val$target == 0]
prc <- pr.curve(scores.class0 = fg, scores.class1 = bg, curve = T)
plot(prc)

roc <- roc.curve(scores.class0 = fg, scores.class1 = bg, curve = T)
plot(roc)

# Cutoffs
pr_df <- data.frame(prc$curve)
names(pr_df) <- c("Recall", "Precision", "Threshold")

thresh_50 <- pr_df %>%
  filter(Precision > .5) %>%
  filter(row_number() == 1)

thresh_25 <- pr_df %>%
  filter(Precision > .25) %>%
  filter(row_number() == 1)

# Feature Importance 
importance_matrix = xgb.importance(model = xgb)
xgb.plot.importance(importance_matrix[80:98,])

# Get Top Features for Prediction Risk Bands
val_all <- cbind(iit_sparse_val, "prediction" = val_predict)

# Look at risk factors by time on ART
group <- val_all %>%
  filter(between(timeOnArt, 24, 2400)) %>%
  mutate(risk = ifelse(prediction >= 0.1, "High", "Low")) %>%
  group_by(risk) %>%
  summarize(count = n(),
            # AvgTimeonART = mean(timeOnArt, na.rm = TRUE),
            AvgMissed30Last5 = 100*mean(missed30_last5, na.rm = TRUE),
            # AvgMissed30All = mean(missed30, na.rm = TRUE),
            AvgUnscheduledRate = 100*mean(unscheduled_rate, na.rm = TRUE),
            AvgOptimizedHIVRegimen = 100*mean(OptimizedHIVRegimenYes, na.rm = TRUE),
            # AvgOtherRegimen = 100*mean(Other_RegimenYes, na.rm = TRUE),
            # AvgStability = 100*mean(StabilityAssessmentStable, na.rm = TRUE),
            AvgNumHIVRegimens = mean(num_hiv_regimens, na.rm = TRUE),
            # PercMale = 100*mean(GenderMale, na.rm = TRUE),
            # PercMarried = 100*mean(MaritalStatusMarried, na.rm = TRUE),
            # AvgHVLRate = 100*mean(recent_hvl_rate, na.rm = TRUE),
            AvgStabilityRate = mean(StabilityAssessmentStable, na.rm = TRUE)
            # AvgRecentPoorARTAdherence = 100*mean(most_recent_art_adherencepoor, na.rm = TRUE),
            # AvgRecentPoorCTXAdherence = 100*mean(most_recent_ctx_adherencepoor, na.rm = TRUE),
            # AvgDiffCareHCWLed = 100*mean(DifferentiatedCareCommunityARTDistributionHCWLed, na.rm = TRUE),
            # AvgFastTrack = 100*mean(DifferentiatedCareFastTrack, na.rm = TRUE)
            )
View(t(group))


# Shapley Values -----------------
source('./shap.R')
## Calculate shap values
shap_result = shap.score.rank(xgb_model = xgb, 
                              X_train = data.matrix(iit_sparse_train[,which(names(iit_sparse_train) != "target")]),
                              shap_approx = F
)

## Prepare data for top N variables
shap_long = shap.prep(shap = shap_result,
                      X_train = data.matrix(iit_sparse_train[,which(names(iit_sparse_train) != "target")]), 
                      top_n = 20
)

## Plot shap overall metrics
plot.shap.summary(data_long = shap_long)

