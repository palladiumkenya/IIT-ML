library(caret)
library(dplyr)
library(PRROC)
library(randomForest)

setwd("~/Kenya/IIT")
iit_list <- readRDS("iit_outlist_1102.rds")

iit_sparse_train <- iit_list$sparse_train 
iit_sparse_train[is.na(iit_sparse_train)] <- "UNKNOWN"
iit_sparse_val <- iit_list$sparse_val
iit_sparse_val[is.na(iit_sparse_val)] <- "UNKNOWN"
iit_simple_train <- iit_list$simple_train
iit_simple_val <- iit_list$simple_val
iit_mice_train <- iit_list$mice_train 
iit_mice_val <- iit_list$mice_val
iit_mice_train <- cbind(iit_mice_train, "target" =  iit_sparse_train$target)
iit_mice_val <- cbind(iit_mice_val, "target" =  iit_sparse_val$target)

# Sparse -----------------

grid_sparse <- expand.grid(model = "rf",
                           sparsity = "sparse",
                           mtry = c(6,8, 10),
                           nodesize = c(10, 5))

for (i in 1:nrow(grid_sparse)){
  print(i)
  set.seed(2231)
  
  rf <- randomForest(
    as.factor(target) ~ .,
    data = iit_sparse_train,
    mtry = grid_sparse[i, 3],
    nodesize = grid_sparse[i, 4]
  )
  
  pred_val = predict(rf, newdata=iit_sparse_val, type = "prob")
  fg <- pred_val[iit_sparse_val$target == 1, 2]
  bg <- pred_val[iit_sparse_val$target == 0, 2]
  prc <- pr.curve(scores.class0 = fg, scores.class1 = bg, curve = T)
  grid_sparse$val_pr_auc[i] <- prc$auc.integral
  
}

# Simple ---------------------

grid_simple <- expand.grid(model = "rf",
                           sparsity = "simple",
                           mtry = c(6,8, 10),
                           nodesize = c(10, 5))

for (i in 1:nrow(grid_simple)){
  print(i)
  set.seed(2231)
  
  rf <- randomForest(
    as.factor(target) ~ .,
    data = iit_simple_train,
    mtry = grid_simple[i, 3],
    nodesize = grid_simple[i, 4]
  )
  
  pred_val = predict(rf, newdata=iit_simple_val, type = "prob")
  fg <- pred_val[iit_simple_val$target == 1, 2]
  bg <- pred_val[iit_simple_val$target == 0, 2]
  prc <- pr.curve(scores.class0 = fg, scores.class1 = bg, curve = T)
  grid_simple$val_pr_auc[i] <- prc$auc.integral
  
}

# Mice ---------------
grid_mice <- expand.grid(model = "rf",
                         sparsity = "mice",
                         mtry = c(6,8, 10),
                         nodesize = c(10, 5))

for (i in 1:nrow(grid_mice)){
  print(i)
  set.seed(2231)
  
  rf <- randomForest(
    as.factor(target) ~ .,
    data = iit_mice_train,
    mtry = grid_mice[i, 3],
    nodesize = grid_mice[i, 4]
  )
  
  pred_val = predict(rf, newdata=iit_mice_val, type = "prob")
  fg <- pred_val[iit_mice_val$target == 1, 2]
  bg <- pred_val[iit_mice_val$target == 0, 2]
  prc <- pr.curve(scores.class0 = fg, scores.class1 = bg, curve = T)
  grid_mice$val_pr_auc[i] <- prc$auc.integral
  
}

grid_all <- rbind(grid_sparse, grid_simple, grid_mice)
saveRDS(grid_all, "rf_grid_1102.rds")



# Adaboost --------------------
library(caret)
library(dplyr)
library(PRROC)
library(JOUSBoost)

setwd("~/Kenya/IIT")
iit_list <- readRDS("iit_outlist_1102.rds")

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

iit_sparse_train <- encodeXGBoost(iit_list$sparse_train) %>% mutate(target = ifelse(target == 0, -1, 1))
iit_sparse_val <- encodeXGBoost(iit_list$sparse_val) %>% mutate(target = ifelse(target == 0, -1, 1))
iit_simple_train <- encodeXGBoost(iit_list$simple_train) %>% mutate(target = ifelse(target == 0, -1, 1))
iit_simple_val <- encodeXGBoost(iit_list$simple_val) %>% mutate(target = ifelse(target == 0, -1, 1))
iit_mice_train <- encodeXGBoost(iit_list$mice_train) %>% mutate(target = ifelse(target == 0, -1, 1))
iit_mice_val <- encodeXGBoost(iit_list$mice_val) %>% mutate(target = ifelse(target == 0, -1, 1))
iit_mice_train <- cbind(iit_mice_train, "target" =  iit_sparse_train$target)
iit_mice_val <- cbind(iit_mice_val, "target" =  iit_sparse_val$target)

# Sparse -----------------

grid_sparse <- expand.grid(model = "adaboost",
                           sparsity = "sparse",
                           tree_depth = c(2,3,4,5),
                           n_rounds = c(100, 200))

for (i in 1:nrow(grid_sparse)){
  print(i)
  set.seed(2231)
  
  mod <- adaboost(data.matrix(iit_sparse_train[,which(names(iit_sparse_train) != "target")]),
                  y = iit_sparse_train$target,
                  tree_depth = grid_sparse[i, 3],
                  n_rounds = grid_sparse[i, 4], 
                  verbose = 1
  )

pred_val = predict(mod, X=iit_sparse_val, type = "prob")
fg <- pred_val[iit_sparse_val$target == 1]
bg <- pred_val[iit_sparse_val$target == -1]
prc <- pr.curve(scores.class0 = fg, scores.class1 = bg, curve = T)
grid_sparse$val_pr_auc[i] <- prc$auc.integral

}

# simple -----------------

grid_simple <- expand.grid(model = "adaboost",
                           sparsity = "simple",
                           tree_depth = c(2,3,4,5),
                           n_rounds = c(100, 200))

for (i in 1:nrow(grid_simple)){
  print(i)
  set.seed(2231)
  
  mod <- adaboost(data.matrix(iit_simple_train[,which(names(iit_simple_train) != "target")]),
                  y = iit_simple_train$target,
                  tree_depth = grid_simple[i, 3],
                  n_rounds = grid_simple[i, 4], 
                  verbose = 1
  )

pred_val = predict(mod, X=iit_simple_val, type = "prob")
fg <- pred_val[iit_simple_val$target == 1]
bg <- pred_val[iit_simple_val$target == -1]
prc <- pr.curve(scores.class0 = fg, scores.class1 = bg, curve = T)
grid_simple$val_pr_auc[i] <- prc$auc.integral

}

# mice -----------------

grid_mice <- expand.grid(model = "adaboost",
                         sparsity = "mice",
                         tree_depth = c(2,3,4,5),
                         n_rounds = c(100, 200))

for (i in 1:nrow(grid_mice)){
  print(i)
  set.seed(2231)
  
  mod <- adaboost(data.matrix(iit_mice_train[,which(names(iit_mice_train) != "target")]),
                  y = iit_mice_train$target,
                  tree_depth = grid_mice[i, 3],
                  n_rounds = grid_mice[i, 4], 
                  verbose = 1
  )

pred_val = predict(mod, X=iit_mice_val, type = "prob")
fg <- pred_val[iit_mice_val$target == 1]
bg <- pred_val[iit_mice_val$target == -1]
prc <- pr.curve(scores.class0 = fg, scores.class1 = bg, curve = T)
grid_mice$val_pr_auc[i] <- prc$auc.integral

}

grid_all <- rbind(grid_sparse, grid_simple, grid_mice)
saveRDS(grid_all, "adaboost_grid_1102.rds")

