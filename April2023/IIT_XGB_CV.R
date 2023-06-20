library(dplyr)
library(caret)
library(xgboost)
library(PRROC)

# First, let's read in and combine input tables ----------------
iit <- readRDS("~/iit_samp_250k_v2.rds")

# Last items ------------------
iit <- iit %>%
  mutate(DifferentiatedCare = case_when(
  grepl("fasttrack", DifferentiatedCare) ~ "fasttrack",
  .default = DifferentiatedCare
))

# # Add in GIS features -------------------------
gis <- readRDS('~/gis_features.rds') %>% dplyr::select(-Latitude, -Longitude)
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

# Let's do temporal cross validation first for sparse -------------------

train_data <- iit %>% 
  ungroup() %>%
  filter(patient_group == 1) %>% 
  select(-patient_group) %>%
  arrange(PredictionDate) %>%
  filter(PredictionDate >= "2021-01-01") %>%
  filter(PredictionDate <= "2022-05-31") #%>%
  # select(-PredictionDate) %>%
  # select(- key) %>%
  # encodeXGBoost()

test_data <- iit %>% 
  ungroup() %>%
  filter(patient_group == 2) %>% 
  select(-patient_group) %>%
  arrange(PredictionDate) %>%
  filter(PredictionDate > "2022-05-31") %>%
  select(-PredictionDate, - key) %>%
  encodeXGBoost()

# Let's do a single cross validation
# Create folds
nfolds <- 10
# cuts <- round(seq(1, nrow(train_data), length.out = nfolds+2))
cuts <- seq(min(train_data$PredictionDate), max(train_data$PredictionDate), length.out = nfolds + 2)
# cutgap <- cuts[2]/2

grid_sparse <- expand.grid(model = "xgboost",
                           sparsity = "sparse",
                           eta = c(0.01, 0.1),
                           max_depth = c(6, 8, 10, 12),
                           cs = c(.3, .5, .7))

start <- Sys.time()
for(i in 1:nrow(grid_sparse)){
  set.seed(2231)
  print(i)
  aucpr <- c()
  num_iter <- c()
  
  for(j in 1:nfolds){
   
    # Set train and validation
    tmp <-  train_data %>%
      group_by(key) %>% 
      mutate(patient_group = sample(1:2, 1, prob = c(0.7, 0.3))) 
    
    train_tmp <- tmp %>%
      ungroup() %>%
      filter(patient_group == 1) %>%
      filter(between(PredictionDate, cuts[1], cuts[j+1])) %>%
      select(-PredictionDate, - key) %>%
      encodeXGBoost()
    
    val_tmp <- tmp %>%
      ungroup() %>%
      filter(patient_group == 2) %>%
      filter(between(PredictionDate, cuts[j+1], cuts[j+2])) %>%
      select(-PredictionDate, - key) %>%
      encodeXGBoost()
    
    train_tmp <- train_tmp %>% select(intersect(names(train_tmp), names(val_tmp)))
    val_tmp <- val_tmp %>% select(intersect(names(train_tmp), names(val_tmp)))
    
    
    # train_tmp <- train_data[cuts[1]:cuts[j+1], ]
    # val_tmp <- train_data[(cuts[j+1]+1):(cuts[j+1]+cutgap), ]
    dtrain <- xgb.DMatrix(data = data.matrix(train_tmp[,which(names(train_tmp) != "Target")]),
                          label = train_tmp$Target)
    dval <- xgb.DMatrix(data = data.matrix(val_tmp[,which(names(val_tmp) != "Target")]),
                        label = val_tmp$Target)
    watchlist <- list(train=dtrain, test=dval)
    
    set.seed(2231)
    xgb <- xgboost::xgb.train(data = dtrain,
                              eta = grid_sparse[i, 3],
                              max_depth = grid_sparse[i, 4],
                              colsample_bytree = grid_sparse[i, 5],
                              nround = 1000,
                              early_stopping_rounds = 25,
                              objective = "binary:logistic",
                              metric = 'aucpr',
                              eval.metric = "aucpr",
                              watchlist = watchlist,
                              verbose = 0
    )
    
    aucpr <- c(aucpr, xgb$best_score)
    num_iter <- c(num_iter, xgb$best_iteration)

  }
  
  grid_sparse$val_pr_auc[i] <- mean(aucpr)
  grid_sparse$num_iter[i] <- mean(num_iter)

}

end <- Sys.time()
print(end - start)

saveRDS(grid_sparse, "xgb_iit_sparse.rds")

train_data_final <- train_data %>%
  select(-PredictionDate) %>%
  select(- key) %>%
  encodeXGBoost()

dtrain <- xgb.DMatrix(data = data.matrix(train_data_final[,which(names(train_data_final) != "Target")]),
                              label = train_data_final$Target)
set.seed(2231)
xgb <- xgb.train(data = dtrain,
                          eta = 0.1,
                          max_depth = 6,
                          colsample_bytree = .3,
                          nround = 150,
                          objective = "binary:logistic",
                          verbose = 1
)

val_predict <- predict(xgb,newdata = data.matrix(test_data[, -which(names(test_data) == "Target")]))
fg <- val_predict[test_data$Target == 1]
bg <- val_predict[test_data$Target == 0]
prc <- pr.curve(scores.class0 = fg, scores.class1 = bg, curve = T)
plot(prc)
roc <- roc.curve(scores.class0 = fg, scores.class1 = bg, curve = T)
plot(roc)

# Now, let's do it with simple imputation ------------

train_data <- iit %>% 
  ungroup() %>%
  filter(patient_group == 1) %>% 
  select(-patient_group) %>%
  filter(PredictionDate >= "2021-01-01") %>%
  filter(PredictionDate <= "2022-05-31")

test_data <- iit %>% 
  ungroup() %>%
  filter(patient_group == 2) %>% 
  select(-patient_group) %>%
  arrange(PredictionDate) %>%
  filter(PredictionDate > "2022-05-31")

# Create folds
nfolds <- 10
cuts <- seq(min(train_data$PredictionDate), max(train_data$PredictionDate), length.out = nfolds + 2)


grid_simple <- expand.grid(model = "xgboost",
                           sparsity = "simple",
                           eta = c(0.01, 0.1),
                           max_depth = c(6, 8, 10, 12),
                           cs = c(.3, .5, .7))

start <- Sys.time()

for(i in 1:nrow(grid_simple)){
  
  aucpr <- c()
  
  for(j in 1:nfolds){
    
    # Set train and validation
    tmp <-  train_data %>%
      group_by(key) %>% 
      mutate(patient_group = sample(1:2, 1, prob = c(0.7, 0.3))) 
    
    train_tmp <- tmp %>%
      ungroup() %>%
      filter(patient_group == 1) %>%
      filter(between(PredictionDate, as.Date(cuts[1]), as.Date(cuts[j+1]))) %>%
      select(-PredictionDate, - key) 
    
    val_tmp <- tmp %>%
      ungroup() %>%
      filter(patient_group == 2) %>%
      filter(between(PredictionDate, cuts[j+1], cuts[j+2])) %>%
      select(-PredictionDate, - key) 
    
    train_tmp <- train_tmp %>% select(intersect(names(train_tmp), names(val_tmp)))
    val_tmp <- val_tmp %>% select(intersect(names(train_tmp), names(val_tmp)))
    
    # Impute
    train_tmp <- data.frame(train_tmp)
    val_tmp <- data.frame(val_tmp)
    
    for(k in which(sapply(train_tmp, class) %in% c('character', 'factor'))){
      train_tmp[, k] <- replaceWithMode(train_tmp, train_tmp, k)
      val_tmp[, k] <- replaceWithMode(train_tmp, val_tmp, k)
    }
    
    for(k in which(sapply(train_tmp, class) %in% c('numeric', 'integer'))){
      train_tmp[, k] <- replaceWithMean(train_tmp, train_tmp, k)
      val_tmp[, k] <- replaceWithMean(train_tmp, val_tmp, k)
    }
    
    # Encode
    train_tmp <- encodeXGBoost(train_tmp)
    val_tmp <- encodeXGBoost(val_tmp)
    
    train_tmp <- train_tmp %>% select(intersect(names(train_tmp), names(val_tmp)))
    val_tmp <- val_tmp %>% select(intersect(names(train_tmp), names(val_tmp)))
    
    dtrain <- xgb.DMatrix(data = data.matrix(train_tmp[,which(names(train_tmp) != "Target")]),
                          label = train_tmp$Target)
    dval <- xgb.DMatrix(data = data.matrix(val_tmp[,which(names(val_tmp) != "Target")]),
                        label = val_tmp$Target)
    watchlist <- list(train=dtrain, test=dval)
    
    set.seed(2231)
    xgb <- xgboost::xgb.train(data = dtrain,
                              eta = grid_simple[i, 3],
                              max_depth = grid_simple[i, 4],
                              colsample_bytree = grid_simple[i, 5],
                              nround = 1000,
                              early_stopping_rounds = 50,
                              objective = "binary:logistic",
                              metric = 'aucpr',
                              eval.metric = "aucpr",
                              watchlist = watchlist,
                              verbose = 1
    )
    
    aucpr <- c(aucpr, xgb$best_score)
  }
  
  grid_simple$val_pr_auc[i] <- mean(aucpr)
  
}

end <- Sys.time()
print(end - start)

saveRDS(grid_simple, "xgb_iit_simple.rds")

# Let's try this for random forest now ----------

train_data <- iit %>%
  ungroup() %>%
  filter(patient_group == 1) %>%
  select(-patient_group) %>%
  arrange(PredictionDate) %>%
  select(-PredictionDate, - key)

test_data <- iit %>%
  ungroup() %>%
  filter(patient_group == 2) %>%
  select(-patient_group) %>%
  arrange(PredictionDate) %>%
  select(-PredictionDate, - key)

# Create folds
nfolds <- 10
cuts <- seq(min(train_data$PredictionDate), max(train_data$PredictionDate), length.out = nfolds + 2)

grid_rf_simple <- expand.grid(model = "rf",
                              sparsity = "simple",
                              mtry = c(6,8, 10),
                              nodesize = c(20, 10, 5))

start <- Sys.time()

for(i in 1:nrow(grid_rf_simple)){
  print(i)
  aucpr <- c()
  
  for(j in 1:nfolds){
    print(j)
    # Set train and validation
    tmp <-  train_data %>%
      group_by(key) %>% 
      mutate(patient_group = sample(1:2, 1, prob = c(0.7, 0.3))) 
    
    train_tmp <- tmp %>%
      ungroup() %>%
      filter(patient_group == 1) %>%
      filter(between(PredictionDate, cuts[1], cuts[j+1])) %>%
      select(-PredictionDate, - key) 
    
    val_tmp <- tmp %>%
      ungroup() %>%
      filter(patient_group == 2) %>%
      filter(between(PredictionDate, cuts[j+1], cuts[j+2])) %>%
      select(-PredictionDate, - key) 
    
    train_tmp <- train_tmp %>% select(intersect(names(train_tmp), names(val_tmp)))
    val_tmp <- val_tmp %>% select(intersect(names(train_tmp), names(val_tmp)))
    
    # # Set train and validation
    # train_tmp <- train_data[cuts[1]:cuts[j+1], ]
    # val_tmp <- train_data[(cuts[j+1]+1):(cuts[j+1]+cutgap), ]
    
    # Impute
    train_tmp <- data.frame(train_tmp)
    val_tmp <- data.frame(val_tmp)
    
    for(k in which(sapply(train_tmp, class) %in% c('character', 'factor'))){
      train_tmp[, k] <- replaceWithMode(train_tmp, train_tmp, k)
      val_tmp[, k] <- replaceWithMode(train_tmp, val_tmp, k)
    }
    
    for(k in which(sapply(train_tmp, class) %in% c('numeric', 'integer'))){
      train_tmp[, k] <- replaceWithMean(train_tmp, train_tmp, k)
      val_tmp[, k] <- replaceWithMean(train_tmp, val_tmp, k)
    }
    
    rf <- randomForest(
      as.factor(Target) ~ .,
      data = train_tmp,
      mtry = grid_rf_simple[i, 3],
      nodesize = grid_rf_simple[i, 4]
    )
    
    pred_val = predict(rf, newdata=val_tmp, type = "prob")
    fg <- pred_val[val_tmp$Target == 1, 2]
    bg <- pred_val[val_tmp$Target == 0, 2]
    prc <- pr.curve(scores.class0 = fg, scores.class1 = bg, curve = T)
    
    aucpr <- c(aucpr, prc$auc.integral)
  }
  
  grid_rf_simple$val_pr_auc[i] <- mean(aucpr)
  
}

saveRDS(grid_rf_simple, "rf_iit_simple.rds")

# Logistic Regression ---------------

train_data <- iit %>% 
  ungroup() %>%
  filter(patient_group == 1) %>% 
  select(-patient_group) %>%
  arrange(PredictionDate) %>%
  select(-PredictionDate, - key) 

test_data <- iit %>% 
  ungroup() %>%
  filter(patient_group == 2) %>% 
  select(-patient_group) %>%
  arrange(PredictionDate) %>%
  select(-PredictionDate, - key) 

# Create folds
nfolds <- 10
cuts <- seq(min(train_data$PredictionDate), max(train_data$PredictionDate), length.out = nfolds + 2)

aucpr <- c()

for(j in 1:nfolds){
  
  # Set train and validation
  tmp <-  train_data %>%
    group_by(key) %>% 
    mutate(patient_group = sample(1:2, 1, prob = c(0.7, 0.3))) 
  
  train_tmp <- tmp %>%
    ungroup() %>%
    filter(patient_group == 1) %>%
    filter(between(PredictionDate, cuts[1], cuts[j+1])) %>%
    select(-PredictionDate, - key) 
  
  val_tmp <- tmp %>%
    ungroup() %>%
    filter(patient_group == 2) %>%
    filter(between(PredictionDate, cuts[j+1], cuts[j+2])) %>%
    select(-PredictionDate, - key) 
  
  train_tmp <- train_tmp %>% select(intersect(names(train_tmp), names(val_tmp)))
  val_tmp <- val_tmp %>% select(intersect(names(train_tmp), names(val_tmp)))
  
  # Impute
  train_tmp <- data.frame(train_tmp)
  val_tmp <- data.frame(val_tmp)
  
  for(k in which(sapply(train_tmp, class) %in% c('character', 'factor'))){
    train_tmp[, k] <- replaceWithMode(train_tmp, train_tmp, k)
    val_tmp[, k] <- replaceWithMode(train_tmp, val_tmp, k)
  }
  
  for(k in which(sapply(train_tmp, class) %in% c('numeric', 'integer'))){
    train_tmp[, k] <- replaceWithMean(train_tmp, train_tmp, k)
    val_tmp[, k] <- replaceWithMean(train_tmp, val_tmp, k)
  }
  
  # Encode
  train_tmp <- encodeXGBoost(train_tmp)
  val_tmp <- encodeXGBoost(val_tmp)
  
  indexesToDrop <- findCorrelation(cor(train_tmp), cutoff = 0.9)
  train_tmp <- train_tmp[, -indexesToDrop]
  val_tmp <- val_tmp[, -indexesToDrop]
  
  set.seed(2231)
  mod <- glm("Target ~ .",
             family = "binomial",
             data = train_tmp)
  
  pred_val <- predict(mod, newdata = val_tmp, type = "response")
  fg <- pred_val[val_tmp$Target == 1]
  bg <- pred_val[val_tmp$Target == 0]
  prc <- pr.curve(scores.class0 = fg, scores.class1 = bg, curve = T)
  aucpr <- c(aucpr, prc$auc.integral)
  
}

print(mean(aucpr))
