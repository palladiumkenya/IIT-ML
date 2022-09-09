# IIT-ML
This repo will contain code on early warning ML models for HIV patient at risk of interruption in treatment

These scripts require extracting data from the National Data Warehouse. In future, the SQL code will be added here as well.

To train models, scripts should be run in the following order:
- IIT_Prep.R (cleans data, generates features)
- IIT_GIS_Collection.R (collects locational attributes - requires first downloading datasets cited in script, links included)
- IIT_Imputation.R (generates versions of the dataset with imputed values to enable models to run that don't allow for missing values)
- XGB.R (conducts grid search of Boosted Tree models and analyzes best performing model with Shap analysis and feature importance)

To generate predictions, use the model generated in steps and above and run the following script:
- IIT_Gen_Preds.R
