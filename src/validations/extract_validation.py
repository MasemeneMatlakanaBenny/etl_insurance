## create the validations that we will need

## Phase 2 after extracting the data:
## import libs first:

import pandas as pd
from lib.config import create_batch,create_categorical_expectations,create_max_expectations,create_min_expectations,validate_expectations,meta_validation



## load the dataset first:
df=pd.read_csv("data/extracted_data.csv")

## create the batch first:
batch=create_batch(df=df)

## create some categorical expectations:
smoker_exp=create_categorical_expectations(column_name="smoker",values=["yes","no"])

gender_exp=create_categorical_expectations(column_name="sex",values=["female","yes"])

region_exp=create_categorical_expectations(column_name="region",values=["southwest","southeast","northwest","southeast"])


## create some numerical expectations:
min_age_exp=create_min_expectations(column_name="age",min_value=18,max_value=25)

## create the max age exp:
max_age_exp=create_max_expectations(column_name="age",min_value=50,max_value=75)


min_charges_exp=create_min_expectations(column_name="charges",min_value=1000,max_value=1500)

max_charges_exp=create_max_expectations(column_name="charges",min_value=2000,max_value=100000)

min_children_exp=create_min_expectations(column_name="children",min_value=0,max_value=1)

max_children_exp=create_max_expectations(column_name="children",min_value=0,max_value=5)

## 
expectation_labels=["smoker_exp","gender_exp","region_exp","min_age_exp","max_age_exp","min_charges_exp",
                    "max_charges_exp","min_charges_exp","max_charges_exp","min_children_exp","max_children_exp"]

expectations=[smoker_exp,gender_exp,region_exp,min_age_exp,max_age_exp,min_charges_exp,max_age_exp,
              min_charges_exp,max_charges_exp,min_children_exp,max_children_exp]


## run the expectations:
results_df=validate_expectations(batch=batch,expectations=expectations,labels=expectation_labels)

## do some lazy data engineering by creating meta validations:

## create the meta batch first:
meta_batch=create_batch(df=results_df)
meta_exp_results=meta_validation(batch=meta_batch,validation_df=results_df)

if meta_exp_results[0]=="success":
    print("Data quality checks all passed well")

else:
    print("Not all data quality checks were passed")


