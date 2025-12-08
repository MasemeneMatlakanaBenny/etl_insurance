## import some libraries and frameworks where necessary first:
import os
import pandas as pd
import hopsworks
from dotenv import load_dotenv

load_dotenv()

##load the dataset first:
df=pd.read_csv("data/transformed_df.csv")

## access the API KEY and PROJECT NAME:
api_key=os.getenv("API_KEY")

project_name=os.getenv("PROJECT_NAME")

## login hopsworks:
project=hopsworks.login(
    api_key_value=api_key,
    project=project_name
)


## create the feature store:
feature_store=project.get_feature_store()


## create the feature group attributes:
feature_group_name="insurance_group"

feature_group_version=1

feature_group_description="insurance for data engineering"

## create the feature group:
feature_group=feature_store.get_or_create_group(
    name=feature_group_name,
    version=feature_group_version,
    description=feature_group_description,
    primary_key=[''],
    event_time=['']
)

## insert the data:
feature_group.insert(df,write_options={"wait_for_job":False})
