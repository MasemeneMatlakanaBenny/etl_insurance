import os
import pandas as pd
from prefect import task,flow

@task(name="data extraction task",
description="Extracting the raw data from the database with the use of SQL",
task_run_name="extraction")
def extract_data():
    from sqlalchemy import create_engine
    from dotenv import load_dotenv

    ## load the dotenv first:
    load_dotenv()

    ## get db name and passwords from env file:
    password=os.getenv("PASSWORD")
    db_name=os.getenv("DB_NAME")
    user=os.getenv("USER")
    engine=create_engine(f"mysql+pymysql://{user}:{password}@127.0.0.1:3306/{db_name}")

    ## then access the db:
    query="SELECT * FROM insurance"
    df=pd.read_sql_squery(query,engine)
    df.to_csv("data/extracted_df.csv",index=False)

    return df


@task(
    name="data extraction validation",
    description="validating the raw data extracted from the database",
    task_run_name="data extraction validation"
)
def validate_extracted_data(raw_df:pd.DataFrame):
    from lib.config import create_batch,create_categorical_expectations,create_max_expectations,create_min_expectations,validate_expectations,meta_validation

    batch=create_batch(df=raw_df)
    ## create some categorical expectations:
    smoker_exp=create_categorical_expectations(column_name="smoker",values=["yes","no"])
    gender_exp=create_categorical_expectations(column_name="sex",values=["female","yes"])
    region_exp=create_categorical_expectations(column_name="region",values=["southwest","southeast","northwest","southeast"])
    
    ## create some numerical expectations:
    min_age_exp=create_min_expectations(column_name="age",min_value=18,max_value=25)
    min_charges_exp=create_min_expectations(column_name="charges",min_value=1000,max_value=1500)
    min_children_exp=create_min_expectations(column_name="children",min_value=0,max_value=1)
    
    ## create the max age exp:
    max_age_exp=create_max_expectations(column_name="age",min_value=50,max_value=75)
    max_charges_exp=create_max_expectations(column_name="charges",min_value=2000,max_value=100000)
    max_children_exp=create_max_expectations(column_name="children",min_value=0,max_value=5)

    
    expectation_labels=["smoker_exp","gender_exp","region_exp","min_age_exp","max_age_exp","min_charges_exp",
                    "max_charges_exp","min_charges_exp","max_charges_exp","min_children_exp","max_children_exp"]
    expectations=[smoker_exp,gender_exp,region_exp,min_age_exp,max_age_exp,min_charges_exp,max_age_exp,
              min_charges_exp,max_charges_exp,min_children_exp,max_children_exp]
    
    ## run the expectations:
    results_df=validate_expectations(batch=batch,expectations=expectations,labels=expectation_labels)
    ## do some lazy data engineering by creating meta validations:

    # ## create the meta batch first:
    meta_batch=create_batch(df=results_df)
    meta_exp_results=meta_validation(batch=meta_batch,validation_df=results_df)


@task(
    name="data transformation",
    description="""
    Transforming the data to ensure that it is indeed clean and ready to be loaded into 
    its final destinations. In this case ,we add the unique id for each user and the datetime stamp
    which would be essential for the feature store
    """

)
def data_transformation(raw_df:pd.DataFrame)->pd.DataFrame:
    """
    Add the userid or uniqueid and the timestamp for each user. 
    Thats the only transformation done in this case since there are no missing values
    """
    import numpy as np
    from datetime import datetime

    ## get the current time first:
    current_time=datetime.now()

    ## get the user ids:
    user_ids=np.arange(1,len(raw_df+1))

    ## add both the current time and user_ids to the raw_df:
    raw_df['unique_id']=user_ids
    raw_df['time']=current_time

    ## save the dataset to the data folder in a csv format:
    raw_df.to_csv("data/transformed_df.csv",index=False)
    return raw_df

@task(
    name="data loading",
    description="Loading the transformed data into the feature store in order to complete the entire ETL workflow",
    task_run_name="data loading"
)
def data_loading(transformed_df:pd.DataFrame):
    """
    Final phase of the ETL pipeline -> Loading the data into its final or desired destination.
    That is the data is now readhy to be used for analysis,machine learning or decision making tasks
    """
    import hopsworks
    from dotenv import load_dotenv

    ## load dotenv first in the component of data loading:
    load_dotenv()

    ## get the project name and api key:
    project_name=os.getenv("PROJECT_NAME")
    api_key=os.getenv("API_KEY")

    ## log into hopsworks:
    project=hopsworks.login(
        project=project_name,
        api_key_value=api_key
    )

    ## get the feature store:
    feature_store=project.get_feature_store()

    ## create the feature group attributes:
    feature_group_name="insurance"
    feature_group_version=1
    feature_group_description="insurance ETL workflow"

    ## create the feature group now:
    feature_group=feature_store.get_or_create_group(
        name=feature_group_name,
        version=feature_group_version,
        description=feature_group_description,
        primary_key=['user_id'],
        event_time=['datetime']

    )

    feature_group.insert(transformed_df,write_options={"wait_for_job":False})


@flow(
    name="ETL Pipeline",
    description="""
              An automated workflow of the ETL Pipeline. 
              Data Extraction -> Save extracted data -> Validate extracted data->
              Meta Data Extraction Validation -> Transform the extracted data->
              Save transformed data -> Validate transformed data ->
              Meta Data Transformation Validation -> Load the data into the feature store

                """,
    flow_run_name="Automated ETL Pipeline Workflow"
)
def etl_pipeline():

    ## get the extracted data:
    extracted_df=extract_data()

    extracted_validations=validate_extracted_data(raw_df=extracted_df)

    transformed_data=data_transformation(raw_df=extracted_df)

    data_loading(transformed_df=transformed_data)


if __name__=="__main__":
    etl_pipeline()

    
