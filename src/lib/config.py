import pandas as pd
import great_expectations as gx
from great_expectations.core import ExpectationConfiguration
from typing import List


def create_batch(df:pd.DataFrame):
    """
    Create the entire batch.
    This includes the data source-> data asset -> batch definition 
    -> batch which are required in order to get started with creating
    the data validations

    """
    ### create the context first:
    context=gx.get_context()

    ##create the data asset and data source:
    data_source=context.data_sources.add_pandas("pandas")

    data_asset=data_source.add_dataframe_asset("data_asset")

    ## create the batch definition and the batch:
    batch_definition=data_asset.add_batch_definition_whole_dataframe("batch")

    batch=batch_definition.get_batch(batch_parameters={"dataframe":df})

    return batch

def create_categorical_expectations(column_name:str,values:List[str]):
    """
    Create the categorical expectations to make sure that our values or
    labels are in the column we defined and test the expectations.
    """
    expectations=gx.expectations.ExpectColumnDistinctValuesToContainSet(
        column=column_name,value_set=values
    )

    return expectations

def create_min_expectations(column_name:str,min_value,max_value):
    """
    Create the minimum expectations to make sure that the column 
    min value is at some level
    """

    min_exp=gx.expectations.ExpectColumnMinToBeBetween(
        column=column_name,min_value=min_value,max_value=max_value
    )

    return min_exp

def create_max_expectations(column_name:str,min_value,max_value):
    """
    Create the maximum expectations to make sure that the column 
    min value is at some level
    """

    max_exp=gx.expectations.ExpectColumnMaxToBeBetween(
        column=column_name,min_value=min_value,max_value=max_value
    )

    return max_exp

def validate_expectations(batch,expectations:List[ExpectationConfiguration],labels:List[str])->pd.DataFrame:
    """
    Run the expectations and validate them with the use of the batch 
    and get the results
    """
    validation_results=[]


    for exp in expectations:
        exp_results=batch.validate(exp)

        validation_results.append(exp_results[0])

    ## create the df:
    validation_df=pd.DataFrame({"expectations":expectations,"results":validation_results})

    return validation_df

def meta_validation(batch,validation_df:pd.DataFrame):

    ## create a simple expectation:
    expectation=gx.expectations.ExpectColumnDistinctValuesToEqualSet(
        column="results",value_set=["success"]
    )

    meta_results=batch.validate(expectation)

    print(meta_results[0])




