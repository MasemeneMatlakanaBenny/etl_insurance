import os
import pandas as pd
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

