import pandas as pd
import numpy as np
from datetime import datetime

df=pd.read_csv("data/extracted_df.csv")

unique_ids=np.arange(1,len(df)+1)
current_time=datetime.now()


df['unique_id']=unique_ids
df['time']=current_time

## save the dataset to the data folder in a csv format:
df.to_csv("data/transformed_df.csv",index=False)





