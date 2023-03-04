import pandas as pd
from sqlalchemy import create_engine
"""
In this file, we will create db connection, mention no_of_rows to get fetched at a time, 
read csv file according to the no_of_rows and will save dataframes into db
"""
db_connection = create_engine('postgresql://postgres:welcome123@localhost:5433/venzo_assignment_db')

csv_file = 'jasandeep.csv'
no_of_rows = 1000000

# We can create dictionary of datatypes for particular column,
# we can fetch particular columns etc. to make process fast
for chunk in pd.read_csv(csv_file, chunksize=no_of_rows):
    chunk.to_sql(name='test_od', con=db_connection, if_exists='append', index=False)
