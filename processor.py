import pandas as pd
import psycopg2
from sqlalchemy import create_engine

# Reading the Excel file into a DataFrame
excel_df = pd.read_excel('your_excel_file.xlsx')

# Connecting to PostgreSQL and reading data into another DataFrame
engine = create_engine('postgresql://username:password@localhost:5432/your_database')
sql_query = "SELECT * FROM your_table"
postgres_df = pd.read_sql_query(sql_query, con=engine)

column_names = []

# Initializing two empty DataFrames with the same columns as the Excel DataFrame
df_even = pd.DataFrame(columns=column_names)
df_odd = pd.DataFrame(columns=column_names)

# Iterating over the rows of the Excel DataFrame
for index, row in excel_df.iterrows():
    if row['even'] % 2 == 0:
        # If the value in the 'even' column is even, add the row to df_even
        df_even = df_even.append(row, ignore_index=True)
    else:
        # If the value in the 'even' column is odd, add the row to df_odd
        df_odd = df_odd.append(row, ignore_index=True)

# Now df_even contains rows with even 'even' values, and df_odd contains rows with odd 'even' values



"""
save the spn_spn matching file in a separate files
save the strippedSPN and stripped SPN in a separate file

all the data of spire and related data of P21

go into each of the sales files saved for specific dates. then take the data from it and then save it into the specific columns, so each of the file has a column_names
    
first go through the spn vs spn and save all the ones that are matching in a file, then take all the ones that does not match and then try stripped spn vs stripped spn.

create a flag = stripped_spn, spn

Specific format:
 - first go through spn_spn, append all the rows of the matching ones into the "A" dataframe and the rest of the rows that is not matching in a separate df
 - then go through the df then do the stripped_spn_stripped_spn and save the ones that has match in a df and then the rest on another df
 - then combine these ones into a file "matching_products"
    - mark the ones that have matched through spn_spn as "spn" in the matching_check column
    - mark the ones that have mathced thorug stripped spn as "stripped_spn"
    - then in the end for the ones that doesn't match, mark them as "not matched"

    now combine all in one file.

all the dfs will have stripped spn and spn columns



"""