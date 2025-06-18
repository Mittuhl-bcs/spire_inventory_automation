import pandas as pd
import os

# Directory containing Excel files
directory = "C:\\Users\\Vserve-User\\Downloads\\Spire\\Spire_sales"

# Initialize an empty list to store all DataFrames
all_data_df_list = []

# Iterate over all Excel files in the directory
for filename in os.listdir(directory):
    if filename.endswith('.csv'):
        # Extract the year from the filename (last four characters before the extension)
        year = filename[-8:-4]
        
        # Read each CSV file into a DataFrame
        file_path = os.path.join(directory, filename)
        df = pd.read_csv(file_path, low_memory=False)
        
        # Strip leading spaces from the 'part_no' column
        df['part_no'] = df['part_no'].str.lstrip()
        
        # Drop rows where 'part_no' is null
        df = df.dropna(subset=['part_no'])
        
        # Add a 'year' column with the extracted year
        df['year'] = year
        
        # Append the DataFrame to the list
        all_data_df_list.append(df)

# Concatenate all DataFrames in the list into a single DataFrame
all_data_df = pd.concat(all_data_df_list, ignore_index=True)

# Group by 'part_no' and 'year', and sum the 'order_qty'
grouped_df = all_data_df.groupby(['part_no', 'year'])['order_qty'].sum().reset_index()

# Pivot the table to have years as columns
pivot_df = grouped_df.pivot_table(index='part_no', columns='year', values='order_qty', fill_value=0).reset_index()

# Add a 'qty' column which is the sum of the quantities across all years
pivot_df['qty'] = pivot_df.iloc[:, 1:].sum(axis=1)

# Reorder the columns to place 'qty' right after 'part_no'
columns = ['part_no', 'qty'] + sorted([col for col in pivot_df.columns if col not in ['part_no', 'qty']])
pivot_df = pivot_df[columns]

# Rename the columns
pivot_df.rename(columns={'part_no': 'part number', 'qty': 'total qty'}, inplace=True)

# Save the result to an Excel file
pivot_df.to_excel("C:\\Users\\Vserve-User\\Downloads\\Spire\\Combined_sales_pivot.xlsx", index=False)

# Print the first few rows of the final DataFrame
print(pivot_df.head())
