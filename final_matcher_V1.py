import pandas as pd
import numpy as np

# Read the CSV and Excel files
dfmatched = pd.read_csv("C:\\Users\\Vserve-User\\Downloads\\Spire\\matched_data.csv")
df_combined_sales = pd.read_excel("C:\\Users\\Vserve-User\\Downloads\\Spire\\Combined_sales_pivot.xlsx")

# Set 'Spire SPN' as the index for dfmatched and 'part number' as the index for df_combined_sales
dfmatched.set_index('Spire SPN', inplace=True)
df_combined_sales.set_index('part number', inplace=True)

# Perform a left join
result = dfmatched.join(df_combined_sales[['total qty', '2022', '2023', '2024']], how='left')

# Rename columns
result.rename(columns={'2022': '2022 qty', '2023': '2023 qty', '2024': '2024 qty'}, inplace=True)

# Fill NaN values with "Not available"
result[['total qty', '2022 qty', '2023 qty', '2024 qty']] = result[['total qty', '2022 qty', '2023 qty', '2024 qty']].fillna("Not available")

# Reset index if needed
result.reset_index(inplace=True)

# Save the result
result.to_csv("C:\\Users\\Vserve-User\\Downloads\\Spire\\final_matched_data.csv", index=False)