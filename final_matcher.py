import pandas as pd
import numpy as np

dfmatched = pd.read_csv("C:\\Users\\Vserve-User\\Downloads\\Spire\\matched_data.csv")
df_combined_sales = pd.read_excel("C:\\Users\\Vserve-User\\Downloads\\Spire\\Combined_sales_pivot.xlsx")


dfmatched["total qty"] = ""
dfmatched["2022 qty"] = ""
dfmatched["2023 qty"] = ""
dfmatched["2024 qty"] = ""

for i, r in dfmatched.iterrows():
    spire_pn = dfmatched.loc[i, "Spire SPN"]
    check_flag = 0

    for j, k in df_combined_sales.iterrows():
        if df_combined_sales.loc[j, "part number"] == spire_pn:
            dfmatched.loc[i, "total qty"] = df_combined_sales.loc[j, "total qty"]
            dfmatched.loc[i, "2022 qty"] = df_combined_sales.loc[j, "2022"]
            dfmatched.loc[i, "2023 qty"] = df_combined_sales.loc[j, "2023"]
            dfmatched.loc[i, "2024 qty"] = df_combined_sales.loc[j, "2024"]

            check_flag = 1
            break
    
    if check_flag == 0:
        dfmatched.loc[i, "total qty"] = "Not available"
        dfmatched.loc[i, "2022 qty"] = "Not available"
        dfmatched.loc[i, "2023 qty"] = "Not available"
        dfmatched.loc[i, "2024 qty"] = "Not available"


dfmatched.to_csv("C:\\Users\\Vserve-User\\Downloads\\Spire\\final_matched_data.csv")



