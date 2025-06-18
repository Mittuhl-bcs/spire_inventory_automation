import pandas as pd
import numpy as np
import re
import BCS_connector

# Load the data
dfspire_orig = pd.read_csv("C:\\Users\\Vserve-User\\Downloads\\Spire\\Spire_inv\\spire_inv_081624.csv")
dfspire = dfspire_orig[["part_no", "onhand_qty"]].groupby('part_no')['onhand_qty'].sum().reset_index()

dfp21 = BCS_connector.reader_fulldf()

print("Read data from files and databases")

# Filter out rows with blank supplier_part_no in dfp21
dfp21 = dfp21[dfp21['supplier_part_no'].notna() & (dfp21['supplier_part_no'] != '')]



# Function to strip non-alphanumeric characters
def strip_non_alphanumeric(s):
    # Check if the input string is None
    if s is None:
        print(f"this is empty: {s}")
        return ""
    
    # Remove '/u' or '/U' from the end of the string, if present
    s = re.sub(r'[/uU]$', '', s)
    # Remove all spaces
    s = s.replace(" ", "")
    # Remove all non-alphanumeric characters
    s = re.sub(r'[^a-zA-Z0-9]', '', s)
    
    return s




# Apply the stripping function
dfspire['stripped_part_no'] = dfspire['part_no'].apply(strip_non_alphanumeric)
dfp21['stripped_supplier_part_no'] = dfp21['supplier_part_no'].apply(strip_non_alphanumeric)

# Function to match DataFrames
def match_dataframes(df1, df2, left_on, right_on, match_criteria):
    merged = pd.merge(df1, df2, left_on=left_on, right_on=right_on, suffixes=('_spire', '_p21'), how='inner')
    merged['match_criteria'] = match_criteria
    return merged[['item_id', 'supplier_part_no', 'stripped_supplier_part_no', 'part_no', 'stripped_part_no', 'onhand_qty', 'match_criteria']]

# Match on supplier_part_no
matched_spn = match_dataframes(dfspire, dfp21, 'part_no', 'supplier_part_no', 'SPN')

# Match on stripped supplier_part_no
matched_stripped = match_dataframes(
    dfspire[~dfspire['part_no'].isin(matched_spn['part_no'])],
    dfp21[~dfp21['supplier_part_no'].isin(matched_spn['supplier_part_no'])],
    'stripped_part_no', 'stripped_supplier_part_no', 'Stripped SPN'
)

# Combine matched results
df_spn_matched = pd.concat([matched_spn, matched_stripped], ignore_index=True)

# Rename columns for consistency
df_spn_matched.columns = ["P21 item id", "P21 supplier PN", "P21 stripped SPN", "Spire SPN", "Spire stripped SPN", "onhand_qty", "match criteria"]

# Handle unmatched entries
unmatched = dfspire[~dfspire['part_no'].isin(df_spn_matched['Spire SPN'])]
unmatched_data = {
    "P21 item id": "Not available",
    "P21 supplier PN": "Not available",
    "P21 stripped SPN": "Not available",
    "Spire SPN": unmatched['part_no'],
    "Spire stripped SPN": unmatched['stripped_part_no'],
    "onhand_qty": unmatched['onhand_qty'],
    "match criteria": "Match not available"
}
unmatched_df = pd.DataFrame(unmatched_data)

# Combine all results
df_spn_matched = pd.concat([df_spn_matched, unmatched_df], ignore_index=True)

print("All processes finished - saving the files to the folders")

# Save outputs
dfp21.to_csv("C:\\Users\\Vserve-User\\Downloads\\Spire\\P21\\p21_original.csv", index=False)
df_spn_matched.to_csv("C:\\Users\\Vserve-User\\Downloads\\Spire\\matched_data.csv", index=False)

print("Files saved!!")
