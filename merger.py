import pandas as pd
import numpy as np
import re
import BCS_connector

# Load the data
dfspire_orig = pd.read_csv("C:\\Users\\Vserve-User\\Downloads\\Spire\\Spire_inv\\spire_inv_081624.csv")
dfspire = dfspire_orig[["part_no", "onhand_qty"]]
dfspire = dfspire.groupby('part_no')['onhand_qty'].sum().reset_index()

dfp21 = BCS_connector.reader_fulldf()

dfp21.to_csv("Check_p21.csv")
# Columns to work with
columns_taken = ["item_id", "supplier_part_no", "part_no", "onhand_qty"]

df_spn_matched = pd.DataFrame()

# Initialize match_check columns
dfspire["match_check"] = ""
dfp21["match_check"] = ""


print("Read data from files and databases")

# Iterate through dfspire rows
for i, r in dfspire.iterrows():
    spn_spire = dfspire.loc[i, "part_no"]

    match_check = 0  # Start as 0 and change to 1 if matched

    # Iterate through dfp21 rows
    for j, q in dfp21.iterrows():
        if dfp21.loc[j, "supplier_part_no"] == spn_spire:
            # Extract relevant data from both DataFrames
            data_to_insert = {
                "P21 item id": dfp21.loc[j, "item_id"],
                "P21 supplier PN": dfp21.loc[j, "supplier_part_no"],
                "P21 stripped SPN": re.sub(r'[^a-zA-Z0-9]', '', dfp21.loc[j, "supplier_part_no"]),
                "Spire SPN": spn_spire,
                "Spire stripped SPN": re.sub(r'[^a-zA-Z0-9]', '', spn_spire),
                "onhand_qty": dfspire.loc[i, "onhand_qty"],
                "match criteria": "SPN"
            }
            df_spn_matched = pd.concat([df_spn_matched, pd.DataFrame([data_to_insert])], ignore_index=True)
            
            match_check = 1  # Mark as matched
            dfspire.loc[i, "match_check"] = "matched"

            if dfp21.loc[j, "match_check"] == "matched":
                dfp21.loc[j, "match_check"] = "Duplicate match"
            elif dfp21.loc[j, "match_check"] == "":
                dfp21.loc[j, "match_check"] = "matched"

    if match_check == 0:
        dfspire.loc[i, "match_check"] = "not matched"


print("First step of process  - SPN matching finished!")
# Process unmatched rows with stripped SPN
dfspire_processed = dfspire[dfspire["match_check"] == "not matched"]

for i, r in dfspire_processed.iterrows():
    spn_spire = re.sub(r'[^a-zA-Z0-9]', '', dfspire_processed.loc[i, "part_no"])
    match_check = 0

    for j, q in dfp21.iterrows():
        spn21 = re.sub(r'[^a-zA-Z0-9]', '', dfp21.loc[j, "supplier_part_no"])
        if spn21 == spn_spire:
            data_to_insert = {
                "P21 item id": dfp21.loc[j, "item_id"],
                "P21 supplier PN": dfp21.loc[j, "supplier_part_no"],
                "P21 stripped SPN": spn21,
                "Spire SPN": spn_spire,
                "Spire stripped SPN": spn_spire,
                "onhand_qty": dfspire_processed.loc[i, "onhand_qty"],
                "match criteria": "Stripped SPN"
            }
            df_spn_matched = pd.concat([df_spn_matched, pd.DataFrame([data_to_insert])], ignore_index=True)
            
            match_check = 1

            if dfp21.loc[j, "match_check"] == "matched":
                dfp21.loc[j, "match_check"] = "Duplicate match"
            elif dfp21.loc[j, "match_check"] == "":
                dfp21.loc[j, "match_check"] = "matched"


print("Second step of process  - Stripped SPN matching finished!")

# Reprocessed unmatched entries
dfspire_reprocessed = dfspire_processed[dfspire_processed["match_check"] == "not matched"]

for i, r in dfspire_reprocessed.iterrows():
    spn_spire = re.sub(r'[^a-zA-Z0-9]', '', dfspire_reprocessed.loc[i, "part_no"])

    for j, q in dfp21.iterrows():
        spn21 = re.sub(r'[^a-zA-Z0-9]', '', dfp21.loc[j, "supplier_part_no"])
        if spn21 == spn_spire:
            raise ValueError("Match found even after reprocessed")

for k, l in dfspire_reprocessed.iterrows():
    data_to_insert = {
        "P21 item id": "Not available",
        "P21 supplier PN": "Not available",
        "P21 stripped SPN": "Not available",
        "Spire SPN": dfspire_reprocessed.loc[k, "part_no"],
        "Spire stripped SPN": re.sub(r'[^a-zA-Z0-9]', '', dfspire_reprocessed.loc[k, "part_no"]),
        "onhand_qty": dfspire_reprocessed.loc[k, "onhand_qty"],
        "match criteria": "Match not available"
    }
    df_spn_matched = pd.concat([df_spn_matched, pd.DataFrame([data_to_insert])], ignore_index=True)

print("All processes finished - saving the files to the folders")

# Save outputs
dfp21.to_csv("C:\\Users\\Vserve-User\\Downloads\\Spire\\P21\\p21_original.csv", index=False)
df_spn_matched.to_csv("C:\\Users\\Vserve-User\\Downloads\\Spire\\matched_data.csv", index=False)

print("Files saved!!")
