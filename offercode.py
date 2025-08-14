import os
import glob
import pandas as pd

# Configuration
folder = r"C:\temp"
pattern = "ES_TARGETED_OFFER_202508*.csv"
output_excel = r"C:\Temp\OfferSummary2.xlsx"

# OfferCodes to filter
offer_filter = {"50468"}

# Columns to read for performance
usecols = ['AccountID', 'OfferCode', 'AcceptanceState', 'AcceptanceTs']

all_rows = []

for filepath in glob.glob(os.path.join(folder, pattern)):
    print(f"Processing {os.path.basename(filepath)} ...")
    for i, chunk in enumerate(pd.read_csv(filepath, chunksize=100000, parse_dates=['AcceptanceTs'], low_memory=False, usecols=usecols)):
        print(f"  Read chunk {i+1} with {len(chunk)} rows")
        filtered = chunk[chunk['OfferCode'].astype(str).isin(offer_filter)].copy()
        all_rows.append(filtered)

print("Concatenating all filtered data...")
df = pd.concat(all_rows, ignore_index=True)

print("Applying vectorized AccountID AcceptanceState filtering rules...")

df['HasCompleted'] = df.groupby('AccountID')['AcceptanceState'].transform(lambda x: (x == 'COMPLETED').any())
print("HasCompleted done...")
df['HasAccepted'] = df.groupby('AccountID')['AcceptanceState'].transform(lambda x: (x == 'ACCEPTED').any())
print("HasAccepted done...")
df['HasNone'] = df.groupby('AccountID')['AcceptanceState'].transform(lambda x: (x == 'NONE').any())
print("HasNone done...")

cond_completed = (df['HasCompleted']) & (df['AcceptanceState'] == 'COMPLETED')
cond_accepted_only = (~df['HasCompleted']) & (df['HasAccepted']) & (df['HasNone']) & (df['AcceptanceState'] == 'ACCEPTED')
cond_keep_all = (~df['HasCompleted']) & (~(df['HasAccepted'] & df['HasNone']))
print("Applying conditions to filter rows...")

df_filtered = df[cond_completed | cond_accepted_only | cond_keep_all].copy()
print("Filtering completed. Dropping temporary columns...")

df_filtered.drop(columns=['HasCompleted', 'HasAccepted', 'HasNone'], inplace=True)

print("Extracting AcceptanceDate from AcceptanceTs...")
df_filtered['AcceptanceDate'] = df_filtered['AcceptanceTs'].dt.date

print("Grouping and counting summary...")
summary = (
    df_filtered.groupby(['AcceptanceDate', 'AcceptanceState', 'OfferCode'])
    .size()
    .reset_index(name='Count')
)

print(f"Exporting summary to Excel: {output_excel}")
summary.to_excel(output_excel, index=False)

print("Done.")
