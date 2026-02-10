import pandas as pd
import json

# numeric labels - 'temp/merged_numeric_labels.csv'
dfn = pd.read_csv('temp/merged_numeric_labels.csv')

# categorical labels - 'temp/merged_categorical_labels.csv'
dfc = pd.read_csv('temp/merged_categorical_labels.csv')
dfc.rename(columns={'category':'values','label_type':'labels'},inplace=True)

# longitudinal labels - 'temp/longitudinal_labels.csv'
dfl = pd.read_csv('temp/happiness_longitudinal.csv')

df = pd.concat([dfn, dfc,dfl], axis=0, ignore_index=True)
df['timestamps'] = df['timestamps'].fillna('')

grouped = df.groupby(['labels', 'HealthCode']).agg({
    'values': list,
    'timestamps': list
})

# 3. Transform to nested dictionary structure
final_dict = {}

# Iterate through the multi-index groups
for (label, health_code), row in grouped.iterrows():
    if label not in final_dict:
        final_dict[label] = {}
    
    final_dict[label][health_code] = {
        "timestamps": row['timestamps'],
        "values": row['values']
    }

# 4. Save to local file
file_name = "source_data/last_labels.json"
with open(file_name, "w") as f:
    json.dump(final_dict, f, indent=2)

print(f"Saved to {file_name}")

a = 0
# {
#   "sleep_diagnosis1": {
#     "e0f99e16-cc25-44c4-8b9e-83efd6d0f923": {
#       "timestamps": [
#         "2015-03-09T12:47:16"
#       ],
#       "values": [
#         true
#       ]
#     }
#     }
#     }