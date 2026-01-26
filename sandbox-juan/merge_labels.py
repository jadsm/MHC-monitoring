import pandas as pd

files = ['sleep_time_categories.csv', 
   'blood_pressure_categories.csv',
       'wake_time_categories.csv', 
          'go_sleep_time_categories.csv', 'bmi_categories.csv','happiness_static_categories.csv']

# merge all labels
df_final = pd.DataFrame()
for file in files:
    df = pd.read_csv('temp/'+file).loc[:,['HealthCode','category']]
    df['label_type'] = file.replace('_categories.csv','')
    df_final = pd.concat([df_final, df], axis=0, ignore_index=True)

# handle exceptions
# file = 'physical_activity_categories.csv'
# df = pd.read_csv(f'temp/{file}').loc[:,['HealthCode','phys_category','vig_category']]
# df['label_type'] = file.replace('_categories.csv','')
# df_final = pd.concat([df_final, df], axis=0, ignore_index=True)
file = 'psychological_factors_categories.csv'
df = pd.read_csv(f'temp/{file}').loc[:,['HealthCode','category','labels']].rename(columns={'labels':'label_type'})
df_final = pd.concat([df_final, df], axis=0, ignore_index=True)

df = pd.read_csv('temp/cat_final.csv').rename(columns={'labels':'label_type','values':'category'}).loc[:,['HealthCode','category','label_type']]
df_final = pd.concat([df_final, df], axis=0, ignore_index=True)

df_final.to_csv('temp/merged_labels.csv', index=False)