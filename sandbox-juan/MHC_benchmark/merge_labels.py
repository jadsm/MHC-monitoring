import pandas as pd

files = ['sleep_time_categories.csv', 
   'blood_pressure_categories.csv',
       'wake_time_categories.csv', 
          'go_sleep_time_categories.csv', 
          'bmi_categories.csv']

# merge all labels - categories
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
df_final.to_csv('temp/merged_categorical_labels.csv', index=False)
aux = df_final.groupby(['label_type','category'])['HealthCode'].nunique().reset_index()
auxt = aux.groupby(['label_type'])['HealthCode'].sum()
aux = aux.merge(auxt,on='label_type',suffixes=('','_total'), how='left',)
aux['proportion'] = aux['HealthCode']/aux['HealthCode_total']*100
aux.drop(columns=['HealthCode_total'], inplace=True)
aux.to_csv('temp/label_counts.csv',index=False)


# handle numeric labels
files = ['numeric_values.csv', 
          'bmi_categories.csv']
df = pd.read_csv('temp/'+files[0])
df2 = pd.read_csv('temp/'+files[1])
df2.rename(columns={'BMI':'values'}, inplace=True)
df2['labels'] = 'BMI'
df.rename(columns={'values_median':'values'}, inplace=True)
df_final = pd.concat([df.loc[:,['HealthCode','values','labels']],
                       df2.loc[:,['HealthCode','values','labels']]], axis=0, ignore_index=True)
df_final.to_csv('temp/merged_numeric_labels.csv', index=False)


df_final.groupby(['labels']).agg({'HealthCode':'nunique',
                                  'values':['mean','median','std','min','max']}).to_csv('temp/value_counts.csv',index=True)
