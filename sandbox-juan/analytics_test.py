import pandas as pd
import firebase_admin
from firebase_admin import credentials
from firebase_admin import firestore
import os

# analytics
df = pd.concat([pd.read_csv(os.path.join('temp',p)) for p in os.listdir('temp') if p.startswith('firestore_temp')],axis=0,ignore_index=True)
df['duration'] = (pd.to_datetime(df['end'],format='ISO8601',utc=True) - pd.to_datetime(df['start'],format='ISO8601',utc=True)).dt.days
df['value_str'] = df['value_str'].fillna('None')
df['value_mask'] = df['value'].isna().astype(int)

df_a = df.groupby(['user_id','metric','value_str']).agg({'start':['min','max','count'],
                                             'value':['min','max','mean'],
                                             'value_mask': 'sum'}).reset_index()
# merge levels 
df_a.columns = ['_'.join(col).strip() if col[1] else col[0] for col in df_a.columns.values]
df_a['span'] = (pd.to_datetime(df_a['start_max'],format='ISO8601',utc=True) - pd.to_datetime(df_a['start_min'],format='ISO8601',utc=True)).dt.days

# clean metric names
df_a['metric'] = df_a['metric'].str.replace('HealthObservations_HKWorkoutTypeIdentifier','Workout')
df_a['metric'] = df_a['metric'].str.replace('HealthObservations_MHCHealthObservationTimedWalkingTestResultIdentifier','TimedWalkingTest')
prefixes_to_clean = ['HealthObservations_HKQuantityTypeIdentifier',
                      'HealthObservations_HKCategoryTypeIdentifier',
                      'HealthObservations_HKDataTypeIdentifier']
for prefix in prefixes_to_clean:
    df_a['metric'] = df_a['metric'].str.replace(prefix, '')
df_a.to_csv('tempfirestore_snapshot.csv')
    