# - BiologicalSex - Male/Female  
# - Diabetes - Has diabetes diagnosis  
# - Hypertension - Has hypertension diagnosis 
# - sleep_diagnosis1 - Has sleep disorder diagnosis  
# - work - Employment status

import pandas as pd
import json

types = {
    'sleep_diagnosis1': 'boolean',
    'atwork': 'ordinal',
    'phys_activity': 'ordinal',
    'sleep_time': 'float',
    'sleep_time1': 'float',
    'vigorous_act': 'ordinal',
    'work': 'boolean',
    'sleep_diagnosis2': 'categorical',
    'happiness': 'float',
    'heart_disease': 'categorical',
    'vascular': 'categorical',
    'feel_worthwhile1': 'ordinal',
    'feel_worthwhile2': 'ordinal',
    'feel_worthwhile3': 'ordinal',
    'feel_worthwhile4': 'ordinal',
    'satisfiedwith_life': 'ordinal',
    'BiologicalSex': 'boolean',
    'WakeUpTime': 'float',
    'GoSleepTime': 'float',
    'HeightCentimeters': 'float',
    'WeightKilograms': 'float',
    'Diabetes': 'boolean',
    'Hypertension': 'boolean',
    'Hdl': 'float',
    'Ldl': 'float',
    'TotalCholesterol': 'float',
    'DiastolicBloodPressure': 'float',
    'SystolicBloodPressure': 'float'
}

# Read JSON string from a file
with open('source_data/labels_enrollment_info.json', 'r') as f:
    df = pd.DataFrame(json.loads(f.read())).T

with open('source_data/labels_labels.json', 'r') as f:
    dflbls,auxlbls_val,auxlbls_cat = pd.DataFrame(),pd.DataFrame(),pd.DataFrame()
    aux = json.loads(f.read())
    for k,v in aux.items():
        df2 = pd.DataFrame(v).T.explode(column=['timestamps','values'])
        df2 = df2.reset_index().rename(columns={'index':'HealthCode'})
        df2['labels'] = k
        dflbls = pd.concat([dflbls, df2], ignore_index=True)
        # summary
        if types[k] == 'boolean' or types[k] == 'categorical':
            aux2 = df2.groupby(['HealthCode','values']).aggregate({'timestamps':['count','min','max']} ).reset_index()   
            aux2.columns = aux2.columns.map('_'.join)
            aux2['labels'] = k
            auxlbls_cat = pd.concat([auxlbls_cat, aux2], ignore_index=True)
        elif types[k] == 'float' or types[k] == 'ordinal':
            df2['values'] = pd.to_numeric(df2['values'], errors='coerce')
            aux2 = df2.groupby('HealthCode').aggregate({'timestamps':['count','min','max'],
                                    'values':['mean','median','max','min']}).reset_index()
            aux2.columns = aux2.columns.map('_'.join)   
        
            aux2['labels'] = k
            auxlbls_val = pd.concat([auxlbls_val, aux2], ignore_index=True)

auxlbls_cat.to_csv('cat.csv')
auxlbls_val.to_csv('val.csv')

auxlbls_cat.groupby('labels').aggregate({'timestamps_count':'sum',
                                         'HealthCode_':'nunique'}).sort_values('HealthCode_',ascending=False)
#                   timestamps_count  HealthCode_
# labels                                         
# sleep_diagnosis1             56541        44498
# work                         56471        44430
# heart_disease                42640        29957
# vascular                     40090        29954
# BiologicalSex                45838        25217
# Diabetes                     24384        10209
# Hypertension                 24385        10209
# sleep_diagnosis2               966          348

auxlbls_val.groupby('labels').aggregate({'timestamps_count':'sum',
                                         'HealthCode_':'nunique'}).sort_values('HealthCode_',ascending=False)
#                         timestamps_count  HealthCode_
# labels                                               
# phys_activity                      56514        44443
# sleep_time1                        56331        44362
# sleep_time                         56335        44359
# vigorous_act                       53863        42497
# atwork                             45072        37066
# satisfiedwith_life                 62261        30563
# feel_worthwhile2                   62207        30553
# feel_worthwhile3                   62171        30546
# feel_worthwhile1                   62188        30545
# feel_worthwhile4                   61888        30444
# WeightKilograms                    47579        25437
# HeightCentimeters                  47579        25437
# GoSleepTime                        47378        25349
# WakeUpTime                         47237        25262
# SystolicBloodPressure              24235        10153
# TotalCholesterol                   23814         9937
# DiastolicBloodPressure             23679         9891
# Hdl                                21682         9187
# Ldl                                15356         6845
# happiness                          41839         4163
