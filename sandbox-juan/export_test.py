
import pandas as pd 
import pandas_gbq as pdg
import os
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "/home/juan/Desktop/Juan/code/.creds/creds-mhc-bq-mater.json"

query = """select patient,`Group`,diagnosis_date,startTime startDate,variable,value  from `imperial-410612.MHC_PH.activity_us2`
where 
(patient in 
(select patient
from `imperial-410612.MHC_PH.activity_us2`
where clean_status = 'clean_1'
and device_rank = 'Watch1'
and cohort = 'UK' and variable in ('StepCount','FlightsClimbedPaceMax','StepCountPaceMax',
                                'FlightsClimbedPaceMean','StepCountPaceMean',
                                'RestingHeartRate','HeartRateVariabilitySDNN')
                                and `Group` in ('Healthy')
group by patient,`Group`,diagnosis_date
having date_diff(max(startTime), min(startTime), DAY) >1000)
or `Group` = 'PAH')
and
clean_status = 'clean_1'
and device_rank = 'Watch1'
and cohort = 'UK' and variable in ('StepCount','FlightsClimbedPaceMax','StepCountPaceMax',
                                'FlightsClimbedPaceMean','StepCountPaceMean',
                                'RestingHeartRate','HeartRateVariabilitySDNN')"""
df = pdg.read_gbq(query, project_id="imperial-410612", dialect="standard")
df.to_parquet("mhc_export.parquet", index=False)


df = pd.read_parquet("mhc_export.parquet")