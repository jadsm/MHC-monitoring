import pandas as pd
import firebase_admin
from firebase_admin import credentials
from firebase_admin import firestore
import os

# 1. Initialize the SDK
cred = credentials.Certificate("/home/juan/Desktop/Juan/code/.creds/creds-myheart-counts-development.json")
firebase_admin.initialize_app(cred)

# 2. Get a reference to the database
db = firestore.client()

user_df = pd.DataFrame()
# doc_ref = db.collection("users").document("3oflMC0bFZfL6xkC4jblequIRXn2")

docs = db.collection("users").stream()

df = pd.DataFrame()
for i0,doc in enumerate(docs):
    try:
    # if True:
        user = doc.to_dict()
        user.update({'user_id': doc.id})
        user_df = pd.concat([user_df, pd.DataFrame(user, index=[0])], ignore_index=True)
        user_df.to_csv(f'firestore_user_temp{doc.id}.csv')

        # List subcollections
        doc_ref = db.collection("users").document(doc.id)
        try:
            doc_ref.collections()
        except Exception as e:
            print(e)
            continue

        subcollections = [sub_col.id for sub_col in doc_ref.collections()]
        A = []
        for i,subcollection in enumerate(subcollections):
            if subcollection.startswith("HealthObservations"):
                metrics = doc_ref.collection(subcollection).stream()
                try:                
                    for metric in metrics:
                        if not 'effectivePeriod' in metric.to_dict() or metric.to_dict()['effectivePeriod'] is None:
                            continue
                        
                        # print(f"Order ID: {metric.id} => {metric.to_dict()['valueString']}")
                        aux = metric.to_dict()['effectivePeriod']
                        aux.update({'value': metric.to_dict()['valueString'] if 'valueString' in metric.to_dict() else None,
                                    'metric': subcollection,
                                    'user_id': doc_ref.id})

                        A.append(aux)
                    # break
                # if i >= 2:
                #     break
                except Exception as e:
                    print(e)
                    continue
        df_aux = pd.DataFrame(A)
        df_aux.to_csv(f'firestore_temp{doc.id}.csv')
        df = pd.concat([df, df_aux], ignore_index=True)
        # if i0 >= 2:
            #     break
    except Exception as e:
        print(e)

# analytics
df = pd.concat([pd.read_csv(p) for p in os.listdir('.') if p.startswith('firestore_temp')],axis=0,ignore_index=True)

df_a = df.groupby(['user_id','metric'])['start'].agg(['min','max','count']).reset_index()
df_a['span'] = (pd.to_datetime(df_a['max'],format='ISO8601') - pd.to_datetime(df_a['min'],format='ISO8601')).dt.days

df_a.to_csv('firestore_snapshot.csv')
    