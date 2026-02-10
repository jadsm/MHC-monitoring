# MHC-monitoring
A repo for tools to monitor the MHC app




project_id: myheart-counts-development 
# To deploy functions:
## firestore_search_for_new_variables:
`gcloud functions deploy firestore-search-for-new-variables \
  --gen2 \
  --runtime=python312 \
  --region=us-central1 \
  --source=. \
  --entry-point=main\
  --trigger-http\
  --memory=256MB \
  --cpu=0.5`

### schedulling
`gcloud functions add-invoker-policy-binding firestore-search-for-new-variables \
  --region=us-central1 \
  --member="serviceAccount:scheduler-etl@myheart-counts-development.iam.gserviceaccount.com"`

`gcloud scheduler jobs create http firestore-search-for-new-variables \
  --schedule="0 5 * * 0" \
  --uri="https://us-central1-my-project.cloudfunctions.net/firestore-search-for-new-variables" \
  --http-method=POST \
  --oidc-service-account-email="scheduler-etl@myheart-counts-development.iam.gserviceaccount.com" \
  --location=us-central1`

## firestore_to_BQ_parser:
`gcloud functions deploy firestore-to-BQ-parser \
  --gen2 \
  --runtime=python312 \
  --region=us-central1 \
  --entry-point=main \
  --source=. \
  --trigger-http \
  --memory=512MB \
  --cpu=1`

`gcloud functions add-invoker-policy-binding firestore-search-for-new-variables \
  --region=us-central1 \
  --member="serviceAccount:scheduler-etl@myheart-counts-development.iam.gserviceaccount.com"`

  `gcloud scheduler jobs create http firestore-to-BQ-parser \
  --schedule="0 5 * * *" \
  --uri="https://us-central1-my-project.cloudfunctions.net/firestore-to-BQ-parser" \
  --http-method=POST \
  --oidc-service-account-email="scheduler-etl@myheart-counts-development.iam.gserviceaccount.com" \
  --location=us-central1`
