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