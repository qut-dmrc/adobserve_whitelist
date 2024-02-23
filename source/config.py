from decouple import config

# A list of the pages you want to search for:
pages= [
# page IDs
]
# BIGQUERY:
# ========

bq_project = 'dmrc-data'

bq_dataset = ''

bq_credentials = config("BQ_CREDENTIAL_FILE") #service account credentials
# If table does not exist it will be created.
# Otherwise, if columns match, data will be appended
