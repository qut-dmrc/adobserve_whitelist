# FACEBOOK:
# ========

# A list of the pages you want to search for:
pages = ['page1','page2','page3', ...]
# BIGQUERY:
# ========

bq_project = 'project'

bq_dataset = 'dataset'

bq_credentials = config("BQ_CREDENTIAL_FILE") #service account credentials
# bq_credentials = 'credentials.json' #service acocunt credentials
# If table does not exist it will be created.
# Otherwise, if columns match, data will be appended

# NOTIFICATION EMAILS:
# ====================

# email_to_notify = 'user@example.com'

# mailgun details
# (find them under the respective domain name here: https://mailgun.com/app/domains)
# mailgun_default_smtp_login = ''
# mailgun_api_base_url = ''
# mailgun_api_key = ''
