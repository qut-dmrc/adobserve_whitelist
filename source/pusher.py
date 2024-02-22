from google.cloud import bigquery
from datetime import datetime
from os import remove
import pandas as pd
import os
import logging


class Pusher:
    def __init__(self, config):
        self.project = config.bq_project
        self.dataset = "whitelist_test"
        self.credentials = config.bq_credentials

    def push_to_gbq(self, file, schema=None):
        [filename, file_ext] = file.split('.')
        date_today = datetime.now().strftime("%Y-%m-%d")
        os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = self.credentials
        # Construct a BigQuery client object.
        client = bigquery.Client()
        table_id = self.dataset+'.'+filename
        table = bigquery.Table(client.project+'.'+table_id)
        # datasets = list(client.list_datasets())
        try:
            client.get_table(table_id) # check if the table exists
            print('Table exists')
        except:
            print('Creating new table')
            table = client.create_table(table)  # create the table if it doesn't

        if schema:
            job_config = bigquery.LoadJobConfig(
                source_format=bigquery.SourceFormat.CSV, 
                skip_leading_rows=1, 
                schema=schema
            )
        else:
            job_config = bigquery.LoadJobConfig(
            source_format=bigquery.SourceFormat.CSV, 
            skip_leading_rows=1, 
            autodetect=True
        )
        job_config.allow_quoted_newlines = True
        if file_ext == 'json':
            df = pd.read_json('results/'+file, orient='records')
            date_col = [date_today]*df.shape[0]
            df['scrapedDate'] = date_col
            df.to_csv("temp.csv", index=False)
            with open("temp.csv", "rb") as source_file:
                job = client.load_table_from_file(source_file, table_id, job_config=job_config)
                job.result()  # Waits for the job to complete.
            remove('temp.csv') # removing temp.csv
        if file_ext == 'csv':
            with open(file, "rb") as source_file:
                job = client.load_table_from_file(source_file, table_id, job_config=job_config)
                job.result()
            remove(file)
        # remove('results/'+file) # removing uploaded file from results

        table = client.get_table(table_id)  # Make an API request.
        ### logging
        print(
            "Loaded {} rows and {} columns to {}".format(
                table.num_rows, len(table.schema), table_id
            )
        )
        logging.basicConfig(filename='info_bq.log', level=logging.INFO)
        logging.info(
            "{}: Loaded {} rows and {} columns to {}".format(
                datetime.now(),table.num_rows, len(table.schema), table_id
            )
        )

