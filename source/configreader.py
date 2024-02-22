from importlib import reload
from os import path

class Config:
    """A Config object contains the variables from the config file and makes
    them accessible"""

    config_path = "config.py"
    config_template = "config_template.py"

    def __init__(self, dataset):
        self.dataset = dataset
        if path.isfile(self.config_path):
            import config
        else:
            raise ImportError(
                'ImportError: No config.py file found. \
Please provide a config.py.\n'
            )

        reload(config)  # necessary to be able to change config

        self.pages = config.pages
        assert type(self.pages) is list

        # configure Google BigQuery
        self.bq_dataset = dataset

        self.bq_project = config.bq_project

        self.bq_credentials = config.bq_credentials

        # self.metadata = { page: (None,None,None,None) for page in self.pages } #(last_collected,page_id,forward_cursor,collation_id))

