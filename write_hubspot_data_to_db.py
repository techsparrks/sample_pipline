import requests

from access_token_config import SLACKBOT_URL
from hubspot_OOP import HubspotRequest

hubspot_request = HubspotRequest()


def upload_data_to_db(table_name, df, sql_con):
    # create a pandas data frame and upload it to MySQL (local test DB)
    df.to_sql(table_name, con=sql_con, if_exists='append', index=False)


def send_error_notification():
    # send a slack notification of anything goes wrong with the pipeline
    url = SLACKBOT_URL
    payload = {'text': 'There has been an error while fetching the data from Hubspot. Please check.'}

    requests.post(url, json=payload)
