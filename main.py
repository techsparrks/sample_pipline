import pandas as pd
from hubspot import HubSpot

from access_token_config import HUBSPOT_ACCESS_TOKEN
from config import COMPANY_FIELDS, DEAL_FIELDS
from db_config import SQA_CONN_PUB, COMPANIES_TABLE
from process_hubspot_data import process_hubspot_data
from read_hubspot_data import get_hubspot_companies_data, get_hubspot_deals_data
from write_hubspot_data_to_db import upload_data_to_db

if __name__ == '__main__':

    hubspot_request = HubSpot(access_token=HUBSPOT_ACCESS_TOKEN)
    deals_data = get_hubspot_deals_data(hubspot_request)
    deals_df = process_hubspot_data(deals_data, DEAL_FIELDS)
    ids_deals = set(deals_df['id'].values.tolist())
    companies_data = get_hubspot_companies_data(hubspot_request)
    companies_df = process_hubspot_data(companies_data, COMPANY_FIELDS)
    ids_comp = set(companies_df['id'].values.tolist())
    compare = ids_comp.intersection(ids_deals)
    final_df = pd.merge(deals_df, companies_df, on='id')
    upload_data_to_db(COMPANIES_TABLE, final_df, SQA_CONN_PUB)
