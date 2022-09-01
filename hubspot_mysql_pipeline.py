import pandas as pd
from hubspot import HubSpot
from config import HUBSPOT_ACCESS_TOKEN
from db_config import SQA_CONN_PUB

# initialize the api client & send request to get all companies
api_client = HubSpot(access_token=HUBSPOT_ACCESS_TOKEN)
all_companies = api_client.crm.companies.get_all()

# simple script to upload company data into a MySQL table
list_of_companies = []
for company in all_companies:
    company_properties = {}
    company = company.to_dict()
    company_properties.update({'company_id': company['id']})
    company_properties.update({'company_name': company['properties']['name']})
    company_properties.update({'created_at': company['properties']['createdate']})
    company_properties.update({'updated_at': company['properties']['hs_lastmodifieddate']})
    list_of_companies.append(company_properties)

# create a pandas data frame and upload it to MySQL (local test DB)
df = pd.DataFrame(list_of_companies)
df.to_sql('companies', con=SQA_CONN_PUB, if_exists='replace', index=False)
