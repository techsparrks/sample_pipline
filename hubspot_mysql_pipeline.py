from hubspot import HubSpot
from config import HUBSPOT_ACCESS_TOKEN

api_client = HubSpot(access_token=HUBSPOT_ACCESS_TOKEN)
all_companies = api_client.crm.companies.get_all()
