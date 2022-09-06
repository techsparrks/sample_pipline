def get_hubspot_companies_data(api_client):
    companies_data = api_client.crm.companies.get_all()
    return companies_data


def get_hubspot_deals_data(api_client):
    deals_data = api_client.crm.deals.get_all()
    return deals_data
