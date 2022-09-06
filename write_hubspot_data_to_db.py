from hubspot_OOP import HubspotRequest

hubspot_request = HubspotRequest()


def upload_data_to_db(table_name, df, sql_con):
    # create a pandas data frame and upload it to MySQL (local test DB)
    df.to_sql(table_name, con=sql_con, if_exists='replace', index=False)
