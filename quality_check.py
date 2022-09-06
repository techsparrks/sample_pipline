import pandas as pd

from db_config import query_get_companies_from_db


def passed_quality_check(database_df, final_df, start_date, end_date):
    mask_database = (database_df['company_created_at'] >= start_date) & (database_df['company_created_at'] <= end_date)
    database_results_count = len(database_df.loc[mask_database])
    mask_final = (final_df['company_created_at'] >= start_date) & (final_df['company_created_at'] <= end_date)
    pipeline_results_count = len(final_df.loc[mask_final])
    return database_results_count == pipeline_results_count


def select_new_data(sqa_con, final_df, start_date, end_date):
    database_df = pd.read_sql(query_get_companies_from_db, con=sqa_con)
    if passed_quality_check(database_df, final_df, start_date, end_date):
        new_entries_df = database_df.merge(final_df, indicator=True, how='left').loc[lambda x: x['_merge'] == 'right']
        return new_entries_df
    else:
        return None
