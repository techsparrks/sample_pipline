import sqlalchemy

USER = 'dev_iliyana'
PASSWORD = 'password'
SERVER = 'localhost'
PORT = '3306'
DATABASE = 'test_db'
SQA_CONN_STR = f"mysql+pymysql://{USER}:{PASSWORD}@{SERVER}:{PORT}/{DATABASE}"
SQA_CONN_PUB_ENGINE = sqlalchemy.create_engine(SQA_CONN_STR)
SQA_CONN_PUB = SQA_CONN_PUB_ENGINE.connect()

COMPANIES_TABLE = 'companies'
