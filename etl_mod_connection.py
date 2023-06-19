from dotenv import load_dotenv
from os import getenv
from simple_salesforce import Salesforce
import psycopg2

load_dotenv()

#^ Variables SF Sandbox DEV3
SF_USERNAME = getenv('SF_USERNAME')
SF_PASSWORD = getenv('SF_PASSWORD')
SF_TOKEN = getenv('SF_TOKEN')
SF_DOMAIN = getenv('SF_DOMAIN')


parametros_sf = {
    "username" : SF_USERNAME,
    "password" : SF_PASSWORD,
    "security_token" : SF_TOKEN,
    "domain" : SF_DOMAIN
}


#* Variables PSQL Ubuntu
PSQL_HOST = getenv('PSQL_HOST')
PSQL_DATABASE = getenv('PSQL_DATABASE')
PSQL_USER = getenv('PSQL_USER')
PSQL_PASSWORD = getenv('PSQL_PASSWORD')
PSQL_PORT = getenv('PSQL_PORT')


parametros_psql = {
    "host": PSQL_HOST,
    "database": PSQL_DATABASE,
    "user": PSQL_USER,
    "password": PSQL_PASSWORD,
    "port":PSQL_PORT
}


#* Se crean las conexiones con los parametros anteriores
def psql():
    return psycopg2.connect(**parametros_psql)

def sf():
    return Salesforce(**parametros_sf)