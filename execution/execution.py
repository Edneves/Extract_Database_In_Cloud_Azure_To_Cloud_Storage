import os
from etl.extract_data import extract_data
import yaml

# SERVICE ACCOUNT JSON KEY PATH
credentials = '/PATH/TO/KEY/JSON'

db = os.environ['DB_NAME']
user = os.environ['USER_DB']
pwd = os.environ['PWD_DB']
host = os.environ['HOST_DB']
port = os.environ['PORT_DB']
path_jar = os.environ['POSTGRESQL_JAR']

# BUCKET NAME IN CLOUD STORAGE
name_bucket = 'landing_z'

# SQL FILE PATH
file_sql = '../sql/query.sql'

def execution(db,user,pwd,host,port,path_jar,name_bucket,file_sql,credentials):
    with open('../config.yaml','r') as config:
        # READ YAML FILE AND RETURN GENERATOR
        params = yaml.safe_load_all(config)
        for param in params:
            # LOOPS THROUGH THE GENERATOR AND RETURNS A DICTIONARY
            tables = param['tables']['list_tables']
            # TURN THE DICTIONARY INTO A LIST THE TABLES
        for table in tables:
            # I GO THROUGH THE LIST AND GET THE COLLUMNS
            columns = param['tables']['list_columns'][table]
            name_blob = f"input/{table}/{table}"
            object = extract_data(db,
                                  user,
                                  pwd,
                                  host,
                                  port,
                                  path_jar,
                                  file_sql,
                                  table,
                                  columns,
                                  name_bucket,
                                  name_blob,
                                  credentials)

            connection = object.connection_db()
            file = object.read_file_sql()
            result_query = object.execute_query(connection, file)
            dataframe = object.generate_dataframe(result_query)
            object.load_data_storage(dataframe)
def main(db,user,pwd,host,port,path_jar,name_bucket,file_sql,credentials):
    execution(db,user,pwd,host,port,path_jar,name_bucket,file_sql,credentials)

if __name__ == "__main__":
    main(db,user,pwd,host,port,path_jar,name_bucket,file_sql,credentials)