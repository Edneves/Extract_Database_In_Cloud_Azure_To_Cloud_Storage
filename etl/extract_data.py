from connection.connect_database import Connect_postgres
from google.cloud import storage
import pandas as pd
from pandas import DataFrame

class extract_data(Connect_postgres):
    def __init__(self, file_sql, table, columns, name_bucket, name_blob, credentials):
        super().__init__()
        self._file_sql: str = file_sql
        self._table: str = table
        self._columns: list = columns
        self._name_bucket: str = name_bucket
        self._name_blob: str = name_blob
        self._credentials: str = credentials
    def read_file_sql(self) -> str:
        # READING THE SQL FILE
        try:
            query = open(self._file_sql, 'r')
            file = query.read()
            query.close()
            print("READ FILE ON SUCCESSES")
            return file
        except Exception as e:
            print("ERROR IN READ SQL FILE", e)
    def execute_query(self, conn, query) -> list:
        # EXECUTION OF THE QUERY CONTAINED IN THE SQL FILE
        try:
            cursor = conn.cursor()
            cursor.execute(query % self._table)
            result = cursor.fetchall()
            del conn
            print("READ QUERY ON SUCCESSES")
            return result
        except Exception as e:
            print("ERROR IN EXECUTION QUERY", e)
    def generate_dataframe(self, data) -> DataFrame:
        # DATAFRAME GENERATION
        if data is not None:
            df = pd.DataFrame(data,columns=self._columns)
            print("DATAFRAME GENERATED ON SUCCESSES")
            print(df)
        return df

    def load_data_storage(self, data) -> None:
        # UPLOAD THE FILE TO CLOUD STORAGE
        client = storage.Client.from_service_account_json(json_credentials_path=self._credentials)
        try:
            bucket = client.bucket(self._name_bucket)
            if bucket.exists():
                # IF THE BUCKET EXISTS, ONLY UPLOAD THE FILE
                blob = bucket.blob(self._name_blob)
                blob.upload_from_string(data.to_csv(), 'txt/csv')
                del data
                print(
                    f"{self._name_blob} uploaded to {self._name_bucket}."
                )
            else:
                # IF THE BUCKET DOESN'T EXIST, CREATE A NEW BUCKET AND UPLOAD THE FILE
                bucket = client.bucket(self._name_bucket)
                bucket.storage_class = "STANDARD"
                bucket = client.create_bucket(bucket, location="southamerica-east1")
                blob = bucket.blob(self._name_blob)
                blob.upload_from_string(data.to_csv(), 'txt/csv')
                del data
                print(
                    "Created bucket {} in {} with storage class {}".format(
                        bucket.name, bucket.location, bucket.storage_class
                    )
                )
                print(
                    f"{self._name_blob} uploaded to {self._name_bucket}."
                )

        except Exception as e:
            print("ERROR IN FILE LOAD CLOUD STORAGE", e)