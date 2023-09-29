import jaydebeapi
class Connect_postgres(object):
    def __init__(self):
        self._db: str = None
        self._user: str = None
        self._pwd: str = None
        self._host: str = None
        self._port: int = None
        self._path_jar: str = None
    def __connection_db(self):
        # CONNECTION IN DATABASE
        try:
            connection = jaydebeapi.connect(
                "org.postgresql.Driver",
                f"jdbc:postgresql://{self._host}:{self._port}/{self._db}",
                {'user': f"{self._user}", 'password': f"{self._pwd}"},
                jars=self._path_jar
            )
            print("CONNECTION EXECUTED ON SUCCESS ")
            return connection
        except Exception as e:
            print("ERROR IN CONNECTION DATABASE", e)

    def set_connect(self, db, user, pwd, host, port, path):
        self._db = db
        self._user = user
        self._pwd = pwd
        self._host = host
        self._port = port
        self._path_jar = path
        conn = self.__connection_db()
        return conn

    def close_connection(self, conn):
        conn.close()
        print("CLOSE CONNECTION!!")