import jaydebeapi
class connect_postgres:
    def __init__(self, db, user, pwd, host, port, path_jar):
        self._db: str = db
        self._user: str = user
        self._pwd: str = pwd
        self._host: str = host
        self._port: int = port
        self._path_jar: str = path_jar
    def connection_db(self) -> str:
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