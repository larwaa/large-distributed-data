import mysql.connector as mysql
import pandas as pd

class Database:
    def __init__(
            self,
            host: str,
            database: str,
            user: str,
            password: str,
            port: int,
        ):
        try:
            self.connection = mysql.connect(host=host, database=database, user=user, password=password, port=port)
        except Exception as e:
            print("ERROR: Failed to connect to db:", e)

        self.cursor = self.connection.cursor()

        print("Connected to:", self.connection.get_server_info())
        # get database information
        self.cursor.execute("select database();")
        database_name = self.cursor.fetchone()
        print("You are connected to the database:", database_name)
        print("-----------------------------------------------\n")

    def close(self):
        """
        Close the connection to the database.
        """

        self.cursor.close()
        self.connection.close()
        print("\n-----------------------------------------------")
        print("Connection to %s is closed" % self.connection.get_server_info())

    def query(self, statement: str, data: tuple | None = None) -> pd.DataFrame:
        """
        Utility method to execute the queries and provide us with a
        nice output
        """
        print("Running statement:\n", statement)
        self.cursor.execute(statement, data)
        result = self.cursor.fetchall()
        description = self.cursor.description
        print("Query Finished")
        
        if (description is None):
            return pd.DataFrame(result)

        headers = [description[0] for description in description]
        return pd.DataFrame(result, columns=headers)

        

