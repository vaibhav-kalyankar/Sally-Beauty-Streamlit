import os
import urllib.parse

import pandas as pd
import pyodbc
from dotenv import load_dotenv
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker


load_dotenv()


def set_env_var():
    global sqlUserName, sqlPassword, sqlDatabaseName, sqlServerName, sqlSchemaName
    sqlUserName = os.getenv("sqlUserName")
    sqlPassword = os.getenv("sqlPassword")
    sqlDatabaseName = os.getenv("sqlDatabaseName")
    sqlServerName = os.getenv("sqlServerName")
    sqlSchemaName = os.getenv("sqlSchemaName")
    return sqlUserName, sqlPassword, sqlDatabaseName, sqlServerName, sqlSchemaName


set_env_var()


class database:
    #@timer_func
    def __init__(self):
        driver = "{ODBC Driver 17 for SQL Server}"
        password = f"{sqlPassword}"
        params = urllib.parse.quote_plus(
            f"""Driver={driver};
                                        Server=tcp:{sqlServerName},1433;
                                        Database={sqlDatabaseName};
                                        Uid={sqlUserName};Pwd={password};
                                        Encrypt=yes;
                                        TrustServerCertificate=no;
                                        Connection Timeout=30;"""
        )
        self.conn_str = "mssql+pyodbc:///?autocommit=true&odbc_connect={}".format(
            params
        )
        self.engine = create_engine(self.conn_str, fast_executemany=True)

    # Fetching the data from the selected table using SQL query
    #@timer_func
    def read_table(self, query):
        print('inside read_table')
        rawData = pd.read_sql(sql=query, con=self.engine.connect())
        return rawData
    
    def read_sql_database(self, query):
        
        # Create an SQLAlchemy engine
        # connection_string=self.conn_str
        # engine = create_engine(connection_string)
        engine=self.engine
        print('self.conn_str',self.conn_str)
        
        # Use the engine to connect and execute the query
        with engine.connect() as connection:
            # Read the SQL query into a DataFrame
            df = pd.read_sql_query(sql=query, con=connection)
            
        return df

    #@timer_func
    def execute_query(self, query):
        connection = self.engine.connect()
        result = connection.execute(query)
        result.close()
        connection.close()

    #@timer_func
    def insert_data(
        self,
        df: pd.DataFrame,
        table_name: str,
        schema_name: str,
        chunksize=None,
        method=None,
    ):
        try:
            df.to_sql(
                table_name,
                con=self.engine,
                if_exists="append",
                index=False,
                chunksize=chunksize,
                schema=schema_name,
                method=method,
            )
        except Exception as e:
            print(e)
