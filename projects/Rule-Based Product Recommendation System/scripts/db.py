from sqlalchemy import create_engine

# from sshtunnel import SSHTunnelForwarder
import pandas as pd
from datetime import datetime

# load .env
import os
from os.path import join, dirname
from dotenv import load_dotenv

dotenv_path = join(dirname(__file__), ".env")
load_dotenv(dotenv_path)

USERNAME_DB1 = os.environ.get("USERNAME_DB1")
PASSWORD_DB1 = os.environ.get("PASSWORD_DB1")
HOST_DB1 = os.environ.get("HOST_DB1")
PORT_DB1 = os.environ.get("PORT_DB1")
USERNAME_DB2 = os.environ.get("USERNAME_DB2")
PASSWORD_DB2 = os.environ.get("PASSWORD_DB2")
HOST_DB2 = os.environ.get("HOST_DB2")
PORT_DB2 = os.environ.get("PORT_DB2")
USERNAME_DB3 = os.environ.get("USERNAME_DB3")
PASSWORD_DB3 = os.environ.get("PASSWORD_DB3")
HOST_DB3 = os.environ.get("HOST_DB3")
PORT_DB3 = os.environ.get("PORT_DB3")
USERNAME_DB4 = os.environ.get("USERNAME_DB4")
PASSWORD_DB4 = os.environ.get("PASSWORD_DB4")
HOST_DB4 = os.environ.get("HOST_DB4")
PORT_DB4 = os.environ.get("PORT_DB4")
USERNAME_DWH = os.environ.get("USERNAME_DWH")
PASSWORD_DWH = os.environ.get("PASSWORD_DWH")
HOST_DWH = os.environ.get("HOST_DWH")
PORT_DWH = os.environ.get("PORT_DWH")
USERNAME_BILLING = os.environ.get("USERNAME_BILLING")
PASSWORD_BILLING = os.environ.get("PASSWORD_BILLING")
HOST_BILLING = os.environ.get("HOST_BILLING")
PORT_BILLING = os.environ.get("PORT_BILLING")

# Define connection generator function
def newConnection(
    con="",
    user="",
    password="",
    host="",
    port="3306",
    dbName="",
    connect_timeout=30,
    charset="utf8",
    driver="pymysql",
):
    res = ""
    try:
        conn = create_engine(
            url="mysql+{driver}://{user}:{password}@{host}:{port}/{dbName}?charset={charset}".format(
                driver=driver,
                user=user,
                password=password,
                host=host,
                port=port,
                dbName=dbName,
                charset=charset,
            ),
            connect_args={"connect_timeout": connect_timeout},
            pool_size=5,
            max_overflow=25,
        )
        conn.connect()
        conn.execute("SET SQL_MODE=ANSI_QUOTES")
        res = "OK"
    except Exception as e:
        conn = "ERROR"
        res = "ERROR"
        print(str(e))
    finally:
        print(
            f"{datetime.now()} - Connection to Con: {con if con != '' else '-'}, DB: {dbName if dbName != '' else 'None'}, {res}"
        )
        return conn


class Connections:
    # Create connections
    def db1(dbName=""):
        return newConnection(
            con="db1",
            user=USERNAME_DB1,
            password=PASSWORD_DB1,
            host=HOST_DB1,
            port=PORT_DB1,
            dbName=dbName,
        )

    def db2(dbName=""):
        return newConnection(
            con="db2",
            user=USERNAME_DB2,
            password=PASSWORD_DB2,
            host=HOST_DB2,
            port=PORT_DB2,
            dbName=dbName,
        )

    def db3(dbName=""):
        return newConnection(
            con="db3",
            user=USERNAME_DB3,
            password=PASSWORD_DB3,
            host=HOST_DB3,
            port=PORT_DB3,
            dbName=dbName,
        )

    def db4(dbName=""):
        return newConnection(
            con="db4",
            user=USERNAME_DB4,
            password=PASSWORD_DB4,
            host=HOST_DB4,
            port=PORT_DB4,
            dbName=dbName,
        )

    def dwh(dbName=""):
        return newConnection(
            con="dwh",
            user=USERNAME_DWH,
            password=PASSWORD_DWH,
            host=HOST_DWH,
            port=PORT_DWH,
            dbName=dbName,
        )

    def billing(dbName=""):
        return newConnection(
            con="billing",
            user=USERNAME_BILLING,
            password=PASSWORD_BILLING,
            host=HOST_BILLING,
            port=PORT_BILLING,
            dbName=dbName,
        )