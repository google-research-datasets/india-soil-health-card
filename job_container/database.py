import logging
import os

import pg8000
import sqlalchemy
from google.cloud.sql.connector import Connector
import google.auth
import google
from google.auth.transport import requests

connector = Connector()

def init_connection_engine():
    """
    print("creating database connection with userName=%s,DATABASE_CONNECTION_NAME=%s,DATABASE_NAME=%s",os.environ["POSTGRES_IAM_USER"],os.environ["DATABASE_CONNECTION_NAME"],os.environ["DATABASE_NAME"])
    def getconn() -> pg8000.dbapi.Connection:
        conn: pg8000.dbapi.Connection = connector.connect(
            os.environ["DATABASE_CONNECTION_NAME"],
            "pg8000",
            user=os.environ["POSTGRES_IAM_USER"],
            db=os.environ["DATABASE_NAME"],
            enable_iam_auth=True,
        )
        return conn

    engine = sqlalchemy.create_engine(
        "postgresql+pg8000://",
        creator=getconn,
    )
    engine.dialect.description_encoding = None
    return engine
    """

def insertOptions(pool,state, district, mandal, village ):
    insert_stmt = sqlalchemy.text(
        """
            INSERT INTO "Options"(state, district,mandal,village)
            VALUES (:state, :district,:mandal,:village)
        """,
        )

    with pool.connect() as db_conn:
        db_conn.execute(insert_stmt, state=state, district=district, mandal=mandal, village=village)


def getAllOptions(pool, ingested, limit):
    if ingested:
        if ingested != "true":
            ingested = False
        else:
            ingested = True

        query_stmt = ""
        if not limit:
            query_stmt = sqlalchemy.text(
                """
                    SELECT state, district, mandal, village from "Options" WHERE ingested = :ingested
                """,)
            data = []
            columns = ["state","district","mandal", "village"]
            with pool.connect() as conn:
                for row in conn.execute(query_stmt,ingested=ingested).fetchall():
                    data.append(dict(zip(columns, row)))
                return data
        else:
            query_stmt = sqlalchemy.text(
                """
                    SELECT state, district, mandal, village from "Options" WHERE ingested = :ingested LIMIT :limit 
                """,)
            data = []
            columns = ["state","district","mandal", "village"]
            with pool.connect() as conn:
                for row in conn.execute(query_stmt, ingested=ingested, limit=int(limit)).fetchall():
                    data.append(dict(zip(columns, row)))
                return data
    else:
        if not limit:
            query_stmt = sqlalchemy.text(
                """
                    SELECT state, district, mandal, village from "Options"
                """,
            )
            data = []
            columns = ["state","district","mandal", "village"]
            with pool.connect() as conn:
                for row in conn.execute(query_stmt).fetchall():
                    data.append(dict(zip(columns, row)))
                return data
        else:
            query_stmt = sqlalchemy.text(
                """
                    SELECT state, district, mandal, village from "Options" LIMIT :limit 
                """,
            )
            data = []
            columns = ["state","district","mandal", "village"]
            with pool.connect() as conn:
                for row in conn.execute(query_stmt, limit = int(limit)).fetchall():
                    data.append(dict(zip(columns, row)))
                return data

def getAllOptionsForState(pool, state, ingested, limit):
    if ingested != "":
         return _getAllOptionsForStateWithIngested(pool, state, ingested, limit)
    else:
         return _getAllOptionsForStateWithoutIngested(pool, state, limit)


def _getAllOptionsForStateWithIngested(pool, state, ingested, limit):
    if ingested != "true":
        ingested = False
    else:
        ingested = True

    query_stmt = ""
    if not limit:
        query_stmt = sqlalchemy.text(
            """
                SELECT state, district, mandal, village from "Options" WHERE state = :state AND ingested = :ingested
            """,)
        data = []
        columns = ["state","district","mandal", "village"]
        with pool.connect() as conn:
            for row in conn.execute(query_stmt, state=state,ingested=ingested).fetchall():
                data.append(dict(zip(columns, row)))
            return data
    else:
        query_stmt = sqlalchemy.text(
            """
                SELECT state, district, mandal, village from "Options" WHERE state = :state AND ingested = :ingested LIMIT :limit 
            """,)
        data = []
        columns = ["state","district","mandal", "village"]
        with pool.connect() as conn:
            for row in conn.execute(query_stmt, state=state,ingested=ingested, limit=int(limit)).fetchall():
                data.append(dict(zip(columns, row)))
            return data

def _getAllOptionsForStateWithoutIngested(pool, state, limit):
    if not limit:
        query_stmt = sqlalchemy.text(
            """
                SELECT state, district, mandal, village from "Options" WHERE state = :state
            """,
        )
        data = []
        columns = ["state","district","mandal", "village"]
        with pool.connect() as conn:
            for row in conn.execute(query_stmt, state=state).fetchall():
                data.append(dict(zip(columns, row)))
            return data
    else:
        query_stmt = sqlalchemy.text(
            """
                SELECT state, district, mandal, village from "Options" WHERE state = :state LIMIT :limit 
            """,
        )
        data = []
        columns = ["state","district","mandal", "village"]
        with pool.connect() as conn:
            for row in conn.execute(query_stmt, state=state, limit = int(limit)).fetchall():
                data.append(dict(zip(columns, row)))
            return data


def confirmVillage(pool,state, district, mandal, village ):
    insert_stmt = sqlalchemy.text(
        """
            UPDATE "Options" SET ingested=TRUE WHERE state = :state AND district = :district AND mandal = :mandal AND village = :village
        """,
        )

    with pool.connect() as db_conn:
        db_conn.execute(insert_stmt, state=state, district=district, mandal=mandal, village=village)


if __name__ == "__main__":
    state = "Assam"
    pool = init_connection_engine()
    """shc_dl = scraper.ShcDL()
    asyncio.run(shc_dl.setup())
    options = asyncio.run(shc_dl.getAllSearchOptionsForState(state))
    print(options)
    for index, row in options.iterrows():
        mandal = row['mandal']
        district = row['district']
        village = row['village']
        insertOptions(pool, state, district, mandal, village)
    """