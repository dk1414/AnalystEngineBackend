import os
import asyncpg
from google.cloud.sql.connector import create_async_connector
from sqlalchemy.ext.asyncio import AsyncEngine, create_async_engine
from google.cloud.sql.connector import IPTypes

async def get_async_engine(db_name: str) -> AsyncEngine:
    """
    Creates and returns a new *Async* SQLAlchemy engine using 
    the Cloud SQL Python Connector in async mode.
    """
    instance_conn_name = os.getenv("INSTANCE_CONNECTION_NAME", "")
    db_user = os.getenv("DB_USER", "readonly_user")
    db_pass = os.getenv("DB_PASS", "")
    db_ip_type_str = os.getenv("DB_IP_TYPE", "PUBLIC").upper()

    if db_ip_type_str == "PRIVATE":
        ip_type = IPTypes.PRIVATE
    else:
        ip_type = IPTypes.PUBLIC

    # Create an async connector
    connector = await create_async_connector()

    async def getconn() -> asyncpg.Connection:
        # Connects asynchronously
        conn: asyncpg.Connection = await connector.connect_async(
            instance_connection_string=instance_conn_name,
            driver="asyncpg",
            user=db_user,
            password=db_pass,
            db=db_name,
            ip_type=ip_type,
        )
        return conn

    # Create an async SQLAlchemy engine with the 'async_creator'
    engine = create_async_engine(
        "postgresql+asyncpg://",
        async_creator=getconn
    )
    return engine


