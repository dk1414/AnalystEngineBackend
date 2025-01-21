import os
import sqlalchemy
from google.cloud.sql.connector import Connector, IPTypes

def get_connection(database_name: str):
    """
    Creates and returns a new SQLAlchemy connection object using the Cloud SQL Python Connector.
    No global variables are used—this function instantiates a fresh Connector and Engine
    each time it’s called.

    Environment variables needed:
      - INSTANCE_CONNECTION_NAME: e.g. "my-project:us-central1:my-instance"
      - DB_USER: e.g. "readonly_user"
      - DB_PASS: password for the above user
      - DB_IP_TYPE: "PUBLIC" or "PRIVATE" (default "PUBLIC")

    :param database_name: The name of the PostgreSQL database to connect to (e.g. "statcast").
    :return: A SQLAlchemy connection object.
    """

    # Read environment variables
    instance_conn_name = os.getenv("INSTANCE_CONNECTION_NAME", "")
    db_user = os.getenv("DB_USER", "readonly_user")
    db_pass = os.getenv("DB_PASS", "")
    db_ip_type_str = os.getenv("DB_IP_TYPE", "PUBLIC").upper()

    # Determine IP type for the connector
    if db_ip_type_str == "PRIVATE":
        ip_type = IPTypes.PRIVATE
    else:
        ip_type = IPTypes.PUBLIC

    # Instantiate a local Connector (not global)
    connector = Connector()

    # Define a connection-creation function for the connector
    def getconn():
        return connector.connect(
            instance_conn_name,
            driver="pg8000",
            user=db_user,
            password=db_pass,
            db=database_name,
            ip_type=ip_type,
        )

    # Create a temporary Engine with the 'creator' callback
    engine = sqlalchemy.create_engine(
        "postgresql+pg8000://",
        creator=getconn,
    )

    # Return a brand-new connection from the engine
    # (The engine and connector will be garbage-collected if not stored)
    return engine.connect()

