import mysql.connector
from mysql.connector import Error

def create_connection(host, user, password, database):
    try:
        conn = mysql.connector.connect(
            host=host,
            user=user,
            password=password,
            database=database
        )
        return conn
    except Error as e:
        raise Exception(f"Database connection error: {e}")
