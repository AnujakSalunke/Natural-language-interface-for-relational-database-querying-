def get_schema(connection):
    cursor = connection.cursor(dictionary=True)
    cursor.execute("SHOW TABLES")
    tables = cursor.fetchall()

    cursor.execute("SELECT DATABASE()")
    db = cursor.fetchone()['DATABASE()']

    schema = {}
    for t in tables:
        table_name = t[f"Tables_in_{db}"]

        cursor.execute(f"DESCRIBE {table_name}")
        columns = cursor.fetchall()

        cursor.execute(f"SELECT * FROM {table_name} LIMIT 3")
        sample = cursor.fetchall()

        cursor.execute(f"""
            SELECT COLUMN_NAME, REFERENCED_TABLE_NAME, REFERENCED_COLUMN_NAME
            FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE
            WHERE TABLE_SCHEMA='{db}'
            AND TABLE_NAME='{table_name}'
            AND REFERENCED_TABLE_NAME IS NOT NULL
        """)
        fks = cursor.fetchall()

        schema[table_name] = {
            "columns": columns,
            "sample": sample,
            "foreign_keys": fks
        }

    cursor.close()
    return schema
