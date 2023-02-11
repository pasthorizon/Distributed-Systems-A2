import psycopg2
from psycopg2 import sql

DB_NAME = 'dist_queue_2'

conn = psycopg2.connect(
        host="localhost",
        user="postgres",
        password="admin",
    )
conn.autocommit = True

cursor = conn.cursor()

cursor.execute(sql.SQL("DROP DATABASE {db_name}")
                .format(db_name = sql.Identifier(DB_NAME)))

cursor.close()
conn.close()