import psycopg2
import os
import pandas
from dotenv import load_dotenv

load_dotenv()

dbname=os.getenv('DATABASE')
host=os.getenv('HOST')
user=os.getenv('USER')

#connect to DB
conn = psycopg2.connect(f"dbname={dbname} user={user} host={host} port=5432")

cur = conn.cursor()

cur.execute("""
            CREATE TABLE IF NOT EXISTS test.jobs (
                id SERIAL PRIMARY KEY,
                name varchar(128) NOT NULL,
                profession varchar(128) NOT NULL
            );
            """)

conn.commit()

cur.execute("""
            INSERT INTO test.jobs(name, profession)
            VALUES 
                ('rahul', 'fullstack_engineer'),
                ('james', 'backend_engineer');
            """)

conn.commit()


cur.execute("SELECT * FROM test.jobs;")

columns = list(cur.description)
records = cur.fetchall()
cur.close

results = []
for row in records:
    row_dict = {}
    for i, col in enumerate(columns):
        row_dict[col.name] = row[i]
    results.append(row_dict)
    
print(results)