import psycopg2
import csv
import time

BATCH_SIZE = 1000  # co ile commit
row_count = 0

conn = psycopg2.connect(
    dbname='testdb',
    user='user',
    password='pass',
    host='localhost',
    port=5432
)
cur = conn.cursor()

start = time.perf_counter()
with open('clients.csv', newline='') as csvfile:
    reader = csv.DictReader(csvfile)
    for row in reader:
        cur.execute(
            "INSERT INTO clients (client_id, first_name, last_name, email) VALUES (%s, %s, %s, %s)",
            (row['client_id'], row['first_name'], row['last_name'], row['email'])
        )
        row_count += 1

        if row_count % BATCH_SIZE == 0:
            conn.commit()

conn.commit()  # final commit
end = time.perf_counter()
print(f"Loaded {row_count} clients in {end - start:.2f} seconds")

cur.close()
conn.close()
