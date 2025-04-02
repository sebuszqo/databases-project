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
with open('accounts.csv', newline='') as csvfile:
    reader = csv.DictReader(csvfile)
    for row in reader:
        cur.execute(
            "INSERT INTO accounts (account_id, client_id, account_number, balance) VALUES (%s, %s, %s, %s)",
            (row['account_id'], row['client_id'], row['account_number'], row['balance'])
        )
        row_count += 1

        if row_count % BATCH_SIZE == 0:
            conn.commit()

conn.commit()  # final commit
end = time.perf_counter()
print(f"Loaded {row_count} clients in {end - start:.2f} seconds")

cur.close()
conn.close()
