import psycopg2
from psycopg2.extras import execute_values
import csv
import time
import statistics

DB_CONFIG = {
    'dbname': 'testdb',
    'user': 'user',
    'password': 'pass',
    'host': 'localhost',
    'port': 5432
}

CSV_FILE = '../data/transactions.csv'
REPEAT_COUNT = 1
TABLE_NAME = 'transactions'

def read_csv(file_path):
    with open(file_path, newline='') as csvfile:
        reader = csv.DictReader(csvfile)
        return list(reader)

def clear_table(conn):
    cur = conn.cursor()
    cur.execute(f'TRUNCATE TABLE {TABLE_NAME} RESTART IDENTITY CASCADE')
    conn.commit()
    cur.close()

def insert_loans(conn, data):
    cur = conn.cursor()
    rows = [
        (row['transaction_id'], row['account_id'], row['amount'], row['transaction_type'], row['transaction_date'])
        for row in data
    ]

    start = time.perf_counter()
    execute_values(
        cur,
        f"INSERT INTO {TABLE_NAME} (transaction_id, account_id, amount, transaction_type, transaction_date) VALUES %s",
        rows
    )
    conn.commit()
    end = time.perf_counter()
    cur.close()

    return len(rows), end - start

def main():
    print("📥 Wczytywanie danych...")
    data = read_csv(CSV_FILE)
    print(f"🔢 Wczytano {len(data)} rekordów.")
    times = []

    for i in range(REPEAT_COUNT):
        print(f"▶️  Iteracja {i+1}/{REPEAT_COUNT}...")

        conn = psycopg2.connect(**DB_CONFIG)
        clear_table(conn)
        count, elapsed = insert_loans(conn, data)
        times.append(elapsed)
        conn.close()

        print(f"⏱️  Czas: {elapsed:.4f} s")

    
    print("\n📊 Podsumowanie:")
    print(f"Średni czas: {statistics.mean(times):.4f} s")
    print(f"Minimalny czas: {min(times):.4f} s")
    print(f"Maksymalny czas: {max(times):.4f} s")
    print(f"Wszystkie czasy: {[round(t, 4) for t in times]}")


if __name__ == "__main__":
    main()
