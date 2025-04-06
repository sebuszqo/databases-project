import psycopg2
from psycopg2.extras import execute_values
import csv
import time
import statistics

# 🔧 Konfiguracja
DB_CONFIG = {
    'dbname': 'testdb',
    'user': 'user',
    'password': 'pass',
    'host': 'localhost',
    'port': 5432
}

CSV_FILE_PATH = '../data/accounts.csv'
TABLE_NAME = 'accounts'
REPEAT_COUNT = 1000

def read_csv_data():
    with open(CSV_FILE_PATH, newline='') as csvfile:
        reader = csv.DictReader(csvfile)
        return list(reader)

def clear_table(conn):
    cur = conn.cursor()
    cur.execute(f'TRUNCATE TABLE {TABLE_NAME} RESTART IDENTITY CASCADE')
    conn.commit()
    cur.close()

def insert_bulk(conn, data):
    cur = conn.cursor()
    rows = [
        (row['account_id'], row['client_id'], row['account_number'], row['balance'])
        for row in data
    ]

    start = time.perf_counter()
    execute_values(
        cur,
        f"INSERT INTO {TABLE_NAME} (account_id, client_id, account_number, balance) VALUES %s",
        rows
    )
    conn.commit()
    end = time.perf_counter()

    cur.close()
    return end - start

def main():
    print("📥 Wczytywanie danych z CSV do pamięci...")
    data = read_csv_data()
    print(f"🔢 Wczytano {len(data)} rekordów.\n")

    times = []

    for i in range(REPEAT_COUNT):
        print(f"▶️  Iteracja {i+1}/{REPEAT_COUNT}...")

        conn = psycopg2.connect(**DB_CONFIG)
        clear_table(conn)
        elapsed = insert_bulk(conn, data)
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
