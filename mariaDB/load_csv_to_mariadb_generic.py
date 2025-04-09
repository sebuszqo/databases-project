import mariadb
import csv
import time
import statistics
import sys
import os

DB_CONFIG = {
    'user': 'user',
    'password': 'pass',
    'host': '127.0.0.1',
    'port': 3306,
    'database': 'testdb'
}

TABLE_NAME = 'transactions'
CSV_FILE = f'../data/{TABLE_NAME}.csv'
REPEAT_COUNT = 1

def read_csv(file_path):
    with open(file_path, newline='') as csvfile:
        reader = csv.DictReader(csvfile)
        headers = reader.fieldnames
        rows = [tuple(row[col] for col in headers) for row in reader]
    return headers, rows

def clear_table(conn, table_name):
    with conn.cursor() as cur:
        cur.execute(f'DELETE FROM {table_name}')
    conn.commit()

def insert_data(conn, table_name, headers, rows):
    placeholders = ', '.join(['%s'] * len(headers))
    columns = ', '.join(headers)
    sql = f"INSERT INTO {table_name} ({columns}) VALUES ({placeholders})"

    with conn.cursor() as cur:
        start = time.perf_counter()
        cur.executemany(sql, rows)
        conn.commit()
        end = time.perf_counter()

    return len(rows), end - start

def main():
    print(f"📥 Wczytywanie danych z {CSV_FILE}...")
    headers, data = read_csv(CSV_FILE)
    print(f"🔢 Wczytano {len(data)} rekordów i {len(headers)} kolumn.")
    times = []

    for i in range(REPEAT_COUNT):
        print(f"▶️  Iteracja {i+1}/{REPEAT_COUNT}...")

        conn = mariadb.connect(**DB_CONFIG)
        clear_table(conn, TABLE_NAME)
        count, elapsed = insert_data(conn, TABLE_NAME, headers, data)
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
