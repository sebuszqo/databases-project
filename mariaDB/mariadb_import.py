import pymysql
import csv
import time
import statistics

DB_CONFIG = {
    'db': 'testdb',
    'user': 'user',
    'password': 'pass',
    'host': 'localhost',
    'port': 3306
}

CSV_FILE = '../data/cards.csv'
REPEAT_COUNT = 100
TABLE_NAME = 'cards'


def read_first_line_csv(file_path):
    file = open(file_path, 'r')
    first_line = file.readline()
    split_line = first_line.split(",")
    return split_line
    
    
def read_csv(file_path):
    with open(file_path, newline='') as csvfile:
        reader = csv.DictReader(csvfile)
        return list(reader)

def clear_table(conn):
    with conn.cursor() as cur:
        cur.execute(f'TRUNCATE TABLE {TABLE_NAME}')
    conn.commit()

def insert_cards(conn, data):
    with conn.cursor() as cur:
        rows = [
            (row['card_id'], row['account_id'], row['card_number'], row['expiry_date'])
            for row in data
        ]
        
        start = time.perf_counter()
        # Batch INSERT – 1 zapytanie z wieloma VALUES
        sql = f"INSERT INTO {TABLE_NAME} (card_id, account_id, card_number, expiry_date) VALUES (%s, %s, %s, %s)"
        cur.executemany(sql, rows)
        conn.commit()
        end = time.perf_counter()

    return len(rows), end - start

def main():
    split_line = read_first_line_csv(CSV_FILE)
    print(split_line)
    # print("📥 Wczytywanie danych...")
    # data = read_csv(CSV_FILE)
    # print(f"🔢 Wczytano {len(data)} rekordów.")
    # times = []

    # for i in range(REPEAT_COUNT):
    #     print(f"▶️  Iteracja {i+1}/{REPEAT_COUNT}...")

    #     conn = pymysql.connect(**DB_CONFIG)
    #     clear_table(conn)
    #     count, elapsed = insert_cards(conn, data)
    #     times.append(elapsed)
    #     conn.close()

    #     print(f"⏱️  Czas: {elapsed:.4f} s")

    # print("\n📊 Podsumowanie:")
    # print(f"Średni czas: {statistics.mean(times):.4f} s")
    # print(f"Minimalny czas: {min(times):.4f} s")
    # print(f"Maksymalny czas: {max(times):.4f} s")
    # print(f"Wszystkie czasy: {[round(t, 4) for t in times]}")

if __name__ == "__main__":
    main()
