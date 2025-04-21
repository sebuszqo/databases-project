import csv
import mariadb
import time
import statistics

orderDB = ["clients", "accounts", "transactions", "cards", "loans"]

DB_CONFIG = {
    'user': 'user',
    'password': 'pass',
    'host': '127.0.0.1',
    'port': 3306,
    'database': 'testdb'
}

def read_csv(file_path):
    with open(file_path, newline='') as csvfile:
        reader = csv.DictReader(csvfile)
        headers = reader.fieldnames
        rows = [tuple(row[col] for col in headers) for row in reader]
    return headers, rows

def insert_data(conn, table_name, headers, rows):
    placeholders = ', '.join(['%s'] * len(headers))
    columns = ', '.join(headers)
    sql = f"INSERT IGNORE INTO {table_name} ({columns}) VALUES ({placeholders})"

    with conn.cursor() as cur:
        cur.executemany(sql, rows)
        conn.commit()

    return len(rows)

def run_sql_query(conn, query):
    start = time.time()
    cursor = conn.cursor()
    cursor.execute(query)
    conn.commit()
    end = time.time()
    return end - start

def main():
    conn_mariadb = mariadb.connect(**DB_CONFIG)

    queries = [
        # ("DELETE FROM clients WHERE client_id = 1;", "Usunięcie klienta o client_id = 1"),
        
        # ("DELETE FROM transactions WHERE account_id IN (SELECT account_id FROM accounts WHERE client_id = 1);", "Usunięcie transakcji dla klienta"),

        ("DELETE FROM accounts WHERE client_id IN (SELECT client_id FROM clients WHERE balance > 1000);", "Usunięcie klientów z balansem > 1000"),

        ("DELETE FROM clients WHERE client_id NOT IN (SELECT DISTINCT client_id FROM transactions);", "Usunięcie klientów bez transakcji"),
    ]


    for query, description in queries:
        times = []
        for i in range(100):  
            print(f"▶️ Iteracja {i+1}/100 - {description}")
            elapsed_time = run_sql_query(conn_mariadb, query)
            times.append(elapsed_time)
            print("Recreating database after deletion process")
            for database in orderDB:
                CSV_FILE = f'../data/{database}.csv'
                headers, data = read_csv(CSV_FILE)
                count = insert_data(conn_mariadb, database, headers, data)

        
        avg_time = statistics.mean(times)
        min_time = min(times)
        max_time = max(times)
        print(f"\n{description}")
        print(f"Średni czas: {avg_time:.4f} sekundy")
        print(f"Minimalny czas: {min_time:.4f} sekundy")
        print(f"Maksymalny czas: {max_time:.4f} sekundy")
        print(f"Wszystkie czasy: {[round(t, 4) for t in times]}")
        print("="*40)
    conn_mariadb.close()

if __name__ == "__main__":
    main()
