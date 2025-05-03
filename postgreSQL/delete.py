import csv
import psycopg2
from psycopg2.extras import execute_values
import time
import statistics

orderDB = ["clients", "accounts", "transactions", "cards", "loans"]

DB_CONFIG_POSTGRES = {
    'dbname': 'testdb',
    'user': 'user',
    'password': 'pass',
    'host': 'localhost',
    'port': 5432
}

def read_csv(file_path):
    with open(file_path, newline='') as csvfile:
        reader = csv.DictReader(csvfile)
        headers = reader.fieldnames
        rows = [tuple(row[col] for col in headers) for row in reader]
    return headers, rows


def clear_table(conn):
    cur = conn.cursor()
    cur.execute(f'TRUNCATE TABLE clients RESTART IDENTITY CASCADE')
    conn.commit()
    cur.close()

def insert_data(conn, table_name, headers, rows):
    start = time.time()
    placeholders = ', '.join(['%s'] * len(headers))  
    columns = ', '.join(headers)
    conflict_column = headers[0]  

    print("dodano")
    sql = f"""
    INSERT INTO {table_name} ({columns})
    VALUES %s
    """

    with conn.cursor() as cur:
        execute_values(cur, sql, rows) 
        print("dodano")
        conn.commit()
    end = time.time()
    return end - start

    
def run_sql_query(conn, query):
    start = time.time()
    cursor = conn.cursor()
    cursor.execute(query)
    conn.commit()
    end = time.time()
    return end - start

def main():
    conn_pg = psycopg2.connect(**DB_CONFIG_POSTGRES)

    queries = [
        # ("DELETE FROM clients WHERE client_id = 1;", "Usunięcie klienta o client_id = 1"),
        
        # ("DELETE FROM transactions WHERE account_id IN (SELECT account_id FROM accounts WHERE client_id = 1);", "Usunięcie transakcji dla klienta"),

        ("DELETE FROM accounts a USING clients c WHERE a.client_id = c.client_id AND a.balance > 1000;", "Usunięcie klientów z balansem > 1000"),

        ("DELETE FROM clients WHERE client_id NOT IN (SELECT DISTINCT client_id FROM transactions);", "Usunięcie klientów bez transakcji"),
    ]


    for query, description in queries:
        times = []
        for database in orderDB:
            CSV_FILE = f'../data/{database}.csv'
            headers, data = read_csv(CSV_FILE)
            time = insert_data(conn_pg, database, headers, data)
            print(f'Added ${database} time in {time}')
        for i in range(10):  
            print(f"▶️ Iteracja {i+1}/10 - {description}")
            elapsed_time = run_sql_query(conn_pg, query)
            times.append(elapsed_time)
            print("Recreating database after deletion process")
            clear_table(conn_pg)
            for database in orderDB:
                CSV_FILE = f'../data/{database}.csv'
                headers, data = read_csv(CSV_FILE)
                time = insert_data(conn_pg, database, headers, data)
                print(f'Added ${database} time in {time}')

        
        avg_time = statistics.mean(times)
        min_time = min(times)
        max_time = max(times)
        print(f"\n{description}")
        print(f"Średni czas: {avg_time:.4f} sekundy")
        print(f"Minimalny czas: {min_time:.4f} sekundy")
        print(f"Maksymalny czas: {max_time:.4f} sekundy")
        print(f"Wszystkie czasy: {[round(t, 4) for t in times]}")
        print("="*40)
    conn_pg.close()

if __name__ == "__main__":
    main()
