import psycopg2
import time
import statistics

DB_CONFIG_POSTGRES = {
    'dbname': 'testdb',
    'user': 'user',
    'password': 'pass',
    'host': 'localhost',
    'port': 5432
}

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
    ("UPDATE clients SET first_name = 'UpdatedName' WHERE client_id = 1;", "Proste zapytanie UPDATE"),
    ("UPDATE clients SET first_name = 'Updated' WHERE client_id IN (1, 2, 3, 4, 5);", "UPDATE z WHERE (wielu rekordów)"),
    ("UPDATE accounts SET balance = balance + 100 FROM clients WHERE accounts.client_id = clients.client_id AND clients.email LIKE '%example.com%';", "UPDATE z JOIN"),
    ("UPDATE accounts SET balance = balance + (SELECT AVG(balance) FROM accounts) WHERE account_id = 1;", "UPDATE z agregacją"),
    ("UPDATE clients SET email = 'newemail@example.com' WHERE client_id IN (SELECT a.client_id FROM accounts a JOIN transactions t ON a.account_id = t.account_id WHERE t.transaction_date > '2023-01-01' AND a.balance > 1000);", "UPDATE z subzapytaniem i JOIN")
    ]

    for query, description in queries:
        times = []
        for i in range(100):  
            print(f"▶️ Iteracja {i+1}/100 - {description}")
            elapsed_time = run_sql_query(conn_pg, query)
            times.append(elapsed_time)

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
