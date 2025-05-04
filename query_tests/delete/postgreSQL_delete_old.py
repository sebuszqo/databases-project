import psycopg2
import time
import statistics

DB_CONFIG = {
    'dbname': 'testdb',
    'user': 'user',
    'password': 'pass',
    'host': '127.0.0.1',
    'port': 5432
}

def run_sql_delete(conn, query):
    start = time.time()
    cursor = conn.cursor()
    cursor.execute(query)
    conn.commit()
    end = time.time()
    return end - start

def main():
    conn = psycopg2.connect(**DB_CONFIG)

    queries = [
        ("DELETE FROM clients WHERE client_id = 1;", "Prosty DELETE"),
        ("DELETE FROM clients WHERE client_id IN (1, 2, 3, 4, 5);", "DELETE wielu rekordów"),
        ("""
        DELETE FROM accounts
        USING clients
        WHERE accounts.client_id = clients.client_id
        AND clients.email LIKE '%@example.com';
        """, "DELETE z JOIN"),
        ("DELETE FROM transactions WHERE transaction_date < '2023-01-01';", "DELETE z warunkiem po dacie"),
        ("""
        DELETE FROM clients
        WHERE client_id IN (
            SELECT client_id
            FROM accounts
            GROUP BY client_id
            HAVING COUNT(account_id) > 2
        );
        """, "DELETE z GROUP BY i HAVING")
    ]

    for query, description in queries:
        times = []
        for i in range(100):
            print(f"▶️ Iteracja {i+1}/100 - {description}")
            elapsed_time = run_sql_delete(conn, query)
            times.append(elapsed_time)

        print_stats(description, times)

    conn.close()

def print_stats(desc, times):
    print(f"\n{desc}")
    print(f"Średni czas: {statistics.mean(times):.4f} s")
    print(f"Minimalny czas: {min(times):.4f} s")
    print(f"Maksymalny czas: {max(times):.4f} s")
    print(f"Wszystkie czasy: {[round(t, 4) for t in times]}")
    print("=" * 40)

if __name__ == "__main__":
    main()
