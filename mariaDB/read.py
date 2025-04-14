import mariadb
import time
import statistics

DB_CONFIG = {
    'user': 'user',
    'password': 'pass',
    'host': '127.0.0.1',
    'port': 3306,
    'database': 'testdb'
}

def run_sql_query(conn, query):
    start = time.time()
    cursor = conn.cursor()
    cursor.execute(query)
    result = cursor.fetchall()
    end = time.time()
    return result, end - start

def main():
    conn_mariadb = mariadb.connect(**DB_CONFIG)

    queries = [
        # ("SELECT * FROM clients LIMIT 100;", "Proste zapytanie SELECT z LIMIT"),
        # ("SELECT * FROM clients WHERE client_id = 1;", "Proste zapytanie SELECT z WHERE"),
        # ("""
        # SELECT clients.first_name, clients.last_name, accounts.account_number
        # FROM clients
        # JOIN accounts ON clients.client_id = accounts.client_id
        # """, "Zapytanie SELECT z JOIN"),
        # ("SELECT COUNT(*) FROM clients;", "Zapytanie SELECT z COUNT"),
        # ("SELECT AVG(balance) FROM accounts;", "Zapytanie SELECT z AVG"),
        # ("""
        # SELECT clients.first_name, clients.last_name, accounts.account_number
        # FROM clients
        # JOIN accounts ON clients.client_id = accounts.client_id
        # WHERE accounts.balance > 1000
        # ORDER BY accounts.balance DESC;
        # """, "Zaawansowane zapytanie z WHERE + ORDER BY"),
        # ("""
        # SELECT transactions.transaction_id, transactions.amount, accounts.account_number
        # FROM transactions
        # JOIN accounts ON transactions.account_id = accounts.account_id
        # WHERE transactions.transaction_date > '2023-01-01'
        # ORDER BY transactions.transaction_date DESC;
        # ""","Zapytanie SELECT z JOIN"),
        ("""SELECT COUNT(*) 
        FROM (
            SELECT c.client_id
            FROM clients c
            JOIN accounts a ON c.client_id = a.client_id
            WHERE a.balance > 50000
            GROUP BY c.client_id
            HAVING COUNT(a.account_id) > 1
        ) AS subquery;
        ""","Zliczanie liczby klientów z więcej niż jednym kontem i saldem powyżej 50000"),
        ("""SELECT c.client_id, COUNT(t.transaction_id)
            FROM transactions AS t
            JOIN accounts AS a ON t.account_id = a.account_id
            JOIN clients AS c ON c.client_id = a.client_id
            WHERE c.client_id BETWEEN 2000 AND 3000
            AND t.transaction_type = 'credit'
            GROUP BY c.client_id;
        ""","Zliczanie liczby transakcji dla klientów o ID 2000 - 3000")
    ]


    for query, description in queries:
        times = []
        for i in range(100):  
            print(f"▶️ Iteracja {i+1}/100 - {description}")
            _, elapsed_time = run_sql_query(conn_mariadb, query)
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
    conn_mariadb.close()

if __name__ == "__main__":
    main()
