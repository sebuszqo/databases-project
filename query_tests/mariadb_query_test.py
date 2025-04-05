import mysql.connector
import time

# Konfiguracja połączenia z MariaDB
conn = mysql.connector.connect(
    host="localhost",
    user="user",
    password="pass",
    database="testdb"
)

cursor = conn.cursor(dictionary=True)


specific_client_transaction_number_query = """
    SELECT COUNT(t.transaction_id)
    FROM transactions AS t
    JOIN accounts AS a ON t.account_id = a.account_id
    JOIN clients AS c ON c.client_id = a.client_id
    WHERE c.client_id = %s
    AND t.transaction_type = 'credit';
"""
s = time.time()
cursor.execute(specific_client_transaction_number_query, (1,))
e = time.time()
result = cursor.fetchone()
print(f"Liczba transakcji credytowych klienta o ID = 1 wynosi: {result}. Czas wykonanai operacji: {e-s}s")


clients_with_multiple_high_balance_accounts = """
    SELECT COUNT(*) 
    FROM (
        SELECT c.client_id
        FROM clients c
        JOIN accounts a ON c.client_id = a.client_id
        WHERE a.balance > 50000
        GROUP BY c.client_id
        HAVING COUNT(a.account_id) > 1
    ) AS subquery;
"""
s = time.time()
cursor.execute(clients_with_multiple_high_balance_accounts)
e = time.time()
result = cursor.fetchone()
print(result)
print(f"Zapytanie wykonane w {e - s:.4f} sekundy.")


# Zamknięcie połączenia
cursor.close()
conn.close()
  