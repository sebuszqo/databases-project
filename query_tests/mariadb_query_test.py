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


# Zamknięcie połączenia
cursor.close()
conn.close()
  