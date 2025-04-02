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

# Wyszukanie klienta o client_id = 1
# specific_client_transaction_number_query = """
#     SELECT COUNT(t.transaction_id)
#     FROM transactions AS t
#     JOIN accounts AS a ON t.account_id = a.account_id
#     JOIN clients AS c ON c.client_id = a.client_id
#     WHERE c.client_id = %s
#     AND t.transaction_type = 'credit';
# """
# s = time.time()
# cursor.execute(specific_client_transaction_number_query, (1,))
# e = time.time()
# result = cursor.fetchone()
# print(f"Liczba transakcji credytowych klienta o ID = 1 wynosi: {result}. Czas wykonanai operacji: {e-s}s")


client_with_more_than_one_card_query = '''
    SELECT c.client_id, c.first_name, c.last_name, COUNT(cr.card_id) AS card_count
    FROM clients AS c
    JOIN accounts AS a ON c.client_id = a.client_id
    JOIN cards AS cr ON a.account_id = cr.account_id
    GROUP BY c.client_id, c.first_name, c.last_name
    HAVING COUNT(cr.card_id) > 1
    LIMIT 10;
'''

s = time.time()
cursor.execute(client_with_more_than_one_card_query)
e = time.time()

result = cursor.fetchall()


for row in result:
    print(f"liczba transakcji credytowych klienta o ID = 1 wynosi: {row}.")


print(f"Zapytanie wykonane w {e - s:.6f} sekundy")

# Zamknięcie połączenia
cursor.close()
conn.close()
  