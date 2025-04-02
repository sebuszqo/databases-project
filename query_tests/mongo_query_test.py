from pymongo import MongoClient
import time
# Konfiguracja połączenia z MongoDB
client = MongoClient("mongodb://localhost:27017/")
db = client["bank_database"]

# Zapytanie do MongoDB, aby zliczyć liczbę transakcji dla klienta o id 1
pipeline = [
    { "$match": { "client_id": 1 } },  # Dopasowanie klienta o id 1
    { "$unwind": "$accounts" },         # Rozwinięcie tablicy "accounts"
    { "$unwind": "$accounts.transactions" },  # Rozwinięcie tablicy "transactions"
    { "$match": {"accounts.transactions.transaction_type": "credit"}},  # Rozwinięcie tablicy "transactions"
    { "$count": "transaction_count" }   # Zliczenie liczby transakcji
]

# Wykonanie zapytania
start_time = time.time()
result = db.clients.aggregate(pipeline)
end_time = time.time()
# Wyświetlenie wyniku
for doc in result:
    print(f"Liczba transakcji dla klienta o ID 1: {doc['transaction_count']}")
    print(f"Czas zapytania: {end_time - start_time:.6f} s\n")