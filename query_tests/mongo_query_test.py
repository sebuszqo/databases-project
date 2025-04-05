from pymongo import MongoClient
import time
# Konfiguracja połączenia z MongoDB
client = MongoClient("mongodb://localhost:27017/")
db = client["bank_database"]

# # Zapytanie do MongoDB, aby zliczyć liczbę transakcji dla klienta o id 1
# count_client_1_transactions = [
#     { "$match": { "client_id": 1 } },  # Dopasowanie klienta o id 1
#     { "$unwind": "$accounts" },         # Rozwinięcie tablicy "accounts"
#     { "$unwind": "$accounts.transactions" },  # Rozwinięcie tablicy "transactions"
#     { "$match": {"accounts.transactions.transaction_type": "credit"}},  # Rozwinięcie tablicy "transactions"
#     { "$count": "transaction_count" }   # Zliczenie liczby transakcji
# ]

# # Wykonanie zapytania
# start_time = time.time()
# result = db.clients.aggregate(count_client_1_transactions)
# end_time = time.time()
# # Wyświetlenie wyniku
# for doc in result:
#     print(f"Liczba transakcji dla klienta o ID 1: {doc['transaction_count']}")
#     print(f"Czas zapytania: {end_time - start_time:.6f} s\n")




clients_with_multiple_high_balance_accounts = [
    {
        '$project': {
            'first_name': 1,
            'last_name': 1,
            'high_balance_accounts': {
                '$size': {
                    '$filter': {
                        'input': "$accounts",
                        'as': "account",
                        'cond': { '$gt': ["$$account.balance", 50000] }
                    }
                }
            }
        }
    },
    { '$match': { 'high_balance_accounts': { '$gt': 1 } } },
    { '$count': "clients_with_multiple_high_balance_accounts" }
]

# Wykonanie zapytania
start_time = time.time()
result = list(db.clients.aggregate(clients_with_multiple_high_balance_accounts))
end_time = time.time()

# Wyświetlenie wyniku
print(result)
print(f"Zapytanie wykonane w {end_time - start_time:.4f} sekundy.")
