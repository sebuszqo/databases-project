import pandas as pd
from pymongo import MongoClient
import time

# Konfiguracja połączenia z MongoDB
client = MongoClient("mongodb://0.0.0.0:27017/")
db = client["bank_database"]

# Wczytanie danych z plików CSV
def load_csv(file_name):
    return pd.read_csv(file_name).to_dict(orient="records")

dict_clients = load_csv("clients.csv")
dict_accounts = load_csv("accounts.csv")
dict_cards = load_csv("cards.csv")
dict_loans = load_csv("loans.csv")
dict_transactions = load_csv("transactions.csv")

# Tworzenie indeksów dla szybszego wyszukiwania
accounts_map = {acc["account_id"]: acc for acc in dict_accounts}
cards_map = {}
transactions_map = {}
loans_map = {}

for card in dict_cards:
    cards_map.setdefault(card["account_id"], []).append(card)

for txn in dict_transactions:
    transactions_map.setdefault(txn["account_id"], []).append(txn)

for loan in dict_loans:
    loans_map.setdefault(loan["client_id"], []).append(loan)

# Tworzenie struktury dokumentowej
for client in dict_clients:
    client_accounts = [accounts_map[acc_id] for acc_id in accounts_map if accounts_map[acc_id]["client_id"] == client["client_id"]]
    for account in client_accounts:
        account["cards"] = cards_map.get(account["account_id"], [])
        account["transactions"] = transactions_map.get(account["account_id"], [])
    client["accounts"] = client_accounts
    client["loans"] = loans_map.get(client["client_id"], [])

# Import danych do MongoDB
db.clients.drop()  # Usunięcie starej kolekcji, jeśli istnieje

# Pomiar czasu (od momentu rozpoczęcia przetwarzania danych)
start_time = time.time()
db.clients.insert_many(dict_clients)  # Insert danych

# Zakończenie pomiaru czasu
time_elapsed = time.time() - start_time
print(f"Dane zostały zaimportowane do MongoDB w {time_elapsed:.2f} sekund.")
