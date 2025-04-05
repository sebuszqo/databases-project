import pandas as pd
import time
import json

# Wczytanie danych z plików CSV
def load_csv(file_name):
    print(f"Wczytywanie pliku {file_name}...")
    return pd.read_csv(file_name).to_dict(orient="records")

print("Rozpoczęcie procesu strukturyzacji danych...")
dict_clients = load_csv("clients.csv")
dict_accounts = load_csv("accounts.csv")
dict_cards = load_csv("cards.csv")
dict_loans = load_csv("loans.csv")
dict_transactions = load_csv("transactions.csv")

print("Tworzenie indeksów dla szybszego wyszukiwania...")
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

# Pomiar czasu strukturyzacji danych
start_time = time.time()
   
print("Łączenie danych w jedną strukturę...")
for client in dict_clients:
    print(f'current client id: {client["client_id"]}')
    client_accounts = [accounts_map[acc_id] for acc_id in accounts_map if accounts_map[acc_id]["client_id"] == client["client_id"]]
    for account in client_accounts:
        print(f'current account id: {account["account_id"]}')
        account["cards"] = cards_map.get(account["account_id"], [])
        account["transactions"] = transactions_map.get(account["account_id"], [])
    client["accounts"] = client_accounts
    client["loans"] = loans_map.get(client["client_id"], [])

print("Zapisywanie struktury do pliku JSON...")
with open("structured_data.json", "w") as f:
    json.dump(dict_clients, f, indent=4)

# Zakończenie pomiaru czasu
time_elapsed = time.time() - start_time
print(f"Struktura danych przygotowana w {time_elapsed:.2f} sekund i zapisana do pliku structured_data.json.")
