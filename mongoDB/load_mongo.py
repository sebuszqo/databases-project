import pandas as pd
from pymongo import MongoClient
import time

# Konfiguracja połączenia z MongoDB
client = MongoClient("mongodb://0.0.0.0:27017/")
db = client["bank_database"]

# Wczytanie danych z plików CSV
df_clients = pd.read_csv("clients.csv")
df_accounts = pd.read_csv("accounts.csv")
df_cards = pd.read_csv("cards.csv")
df_loans = pd.read_csv("loans.csv")
df_transactions = pd.read_csv("transactions.csv")

# Konwersja do listy słowników
dict_clients = df_clients.to_dict(orient="records")
dict_accounts = df_accounts.to_dict(orient="records")
dict_cards = df_cards.to_dict(orient="records")
dict_loans = df_loans.to_dict(orient="records")
dict_transactions = df_transactions.to_dict(orient="records")




# Tworzenie struktury dokumentowej
for client in dict_clients:
    client["accounts"] = []
    print("Klient") 
    for account in dict_accounts:
        if account["client_id"] == client["client_id"]:
            account["cards"] = [card for card in dict_cards if card["account_id"] == account["account_id"]]
            account["transactions"] = [txn for txn in dict_transactions if txn["account_id"] == account["account_id"]]
            client["accounts"].append(account)
    client["loans"] = [loan for loan in dict_loans if loan["client_id"] == client["client_id"]]

# Import danych do MongoDB
db.clients.drop()  # Usunięcie starej kolekcji, jeśli istnieje
# Pomiar czasu (od momentu rozpoczęcia przetwarzania danych)
start_time = time.time()
db.clients.insert_many(dict_clients)  # Insert danych

# Zakończenie pomiaru czasu
time_elapsed = time.time() - start_time
print(f"Dane zostały zaimportowane do MongoDB w {time_elapsed:.2f} sekund.")
