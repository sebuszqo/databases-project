import json
import time
from pymongo import MongoClient
from pymongo.errors import ConnectionFailure, BulkWriteError

# Konfiguracja połączenia z MongoDB
try:
    client = MongoClient("mongodb://localhost:27017/", serverSelectionTimeoutMS=5000)
    client.admin.command('ping')  # Sprawdzenie połączenia
    print("Połączono z MongoDB!")
except ConnectionFailure:
    print("Błąd: Nie można połączyć się z MongoDB.")
    exit(1)

# Wybór bazy danych
db = client["bank_database"]

# Wczytanie danych z pliku JSON
try:
    with open("data/structured_data.json", "r", encoding="utf-8") as f:
        dict_clients = json.load(f)
        if not isinstance(dict_clients, list):
            raise ValueError("Plik JSON nie zawiera listy rekordów.")
except (FileNotFoundError, json.JSONDecodeError, ValueError) as e:
    print(f"Błąd: Nie można wczytać pliku JSON ({e}).")
    exit(1)

# Pomiar czasu importu
start_time = time.time()

db.clients.drop()  # Usunięcie starej kolekcji
print("Stara kolekcja 'clients' została usunięta.")

# Import danych do MongoDB
try:
    result = db.clients.insert_many(dict_clients)
    print(f"Zaimportowano {len(result.inserted_ids)} rekordów do MongoDB.")
except BulkWriteError as e:
    print(f"Błąd podczas importu danych: {e.details}")
    exit(1)

# Zakończenie pomiaru czasu
time_elapsed = time.time() - start_time
print(f"Dane zostały zaimportowane do MongoDB w {time_elapsed:.2f} sekund.")
