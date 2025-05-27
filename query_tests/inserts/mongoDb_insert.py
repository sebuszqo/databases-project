import json
import time
import statistics
import os
import copy
from pymongo import MongoClient

# 🔍 Ścieżka do pliku JSON względem lokalizacji tego pliku .py
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
JSON_FILE = os.path.join(BASE_DIR, "../../data/structured_data.json")

# 🔧 Konfiguracja MongoDB
DB_NAME = "testdb"
COLLECTION_NAME = "clients"
REPEAT_COUNT = 5
RECORD_COUNTS = [50000, 100000, 150000, 200000, 500000, 1000000]

# 🔌 Połączenie z MongoDB
client = MongoClient("mongodb://localhost:27017/")
db = client[DB_NAME]
collection = db[COLLECTION_NAME]

# 📥 Wczytanie danych z JSON
def load_structured_clients(path):
    if not os.path.exists(path):
        raise FileNotFoundError(f"❌ Nie znaleziono pliku JSON: {os.path.abspath(path)}")
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)

# 📦 Wstawianie dokumentów z pomiarem czasu
def insert_clients(clients):
    collection.drop()
    start = time.perf_counter()
    collection.insert_many(clients)
    end = time.perf_counter()
    return end - start

# ▶️ Benchmark
def main():
    print("📥 Wczytywanie danych z:", JSON_FILE)
    base_clients = load_structured_clients(JSON_FILE)
    base_count = len(base_clients)
    print(f"🔢 Wczytano {base_count} rekordów z pliku JSON.")

    for count in RECORD_COUNTS:
        # 🔁 Rozszerz dane, jeśli potrzeba więcej niż w pliku
        if count > base_count:
            print(f"🔁 Powielam dane do {count} rekordów...")
            repeat_factor = (count // base_count) + 1
            expanded_clients = (base_clients * repeat_factor)[:count]
        else:
            expanded_clients = base_clients[:count]

        # 🧼 Przygotuj czyste kopie bez _id i z nowym client_id
        clean_clients = []
        for idx in range(count):
            new_client = copy.deepcopy(expanded_clients[idx])
            new_client["client_id"] = idx + 1
            new_client.pop("_id", None)
            clean_clients.append(new_client)

        times = []

        print(f"\n🧪 TEST INSERT – {count} klientów (zagnieżdżone dokumenty):")
        for i in range(REPEAT_COUNT):
            print(f"▶️ Iteracja {i+1}/{REPEAT_COUNT}...")
            elapsed = insert_clients(clean_clients)
            times.append(elapsed)
            print(f"⏱️  Czas: {elapsed:.4f} s")

        # 📊 Statystyki
        print(f"\n📊 Podsumowanie dla {count} dokumentów:")
        print(f"Średni czas: {statistics.mean(times):.4f} s")
        print(f"Minimalny czas: {min(times):.4f} s")
        print(f"Maksymalny czas: {max(times):.4f} s")
        print(f"Wszystkie czasy: {[round(t, 4) for t in times]}")
        print("=" * 50)

if __name__ == "__main__":
    main()
