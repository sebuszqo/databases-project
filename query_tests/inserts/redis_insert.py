import redis
import json
import time
import statistics
import os

# Połączenie z Redis
r = redis.Redis(host='localhost', port=6379, decode_responses=True)

# Testy: (liczba rekordów, plik json, prefix, id_key, typ struktury)
RECORD_TESTS = [
    (50000, "loans.json", "loan", "loan_id", "hash"),               # loans – 50k
    (100000, "clients.json", "client", "client_id", "hash"),        # clients – 100k
    (150000, "accounts.json", "account", "account_id", "hash"),     # accounts – 150k
    (200000, "cards.json", "card", "card_id", "hash"),              # cards – 200k
    (500000, "transactions.json", "transactions", None, "list"),    # transactions – 1M
    (1000000, "transactions.json", "transactions", None, "list")    # transactions – 1M
]



BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_PATH = os.path.join(BASE_DIR, "../../data")
REPEAT_COUNT = 5
CHUNK_SIZE = 10000  # ile rekordów w jednej paczce do Redisa

# Wczytaj JSON
def load_json(path):
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)

# Insert danych jako HASH z chunkowaniem
def insert_hash_data(data, prefix, id_key):
    r.flushdb()
    time.sleep(0.2)
    start = time.perf_counter()
    for i in range(0, len(data), CHUNK_SIZE):
        pipe = r.pipeline()
        chunk = data[i:i+CHUNK_SIZE]
        for record in chunk:
            key = f"{prefix}:{record[id_key]}"
            pipe.hset(key, mapping=record)
        pipe.execute()
    return time.perf_counter() - start

# Insert danych jako LISTA (np. transactions)
def insert_list_data(data, key):
    r.flushdb()
    time.sleep(0.2)
    start = time.perf_counter()
    for i in range(0, len(data), CHUNK_SIZE):
        pipe = r.pipeline()
        chunk = data[i:i+CHUNK_SIZE]
        for record in chunk:
            pipe.rpush(key, json.dumps(record))
        pipe.execute()
    return time.perf_counter() - start

# Benchmark
def main():
    for count, filename, prefix, id_key, dtype in RECORD_TESTS:
        file_path = os.path.join(DATA_PATH, filename)
        all_data = load_json(file_path)
        if len(all_data) < count:
            print(f"⚠️ Za mało danych w pliku {filename}, potrzebne: {count}")
            continue

        sample = all_data[:count]
        times = []

        print(f"\n🧪 TEST INSERT Redis – {count} rekordów ({prefix}):")
        for i in range(REPEAT_COUNT):
            print(f"▶️ Iteracja {i+1}/{REPEAT_COUNT}...")
            if dtype == "hash":
                elapsed = insert_hash_data(sample, prefix, id_key)
            else:
                elapsed = insert_list_data(sample, prefix)
            times.append(elapsed)
            print(f"⏱️  Czas: {elapsed:.4f} s")

        # Podsumowanie
        print(f"\n📊 Podsumowanie dla {count} rekordów ({prefix}):")
        print(f"Średni czas: {statistics.mean(times):.4f} s")
        print(f"Minimalny czas: {min(times):.4f} s")
        print(f"Maksymalny czas: {max(times):.4f} s")
        print(f"Wszystkie czasy: {[round(t, 4) for t in times]}")
        print("=" * 50)

if __name__ == "__main__":
    main()
