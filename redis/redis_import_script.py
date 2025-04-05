import redis
import json
import time

# Połączenie z Redis
redis_client = redis.Redis(host="localhost", port=6379, decode_responses=True)

def import_hash_data(file_name, key_prefix, id_field):
    """ Import danych do Redis jako Hash """
    start_time = time.time()
    
    with open(file_name, "r", encoding="utf-8") as f:
        records = json.load(f)

    for record in records:
        record_id = record[id_field]
        redis_client.hset(f"{key_prefix}:{record_id}", mapping=record)

    elapsed_time = time.time() - start_time
    print(f"Zaimportowano {len(records)} transakcji z {file_name} w {elapsed_time:.2f} sekundy")

    return elapsed_time

def import_list_data(file_name, key_prefix):
    """ Import danych do Redis jako listy (rpush) """
    start_time = time.time()
    
    with open(file_name, "r", encoding="utf-8") as f:
        records = json.load(f)

    for record in records:
        account_id = record["account_id"]
        redis_client.rpush(f"{key_prefix}:{account_id}", json.dumps(record))

    elapsed_time = time.time() - start_time
    print(f"Zaimportowano {len(records)} transakcji z {file_name} w {elapsed_time:.2f} sekundy")

# Pomiar całkowitego czasu importu
total_start_time = time.time()

print("\n Rozpoczynam import danych do Redis...\n")

# Importowanie tabel i zapisanie czasów operacji
import_hash_data("data/clients.json", "client", "client_id")
import_hash_data("data/accounts.json", "account", "account_id")
import_hash_data("data/cards.json", "card", "card_id")
import_hash_data("data/loans.json", "loan", "loan_id")
import_list_data("data/transactions.json", "transactions")

# Pomiar końcowego czasu
total_elapsed_time = time.time() - total_start_time

print(f"\n✅ Całkowity czas importu wszystkich danych: {total_elapsed_time:.2f} sekundy\n")
