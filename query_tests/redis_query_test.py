import redis
import json
import time

# Połączenie z Redis
r = redis.Redis(host='localhost', port=6379, decode_responses=True)

# Pobranie identyfikatorów kont klienta
s_time = time.time()
client_id = 1
account_keys = r.keys(f"account:*")  # Pobranie wszystkich kluczy kont
account_ids = [
    key.split(":")[1] for key in account_keys
    if r.hget(key, "client_id") == str(client_id)
]

# Pobranie i zliczenie transakcji
transaction_count = 0

for account_id in account_ids:
    transaction_key = f"transactions:{account_id}"
    transactions = r.lrange(transaction_key, 0, -1)  # Pobranie wszystkich transakcji jako lista JSON-ów
    
    for transaction_json in transactions:
        transaction = json.loads(transaction_json)  # Parsowanie JSON-a
        if transaction.get("transaction_type") == "credit":
            transaction_count += 1
e_time = time.time()

print(f"Liczba transakcji dla client_id={client_id}: {transaction_count}")
print(f"czas operacji {e_time - s_time}")