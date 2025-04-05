import redis
import json
import time

# Połączenie z Redis
redis_client = redis.Redis(host="localhost", port=6379, decode_responses=True)

# 1️⃣ Pobranie wszystkich kont dla konkretnego klienta i ich danych
start_time = time.time()
client_id = "12345"

# Pobranie listy kont klienta (przyjmujemy, że konta są powiązane przez pole client_id w Redis)
account_keys = redis_client.keys(f"account:*")
client_accounts = [redis_client.hgetall(acc) for acc in account_keys if redis_client.hget(acc, "client_id") == client_id]

end_time = time.time()
print(f"Klient {client_id} ma {len(client_accounts)} kont.")
print(f"Czas zapytania: {end_time - start_time:.6f} s\n")

# 2️⃣ Pobranie sumy wszystkich środków na kontach klienta
start_time = time.time()
total_balance = sum(float(acc["balance"]) for acc in client_accounts if "balance" in acc)
end_time = time.time()

print(f"Łączny stan kont klienta {client_id}: {total_balance} zł")
print(f"Czas zapytania: {end_time - start_time:.6f} s\n")

# 3️⃣ Pobranie wszystkich transakcji klienta
start_time = time.time()
transactions = []
for acc in client_accounts:
    account_id = acc["account_id"]
    transactions.extend([json.loads(txn) for txn in redis_client.lrange(f"transactions:{account_id}", 0, -1)])

end_time = time.time()
print(f"Klient {client_id} ma {len(transactions)} transakcji.")
print(f"Czas zapytania: {end_time - start_time:.6f} s\n")

# 4️⃣ Zliczenie liczby transakcji powyżej 1000 zł dla kont klienta
start_time = time.time()
high_value_transactions = [txn for txn in transactions if txn["amount"] > 1000]
end_time = time.time()

print(f"Liczba transakcji powyżej 1000 zł: {len(high_value_transactions)}")
print(f"Czas zapytania: {end_time - start_time:.6f} s\n")
