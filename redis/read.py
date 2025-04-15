import redis
import time
import statistics
import json

REDIS_CONFIG = {
    "host": "localhost",
    "port": 6379,
    "db": 0
}

def run_redis_query(fn):
    start = time.time()
    result = fn()
    end = time.time()
    return result, end - start

def main():
    r = redis.Redis(**REDIS_CONFIG)

    queries = [
        # 1. Pobierz 100 klientów (symulacja SELECT * LIMIT 100)
        (lambda: [r.hgetall(f"client:{i}") for i in range(1, 101)],
         "Proste zapytanie SELECT z LIMIT"),

        # 2. Pobierz jednego klienta po ID
        (lambda: r.hgetall("client:1"),
         "Proste zapytanie SELECT z WHERE"),

        # 3. Pobierz klienta + jego konta (JOIN client -> account)
        (lambda: [
            {**r.hgetall("client:1"), **r.hgetall(f"account:{i}")} 
            for i in range(1, 6) if r.hget(f"account:{i}", "client_id") == b"1"
        ],
         "Zapytanie SELECT z JOIN (client -> accounts)"),

        # 4. Policz liczbę klientów (COUNT)
        (lambda: len([key for key in r.scan_iter("client:*")]),
         "Zapytanie SELECT z COUNT"),

        # 5. Średni balance (AVG)
        (lambda: sum(
            [float(r.hget(key, "balance") or 0) for key in r.scan_iter("account:*")]
        ) / len([key for key in r.scan_iter("account:*")]),
         "Zapytanie SELECT z AVG"),

        # 6. Pobierz klientów z kontami o balansie > 1000 i posortuj malejąco
        (lambda: sorted([
            {
                "client": r.hgetall(f"client:{r.hget(key, 'client_id').decode()}"),
                "account_number": r.hget(key, "account_number").decode(),
                "balance": float(r.hget(key, "balance"))
            }
            for key in r.scan_iter("account:*")
            if float(r.hget(key, "balance") or 0) > 1000
        ], key=lambda x: -x["balance"]),
         "Zaawansowane zapytanie z WHERE + ORDER BY"),

        # 7. Pobierz transakcje po dacie > 2023-01-01 i posortuj malejąco
        (lambda: sorted([
            json.loads(t)
            for key in r.scan_iter("transactions:*")
            for t in r.lrange(key, 0, -1)
            if json.loads(t)["transaction_date"] > "2023-01-01"
        ], key=lambda x: x["transaction_date"], reverse=True),
         "Zapytanie SELECT z JOIN (transactions)")
    ]

    for fn, description in queries:
        times = []
        for i in range(100):
            print(f"▶️ Iteracja {i+1}/100 - {description}")
            _, elapsed = run_redis_query(fn)
            times.append(elapsed)

        avg_time = statistics.mean(times)
        min_time = min(times)
        max_time = max(times)
        print(f"\n{description}")
        print(f"Średni czas: {avg_time:.4f} sekundy")
        print(f"Minimalny czas: {min_time:.4f} sekundy")
        print(f"Maksymalny czas: {max_time:.4f} sekundy")
        print(f"Wszystkie czasy: {[round(t, 4) for t in times]}")
        print("=" * 40)

    r.close()

if __name__ == "__main__":
    main()
