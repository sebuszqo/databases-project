import redis
import time
import statistics
import json

REDIS_CONFIG = {
    "host": "localhost",
    "port": 6379,
    "db": 0
}

def run_redis_update(fn):
    start = time.time()
    fn()
    end = time.time()
    return end - start

def main():
    r = redis.Redis(**REDIS_CONFIG)

    updates = [
        # 1. UPDATE jednego klienta
        (
            lambda: r.hset("client:1", "first_name", "UpdatedName"),
            "Proste zapytanie UPDATE"
        ),

        # 2. UPDATE wielu klientów
        (
            lambda: [r.hset(f"client:{i}", "first_name", "Updated") for i in [1, 2, 3, 4, 5]],
            "UPDATE z WHERE (wielu rekordów)"
        ),

        # 3. JOIN: zaktualizuj balance dla klientów z emailem kończącym się na @example.com
        (
            lambda: [
                r.hincrbyfloat(account_key, "balance", 100)
                for account_key in r.scan_iter("account:*")
                if r.hget(f"client:{r.hget(account_key, 'client_id').decode()}", "email").decode().endswith("@example.com")
            ],
            "UPDATE z JOIN"
        ),

        # 4. UPDATE z agregacją – dodaj średni balans do account_id=1
        (
            lambda: (
                lambda avg_balance: r.hincrbyfloat("account:1", "balance", avg_balance)
                if avg_balance is not None else None
            )(
                sum(
                    float(r.hget(key, "balance") or 0)
                    for key in r.scan_iter("account:*")
                ) / max(1, len(list(r.scan_iter("account:*"))))
            ),
            "UPDATE z agregacją"
        ),

        # 5. UPDATE klientów z kontem mającym transakcję po 2023 i balance > 1000
        (
            lambda: [
                r.hset(f"client:{r.hget(account_key, 'client_id').decode()}", "email", "newemail@example.com")
                for account_key in r.scan_iter("account:*")
                if float(r.hget(account_key, "balance") or 0) > 1000 and any(
                    json.loads(tr)["transaction_date"] > "2023-01-01"
                    for tr in r.lrange(f"transactions:{account_key.decode().split(':')[1]}", 0, -1)
                )
            ],
            "UPDATE z subzapytaniem i JOIN"
        )
    ]

    for update_fn, description in updates:
        times = []
        for i in range(100):
            print(f"▶️ Iteracja {i+1}/100 - {description}")
            elapsed_time = run_redis_update(update_fn)
            times.append(elapsed_time)

        avg = statistics.mean(times)
        print(f"\n{description}")
        print(f"Średni czas: {avg:.4f} s")
        print(f"Minimalny czas: {min(times):.4f} s")
        print(f"Maksymalny czas: {max(times):.4f} s")
        print(f"Wszystkie czasy: {[round(t, 4) for t in times]}")
        print("=" * 40)

    r.close()

if __name__ == "__main__":
    main()
