import redis
import time
import statistics
import json

r = redis.Redis(host='localhost', port=6379, db=0)

def run_redis_delete(fn):
    start = time.time()
    fn()
    end = time.time()
    return end - start

def main():
    deletes = [
        # 1. DELETE client:1
        (lambda: r.delete("client:1"), "Prosty DELETE"),

        # 2. DELETE client:{1-5}
        (lambda: [r.delete(f"client:{i}") for i in [1, 2, 3, 4, 5]], "DELETE wielu rekordów"),

        # 3. DELETE kont klientów z emailem @example.com
        (lambda: [
            r.delete(key.decode())
            for key in r.scan_iter("account:*")
            if r.hget(key, "client_id") is not None and
            r.hget(f"client:{r.hget(key, 'client_id').decode()}", "email") is not None and
            r.hget(f"client:{r.hget(key, 'client_id').decode()}", "email").decode().endswith("@example.com")
        ], "DELETE z JOIN"),  


        # 4. DELETE transakcji sprzed 2023
        (lambda: [
            r.delete(key.decode())
            if all(json.loads(t)["transaction_date"] < "2023-01-01" for t in r.lrange(key, 0, -1))
            else r.ltrim(key, 0, -1)  # clear list
            for key in r.scan_iter("transactions:*")
        ], "DELETE z warunkiem po dacie"),

        # 5. DELETE klientów z więcej niż 2 kontami
        (lambda: [
            r.delete(f"client:{client_id.decode()}")
            for client_id in r.scan_iter("client:*")
            if len([
                key for key in r.scan_iter("account:*")
                if r.hget(key, "client_id") == client_id
            ]) > 2
        ], "DELETE z GROUP BY i HAVING")
    ]

    for delete_fn, desc in deletes:
        times = []
        for i in range(100):
            print(f"▶️ Iteracja {i+1}/100 - {desc}")
            elapsed = run_redis_delete(delete_fn)
            times.append(elapsed)

        print_stats(desc, times)

    r.close()

def print_stats(desc, times):
    print(f"\n{desc}")
    print(f"Średni czas: {statistics.mean(times):.4f} s")
    print(f"Minimalny czas: {min(times):.4f} s")
    print(f"Maksymalny czas: {max(times):.4f} s")
    print(f"Wszystkie czasy: {[round(t, 4) for t in times]}")
    print("=" * 40)

if __name__ == "__main__":
    main()
