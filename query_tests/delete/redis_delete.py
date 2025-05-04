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
        # 1. Usunięcie klienta o client_id = 1
        (
            lambda: r.delete("client:1"),
            "1. Usunięcie klienta o client_id = 1."
        ),

        # 2. Usunięcie transakcji powiązanych z klientem
        (
            lambda: [
                r.delete(f"transactions:{r.hget(acc_key, 'account_id').decode()}")
                for acc_key in r.scan_iter("account:*")
                if r.hget(acc_key, "client_id") == b"1"
            ],
            "2. Usunięcie transakcji powiązanych z klientem:"
        ),

        # 3. Usunięcie kont o saldzie powyżej 1000
        (
            lambda: [
                r.delete(key.decode())
                for key in r.scan_iter("account:*")
                if float(r.hget(key, "balance") or 0) > 1000
            ],
            "3. Usunięcie kont o saldzie powyżej 1000."
        ),

        # 4. Usunięcie klientów bez transakcji
        (
            lambda: [
                r.delete(client_key.decode())
                for client_key in r.scan_iter("client:*")
                if not any(
                    r.exists(f"transactions:{r.hget(acc_key, 'account_id').decode()}")
                    for acc_key in r.scan_iter("account:*")
                    if r.hget(acc_key, "client_id") == client_key.split(b":")[1]
                )
            ],
            "4. Usunięcie klientów bez transakcji."
        )
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
