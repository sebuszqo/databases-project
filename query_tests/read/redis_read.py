import redis
import time
import statistics
import json

r = redis.Redis(host="localhost", port=6379, db=0)

def run_redis_query(fn):
    start = time.time()
    result = fn()
    end = time.time()
    return result, end - start

def main():
    queries = [
        # 1. Proste SELECT z LIMIT
        (
            lambda: [r.hgetall(f"client:{i}") for i in range(1, 101)],
            "1. Proste zapytanie SELECT z LIMIT"
        ),

        # 2. SELECT z WHERE
        (
            lambda: r.hgetall("client:1"),
            "2. Proste zapytanie SELECT z WHERE"
        ),

        # 3. JOIN client -> account
        (
            lambda: [
                {
                    **r.hgetall(f"client:{r.hget(acc_key, 'client_id').decode()}"),
                    "account_number": r.hget(acc_key, "account_number").decode()
                }
                for acc_key in r.scan_iter("account:*")
                if r.hget(acc_key, "client_id")
            ],
            "3. Zapytanie SELECT z JOIN"
        ),

        # 4. COUNT klientów
        (
            lambda: len(list(r.scan_iter("client:*"))),
            "4. Zapytanie SELECT z COUNT"
        ),

        # 5. AVG(balance)
        (
            lambda: (
                lambda accs: sum(accs) / len(accs)
                if accs else 0
            )([
                float(r.hget(acc_key, "balance") or 0)
                for acc_key in r.scan_iter("account:*")
                if r.hexists(acc_key, "balance")
            ]),
            "5. Zapytanie SELECT z AVG"
        ),

        # 6. JOIN + WHERE + ORDER
        (
            lambda: sorted([
                {
                    "client": r.hgetall(f"client:{r.hget(acc_key, 'client_id').decode()}"),
                    "account_number": r.hget(acc_key, "account_number").decode(),
                    "balance": float(r.hget(acc_key, "balance"))
                }
                for acc_key in r.scan_iter("account:*")
                if float(r.hget(acc_key, "balance") or 0) > 1000
            ], key=lambda x: -x["balance"]),
            "6. Zaawansowane zapytanie z WHERE + ORDER BY"
        ),

        # 7. JOIN: transactions po dacie
        (
            lambda: sorted([
                {
                    "transaction_id": json.loads(tr)["transaction_id"],
                    "amount": json.loads(tr)["amount"],
                    "account_number": r.hget(acc_key, "account_number").decode()
                }
                for acc_key in r.scan_iter("account:*")
                for tr in r.lrange(f"transactions:{r.hget(acc_key, 'account_id').decode()}", 0, -1)
                if json.loads(tr)["transaction_date"] > "2023-01-01"
            ], key=lambda x: x["transaction_id"], reverse=True),
            "7. Zapytanie SELECT z JOIN (transactions)"
        ),

        # 8. Klienci z >1 kontem i saldem > 50000
        (
            lambda: sum(
                1 for client_key in r.scan_iter("client:*")
                if sum(
                    1 for acc_key in r.scan_iter("account:*")
                    if r.hget(acc_key, "client_id") == client_key.split(b":")[1] and float(r.hget(acc_key, "balance") or 0) > 50000
                ) > 1
            ),
            "8. Zliczenie klientów posiadających więcej niż jedno konto i saldo powyżej 50000."
        ),

        # 9. Zliczenie transakcji credit dla klientów z ID 2000–3000
        (
            lambda: {
                client_id: sum(
                    1
                    for acc_key in r.scan_iter("account:*")
                    if r.hget(acc_key, "client_id") == str(client_id).encode()
                    for tr in r.lrange(f"transactions:{r.hget(acc_key, 'account_id').decode()}", 0, -1)
                    if json.loads(tr)["transaction_type"] == "credit"
                )
                for client_id in range(2000, 3001)
            },
            "9. Zliczenie transakcji dla klientów o client_id w zakresie od 2000 do 3000."
        )
    ]

    for fn, desc in queries:
        times = []
        for i in range(100):
            print(f"▶️ Iteracja {i+1}/100 - {desc}")
            _, elapsed = run_redis_query(fn)
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
