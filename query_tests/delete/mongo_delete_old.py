from pymongo import MongoClient
import time
import statistics

client = MongoClient("localhost", 27017)
db = client["testdb"]
col = db.clients

def run_mongo_delete(operation):
    start = time.time()
    operation()
    end = time.time()
    return end - start

def main():
    deletes = [
        # 1
        (lambda: col.delete_one({"client_id": 1}), "Prosty DELETE"),
        # 2
        (lambda: col.delete_many({"client_id": {"$in": [1, 2, 3, 4, 5]}}), "DELETE wielu rekordów"),
        # 3
        (lambda: col.update_many(
            {"email": {"$regex": "@example.com$"}},
            {"$pull": {"accounts": {}}}
        ), "DELETE z JOIN (czyści konta klientów z emailem *.example.com)"),
        # 4
        (lambda: col.update_many(
            {},
            {"$pull": {"accounts.$[].transactions": {"transaction_date": {"$lt": "2023-01-01"}}}}
        ), "DELETE z warunkiem po dacie (transakcje)"),
        # 5
        (lambda: col.delete_many({
            "$expr": {
                "$gt": [
                    {"$size": "$accounts"}, 2
                ]
            }
        }), "DELETE z GROUP BY i HAVING (klienci z >2 kontami)")
    ]

    for delete_fn, desc in deletes:
        times = []
        for i in range(100):
            print(f"▶️ Iteracja {i+1}/100 - {desc}")
            elapsed = run_mongo_delete(delete_fn)
            times.append(elapsed)

        print_stats(desc, times)

    client.close()

def print_stats(desc, times):
    print(f"\n{desc}")
    print(f"Średni czas: {statistics.mean(times):.4f} s")
    print(f"Minimalny czas: {min(times):.4f} s")
    print(f"Maksymalny czas: {max(times):.4f} s")
    print(f"Wszystkie czasy: {[round(t, 4) for t in times]}")
    print("=" * 40)

if __name__ == "__main__":
    main()
