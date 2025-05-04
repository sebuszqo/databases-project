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
        # 1. Usunięcie klienta o client_id = 1
        (
            lambda: col.delete_one({"client_id": 1}),
            "1. Usunięcie klienta o client_id = 1."
        ),

        # 2. Usunięcie transakcji powiązanych z klientem
        (
            lambda: (
                lambda client: col.update_one(
                    {"client_id": 1},
                    {"$set": {
                        "accounts": [
                            {**acc, "transactions": []}
                            for acc in client.get("accounts", [])
                        ]
                    }}
                ) if client else None
            )(col.find_one({"client_id": 1})),
            "2. Usunięcie transakcji powiązanych z klientem:"
        ),

        # 3. Usunięcie kont o saldzie powyżej 1000
        (
            lambda: col.update_many(
                {},
                {"$pull": {"accounts": {"balance": {"$gt": 1000}}}}
            ),
            "3. Usunięcie kont o saldzie powyżej 1000."
        ),

        # 4. Usunięcie klientów bez transakcji
        (
            lambda: col.delete_many({
                "$expr": {
                    "$eq": [
                        {
                            "$size": {
                                "$filter": {
                                    "input": {
                                        "$reduce": {
                                            "input": "$accounts",
                                            "initialValue": [],
                                            "in": {"$concatArrays": ["$$value", "$$this.transactions"]}
                                        }
                                    },
                                    "as": "t",
                                    "cond": {"$ne": ["$$t", None]}
                                }
                            }
                        }, 0
                    ]
                }
            }),
            "4. Usunięcie klientów bez transakcji."
        )
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

