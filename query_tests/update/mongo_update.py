from pymongo import MongoClient
import time
import statistics

DB_CONFIG = {
    "host": "localhost",
    "port": 27017,
    "db": "testdb"
}

def run_mongo_update(collection, update_fn):
    start = time.time()
    update_fn(collection)
    end = time.time()
    return end - start

def main():
    client = MongoClient(DB_CONFIG["host"], DB_CONFIG["port"])
    db = client[DB_CONFIG["db"]]
    col = db.clients

    updates = [
        # 1. Prosty UPDATE jednego klienta
        (
            lambda c: c.update_one(
                {"client_id": 1},
                {"$set": {"first_name": "UpdatedName"}}
            ),
            "Proste zapytanie UPDATE"
        ),

        # 2. UPDATE wielu klientów po ID
        (
            lambda c: c.update_many(
                {"client_id": {"$in": [1, 2, 3, 4, 5]}},
                {"$set": {"first_name": "Updated"}}
            ),
            "UPDATE z WHERE (wielu rekordów)"
        ),

        # 3. UPDATE JOIN clients ↔ accounts, aktualizacja balance
        (
            lambda c: c.update_many(
                {
                    "accounts": {
                        "$elemMatch": {
                            "balance": {"$exists": True}
                        }
                    },
                    "email": {"$regex": "@example.com$"}
                },
                {
                    "$inc": {"accounts.$[].balance": 100}
                }
            ),
            "UPDATE z JOIN"
        ),

        # 4. UPDATE z agregacją — zwiększ balance o średni balans (dla account_id=1)
        (
            lambda c: (
                lambda avg_balance: c.update_one(
                    {"accounts.account_id": 1},
                    {"$inc": {"accounts.$.balance": avg_balance}}
                ) if avg_balance is not None else None
            )(
                next(
                    db.clients.aggregate([
                        {"$unwind": "$accounts"},
                        {"$group": {"_id": None, "avg_balance": {"$avg": "$accounts.balance"}}}
                    ]),
                    {"avg_balance": 0}
                )["avg_balance"]
            ),
            "UPDATE z agregacją"
        ),


        # 5. UPDATE email jeśli klient ma konto z transakcją po 2023 i balance > 1000
        (
            lambda c: c.update_many(
                {
                    "accounts": {
                        "$elemMatch": {
                            "balance": {"$gt": 1000},
                            "transactions": {
                                "$elemMatch": {
                                    "transaction_date": {"$gt": "2023-01-01"}
                                }
                            }
                        }
                    }
                },
                {"$set": {"email": "newemail@example.com"}}
            ),
            "UPDATE z subzapytaniem i JOIN"
        )
    ]

    for update_fn, description in updates:
        times = []
        for i in range(100):
            print(f"▶️ Iteracja {i+1}/100 - {description}")
            elapsed_time = run_mongo_update(col, update_fn)
            times.append(elapsed_time)

        avg = statistics.mean(times)
        print(f"\n{description}")
        print(f"Średni czas: {avg:.4f} s")
        print(f"Minimalny czas: {min(times):.4f} s")
        print(f"Maksymalny czas: {max(times):.4f} s")
        print(f"Wszystkie czasy: {[round(t, 4) for t in times]}")
        print("=" * 40)

    client.close()

if __name__ == "__main__":
    main()
