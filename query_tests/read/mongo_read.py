from pymongo import MongoClient
import time
import statistics

DB_CONFIG_MONGO = {
    'host': 'localhost',
    'port': 27017,
    'db': 'testdb'
}

def run_mongo_query(collection, pipeline):
    start = time.time()
    result = list(collection.aggregate(pipeline))
    end = time.time()
    return result, end - start

def main():
    client = MongoClient(DB_CONFIG_MONGO['host'], DB_CONFIG_MONGO['port'])
    db = client[DB_CONFIG_MONGO['db']]
    collection = db.clients

    queries = [
        # 1. Proste zapytanie SELECT z LIMIT
        ([
            {"$limit": 100}
        ], "Proste zapytanie SELECT z LIMIT"),

        # 2. Proste zapytanie SELECT z WHERE
        ([
            {"$match": {"client_id": 1}}
        ], "Proste zapytanie SELECT z WHERE"),

        # 3. JOIN (accounts w zagnieżdżonych dokumentach)
        ([
            {"$unwind": "$accounts"},
            {"$project": {
                "first_name": 1,
                "last_name": 1,
                "account_number": "$accounts.account_number"
            }}
        ], "Zapytanie SELECT z JOIN (accounts)"),

        # 4. COUNT klientów
        ([
            {"$count": "client_count"}
        ], "Zapytanie SELECT z COUNT"),

        # 5. Średni balans z accounts
        ([
            {"$unwind": "$accounts"},
            {"$group": {"_id": None, "avg_balance": {"$avg": "$accounts.balance"}}}
        ], "Zapytanie SELECT z AVG"),

        # 6. Zaawansowane zapytanie z WHERE + ORDER BY
        ([
            {"$unwind": "$accounts"},
            {"$match": {"accounts.balance": {"$gt": 1000}}},
            {"$sort": {"accounts.balance": -1}},
            {"$project": {
                "first_name": 1,
                "last_name": 1,
                "account_number": "$accounts.account_number",
                "balance": "$accounts.balance"
            }}
        ], "Zaawansowane zapytanie z WHERE + ORDER BY"),

        # 7. JOIN na transactions (zagnieżdżone dane)
        ([
            {"$unwind": "$accounts"},
            {"$unwind": "$accounts.transactions"},
            {"$match": {"accounts.transactions.transaction_date": {"$gt": "2023-01-01"}}},
            {"$sort": {"accounts.transactions.transaction_date": -1}},
            {"$project": {
                "transaction_id": "$accounts.transactions.transaction_id",
                "amount": "$accounts.transactions.amount",
                "account_number": "$accounts.account_number"
            }}
        ], "Zapytanie SELECT z JOIN (transactions)"),
                # 8. Klienci z >1 kontem i saldem >50k
        ([
            {"$unwind": "$accounts"},
            {"$match": {"accounts.balance": {"$gt": 50000}}},
            {"$group": {"_id": "$client_id", "count": {"$sum": 1}}},
            {"$match": {"count": {"$gt": 1}}},
            {"$count": "qualified_clients"}
        ], "8. Zliczenie klientów posiadających więcej niż jedno konto i saldo powyżej 50000."),

        # 9. Zliczenie transakcji typu credit dla client_id 2000–3000
        ([
            {"$match": {"client_id": {"$gte": 2000, "$lte": 3000}}},
            {"$unwind": "$accounts"},
            {"$unwind": "$accounts.transactions"},
            {"$match": {"accounts.transactions.transaction_type": "credit"}},
            {"$group": {
                "_id": "$client_id",
                "count": {"$sum": 1}
            }}
        ], "9. Zliczenie transakcji dla klientów o client_id w zakresie od 2000 do 3000.")

    ]

    for pipeline, description in queries:
        times = []
        for i in range(100):
            print(f"▶️ Iteracja {i+1}/100 - {description}")
            _, elapsed_time = run_mongo_query(collection, pipeline)
            times.append(elapsed_time)

        avg_time = statistics.mean(times)
        min_time = min(times)
        max_time = max(times)

        print(f"\n{description}")
        print(f"Średni czas: {avg_time:.4f} sekundy")
        print(f"Minimalny czas: {min_time:.4f} sekundy")
        print(f"Maksymalny czas: {max_time:.4f} sekundy")
        print(f"Wszystkie czasy: {[round(t, 4) for t in times]}")
        print("=" * 40)

    client.close()

if __name__ == "__main__":
    main()
