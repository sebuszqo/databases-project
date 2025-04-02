import mysql.connector
import time
import psycopg2
import time
from pymongo import MongoClient
import time
import redis
import time

def test_postgres():
    conn = psycopg2.connect(
        dbname='testdb',
        user='user',
        password='pass',
        host='localhost',
        port=5432
    )
    cur = conn.cursor()

    start = time.perf_counter()
    cur.execute("INSERT INTO test_table (name, amount) VALUES ('John', 1000);")
    conn.commit()
    end = time.perf_counter()
    print(f"PostgreSQL INSERT: {end - start:.6f} sec")

    start = time.perf_counter()
    cur.execute("SELECT * FROM test_table;")
    cur.fetchall()
    end = time.perf_counter()
    print(f"PostgreSQL SELECT: {end - start:.6f} sec")

    cur.close()
    conn.close()



def test_mongo():
    client = MongoClient('localhost', 27017)
    db = client.testdb
    collection = db.test_collection

    start = time.perf_counter()
    collection.insert_one({"name": "John", "amount": 1000})
    end = time.perf_counter()
    print(f"MongoDB INSERT: {end - start:.6f} sec")

    start = time.perf_counter()
    list(collection.find({"name": "John"}))
    end = time.perf_counter()
    print(f"MongoDB SELECT: {end - start:.6f} sec")

def test_redis():
    r = redis.Redis(host='localhost', port=6379, db=0)

    start = time.perf_counter()
    r.set('client:1', '{"name": "John", "amount": 1000}')
    end = time.perf_counter()
    print(f"Redis SET: {end - start:.6f} sec")

    start = time.perf_counter()
    r.get('client:1')
    end = time.perf_counter()
    print(f"Redis GET: {end - start:.6f} sec")


if __name__ == "__main__":
    test_postgres()
    test_mariadb()
    test_mongo()
    test_redis()
