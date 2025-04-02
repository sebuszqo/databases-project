import pandas as pd
from sqlalchemy import create_engine
import time

# Konfiguracja połączenia MariaDB
MARIADB_CONN = "mysql+pymysql://user:pass@localhost:3306/testdb"

# Tworzenie silnika MariaDB
mariadb_engine = create_engine(MARIADB_CONN)

def import_csv_to_db(engine, table_name, file_path):
    start_time = time.time()
    df = pd.read_csv(file_path)
    df.to_sql(table_name, engine, if_exists='replace', index=False)
    elapsed_time = time.time() - start_time
    print(f"Dane z {file_path} zaimportowane do tabeli {table_name} w {elapsed_time:.2f} sekundy.")

def import_mariadb_data():
    data_files = {
        "transactions": "transactions.csv",
        "loans": "loans.csv",
        "clients": "clients.csv",
        "cards": "cards.csv",
        "accounts": "accounts.csv",
    }

    for table, file in data_files.items():
        import_csv_to_db(mariadb_engine, table, file)

if __name__ == "__main__":
    import_mariadb_data()
