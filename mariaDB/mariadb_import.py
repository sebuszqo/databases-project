import pandas as pd
from sqlalchemy import create_engine, text
import time

# Konfiguracja połączenia MariaDB
MARIADB_CONN = "mysql+pymysql://user:pass@localhost:3306/testdb"
mariadb_engine = create_engine(MARIADB_CONN)

def insert_csv_to_db(engine, table_name, file_path):
    start_time = time.time()
    df = pd.read_csv(file_path)

    # Przygotuj listę kolumn do inserta
    columns = df.columns.tolist()
    placeholders = ', '.join([':%s' % col for col in columns])
    column_names = ', '.join([f"`{col}`" for col in columns])
    insert_sql = f"INSERT INTO `{table_name}` ({column_names}) VALUES ({placeholders})"
  
    with engine.begin() as conn:  # użycie transakcji
        for _, row in df.iterrows():
            conn.execute(text(insert_sql), row.to_dict())

    elapsed_time = time.time() - start_time
    print(f"Dane z {file_path} zaimportowane do tabeli {table_name} w {elapsed_time:.2f} sekundy.")

def import_mariadb_data():
    data_files = {
    "clients": "data/clients.csv",
    "accounts": "data/accounts.csv",
    "transactions": "data/transactions.csv",
    "cards": "data/cards.csv",
    "loans": "data/loans.csv",
}

    for table, file in data_files.items():
        insert_csv_to_db(mariadb_engine, table, file)

if __name__ == "__main__":
    import_mariadb_data()
