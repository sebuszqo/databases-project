import pandas as pd
import time

# Lista plików CSV
csv_files = {
    "clients": "clients.csv",
    "accounts": "accounts.csv",
    "cards": "cards.csv",
    "loans": "loans.csv",
    "transactions": "transactions.csv"
}

start_time = time.time()

# Wczytanie danych
data = {key: pd.read_csv(file) for key, file in csv_files.items()}

# Konwersja na listy słowników
for key in data:
    data[key] = data[key].to_dict(orient="records")

# Zapisanie danych do plików JSON
for key, records in data.items():
    output_file = f"{key}.json"
    pd.DataFrame(records).to_json(output_file, orient="records", indent=4)
    print(f"Zapisano {len(records)} rekordów do {output_file}")

time_elapsed = time.time() - start_time
print(f"Przygotowanie danych zakończone w {time_elapsed:.2f} sekund.")
