# import pandas as pd
# import matplotlib.pyplot as plt

# # Dane
# data = pd.DataFrame({
#     "engine": [
#         "PostgreSQL", "PostgreSQL", "PostgreSQL", "PostgreSQL", "PostgreSQL", "PostgreSQL",
#         "MariaDB", "MariaDB", "MariaDB", "MariaDB", "MariaDB", "MariaDB"
#     ],
#     "record_count": [
#         50000, 100000, 150000, 200000, 500000, 1000000,
#         50000, 100000, 150000, 200000, 500000, 1000000
#     ],
#     "avg_time": [
#         1.2922, 1.9651, 3.7151, 4.7962, 12.5136, 26.7918,
#         0.1808, 0.1935, 0.5303, 0.7043, 4.5408, 4.5408
#     ],
#     "min_time": [
#         0.5350, 1.1257, 2.0636, 2.6060, 10.3648, 22.2233,
#         0.1600, 0.1706, 0.4977, 0.5919, 3.2939, 3.2939
#     ],
#     "max_time": [
#         1.6564, 2.3309, 4.5693, 5.5394, 13.7755 ,28.2220,
#         0.2657, 0.3341, 0.8393, 0.7695, 4.5408, 6.1127
#     ]
# })

# # Oblicz rekordy na sekundę
# data["records_per_second"] = data["record_count"] / data["avg_time"]

# # Wykres INSERT: średni czas + min/max
# plt.figure(figsize=(10, 6))
# for engine, group in data.groupby("engine"):
#     plt.errorbar(
#         group["record_count"], group["avg_time"],
#         yerr=[group["avg_time"] - group["min_time"], group["max_time"] - group["avg_time"]],
#         label=engine, marker='o', capsize=5
#     )

# plt.title("Średni czas INSERT (z min/max) – PostgreSQL vs MariaDB")
# plt.xlabel("Liczba rekordów")
# plt.ylabel("Czas [s]")
# plt.grid(True)
# plt.legend()
# plt.tight_layout()
# plt.savefig("insert_time_all_points.png")


import pandas as pd
import matplotlib.pyplot as plt
import statistics

# SELECTS
data_select = {
    "engine": [
        "PostgreSQL", "PostgreSQL", "PostgreSQL", "PostgreSQL", "PostgreSQL", "PostgreSQL", "PostgreSQL", "PostgreSQL", "PostgreSQL", 
        "MariaDB", "MariaDB", "MariaDB", "MariaDB", "MariaDB", "MariaDB", "MariaDB",  "MariaDB", "MariaDB",
        # "MongoDB", "MongoDB", "MongoDB", "MongoDB", "MongoDB", "MongoDB", "MongoDB", "MongoDB", "MongoDB",
        # "Redis", "Redis", "Redis", "Redis", "Redis", "Redis", "Redis", "Redis", "Redis",
    ],
    "query": [
        "1. Pobranie 100 pierwszych rekordów z tabeli clients.",
        "2. Pobranie danych klienta o client_id = 1 z tabeli clients.",
        "3. Pobranie imienia i nazwiska klienta oraz numeru konta z tabel clients i accounts.",
        "4. Uzyskanie liczby wszystkich rekordów w tabeli clients.",
        "5. Obliczenie średniego stanu konta w tabeli accounts.",
        "6. Pobranie klientów, których saldo jest większe niż 1000, posortowanych malejąco po saldzie.",
        "7. Pobranie danych transakcji z dnia 2023-01-01 i późniejszych, posortowanych według daty transakcji.",
        "8. Zliczenie klientów posiadających więcej niż jedno konto i saldo powyżej 50000.",
        "9. Zliczenie transakcji dla klientów o client_id w zakresie od 2000 do 3000.",
        
        "1. Pobranie 100 pierwszych rekordów z tabeli clients.",
        "2. Pobranie danych klienta o client_id = 1 z tabeli clients.",
        "3. Pobranie imienia i nazwiska klienta oraz numeru konta z tabel clients i accounts.",
        "4. Uzyskanie liczby wszystkich rekordów w tabeli clients.",
        "5. Obliczenie średniego stanu konta w tabeli accounts.",
        "6. Pobranie klientów, których saldo jest większe niż 1000, posortowanych malejąco po saldzie.",
        "7. Pobranie danych transakcji z dnia 2023-01-01 i późniejszych, posortowanych według daty transakcji.",
        "8. Zliczenie klientów posiadających więcej niż jedno konto i saldo powyżej 50000.",
        "9. Zliczenie transakcji dla klientów o client_id w zakresie od 2000 do 3000.",
        
        # "Pobranie 100 pierwszych rekordów z tabeli clients.",
        # "Pobranie danych klienta o client_id = 1 z tabeli clients.",
        # "Pobranie imienia i nazwiska klienta oraz numeru konta z tabel clients i accounts.",
        # "Uzyskanie liczby wszystkich rekordów w tabeli clients.",
        # "Obliczenie średniego stanu konta w tabeli accounts.",
        # "Pobranie klientów, których saldo jest większe niż 1000, posortowanych malejąco po saldzie.",
        # "Pobranie danych transakcji z dnia 2023-01-01 i późniejszych, posortowanych według daty transakcji.",
        # "Zliczenie klientów posiadających więcej niż jedno konto i saldo powyżej 50000.",
        # "Zliczenie transakcji dla klientów o client_id w zakresie od 2000 do 3000.",
        
        # "Pobranie 100 pierwszych rekordów z tabeli clients.",
        # "Pobranie danych klienta o client_id = 1 z tabeli clients.",
        # "Pobranie imienia i nazwiska klienta oraz numeru konta z tabel clients i accounts.",
        # "Uzyskanie liczby wszystkich rekordów w tabeli clients.",
        # "Obliczenie średniego stanu konta w tabeli accounts.",
        # "Pobranie klientów, których saldo jest większe niż 1000, posortowanych malejąco po saldzie.",
        # "Pobranie danych transakcji z dnia 2023-01-01 i późniejszych, posortowanych według daty transakcji.",
        # "Zliczenie klientów posiadających więcej niż jedno konto i saldo powyżej 50000.",
        # "Zliczenie transakcji dla klientów o client_id w zakresie od 2000 do 3000.",
    ],
    "avg_time": [
        0.0006, 0.0003, 0.0728, 0.0022, 0.0073, 0.1114, 0.6821, 0.1279, 0.1062,
        0.0004, 0.0003, 0.1340, 0.0051, 0.0117, 0.2480, 1.5113, 0.1282, 0.0123,
        # 0.0002, 0.0004, 0.045, 0.0002, 0.0004, 0.045, 0.6622,  0.022, 0.4322,
        # 0.0010, 0.0005, 0.0700, 0.0002, 0.0004, 0.045, 0.6622, 0.022, 0.4322,
    ],
    "min_time": [
        0.0003, 0.0002, 0.0683, 0.0020, 0.0067, 0.1061, 0.4377, 0.1116, 0.0961,
        0.0002, 0.0002, 0.1246, 0.0048, 0.0111, 0.2340, 1.3944, 0.1189, 0.0103,
        # 0.0001, 0.0003, 0.022, 0.0001, 0.0003, 0.022, 0.4322,  0.022, 0.4322,
        # 0.0007, 0.0003, 0.0500, 0.0001, 0.0003, 0.022, 0.4322,  0.022, 0.4322,
    ],
    "max_time": [
        0.0038, 0.0016, 0.1030, 0.0051, 0.0148, 0.1430, 1.1679, 0.2877, 0.1533, 
        0.0082, 0.0011, 0.2609, 0.0059, 0.0208, 0.2936, 1.8861, 0.2068, 0.0267,
        # 0.0010, 0.0010, 0.100, 0.0010, 0.0010, 0.100, 1.222, 0.022, 0.4322,
        # 0.0040, 0.0010, 0.1500, 0.0010, 0.0010, 0.100, 1.222, 0.022, 0.4322,
    ]
}

# UPDATES
data_update = {
    "engine": [
        "PostgreSQL", "PostgreSQL", "PostgreSQL", "PostgreSQL", "PostgreSQL", 
        "MariaDB", "MariaDB", "MariaDB", "MariaDB", "MariaDB",
        # "MongoDB", "MongoDB", "MongoDB", "MongoDB", "MongoDB", 
        # "Redis", "Redis", "Redis", "Redis", "Redis",
    ],
    "query": [
        "1. Aktualizacja jednego pola dla klienta o konkretnym client_id.",
        "2. Zmiana first_name dla wielu klientów.",
        "3. Aktualizacja salda konta z użyciem JOIN z tabelą clients.",
        "4. Aktualizacja salda konta na podstawie średniego salda wszystkich kont.",
        "5. Aktualizacja adresu e-mail na podstawie subzapytania z transakcjami i saldem powyżej 1000.",
        
        "1. Aktualizacja jednego pola dla klienta o konkretnym client_id.",
        "2. Zmiana first_name dla wielu klientów.",
        "3. Aktualizacja salda konta z użyciem JOIN z tabelą clients.",
        "4. Aktualizacja salda konta na podstawie średniego salda wszystkich kont.",
        "5. Aktualizacja adresu e-mail na podstawie subzapytania z transakcjami i saldem powyżej 1000.",
        
        # NOSQL 1
        # "1. Aktualizacja jednego pola dla klienta o konkretnym client_id.",
        # "2. Zmiana first_name dla wielu klientów.",
        # "3. Aktualizacja salda konta z użyciem JOIN z tabelą clients.",
        # "4. Aktualizacja salda konta na podstawie średniego salda wszystkich kont.",
        # "5. Aktualizacja adresu e-mail na podstawie subzapytania z transakcjami i saldem powyżej 1000.",
        
        # NOSQL 2
        #  "1. Aktualizacja jednego pola dla klienta o konkretnym client_id.",
        # "2. Zmiana first_name dla wielu klientów.",
        # "3. Aktualizacja salda konta z użyciem JOIN z tabelą clients.",
        # "4. Aktualizacja salda konta na podstawie średniego salda wszystkich kont.",
        # "5. Aktualizacja adresu e-mail na podstawie subzapytania z transakcjami i saldem powyżej 1000.",
    ],
    "avg_time": [
        0.0009, 0.0008, 0.3727, 0.0941, 0.7795, 
        0.0007, 0.0006, 0.1864,  0.0195, 0.3281,
        # 0.0009, 0.0008, 0.3727, 0.0941, 0.7795, # NOSQL1
        # 0.0009, 0.0008, 0.3727, 0.0941, 0.7795,  # NOSQL2
    ],
    "min_time": [
        0.0007,  0.0006, 0.3195, 0.0842, 0.4919,
        0.0005, 0.0005, 0.1667, 0.0183, 0.2962,
        # 0.0009, 0.0008, 0.3727, 0.0941, 0.7795, # NOSQL1
        # 0.0009, 0.0008, 0.3727, 0.0941, 0.7795,  # NOSQL2
    ],
    "max_time": [
        0.0076, 0.0021, 0.5581, 0.1234, 1.1961,
        0.0037, 0.0018, 0.2667, 0.0346, 0.5525,
        # 0.0009, 0.0008, 0.3727, 0.0941, 0.7795, # NOSQL1
        # 0.0009, 0.0008, 0.3727, 0.0941, 0.7795,  # NOSQL2
    ]
}

data_delete = {
    "engine": [
        "PostgreSQL", "PostgreSQL", "PostgreSQL", "PostgreSQL", "PostgreSQL", 
        "MariaDB", "MariaDB", "MariaDB", "MariaDB", "MariaDB",
        # "MongoDB", "MongoDB", "MongoDB", "MongoDB", "MongoDB", 
        # "Redis", "Redis", "Redis", "Redis", "Redis",
    ],
    "query": [
        "1. Usunięcie klienta o client_id = 1.",
        "2. Usunięcie transakcji powiązanych z klientem:",
        "3. Usunięcie kont o saldzie powyżej 1000.",
        "4. Usunięcie klientów bez transakcji.",
        
        "1. Usunięcie klienta o client_id = 1.",
        "2. Usunięcie transakcji powiązanych z klientem:",
        "3. Usunięcie kont o saldzie powyżej 1000.",
        "4. Usunięcie klientów bez transakcji.",
        
        # NOSQL 1
        # "1. Usunięcie klienta o client_id = 1.",
        # "2. Usunięcie transakcji powiązanych z klientem:",
        # "3. Usunięcie kont o saldzie powyżej 1000.",
        # "4. Usunięcie klientów bez transakcji.",
        
        # NOSQL 2
        # "1. Usunięcie klienta o client_id = 1.",
        # "2. Usunięcie transakcji powiązanych z klientem:",
        # "3. Usunięcie kont o saldzie powyżej 1000.",
        # "4. Usunięcie klientów bez transakcji.",
    ],
    "avg_time": [
        # 0.0007,
        0.0015, 0.1682, 4.0429, 0.0513
        # 0.0009, 0.0008, 0.3727, 0.0941, 0.7795, # NOSQL1
        # 0.0009, 0.0008, 0.3727, 0.0941, 0.7795,  # NOSQL2
    ],
    "min_time": [
        # 0.0005,
        0.0009, 0.1625, 3.6912, 0.0483
        # 0.0009, 0.0008, 0.3727, 0.0941, 0.7795, # NOSQL1
        # 0.0009, 0.0008, 0.3727, 0.0941, 0.7795,  # NOSQL2
    ],
    "max_time": [
        # 0.0
        0.0036, 0.2158, 5.5504, 0.0768
        # 0.0009, 0.0008, 0.3727, 0.0941, 0.7795, # NOSQL1
        # 0.0009, 0.0008, 0.3727, 0.0941, 0.7795,  # NOSQL2
    ]
}


# SELECT 
# df = pd.DataFrame(data_select)

# UPDATE
df = pd.DataFrame(data_update)

def plot_query_times(df):
    queries = df['query'].unique()

    for query in queries:
        query_data = df[df['query'] == query]
        
        plt.figure(figsize=(10, 6))
        for engine in query_data['engine'].unique():
            engine_data = query_data[query_data['engine'] == engine]
            plt.errorbar(
                engine_data['engine'], 
                engine_data['avg_time'], 
                yerr=[engine_data['avg_time'] - engine_data['min_time'], engine_data['max_time'] - engine_data['avg_time']],
                label=f'{engine}', 
                marker='o', 
                capsize=10
            )

        plt.title(f"{query}")
        plt.xlabel("Silnik")
        plt.ylabel("Czas [s]")
        plt.grid(True)
        plt.legend()
        plt.tight_layout()


        plt.savefig(f"query_times_{query}.png")
        plt.close() 

plot_query_times(df)
