import pandas as pd
import matplotlib.pyplot as plt

# Dane
data = pd.DataFrame({
    "engine": [
        "PostgreSQL", "PostgreSQL", "PostgreSQL", "PostgreSQL", "PostgreSQL", "PostgreSQL",
        "MariaDB", "MariaDB", "MariaDB", "MariaDB", "MariaDB", "MariaDB"
    ],
    "record_count": [
        50000, 100000, 150000, 200000, 500000, 1000000,
        50000, 100000, 150000, 200000, 500000, 1000000
    ],
    "avg_time": [
        1.2922, 1.9651, 3.7151, 4.7962, 12.5136, 26.7918,
        0.1808, 0.1935, 0.5303, 0.7043, 4.5408, 4.5408
    ],
    "min_time": [
        0.5350, 1.1257, 2.0636, 2.6060, 10.3648, 22.2233,
        0.1600, 0.1706, 0.4977, 0.5919, 3.2939, 3.2939
    ],
    "max_time": [
        1.6564, 2.3309, 4.5693, 5.5394, 13.7755 ,28.2220,
        0.2657, 0.3341, 0.8393, 0.7695, 4.5408, 6.1127
    ]
})

# Oblicz rekordy na sekundę
data["records_per_second"] = data["record_count"] / data["avg_time"]

# Wykres INSERT: średni czas + min/max
plt.figure(figsize=(10, 6))
for engine, group in data.groupby("engine"):
    plt.errorbar(
        group["record_count"], group["avg_time"],
        yerr=[group["avg_time"] - group["min_time"], group["max_time"] - group["avg_time"]],
        label=engine, marker='o', capsize=5
    )

plt.title("Średni czas INSERT (z min/max) – PostgreSQL vs MariaDB")
plt.xlabel("Liczba rekordów")
plt.ylabel("Czas [s]")
plt.grid(True)
plt.legend()
plt.tight_layout()
plt.savefig("insert_time_all_points.png")
