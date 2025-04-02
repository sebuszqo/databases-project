from faker import Faker
import csv
import random
from tqdm import tqdm
from datetime import datetime, timedelta

fake = Faker()

NUM_CLIENTS = 100_000
NUM_ACCOUNTS = 150_000
NUM_TRANSACTIONS = 1_000_000
NUM_CARDS = 200_000
NUM_LOANS = 50_000

def generate_clients():
    with open('clients.csv', 'w', newline='') as file:
        writer = csv.writer(file)
        writer.writerow(['client_id', 'first_name', 'last_name', 'email'])
        for client_id in tqdm(range(1, NUM_CLIENTS + 1), desc="Generating clients"):
            writer.writerow([client_id, fake.first_name(), fake.last_name(), fake.email()])

def generate_accounts():
    with open('accounts.csv', 'w', newline='') as file:
        writer = csv.writer(file)
        writer.writerow(['account_id', 'client_id', 'account_number', 'balance'])
        for account_id in tqdm(range(1, NUM_ACCOUNTS + 1), desc="Generating accounts"):
            client_id = random.randint(1, NUM_CLIENTS)
            account_number = fake.bban()
            balance = round(random.uniform(0, 100_000), 2)
            writer.writerow([account_id, client_id, account_number, balance])

def generate_transactions():
    with open('transactions.csv', 'w', newline='') as file:
        writer = csv.writer(file)
        writer.writerow(['transaction_id', 'account_id', 'amount', 'transaction_type', 'transaction_date'])
        for transaction_id in tqdm(range(1, NUM_TRANSACTIONS + 1), desc="Generating transactions"):
            account_id = random.randint(1, NUM_ACCOUNTS)
            amount = round(random.uniform(1, 10_000), 2)
            transaction_type = random.choice(['credit', 'debit'])
            transaction_date = fake.date_between(start_date='-3y', end_date='today')
            writer.writerow([transaction_id, account_id, amount, transaction_type, transaction_date])

def generate_cards():
    with open('cards.csv', 'w', newline='') as file:
        writer = csv.writer(file)
        writer.writerow(['card_id', 'account_id', 'card_number', 'expiry_date'])
        for card_id in tqdm(range(1, NUM_CARDS + 1), desc="Generating cards"):
            account_id = random.randint(1, NUM_ACCOUNTS)
            card_number = fake.credit_card_number()
            expiry_date = fake.date_between(start_date='today', end_date='+5y')
            writer.writerow([card_id, account_id, card_number, expiry_date])

def generate_loans():
    with open('loans.csv', 'w', newline='') as file:
        writer = csv.writer(file)
        writer.writerow(['loan_id', 'client_id', 'loan_amount', 'interest_rate', 'loan_date'])
        for loan_id in tqdm(range(1, NUM_LOANS + 1), desc="Generating loans"):
            client_id = random.randint(1, NUM_CLIENTS)
            loan_amount = round(random.uniform(1_000, 500_000), 2)
            interest_rate = round(random.uniform(2.0, 10.0), 2)
            loan_date = fake.date_between(start_date='-5y', end_date='today')
            writer.writerow([loan_id, client_id, loan_amount, interest_rate, loan_date])

if __name__ == "__main__":
    generate_clients()
    generate_accounts()
    generate_transactions()
    generate_cards()
    generate_loans()

    print("\n✅ Data generation completed! CSV files ready.")



