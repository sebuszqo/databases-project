-- 1. Tabela klientów
CREATE TABLE clients (
    client_id INT PRIMARY KEY,
    first_name VARCHAR(50),
    last_name VARCHAR(50),
    email VARCHAR(100)
);

-- 2. Tabela kont bankowych
CREATE TABLE accounts (
    account_id INT PRIMARY KEY,
    client_id INT,
    account_number VARCHAR(50),
    balance DECIMAL(15,2),
    FOREIGN KEY (client_id) REFERENCES clients(client_id)
);

-- 3. Tabela transakcji
CREATE TABLE transactions (
    transaction_id INT PRIMARY KEY,
    account_id INT,
    amount DECIMAL(15,2),
    transaction_type VARCHAR(50),
    transaction_date DATE,
    FOREIGN KEY (account_id) REFERENCES accounts(account_id)
);

-- 4. Tabela kart
CREATE TABLE cards (
    card_id INT PRIMARY KEY,
    account_id INT,
    card_number VARCHAR(50),
    expiry_date DATE,
    FOREIGN KEY (account_id) REFERENCES accounts(account_id)
);

-- 5. Tabela kredytów
CREATE TABLE loans (
    loan_id INT PRIMARY KEY,
    client_id INT,
    loan_amount DECIMAL(15,2),
    interest_rate DECIMAL(5,2),
    loan_date DATE,
    FOREIGN KEY (client_id) REFERENCES clients(client_id)
);


