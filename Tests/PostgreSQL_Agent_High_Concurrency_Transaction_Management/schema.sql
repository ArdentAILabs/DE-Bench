-- Accounts table for payment processing system
CREATE TABLE accounts (
    account_id VARCHAR(50) PRIMARY KEY,
    user_id VARCHAR(50) NOT NULL,
    account_type VARCHAR(20) NOT NULL CHECK (account_type IN ('CHECKING', 'SAVINGS', 'MERCHANT', 'WALLET')),
    balance DECIMAL(15, 2) NOT NULL DEFAULT 0.00,
    currency VARCHAR(3) DEFAULT 'USD',
    status VARCHAR(20) DEFAULT 'ACTIVE' CHECK (status IN ('ACTIVE', 'SUSPENDED', 'CLOSED')),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    version INTEGER DEFAULT 0,  -- Optimistic locking for concurrent updates
    CONSTRAINT positive_balance CHECK (balance >= 0)
);

-- Transaction records table for audit trail and processing
CREATE TABLE transactions (
    transaction_id VARCHAR(50) PRIMARY KEY,
    from_account_id VARCHAR(50) REFERENCES accounts(account_id),
    to_account_id VARCHAR(50) REFERENCES accounts(account_id),
    amount DECIMAL(15, 2) NOT NULL,
    currency VARCHAR(3) DEFAULT 'USD',
    transaction_type VARCHAR(20) NOT NULL CHECK (transaction_type IN ('TRANSFER', 'DEPOSIT', 'WITHDRAWAL', 'PAYMENT', 'REFUND')),
    status VARCHAR(20) DEFAULT 'PENDING' CHECK (status IN ('PENDING', 'PROCESSING', 'COMPLETED', 'FAILED', 'ROLLED_BACK')),
    description TEXT,
    idempotency_key VARCHAR(100) UNIQUE,  -- For preventing duplicate transactions
    initiated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    completed_at TIMESTAMP,
    error_message TEXT,
    retry_count INTEGER DEFAULT 0,
    CONSTRAINT positive_amount CHECK (amount > 0),
    CONSTRAINT valid_accounts CHECK (
        (transaction_type IN ('DEPOSIT', 'WITHDRAWAL') AND (from_account_id IS NULL OR to_account_id IS NULL)) OR
        (transaction_type IN ('TRANSFER', 'PAYMENT', 'REFUND') AND from_account_id IS NOT NULL AND to_account_id IS NOT NULL)
    )
);

-- Balance history for audit compliance and reconciliation
CREATE TABLE balance_history (
    id SERIAL PRIMARY KEY,
    account_id VARCHAR(50) REFERENCES accounts(account_id),
    transaction_id VARCHAR(50) REFERENCES transactions(transaction_id),
    balance_before DECIMAL(15, 2) NOT NULL,
    balance_after DECIMAL(15, 2) NOT NULL,
    change_amount DECIMAL(15, 2) NOT NULL,
    operation_type VARCHAR(10) NOT NULL CHECK (operation_type IN ('DEBIT', 'CREDIT')),
    timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Transaction locks table for managing concurrent access
CREATE TABLE transaction_locks (
    lock_id VARCHAR(100) PRIMARY KEY,
    account_id VARCHAR(50) NOT NULL,
    transaction_id VARCHAR(50) NOT NULL,
    lock_type VARCHAR(20) NOT NULL CHECK (lock_type IN ('READ', 'write')),
    acquired_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    expires_at TIMESTAMP NOT NULL
);

-- Performance indexes for concurrent access
CREATE INDEX idx_accounts_user_id ON accounts(user_id);
CREATE INDEX idx_accounts_status ON accounts(status);
CREATE INDEX idx_transactions_from_account ON transactions(from_account_id);
CREATE INDEX idx_transactions_to_account ON transactions(to_account_id);
CREATE INDEX idx_transactions_status ON transactions(status);
CREATE INDEX idx_transactions_initiated_at ON transactions(initiated_at);
CREATE INDEX idx_balance_history_account_timestamp ON balance_history(account_id, timestamp);
CREATE INDEX idx_transaction_locks_account ON transaction_locks(account_id);

-- Trigger to update account updated_at timestamp
CREATE OR REPLACE FUNCTION update_account_timestamp()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = CURRENT_TIMESTAMP;
    NEW.version = OLD.version + 1;  -- Increment version for optimistic locking
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trigger_update_account_timestamp
    BEFORE UPDATE ON accounts
    FOR EACH ROW
    EXECUTE FUNCTION update_account_timestamp();

-- Function to clean up expired locks
CREATE OR REPLACE FUNCTION cleanup_expired_locks()
RETURNS INTEGER AS $$
DECLARE
    deleted_count INTEGER;
BEGIN
    DELETE FROM transaction_locks WHERE expires_at < CURRENT_TIMESTAMP;
    GET DIAGNOSTICS deleted_count = ROW_COUNT;
    RETURN deleted_count;
END;
$$ LANGUAGE plpgsql;

-- Insert test accounts for concurrent testing
INSERT INTO accounts (account_id, user_id, account_type, balance) VALUES
('ACC_001', 'user_alice', 'CHECKING', 1000.00),
('ACC_002', 'user_bob', 'CHECKING', 1500.00),
('ACC_003', 'user_carol', 'SAVINGS', 2000.00),
('ACC_004', 'user_dave', 'WALLET', 750.00),
('ACC_005', 'merchant_store', 'MERCHANT', 5000.00);

-- Insert some initial transaction history to simulate existing system
INSERT INTO transactions (transaction_id, to_account_id, amount, transaction_type, status, description, completed_at) VALUES
('TXN_SEED_001', 'ACC_001', 1000.00, 'DEPOSIT', 'COMPLETED', 'Initial deposit for Alice', CURRENT_TIMESTAMP - INTERVAL '1 day'),
('TXN_SEED_002', 'ACC_002', 1500.00, 'DEPOSIT', 'COMPLETED', 'Initial deposit for Bob', CURRENT_TIMESTAMP - INTERVAL '1 day'),
('TXN_SEED_003', 'ACC_003', 2000.00, 'DEPOSIT', 'COMPLETED', 'Initial deposit for Carol', CURRENT_TIMESTAMP - INTERVAL '1 day'),
('TXN_SEED_004', 'ACC_004', 750.00, 'DEPOSIT', 'COMPLETED', 'Initial deposit for Dave', CURRENT_TIMESTAMP - INTERVAL '1 day'),
('TXN_SEED_005', 'ACC_005', 5000.00, 'DEPOSIT', 'COMPLETED', 'Initial deposit for Merchant', CURRENT_TIMESTAMP - INTERVAL '1 day');

-- Insert corresponding balance history
INSERT INTO balance_history (account_id, transaction_id, balance_before, balance_after, change_amount, operation_type) VALUES
('ACC_001', 'TXN_SEED_001', 0.00, 1000.00, 1000.00, 'CREDIT'),
('ACC_002', 'TXN_SEED_002', 0.00, 1500.00, 1500.00, 'CREDIT'),
('ACC_003', 'TXN_SEED_003', 0.00, 2000.00, 2000.00, 'CREDIT'),
('ACC_004', 'TXN_SEED_004', 0.00, 750.00, 750.00, 'CREDIT'),
('ACC_005', 'TXN_SEED_005', 0.00, 5000.00, 5000.00, 'CREDIT');
