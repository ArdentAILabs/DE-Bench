-- Create distributed_locks table for DE-Bench resource locking
CREATE TABLE distributed_locks (
    resource_id TEXT PRIMARY KEY,
    holder_id TEXT NOT NULL,
    acquired_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    expires_at TIMESTAMPTZ NOT NULL,
    metadata JSONB DEFAULT '{}'::jsonb
);

-- Create index on expires_at for efficient cleanup queries
CREATE INDEX idx_distributed_locks_expires_at ON distributed_locks(expires_at);

-- Create index on holder_id for efficient queries by lock holder
CREATE INDEX idx_distributed_locks_holder_id ON distributed_locks(holder_id);

-- Function to peek at lock status (returns true if lock exists and is not expired)
CREATE OR REPLACE FUNCTION peek_lock_status(p_resource_id TEXT)
RETURNS BOOLEAN
LANGUAGE plpgsql
AS $$
BEGIN
    -- Check if lock exists and is not expired
    RETURN EXISTS (
        SELECT 1 FROM distributed_locks 
        WHERE resource_id = p_resource_id 
        AND expires_at > NOW()
    );
END;
$$;

-- Function to atomically try to acquire a lock
CREATE OR REPLACE FUNCTION try_acquire_lock(
    p_resource_id TEXT, 
    p_holder_id TEXT, 
    p_expires_at TIMESTAMPTZ
)
RETURNS BOOLEAN
LANGUAGE plpgsql
AS $$
DECLARE
    lock_acquired BOOLEAN := FALSE;
BEGIN
    -- First, clean up any expired locks for this resource
    DELETE FROM distributed_locks 
    WHERE resource_id = p_resource_id 
    AND expires_at <= NOW();
    
    -- Try to insert the new lock
    BEGIN
        INSERT INTO distributed_locks (resource_id, holder_id, expires_at)
        VALUES (p_resource_id, p_holder_id, p_expires_at);
        lock_acquired := TRUE;
    EXCEPTION 
        WHEN unique_violation THEN
            -- Lock already exists and is not expired
            lock_acquired := FALSE;
    END;
    
    RETURN lock_acquired;
END;
$$;

-- Function to release a lock (only if held by the specified holder)
CREATE OR REPLACE FUNCTION release_lock(
    p_resource_id TEXT,
    p_holder_id TEXT
)
RETURNS BOOLEAN
LANGUAGE plpgsql
AS $$
DECLARE
    rows_deleted INTEGER;
BEGIN
    -- Delete the lock only if it's held by the specified holder
    DELETE FROM distributed_locks 
    WHERE resource_id = p_resource_id 
    AND holder_id = p_holder_id;
    
    GET DIAGNOSTICS rows_deleted = ROW_COUNT;
    
    -- Return true if we successfully deleted a lock
    RETURN rows_deleted > 0;
END;
$$;

-- Function to clean up expired locks (useful for maintenance)
CREATE OR REPLACE FUNCTION cleanup_expired_locks()
RETURNS INTEGER
LANGUAGE plpgsql
AS $$
DECLARE
    rows_deleted INTEGER;
BEGIN
    DELETE FROM distributed_locks WHERE expires_at <= NOW();
    GET DIAGNOSTICS rows_deleted = ROW_COUNT;
    RETURN rows_deleted;
END;
$$;