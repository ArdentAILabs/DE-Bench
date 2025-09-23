# Distributed Locking Mechanism

This directory contains a comprehensive distributed locking system for DE-Bench using Supabase PostgreSQL backend.

## Architecture

### `DEBenchDB.py`
- **Purpose**: Database connection management and singleton instance
- **Responsibilities**:
  - Supabase client initialization
  - Environment variable validation  
  - Unique instance ID generation
  - Database connection lifecycle
- **Singleton**: Exports `de_bench_db` instance for shared use

### `DistributedLock.py`
- **Purpose**: Distributed locking interface and operations
- **Responsibilities**:
  - All locking methods (`peek_lock`, `acquire_lock`, `release_lock`, `with_lock`)
  - Lock expiry and cleanup functionality
  - Timeout and polling logic
- **Dependencies**: Uses `de_bench_db` singleton for all database operations

### `test_locking.py`
- **Purpose**: Comprehensive test suite validating all locking functionality
- **Coverage**: 8 comprehensive test scenarios including multiprocess stress testing

## Usage

### Basic Lock Operations
```python
from DistributedLock import DistributedLock

lock = DistributedLock()

# Check if resource is locked
if not lock.peek_lock("my_resource"):
    # Try to acquire lock
    if lock.acquire_lock("my_resource", timeout=30):
        try:
            # Do work with exclusive access
            pass
        finally:
            lock.release_lock("my_resource")
```

### Context Manager (Recommended)
```python
from DistributedLock import DistributedLock

lock = DistributedLock()

# Automatic lock management
with lock.with_lock("my_resource", timeout=30) as acquired:
    if acquired:
        # Do work with exclusive access - lock automatically released
        pass
```

### Direct Database Access
```python
from DEBenchDB import de_bench_db

# Access database connection directly if needed
print(f"Instance ID: {de_bench_db.instance_id}")
result = de_bench_db.supabase.rpc("custom_function").execute()
```

## Environment Setup

The system requires these environment variables:
```bash
export DE_BENCH_DB_URL="https://your-project.supabase.co"
export DE_BENCH_DB_SERVICE_KEY="your-service-key"
```

Or create a `.env` file:
```
DE_BENCH_DB_URL=https://your-project.supabase.co  
DE_BENCH_DB_SERVICE_KEY=your-service-key
```

## Testing

Run the comprehensive test suite:
```bash
cd Environment/DEBenchDatabase
python test_locking.py
```

The test suite includes:
- ✅ **Basic Operations**: All core locking methods
- ✅ **Context Manager**: Automatic cleanup testing
- ✅ **Timeout & Polling**: Parameter validation with timing
- ✅ **Edge Cases**: Error conditions and invalid usage
- ✅ **Cleanup Method**: Expired lock maintenance
- ✅ **Multiprocess Mutual Exclusion**: Real concurrency validation
- ✅ **Stress Testing**: 8-process high-concurrency testing

## Database Schema

The system uses PostgreSQL functions for atomic operations:
- `peek_lock_status(resource_id)` - Check lock status
- `try_acquire_lock(resource_id, holder_id, expires_at)` - Atomic acquisition
- `release_lock(resource_id, holder_id)` - Safe release with ownership validation
- `cleanup_expired_locks()` - Maintenance function

Lock expiry is set to 45 minutes by default to handle crashed processes.

## Architecture Benefits

This design provides:
- **Separation of Concerns**: Database management vs. locking logic
- **Singleton Pattern**: Shared database connection across all locks
- **Clean Interface**: Simple locking API independent of database details
- **Testability**: Easy to test locking logic with shared database instance
- **Scalability**: Single database connection supports multiple lock instances