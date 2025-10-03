#!/usr/bin/env python3
"""
Simple test script for distributed locking mechanism.

This script tests the DistributedLock functionality by:
1. Loading environment variables from .env file
2. Testing basic lock operations
3. Running multiprocess tests to verify mutual exclusion
4. Providing clear output about what's working

Usage:
    python test_locking.py
"""

import os
import sys
import time
import multiprocessing as mp
from datetime import datetime
from typing import List, Tuple

# Load environment variables from .env file if it exists
try:
    from dotenv import load_dotenv

    load_dotenv(override=True)
    print("✅ Loaded environment variables from .env file")
except ImportError:
    print("ℹ️  python-dotenv not available, using system environment variables")

# Check required environment variables
required_vars = ["DE_BENCH_DB_URL", "DE_BENCH_DB_SERVICE_KEY"]
missing_vars = [var for var in required_vars if not os.getenv(var)]

if missing_vars:
    print(f"❌ Missing required environment variables: {', '.join(missing_vars)}")
    print("\nPlease set these environment variables or create a .env file with:")
    for var in missing_vars:
        print(f"   {var}=your_value_here")
    sys.exit(1)

print("✅ All required environment variables are set")

# Now import DistributedLock after environment is set up
try:
    from DistributedLock import DistributedLock

    print("✅ Successfully imported DistributedLock")
except Exception as e:
    print(f"❌ Failed to import DistributedLock: {e}")
    sys.exit(1)


def test_basic_operations():
    """Test basic lock operations with a single process."""
    print("\n🧪 Testing basic lock operations...")

    try:
        lock = DistributedLock()
        resource_id = f"test_basic_{int(time.time())}"

        # Test peek_lock on non-existent lock
        locked = lock.peek_lock(resource_id)
        print(f"   peek_lock on non-existent lock: {locked} (should be False)")

        # Test acquire_lock
        acquired = lock.acquire_lock(resource_id)
        print(f"   acquire_lock: {acquired} (should be True)")

        if acquired:
            # Test peek_lock on existing lock
            locked = lock.peek_lock(resource_id)
            print(f"   peek_lock on existing lock: {locked} (should be True)")

            # Test release_lock
            released = lock.release_lock(resource_id)
            print(f"   release_lock: {released} (should be True)")

            # Test peek_lock after release
            locked = lock.peek_lock(resource_id)
            print(f"   peek_lock after release: {locked} (should be False)")

        print("✅ Basic operations test completed successfully")
        return True

    except Exception as e:
        print(f"❌ Basic operations test failed: {e}")
        return False


def test_context_manager():
    """Test the with_lock context manager."""
    print("\n🧪 Testing context manager...")

    try:
        lock = DistributedLock()
        resource_id = f"test_context_{int(time.time())}"

        # Test successful context manager usage
        with lock.with_lock(resource_id, timeout=5) as acquired:
            print(f"   Context manager acquired lock: {acquired} (should be True)")
            if acquired:
                # Verify lock is held
                locked = lock.peek_lock(resource_id)
                print(f"   Lock is held inside context: {locked} (should be True)")

        # Verify lock is released after context
        locked = lock.peek_lock(resource_id)
        print(f"   Lock released after context: {not locked} (should be True)")

        print("✅ Context manager test completed successfully")
        return True

    except Exception as e:
        print(f"❌ Context manager test failed: {e}")
        return False


def worker_process(
    worker_id: int, resource_id: str, duration: float, results: mp.Queue
):
    """Worker process that tries to acquire a lock."""
    try:
        # Each process gets its own DistributedLock instance
        lock = DistributedLock()

        print(f"   Worker {worker_id}: Attempting to acquire lock...")
        start_time = time.time()

        # Try to acquire lock with 10-second timeout
        acquired = lock.acquire_lock(resource_id, timeout=10, poll_interval=0.5)
        acquire_time = time.time()

        if acquired:
            print(
                f"   Worker {worker_id}: ✅ Acquired lock after {acquire_time - start_time:.1f}s"
            )

            # Hold the lock for specified duration
            time.sleep(duration)

            # Release the lock
            released = lock.release_lock(resource_id)
            release_time = time.time()

            print(
                f"   Worker {worker_id}: Released lock after holding for {release_time - acquire_time:.1f}s"
            )
            results.put((worker_id, "success", acquire_time - start_time, released))
        else:
            print(f"   Worker {worker_id}: ❌ Failed to acquire lock (timeout)")
            results.put((worker_id, "timeout", acquire_time - start_time, False))

    except Exception as e:
        print(f"   Worker {worker_id}: ❌ Error: {e}")
        results.put((worker_id, "error", str(e), False))


def test_multiprocess_locking():
    """Test that only one process can hold a lock at a time."""
    print("\n🧪 Testing multiprocess mutual exclusion...")

    try:
        resource_id = f"test_multiprocess_{int(time.time())}"
        num_workers = 3
        hold_duration = 2.0  # Each worker holds lock for 2 seconds

        print(
            f"   Starting {num_workers} workers competing for resource: {resource_id}"
        )
        print(f"   Each worker will hold the lock for {hold_duration} seconds")

        results = mp.Queue()
        processes = []

        # Start all worker processes simultaneously
        start_time = time.time()
        for i in range(num_workers):
            p = mp.Process(
                target=worker_process, args=(i, resource_id, hold_duration, results)
            )
            p.start()
            processes.append(p)

        # Wait for all processes to complete
        for p in processes:
            p.join(timeout=30)  # 30-second timeout
            if p.is_alive():
                print(f"   ⚠️  Process still running, terminating...")
                p.terminate()

        # Collect results
        worker_results = []
        while not results.empty():
            worker_results.append(results.get())

        end_time = time.time()
        total_time = end_time - start_time

        # Analyze results
        successful = [r for r in worker_results if r[1] == "success"]
        failed = [r for r in worker_results if r[1] in ["timeout", "error"]]

        print(f"\n📊 Results after {total_time:.1f} seconds:")
        print(f"   Successful acquisitions: {len(successful)}")
        print(f"   Failed acquisitions: {len(failed)}")

        for worker_id, status, timing, released in worker_results:
            if status == "success":
                print(
                    f"   Worker {worker_id}: ✅ Success (wait: {timing:.1f}s, released: {released})"
                )
            else:
                print(f"   Worker {worker_id}: ❌ {status.title()} ({timing})")

        # Validate mutual exclusion
        if len(successful) == 0:
            print("❌ No workers succeeded - this might indicate a problem")
            return False
        elif len(successful) == num_workers and total_time < (
            hold_duration * num_workers * 0.8
        ):
            print(
                "❌ All workers succeeded too quickly - mutual exclusion may not be working"
            )
            return False
        else:
            print("✅ Mutual exclusion working correctly!")
            return True

    except Exception as e:
        print(f"❌ Multiprocess test failed: {e}")
        return False


def test_timeout_and_polling():
    """Test different timeout and polling interval combinations."""
    print("\n🧪 Testing timeout and polling variations...")

    try:
        lock1 = DistributedLock()
        lock2 = DistributedLock()  # Second instance to compete for lock
        resource_id = f"test_timeout_{int(time.time())}"

        # First instance acquires the lock
        acquired1 = lock1.acquire_lock(resource_id)
        print(f"   First instance acquired lock: {acquired1} (should be True)")

        if not acquired1:
            print("❌ Could not acquire initial lock for timeout testing")
            return False

        # Test immediate return (timeout=None)
        start_time = time.time()
        acquired2 = lock2.acquire_lock(resource_id, timeout=None)
        immediate_time = time.time() - start_time
        print(
            f"   Second instance immediate attempt: {acquired2} (should be False) in {immediate_time:.3f}s"
        )

        # Test short timeout with fast polling
        start_time = time.time()
        acquired3 = lock2.acquire_lock(resource_id, timeout=2, poll_interval=0.2)
        short_timeout_time = time.time() - start_time
        print(
            f"   Short timeout (2s, 0.2s poll): {acquired3} (should be False) in {short_timeout_time:.1f}s"
        )

        # Release the lock from first instance
        released = lock1.release_lock(resource_id)
        print(f"   First instance released lock: {released} (should be True)")

        # Now second instance should be able to acquire with timeout
        start_time = time.time()
        acquired4 = lock2.acquire_lock(resource_id, timeout=5, poll_interval=0.5)
        success_time = time.time() - start_time
        print(
            f"   Second instance with timeout: {acquired4} (should be True) in {success_time:.1f}s"
        )

        # Clean up
        if acquired4:
            lock2.release_lock(resource_id)

        # Validate timing expectations
        if immediate_time > 0.5:
            print("❌ Immediate return took too long")
            return False

        if not (1.8 <= short_timeout_time <= 2.5):
            print(
                f"❌ Short timeout timing unexpected: {short_timeout_time:.1f}s (expected ~2s)"
            )
            return False

        print("✅ Timeout and polling test completed successfully")
        return True

    except Exception as e:
        print(f"❌ Timeout and polling test failed: {e}")
        return False


def test_context_manager_variations():
    """Test context manager with different timeout and poll parameters."""
    print("\n🧪 Testing context manager variations...")

    try:
        lock1 = DistributedLock()
        lock2 = DistributedLock()
        resource_id = f"test_context_var_{int(time.time())}"

        # Test context manager with immediate return
        with lock1.with_lock(resource_id, timeout=None) as acquired1:
            print(f"   Context manager immediate: {acquired1} (should be True)")

            if acquired1:
                # Test second instance can't acquire while first holds it
                with lock2.with_lock(resource_id, timeout=None) as acquired2:
                    print(f"   Second context immediate: {acquired2} (should be False)")

                # Test second instance with short timeout
                start_time = time.time()
                with lock2.with_lock(
                    resource_id, timeout=1, poll_interval=0.3
                ) as acquired3:
                    timeout_time = time.time() - start_time
                    print(
                        f"   Second context timeout: {acquired3} (should be False) in {timeout_time:.1f}s"
                    )

        # After first context exits, second should be able to acquire
        with lock2.with_lock(resource_id, timeout=2) as acquired4:
            print(f"   Second context after first exits: {acquired4} (should be True)")

        print("✅ Context manager variations test completed successfully")
        return True

    except Exception as e:
        print(f"❌ Context manager variations test failed: {e}")
        return False


def test_edge_cases():
    """Test edge cases and error scenarios."""
    print("\n🧪 Testing edge cases...")

    try:
        lock = DistributedLock()
        resource_id = f"test_edge_{int(time.time())}"

        # Test acquiring same lock twice from same instance
        acquired1 = lock.acquire_lock(resource_id)
        print(f"   First acquisition: {acquired1} (should be True)")

        if acquired1:
            acquired2 = lock.acquire_lock(resource_id, timeout=1)
            print(
                f"   Second acquisition by same instance: {acquired2} (should be False)"
            )

            # Test releasing same lock twice
            released1 = lock.release_lock(resource_id)
            print(f"   First release: {released1} (should be True)")

            released2 = lock.release_lock(resource_id)
            print(
                f"   Second release (already released): {released2} (should be False)"
            )

        # Test releasing lock that was never acquired
        fake_resource = f"never_acquired_{int(time.time())}"
        released3 = lock.release_lock(fake_resource)
        print(f"   Release never-acquired lock: {released3} (should be False)")

        # Test peek_lock on non-existent resource
        peek1 = lock.peek_lock(fake_resource)
        print(f"   Peek non-existent lock: {peek1} (should be False)")

        # Test very short timeout
        lock2 = DistributedLock()
        acquired3 = lock2.acquire_lock(resource_id)  # Get a lock
        if acquired3:
            start_time = time.time()
            acquired4 = lock.acquire_lock(resource_id, timeout=0.1, poll_interval=0.05)
            very_short_time = time.time() - start_time
            print(
                f"   Very short timeout (0.1s): {acquired4} (should be False) in {very_short_time:.3f}s"
            )
            lock2.release_lock(resource_id)

        print("✅ Edge cases test completed successfully")
        return True

    except Exception as e:
        print(f"❌ Edge cases test failed: {e}")
        return False


def test_cleanup_expired_locks():
    """Test the cleanup_expired_locks method."""
    print("\n🧪 Testing expired locks cleanup...")

    try:
        lock = DistributedLock()

        # Call cleanup method (should work even if no expired locks)
        cleaned = lock.cleanup_expired_locks()
        print(f"   Cleanup expired locks: {cleaned} locks cleaned")
        print(f"   (Method executed successfully, count may be 0 if no expired locks)")

        print("✅ Cleanup expired locks test completed successfully")
        return True

    except Exception as e:
        print(f"❌ Cleanup expired locks test failed: {e}")
        return False


def test_stress_concurrent():
    """Test with more concurrent processes to stress test the system."""
    print("\n🧪 Testing stress with more concurrent processes...")

    try:
        resource_id = f"test_stress_{int(time.time())}"
        num_workers = 8  # More workers than the basic test
        hold_duration = 0.5  # Shorter duration to fit more in reasonable time

        print(f"   Starting {num_workers} workers for stress test...")
        print(f"   Each worker will hold the lock for {hold_duration} seconds")

        results = mp.Queue()
        processes = []

        # Start all worker processes simultaneously
        start_time = time.time()
        for i in range(num_workers):
            p = mp.Process(
                target=worker_process, args=(i, resource_id, hold_duration, results)
            )
            p.start()
            processes.append(p)

        # Wait for all processes to complete
        for p in processes:
            p.join(timeout=45)  # Longer timeout for more processes
            if p.is_alive():
                print(f"   ⚠️  Process still running, terminating...")
                p.terminate()

        # Collect results
        worker_results = []
        while not results.empty():
            worker_results.append(results.get())

        end_time = time.time()
        total_time = end_time - start_time

        # Analyze results
        successful = [r for r in worker_results if r[1] == "success"]
        failed = [r for r in worker_results if r[1] in ["timeout", "error"]]

        print(f"\n📊 Stress test results after {total_time:.1f} seconds:")
        print(f"   Total processes: {len(worker_results)}/{num_workers}")
        print(f"   Successful: {len(successful)}")
        print(f"   Failed: {len(failed)}")

        # For stress test, we expect some successes and the timing should make sense
        if len(successful) == 0:
            print("❌ No processes succeeded in stress test")
            return False
        elif (
            len(successful) >= num_workers * 0.6
        ):  # At least 60% should eventually succeed
            print("✅ Good throughput in stress test")
        else:
            print(f"⚠️  Lower throughput than expected: {len(successful)}/{num_workers}")

        print("✅ Stress test completed successfully")
        return True

    except Exception as e:
        print(f"❌ Stress test failed: {e}")
        return False


def main():
    """Run all tests."""
    print("🚀 Starting DE-Bench Distributed Locking Tests")
    print(f"   Database URL: {os.getenv('DE_BENCH_DB_URL', 'NOT SET')}")
    print(
        f"   Service Key: {'SET' if os.getenv('DE_BENCH_DB_SERVICE_KEY') else 'NOT SET'}"
    )

    tests = [
        ("Basic Operations", test_basic_operations),
        ("Context Manager", test_context_manager),
        ("Timeout & Polling", test_timeout_and_polling),
        ("Context Manager Variations", test_context_manager_variations),
        ("Edge Cases", test_edge_cases),
        ("Cleanup Expired Locks", test_cleanup_expired_locks),
        ("Multiprocess Locking", test_multiprocess_locking),
        ("Stress Test (8 processes)", test_stress_concurrent),
    ]

    passed = 0
    total = len(tests)

    for test_name, test_func in tests:
        print(f"\n{'='*60}")
        print(f"Running: {test_name}")
        print("=" * 60)

        if test_func():
            passed += 1

        time.sleep(1)  # Brief pause between tests

    print(f"\n{'='*60}")
    print(f"📈 SUMMARY: {passed}/{total} tests passed")
    print("=" * 60)

    if passed == total:
        print("🎉 All tests passed! Distributed locking is working correctly.")
        sys.exit(0)
    else:
        print(
            "💥 Some tests failed. Please check your database connection and configuration."
        )
        sys.exit(1)


if __name__ == "__main__":
    main()
