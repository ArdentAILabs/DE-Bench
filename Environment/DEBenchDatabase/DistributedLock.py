import time
import logging
from typing import Optional
from contextlib import contextmanager
from datetime import datetime, timedelta

from DEBenchDB import de_bench_db


class DistributedLock:
    """
    Distributed locking mechanism using Supabase PostgreSQL backend.

    This class provides methods for acquiring, releasing, and managing
    distributed locks across multiple processes/instances.

    Uses the de_bench_db singleton for all database operations.
    """

    def __init__(self):
        """Initialize the distributed lock interface."""
        self.logger = logging.getLogger(__name__)

    @property
    def db(self):
        """Access to the de_bench_db singleton."""
        return de_bench_db

    def peek_lock(self, resource_id: str) -> bool:
        """
        Check if a lock exists for the given resource_id and is not expired.

        Args:
            resource_id: Unique identifier for the resource to check

        Returns:
            bool: True if lock exists and is active, False otherwise
        """
        try:
            # Call the PostgreSQL function to peek at lock status
            result = self.db.supabase.rpc(
                "peek_lock_status", {"p_resource_id": resource_id}
            ).execute()
            return result.data if result.data is not None else False

        except Exception as e:
            self.logger.error(f"Error peeking lock for resource {resource_id}: {e}")
            return False

    def acquire_lock(
        self,
        resource_id: str,
        timeout: Optional[int] = None,
        poll_interval: float = 1.0,
    ) -> bool:
        """
        Try to acquire a lock for the given resource_id.

        Args:
            resource_id: Unique identifier for the resource to lock
            timeout: Maximum time in seconds to wait for lock (None = no waiting)
            poll_interval: Time in seconds between retry attempts

        Returns:
            bool: True if lock was acquired, False otherwise
        """
        # Calculate expiration time (45 minutes from now by default)
        expires_at = datetime.utcnow() + timedelta(
            minutes=self.db.default_timeout_minutes
        )
        expires_at_iso = expires_at.isoformat() + "Z"

        try:
            # Try to acquire lock immediately
            result = self.db.supabase.rpc(
                "try_acquire_lock",
                {
                    "p_resource_id": resource_id,
                    "p_holder_id": self.db.instance_id,
                    "p_expires_at": expires_at_iso,
                },
            ).execute()

            if result.data:
                self.logger.info(
                    f"Successfully acquired lock for resource: {resource_id}"
                )
                return True

            # If no timeout specified, return immediately
            if timeout is None:
                self.logger.info(
                    f"Lock for resource {resource_id} is held by another process"
                )
                return False

            # Wait and retry logic
            start_time = time.time()
            while time.time() - start_time < timeout:
                time.sleep(poll_interval)

                # Recalculate expiration time for each attempt
                expires_at = datetime.utcnow() + timedelta(
                    minutes=self.db.default_timeout_minutes
                )
                expires_at_iso = expires_at.isoformat() + "Z"

                try:
                    result = self.db.supabase.rpc(
                        "try_acquire_lock",
                        {
                            "p_resource_id": resource_id,
                            "p_holder_id": self.db.instance_id,
                            "p_expires_at": expires_at_iso,
                        },
                    ).execute()

                    if result.data:
                        self.logger.info(
                            f"Successfully acquired lock for resource: {resource_id} after waiting"
                        )
                        return True

                except Exception as e:
                    self.logger.error(
                        f"Error during retry for resource {resource_id}: {e}"
                    )

            self.logger.info(f"Timeout waiting for lock on resource: {resource_id}")
            return False

        except Exception as e:
            self.logger.error(f"Error acquiring lock for resource {resource_id}: {e}")
            return False

    def release_lock(self, resource_id: str) -> bool:
        """
        Release a lock for the given resource_id if held by this instance.

        Args:
            resource_id: Unique identifier for the resource to unlock

        Returns:
            bool: True if lock was released, False otherwise
        """
        try:
            result = self.db.supabase.rpc(
                "release_lock",
                {"p_resource_id": resource_id, "p_holder_id": self.db.instance_id},
            ).execute()

            success = result.data if result.data is not None else False

            if success:
                self.logger.info(
                    f"Successfully released lock for resource: {resource_id}"
                )
            else:
                self.logger.warning(
                    f"Could not release lock for resource {resource_id} (not held by this instance)"
                )

            return success

        except Exception as e:
            self.logger.error(f"Error releasing lock for resource {resource_id}: {e}")
            return False

    @contextmanager
    def with_lock(
        self,
        resource_id: str,
        timeout: Optional[int] = None,
        poll_interval: float = 1.0,
    ):
        """
        Context manager for automatic lock acquisition and release.

        Args:
            resource_id: Unique identifier for the resource to lock
            timeout: Maximum time in seconds to wait for lock (None = no waiting)
            poll_interval: Time in seconds between retry attempts

        Yields:
            bool: True if lock was acquired successfully

        Example:
            with de_bench_db.with_lock("my_resource", timeout=30) as acquired:
                if acquired:
                    # Do work with locked resource
                    pass
        """
        acquired = self.acquire_lock(resource_id, timeout, poll_interval)
        try:
            yield acquired
        finally:
            if acquired:
                self.release_lock(resource_id)

    def cleanup_expired_locks(self) -> int:
        """
        Clean up expired locks from the database.

        Returns:
            int: Number of expired locks cleaned up
        """
        try:
            result = self.db.supabase.rpc("cleanup_expired_locks").execute()
            count = result.data if result.data is not None else 0
            self.logger.info(f"Cleaned up {count} expired locks")
            return count

        except Exception as e:
            self.logger.error(f"Error cleaning up expired locks: {e}")
            return 0


# Singleton instance of the DistributedLock class
distributed_lock = DistributedLock()
