import os
import uuid
import logging

try:
    from supabase import create_client, Client
except ImportError:
    raise ImportError(
        "supabase package is required. Install with: pip install supabase"
    )


class DEBenchDB:
    def __init__(self):
        """Initialize DEBenchDB with Supabase client and unique instance ID."""
        # Generate unique instance ID for this DEBenchDB instance
        self.instance_id = str(uuid.uuid4())

        # Initialize Supabase client
        url = os.getenv("DE_BENCH_DB_URL")
        key = os.getenv("DE_BENCH_DB_SERVICE_KEY")

        if not url or not key:
            raise ValueError(
                "Missing required environment variables: DE_BENCH_DB_URL and "
                "DE_BENCH_DB_SERVICE_KEY"
            )

        self.supabase: Client = create_client(url, key)
        self.logger = logging.getLogger(__name__)

        # Default lock timeout (45 minutes)
        self.default_timeout_minutes = 45


# Singleton instance of the DEBenchDB class
de_bench_db = DEBenchDB()
