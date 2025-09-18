#!/usr/bin/env python3
"""
PostgreSQL Database Maintenance Script

This script provides utilities for managing PostgreSQL databases including
listing and clearing user databases (excluding system databases).

Requirements:
    - psycopg2-binary (already included in requirements.txt)
    - python-dotenv (already included in requirements.txt)

Usage:
    1. Create a .env file in the same directory with:
       POSTGRES_HOSTNAME=localhost
       POSTGRES_PORT=5432
       POSTGRES_USERNAME=your_username
       POSTGRES_PASSWORD=your_password

    2. Or set environment variables:
       export POSTGRES_HOSTNAME=localhost
       export POSTGRES_PORT=5432
       export POSTGRES_USERNAME=your_username
       export POSTGRES_PASSWORD=your_password

    3. Run the script with subcommands:
       # List all user databases
       python postgres_maintenance.py --list-dbs

       # Clear all user databases (with confirmation)
       python postgres_maintenance.py --clear-dbs

    Or run without .env file or environment variables and enter details when prompted.
"""

import argparse
import psycopg2
import sys
from typing import List, Optional
from dotenv import load_dotenv


def connect_to_postgres(
    hostname: str, port: str, username: str, password: str
) -> Optional[psycopg2.extensions.connection]:
    """
    Connect to PostgreSQL database.

    Args:
        hostname: PostgreSQL server hostname
        port: PostgreSQL server port
        username: Database username
        password: Database password

    Returns:
        Database connection object or None if connection fails
    """
    try:
        connection = psycopg2.connect(
            host=hostname,
            port=port,
            user=username,
            password=password,
            database="postgres",  # Connect to default postgres database
        )
        print(f"Successfully connected to PostgreSQL server at {hostname}:{port}")
        return connection
    except psycopg2.Error as e:
        print(f"Error connecting to PostgreSQL: {e}")
        return None


def list_databases(connection: psycopg2.extensions.connection) -> List[str]:
    """
    List all databases except system databases.

    Args:
        connection: PostgreSQL database connection

    Returns:
        List of database names
    """
    try:
        cursor = connection.cursor()

        # Query to get all databases except 'postgres', 'template0', and 'template1'
        query = """
        SELECT datname 
        FROM pg_database 
        WHERE datname NOT IN ('postgres', 'template0', 'template1')
        AND datistemplate = false
        ORDER BY datname;
        """

        cursor.execute(query)
        databases = [row[0] for row in cursor.fetchall()]
        cursor.close()

        return databases

    except psycopg2.Error as e:
        print(f"Error querying databases: {e}")
        return []


def drop_database(connection: psycopg2.extensions.connection, db_name: str) -> bool:
    """
    Drop a database.

    Args:
        connection: PostgreSQL database connection
        db_name: Name of database to drop

    Returns:
        True if successful, False otherwise
    """
    try:
        # Ensure we're not in a transaction and set autocommit
        connection.rollback()  # End any existing transaction
        connection.autocommit = True
        cursor = connection.cursor()

        # Terminate all connections to the database before dropping
        terminate_query = """
        SELECT pg_terminate_backend(pid)
        FROM pg_stat_activity
        WHERE datname = %s AND pid <> pg_backend_pid();
        """
        cursor.execute(terminate_query, (db_name,))

        # Small delay to ensure connections are terminated
        import time

        time.sleep(0.1)

        # Drop the database using safe identifier quoting
        drop_query = f'DROP DATABASE "{db_name}";'
        cursor.execute(drop_query)
        cursor.close()
        print(f"✓ Successfully dropped database: {db_name}")
        return True

    except psycopg2.Error as e:
        print(f"✗ Error dropping database {db_name}: {e}")
        # Try to rollback and reset connection state
        try:
            connection.rollback()
            connection.autocommit = True
        except:
            pass
        return False


def get_connection_params():
    """
    Get database connection parameters from environment or user input.

    Returns:
        Tuple of (hostname, port, username, password)
    """
    # Load environment variables from .env file
    load_dotenv()

    # Get connection parameters from environment variables or prompt user
    import os

    postgres_hostname = os.getenv("POSTGRES_HOSTNAME")
    postgres_port = os.getenv("POSTGRES_PORT", "5432")
    postgres_username = os.getenv("POSTGRES_USERNAME")
    postgres_password = os.getenv("POSTGRES_PASSWORD")

    # If environment variables are not set, prompt user for input
    if not postgres_hostname:
        postgres_hostname = input("Enter PostgreSQL hostname: ")
    if not postgres_username:
        postgres_username = input("Enter PostgreSQL username: ")
    if not postgres_password:
        postgres_password = input("Enter PostgreSQL password: ")

    return postgres_hostname, postgres_port, postgres_username, postgres_password


def cmd_list_dbs():
    """
    Command to list all user databases.
    """
    print("🔍 Listing PostgreSQL databases...")

    # Get connection parameters
    hostname, port, username, password = get_connection_params()

    print(f"\nConnecting to PostgreSQL server...")
    print(f"Hostname: {hostname}")
    print(f"Port: {port}")
    print(f"Username: {username}")

    # Connect to PostgreSQL
    connection = connect_to_postgres(hostname, port, username, password)

    if connection is None:
        print("Failed to connect to PostgreSQL. Exiting.")
        sys.exit(1)

    try:
        # List databases
        print("\n" + "=" * 50)
        print("USER DATABASES (excluding system databases):")
        print("=" * 50)

        databases = list_databases(connection)

        if databases:
            for i, db_name in enumerate(databases, 1):
                print(f"{i:2d}. {db_name}")
            print(f"\nTotal databases found: {len(databases)}")
        else:
            print("No user databases found.")

    finally:
        # Close the connection
        connection.close()
        print("\nConnection closed.")


def cmd_clear_dbs():
    """
    Command to clear all user databases with confirmation.
    """
    print("🗑️  Clearing PostgreSQL databases...")

    # Get connection parameters
    hostname, port, username, password = get_connection_params()

    print(f"\nConnecting to PostgreSQL server...")
    print(f"Hostname: {hostname}")
    print(f"Port: {port}")
    print(f"Username: {username}")

    # Connect to PostgreSQL
    connection = connect_to_postgres(hostname, port, username, password)

    if connection is None:
        print("Failed to connect to PostgreSQL. Exiting.")
        sys.exit(1)

    try:
        # First, list databases to show what will be deleted
        print("\n" + "=" * 50)
        print("DATABASES TO BE DELETED:")
        print("=" * 50)

        databases = list_databases(connection)

        if not databases:
            print("No user databases found to delete.")
            return

        for i, db_name in enumerate(databases, 1):
            print(f"{i:2d}. {db_name}")

        print(f"\nTotal databases to delete: {len(databases)}")

        # Show warning and ask for confirmation
        print("\n" + "⚠️ " * 20)
        print("WARNING: This will PERMANENTLY DELETE all the databases listed above!")
        print("This action CANNOT be undone!")
        print("⚠️ " * 20)

        confirmation = input(
            f"\nTo confirm deletion of {len(databases)} database(s), type 'YES' (all caps): "
        )

        if confirmation != "YES":
            print("❌ Operation cancelled. Databases were NOT deleted.")
            return

        # Proceed with deletion
        print(f"\n🗑️  Proceeding to delete {len(databases)} database(s)...")

        successful_deletions = 0
        failed_deletions = 0

        for db_name in databases:
            if drop_database(connection, db_name):
                successful_deletions += 1
            else:
                failed_deletions += 1

        # Summary
        print("\n" + "=" * 50)
        print("DELETION SUMMARY:")
        print("=" * 50)
        print(f"✓ Successfully deleted: {successful_deletions} database(s)")
        if failed_deletions > 0:
            print(f"✗ Failed to delete: {failed_deletions} database(s)")
        print(f"Total processed: {len(databases)} database(s)")

    finally:
        # Close the connection
        connection.close()
        print("\nConnection closed.")


def main():
    """
    Main function to parse arguments and execute commands.
    """
    parser = argparse.ArgumentParser(
        description="PostgreSQL Database Maintenance Tool",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  %(prog)s --list-dbs     List all user databases
  %(prog)s --clear-dbs    Clear all user databases (with confirmation)
        """,
    )

    # Create mutually exclusive group for subcommands
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument(
        "--list-dbs",
        action="store_true",
        help="List all user databases (excluding system databases)",
    )
    group.add_argument(
        "--clear-dbs",
        action="store_true",
        help="Clear all user databases with confirmation (DANGEROUS!)",
    )

    args = parser.parse_args()

    # Execute the appropriate command
    if args.list_dbs:
        cmd_list_dbs()
    elif args.clear_dbs:
        cmd_clear_dbs()


if __name__ == "__main__":
    main()
