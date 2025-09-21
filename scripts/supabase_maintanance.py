#!/usr/bin/env python3
"""
Supabase User Maintenance Script

This script provides utilities for managing Supabase test users including
listing and clearing test users that have emails starting with 'test-'.

Requirements:
    - supabase (already included in requirements.txt)
    - python-dotenv (already included in requirements.txt)

Usage:
    1. Create a .env file in the same directory with:
       SUPABASE_URL=your_supabase_url
       SUPABASE_SERVICE_ROLE_KEY=your_service_role_key

    2. Or set environment variables:
       export SUPABASE_URL=your_supabase_url
       export SUPABASE_SERVICE_ROLE_KEY=your_service_role_key

    3. Run the script with subcommands:
       # List all test users
       python supabase_maintanance.py --list-users

       # Clear all test users (with confirmation)
       python supabase_maintanance.py --clear-users

    Or run without .env file or environment variables and enter details when prompted.
"""

import argparse
import sys
import os
from typing import List, Optional, Dict, Any
from dotenv import load_dotenv
from supabase import create_client, Client


def connect_to_supabase(url: str, service_role_key: str) -> Optional[Client]:
    """
    Connect to Supabase using service role key.

    Args:
        url: Supabase project URL
        service_role_key: Supabase service role key

    Returns:
        Supabase client object or None if connection fails
    """
    try:
        supabase_client = create_client(url, service_role_key)
        # Test the connection by trying to get auth admin
        test_response = supabase_client.auth.admin.list_users()
        print(f"Successfully connected to Supabase at {url}")
        return supabase_client
    except Exception as e:
        print(f"Error connecting to Supabase: {e}")
        return None


def list_test_users(supabase_client: Client) -> List[Dict[str, Any]]:
    """
    List all test users with emails starting with 'test-' pattern.

    Args:
        supabase_client: Supabase client connection

    Returns:
        List of user dictionaries containing user information
    """
    try:
        # Get all users from Supabase Auth
        print("🔍 Fetching users from Supabase...")
        response = supabase_client.auth.admin.list_users()
        
        # Debug: Print response structure
        print(f"📊 Response type: {type(response)}")
        print(f"📊 Response attributes: {dir(response)}")
        
        # Try different ways to access users
        all_users = []
        if isinstance(response, list):
            # Response is a direct list of users
            all_users = response
            print(f"📊 Found {len(all_users)} users via direct list")
        elif hasattr(response, 'users'):
            all_users = response.users
            print(f"📊 Found {len(all_users)} users via response.users")
        elif hasattr(response, 'data'):
            all_users = response.data
            print(f"📊 Found {len(all_users)} users via response.data")
        else:
            print(f"📊 Response content: {response}")
            # Maybe it's a dict-like object?
            if isinstance(response, dict):
                if 'users' in response:
                    all_users = response['users']
                    print(f"📊 Found {len(all_users)} users via response['users']")
                elif 'data' in response:
                    all_users = response['data']
                    print(f"📊 Found {len(all_users)} users via response['data']")

        print(f"📊 Total users found: {len(all_users)}")
        
        # Show first few users (without sensitive info) for debugging
        if all_users and len(all_users) > 0:
            print("📊 Sample user structure:")
            first_user = all_users[0]
            print(f"   Type: {type(first_user)}")
            print(f"   Attributes: {dir(first_user) if hasattr(first_user, '__dict__') else 'Not an object'}")
            if hasattr(first_user, 'email'):
                print(f"   Sample email: {first_user.email[:10]}...")
            elif isinstance(first_user, dict) and 'email' in first_user:
                print(f"   Sample email: {first_user['email'][:10]}...")

        # Filter users that match the test pattern
        test_users = []
        for user in all_users:
            # Try different ways to get email
            email = None
            if hasattr(user, 'email'):
                email = user.email
            elif isinstance(user, dict) and 'email' in user:
                email = user['email']
            
            if email and email.startswith('test-'):
                # Try different ways to get user data
                user_id = None
                if hasattr(user, 'id'):
                    user_id = user.id
                elif isinstance(user, dict) and 'id' in user:
                    user_id = user['id']
                    
                created_at = 'Unknown'
                if hasattr(user, 'created_at'):
                    created_at = user.created_at
                elif isinstance(user, dict) and 'created_at' in user:
                    created_at = user['created_at']
                    
                email_confirmed_at = 'Unknown'
                if hasattr(user, 'email_confirmed_at'):
                    email_confirmed_at = user.email_confirmed_at
                elif isinstance(user, dict) and 'email_confirmed_at' in user:
                    email_confirmed_at = user['email_confirmed_at']
                
                test_users.append({
                    'id': user_id,
                    'email': email,
                    'created_at': created_at,
                    'email_confirmed_at': email_confirmed_at
                })

        print(f"📊 Test users found: {len(test_users)}")
        return test_users

    except Exception as e:
        print(f"Error querying users: {e}")
        import traceback
        print(f"Full error: {traceback.format_exc()}")
        return []


def delete_user(supabase_client: Client, user_id: str, user_email: str) -> bool:
    """
    Delete a user from Supabase Auth.

    Args:
        supabase_client: Supabase client connection
        user_id: User ID to delete
        user_email: User email (for logging purposes)

    Returns:
        True if successful, False otherwise
    """
    try:
        # Delete the user using admin API
        supabase_client.auth.admin.delete_user(user_id)
        print(f"✓ Successfully deleted user: {user_email} (ID: {user_id})")
        return True

    except Exception as e:
        print(f"✗ Error deleting user {user_email} (ID: {user_id}): {e}")
        return False


def get_connection_params():
    """
    Get Supabase connection parameters from environment or user input.

    Returns:
        Tuple of (url, service_role_key)
    """
    # Load environment variables from .env file
    load_dotenv()

    # Get connection parameters from environment variables or prompt user
    supabase_url = os.getenv("SUPABASE_URL")
    supabase_service_role_key = os.getenv("SUPABASE_SERVICE_ROLE_KEY")

    # If environment variables are not set, prompt user for input
    if not supabase_url:
        supabase_url = input("Enter Supabase URL: ")
    if not supabase_service_role_key:
        supabase_service_role_key = input("Enter Supabase Service Role Key: ")

    return supabase_url, supabase_service_role_key


def cmd_list_users():
    """
    Command to list all test users.
    """
    print("🔍 Listing Supabase test users...")

    # Get connection parameters
    url, service_role_key = get_connection_params()

    print(f"\nConnecting to Supabase...")
    print(f"URL: {url}")

    # Connect to Supabase
    supabase_client = connect_to_supabase(url, service_role_key)

    if supabase_client is None:
        print("Failed to connect to Supabase. Exiting.")
        sys.exit(1)

    try:
        # List test users
        print("\n" + "=" * 60)
        print("TEST USERS (emails starting with 'test-'):")
        print("=" * 60)

        test_users = list_test_users(supabase_client)

        if test_users:
            for i, user in enumerate(test_users, 1):
                print(f"{i:2d}. {user['email']}")
                print(f"    ID: {user['id']}")
                print(f"    Created: {user['created_at']}")
                print(f"    Email Confirmed: {user['email_confirmed_at']}")
                print()
            print(f"Total test users found: {len(test_users)}")
        else:
            print("No test users found.")

    except Exception as e:
        print(f"Error during user listing: {e}")


def cmd_clear_users():
    """
    Command to clear all test users with confirmation.
    """
    print("🗑️  Clearing Supabase test users...")

    # Get connection parameters
    url, service_role_key = get_connection_params()

    print(f"\nConnecting to Supabase...")
    print(f"URL: {url}")

    # Connect to Supabase
    supabase_client = connect_to_supabase(url, service_role_key)

    if supabase_client is None:
        print("Failed to connect to Supabase. Exiting.")
        sys.exit(1)

    try:
        # First, list users to show what will be deleted
        print("\n" + "=" * 60)
        print("TEST USERS TO BE DELETED:")
        print("=" * 60)

        test_users = list_test_users(supabase_client)

        if not test_users:
            print("No test users found to delete.")
            return

        for i, user in enumerate(test_users, 1):
            print(f"{i:2d}. {user['email']} (ID: {user['id']})")

        print(f"\nTotal users to delete: {len(test_users)}")

        # Show warning and ask for confirmation
        print("\n" + "⚠️ " * 20)
        print("WARNING: This will PERMANENTLY DELETE all the test users listed above!")
        print("This action CANNOT be undone!")
        print("These users will be completely removed from Supabase Auth!")
        print("⚠️ " * 20)

        confirmation = input(
            f"\nTo confirm deletion of {len(test_users)} user(s), type 'YES' (all caps): "
        )

        if confirmation != "YES":
            print("❌ Operation cancelled. Users were NOT deleted.")
            return

        # Proceed with deletion
        print(f"\n🗑️  Proceeding to delete {len(test_users)} user(s)...")

        successful_deletions = 0
        failed_deletions = 0

        for user in test_users:
            if delete_user(supabase_client, user['id'], user['email']):
                successful_deletions += 1
            else:
                failed_deletions += 1

        # Summary
        print("\n" + "=" * 50)
        print("DELETION SUMMARY:")
        print("=" * 50)
        print(f"✓ Successfully deleted: {successful_deletions} user(s)")
        if failed_deletions > 0:
            print(f"✗ Failed to delete: {failed_deletions} user(s)")
        print(f"Total processed: {len(test_users)} user(s)")

    except Exception as e:
        print(f"Error during user cleanup: {e}")


def main():
    """
    Main function to parse arguments and execute commands.
    """
    parser = argparse.ArgumentParser(
        description="Supabase Test User Maintenance Tool",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  %(prog)s --list-users     List all test users (emails starting with 'test-')
  %(prog)s --clear-users    Clear all test users (with confirmation)
        """,
    )

    # Create mutually exclusive group for subcommands
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument(
        "--list-users",
        action="store_true",
        help="List all test users with emails starting with 'test-'",
    )
    group.add_argument(
        "--clear-users",
        action="store_true",
        help="Clear all test users with confirmation (DANGEROUS!)",
    )

    args = parser.parse_args()

    # Execute the appropriate command
    if args.list_users:
        cmd_list_users()
    elif args.clear_users:
        cmd_clear_users()


if __name__ == "__main__":
    main()
