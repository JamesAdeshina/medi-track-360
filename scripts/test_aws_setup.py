#!/usr/bin/env python
"""
Test script to verify AWS credentials are properly loaded
"""

import os
import sys

sys.path.append('.')

from config.aws_config import aws_config


def test_environment_variables():
    """Test that environment variables are set"""
    print("Testing environment variables...")

    required_vars = ['AWS_ACCESS_KEY_ID', 'AWS_SECRET_ACCESS_KEY']

    for var in required_vars:
        value = os.getenv(var)
        if value:
            # Show first and last few chars for verification (not full key)
            masked = f"{value[:4]}...{value[-4:]}" if len(value) > 8 else "***"
            print(f"  ✓ {var}: {masked}")
        else:
            print(f"  ✗ {var}: NOT SET")

    return all(os.getenv(var) for var in required_vars)


def main():
    print("=" * 60)
    print("AWS Configuration Test")
    print("=" * 60)

    # Test 1: Environment variables
    env_ok = test_environment_variables()

    if not env_ok:
        print("\nERROR: Environment variables not set properly.")
        print("\nPlease ensure:")
        print("1. You have a .env file in the docker/ directory")
        print("2. It contains AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY")
        print("3. Docker Compose is loading the .env file")
        return False

    # Test 2: AWS connection
    print("\nTesting AWS connection...")
    if aws_config.test_connection():
        print("✓ AWS connection successful")
        return True
    else:
        print("✗ AWS connection failed")
        return False


if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)