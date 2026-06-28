"""
Twilio Configuration Helper
===========================
Run this script after filling in your .env file to automatically
configure your Twilio phone number to point to your AI receptionist.

Usage:
    python configure_twilio.py --server-url https://your-server-url.com
"""

import argparse
import os
from dotenv import load_dotenv

load_dotenv()

def configure_twilio(server_url: str):
    """Configure Twilio phone number webhooks to point to the AI receptionist."""
    try:
        from twilio.rest import Client
    except ImportError:
        print("❌ Twilio not installed. Run: pip install twilio")
        return

    account_sid = os.getenv("TWILIO_ACCOUNT_SID")
    auth_token  = os.getenv("TWILIO_AUTH_TOKEN")
    phone_number = os.getenv("TWILIO_PHONE_NUMBER")

    if not all([account_sid, auth_token, phone_number]):
        print("❌ Missing Twilio credentials in .env file.")
        print("   Please fill in TWILIO_ACCOUNT_SID, TWILIO_AUTH_TOKEN, and TWILIO_PHONE_NUMBER")
        return

    # Normalize server URL
    server_url = server_url.rstrip("/")

    client = Client(account_sid, auth_token)

    # Find the phone number SID
    numbers = client.incoming_phone_numbers.list(phone_number=phone_number)
    if not numbers:
        print(f"❌ Phone number {phone_number} not found in your Twilio account.")
        return

    number = numbers[0]

    # Update webhooks
    number.update(
        voice_url=f"{server_url}/incoming-call",
        voice_method="POST",
        status_callback=f"{server_url}/call-status",
        status_callback_method="POST",
    )

    print(f"✅ Twilio phone number {phone_number} configured successfully!")
    print(f"   Incoming call webhook: {server_url}/incoming-call")
    print(f"   Status callback:       {server_url}/call-status")
    print()
    print("📞 Your AI receptionist is ready to answer calls!")
    print(f"   Call {phone_number} to test it.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Configure Twilio for Cascade RV Solar AI Receptionist")
    parser.add_argument(
        "--server-url",
        required=True,
        help="Public URL of your AI receptionist server (e.g., https://abc123.ngrok.io)"
    )
    args = parser.parse_args()
    configure_twilio(args.server_url)
