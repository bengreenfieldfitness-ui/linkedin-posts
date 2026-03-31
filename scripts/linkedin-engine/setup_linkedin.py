"""
LinkedIn Developer App Setup Helper
=====================================
Handles the full OAuth 2.0 flow to get your LinkedIn access token and person URN.

Prerequisites:
1. Go to https://www.linkedin.com/developers/apps and create an app
2. App Name: anything you want (e.g., "My Content Publisher")
3. LinkedIn Page: Associate with your company page
4. Products tab: Request "Share on LinkedIn" and "Sign In with LinkedIn using OpenID Connect"
5. Auth tab: Add redirect URL: http://localhost:8080/callback
6. Copy your Client ID and Client Secret

Usage:
    python setup_linkedin.py
"""

import http.server
import json
import os
import sys
import threading
import urllib.parse
import webbrowser
from pathlib import Path

import requests

PORT = 8080
REDIRECT_URI = f"http://localhost:{PORT}/callback"
SCOPES = "openid profile w_member_social"

auth_code = None
server_done = threading.Event()


class CallbackHandler(http.server.BaseHTTPRequestHandler):
    def do_GET(self):
        global auth_code
        parsed = urllib.parse.urlparse(self.path)

        if parsed.path == "/callback":
            params = urllib.parse.parse_qs(parsed.query)

            if "code" in params:
                auth_code = params["code"][0]
                self.send_response(200)
                self.send_header("Content-Type", "text/html")
                self.end_headers()
                self.wfile.write(b"""
                <html><body style="font-family: sans-serif; text-align: center; padding-top: 100px;">
                <h1>Authorization successful!</h1>
                <p>You can close this tab and return to the terminal.</p>
                </body></html>
                """)
            elif "error" in params:
                error = params.get("error", ["unknown"])[0]
                desc = params.get("error_description", [""])[0]
                self.send_response(400)
                self.send_header("Content-Type", "text/html")
                self.end_headers()
                self.wfile.write(f"""
                <html><body style="font-family: sans-serif; text-align: center; padding-top: 100px;">
                <h1>Authorization failed</h1>
                <p>Error: {error}</p>
                <p>{desc}</p>
                </body></html>
                """.encode())
            else:
                self.send_response(400)
                self.end_headers()

            server_done.set()
        else:
            self.send_response(404)
            self.end_headers()

    def log_message(self, format, *args):
        pass


def exchange_code_for_token(client_id: str, client_secret: str, code: str) -> dict:
    """Exchange authorization code for access token."""
    resp = requests.post(
        "https://www.linkedin.com/oauth/v2/accessToken",
        data={
            "grant_type": "authorization_code",
            "code": code,
            "redirect_uri": REDIRECT_URI,
            "client_id": client_id,
            "client_secret": client_secret,
        },
        headers={"Content-Type": "application/x-www-form-urlencoded"},
        timeout=30,
    )
    resp.raise_for_status()
    return resp.json()


def get_person_urn(access_token: str) -> str:
    """Fetch the authenticated user's Person URN."""
    resp = requests.get(
        "https://api.linkedin.com/v2/userinfo",
        headers={"Authorization": f"Bearer {access_token}"},
        timeout=15,
    )
    resp.raise_for_status()
    data = resp.json()
    sub = data.get("sub", "")
    name = data.get("name", "Unknown")
    print(f"\nAuthenticated as: {name}")
    return f"urn:li:person:{sub}"


def main():
    print("=" * 60)
    print("LinkedIn Developer App Setup")
    print("=" * 60)
    print()
    print("Before running this, make sure you've:")
    print("  1. Created an app at https://www.linkedin.com/developers/apps")
    print("  2. Requested 'Share on LinkedIn' + 'Sign In with LinkedIn' products")
    print(f"  3. Added redirect URL: {REDIRECT_URI}")
    print()

    client_id = input("Enter your Client ID: ").strip()
    if not client_id:
        print("Client ID is required.")
        sys.exit(1)

    client_secret = input("Enter your Client Secret: ").strip()
    if not client_secret:
        print("Client Secret is required.")
        sys.exit(1)

    server = http.server.HTTPServer(("localhost", PORT), CallbackHandler)
    server_thread = threading.Thread(target=server.serve_forever, daemon=True)
    server_thread.start()

    auth_url = (
        "https://www.linkedin.com/oauth/v2/authorization?"
        + urllib.parse.urlencode({
            "response_type": "code",
            "client_id": client_id,
            "redirect_uri": REDIRECT_URI,
            "scope": SCOPES,
        })
    )

    print(f"\nOpening browser for authorization...")
    print(f"If it doesn't open, go to:\n{auth_url}\n")
    webbrowser.open(auth_url)

    print("Waiting for authorization callback...")
    server_done.wait(timeout=300)
    server.shutdown()

    if not auth_code:
        print("\nAuthorization failed or timed out.")
        sys.exit(1)

    print("\nAuthorization code received. Exchanging for access token...")

    try:
        token_data = exchange_code_for_token(client_id, client_secret, auth_code)
    except Exception as e:
        print(f"\nToken exchange failed: {e}")
        sys.exit(1)

    access_token = token_data.get("access_token", "")
    expires_in = token_data.get("expires_in", 0)
    expires_days = expires_in // 86400

    if not access_token:
        print(f"\nNo access token in response: {token_data}")
        sys.exit(1)

    print(f"Access token obtained (expires in {expires_days} days)")

    try:
        person_urn = get_person_urn(access_token)
    except Exception as e:
        print(f"\nFailed to get Person URN: {e}")
        print("You can still use the access token. Find your URN manually.")
        person_urn = "urn:li:person:YOUR_ID_HERE"

    print()
    print("=" * 60)
    print("SUCCESS! Add these to your .env file:")
    print("=" * 60)
    print()
    print(f"LINKEDIN_ACCESS_TOKEN={access_token}")
    print(f"LINKEDIN_PERSON_URN={person_urn}")
    print(f"LINKEDIN_CLIENT_ID={client_id}")
    print(f"LINKEDIN_CLIENT_SECRET={client_secret}")
    print()
    print("=" * 60)
    print("Also add these as GitHub repo secrets if using GitHub Actions:")
    print("=" * 60)
    print()
    print(f"  LINKEDIN_ACCESS_TOKEN = {access_token[:20]}...")
    print(f"  LINKEDIN_PERSON_URN   = {person_urn}")
    print()
    print(f"Token expires in {expires_days} days. The pipeline will")
    print("send a Slack alert when it detects expiry (401 error).")
    print()

    # Offer to append to .env
    env_path = Path(__file__).resolve().parent / ".env"
    append = input(f"Append to {env_path}? (y/n): ").strip().lower()

    if append == "y":
        with open(env_path, "a", encoding="utf-8") as f:
            f.write(f"\n# LinkedIn API (added by setup_linkedin.py)\n")
            f.write(f"LINKEDIN_ACCESS_TOKEN={access_token}\n")
            f.write(f"LINKEDIN_PERSON_URN={person_urn}\n")
            f.write(f"LINKEDIN_CLIENT_ID={client_id}\n")
            f.write(f"LINKEDIN_CLIENT_SECRET={client_secret}\n")
        print(f"Appended to {env_path}")
    else:
        print("Skipped. Copy the values above manually.")

    print("\nDone! Test with: python main.py --dry-run")


if __name__ == "__main__":
    main()
