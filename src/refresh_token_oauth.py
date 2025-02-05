import os
import secrets
import requests


def oauth_token_flow():
    state = secrets.token_urlsafe(16)  # Generates a secure random URL-safe string
    print(f"Generated state: {state}")

    AUTHORIZE_URL = "https://hh.ru/oauth/authorize"
    TOKEN_URL = "https://api.hh.ru/token"

    client_id = "client_id"
    client_secret = "client_secret"
    redirect_uri = "http://localhost:8000/auth"
    refresh_token = os.getenv("HH_REFRESH_TOKEN")

    # # Step 1: Direct user to the authorization page
    # print(f"Navigate to this URL to authorize:")
    # print(
    #     f"{AUTHORIZE_URL}?response_type=code&client_id={client_id}&state={state}&redirect_uri={redirect_uri}"
    # )
    #
    # # Step 2: User is redirected to your redirect_uri with a code
    # authorization_code = input("Paste the authorization code here: ")
    #
    # # Step 3: Exchange authorization_code for access and refresh tokens
    # token_data = {
    #     "grant_type": "authorization_code",
    #     "code": authorization_code,
    #     "redirect_uri": redirect_uri,
    #     "client_id": client_id,
    #     "client_secret": client_secret,
    # }

    token_headers = {
        "Content-Type": "application/x-www-form-urlencoded",
    }

    # Step 4: Exchange authorization_code for access and refresh tokens
    # response = requests.post(TOKEN_URL, data=token_data, headers=token_headers)
    # response.raise_for_status()

    # Steps 5 (after previous token expired): Exchange refresh_token for access and refresh tokens
    refresh_token_data = {
        "grant_type": "refresh_token",
        "redirect_uri": redirect_uri,
        "refresh_token": refresh_token
    }
    response = requests.post(TOKEN_URL, data=refresh_token_data, headers=token_headers)
    response.raise_for_status()

    tokens = response.json()
    access_token = tokens["access_token"]
    refresh_token = tokens["refresh_token"]
    expires_in = tokens["expires_in"]

    print(f"Access Token: {access_token}")
    print(f"Refresh Token: {refresh_token}")
    print(f"Expires: {expires_in}")

if __name__ == "__main__":
    oauth_token_flow()