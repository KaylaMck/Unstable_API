import requests
import csv
import time  # For handling delays and tracking how long the script runs
import boto3
import os
from dotenv import load_dotenv
from datetime import datetime

load_dotenv()

def get_new_token():
    
    base_url = os.getenv("API_BASE_URL")
    login_url = f"{base_url}/login"

    credentials = {
        "username": os.getenv("API_USERNAME"),
        "password": os.getenv("API_PASSWORD")
    }

    response = requests.post(login_url, json=credentials)

    if response.status_code == 200:
        token = response.json().get("access_token")
        print("Successfully obtained new token.")
        return token
    else:
        print(f"Failed to obtain token. Status code: {response.status_code}")
        return None
    

def fetch_customer_data(page_number, token, max_retries=5):

    base_url = os.getenv("API_BASE_URL")
    customer_url = f"{base_url}/customers"

    headers = {
        "Authorization": f"Bearer {token}"
    }

    params = {
        "page": page_number,
        "limit": 100
    }

    attempt = 0

    while attempt < max_retries:
        try:
            response = requests.get(customer_url, headers=headers, params=params, timeout=10)

            if response.status_code == 200:
                return True, response.json()
            
            elif response.status_code == 429: #429 is the error code for rate limiting
                wait_time = (2 ** attempt) + 1 # Exponential backoff calculation - increases wait time with each attempt
                print(f"Rate limit exceeded. Retrying in {wait_time} seconds...")
                time.sleep(wait_time) # Exponential backoff - pauses the script for the calculated wait time
                attempt += 1 # Increment attempt counter

            elif response.status_code in [500, 503]: #Server errors
                wait_time = (2 ** attempt)
                print(f"Server error {response.status_code} on page {page_number}. Retry {attempt + 1}/{max_retries} in {wait_time} seconds...")
                time.sleep(wait_time)
                attempt += 1

            elif response.status_code == 403: #403 is the error code for forbidden access, often due to expired tokens
                # We do not retry on token expiration, we just return the status
                print(f"Token expired on page {page_number}. Need to refresh token.")
                return False, "TOKEN_EXPIRED" 
            
            else:
                print(f"Unexpected error: {response.status_code} on page {page_number}")

        except requests.exceptions.Timeout: # Handling timeout exceptions
            wait_time = (2 ** attempt)
            print(f"Timeout on page {page_number}. Retry {attempt + 1}/{max_retries} in {wait_time} seconds...")
            time.sleep(wait_time)
            attempt += 1

        except Exception as e: # Catch-all for any other exceptions. Fails immediately.
            print(f"An error occurred: {e} on page {page_number}")
            return False, None
        
    print(f"Failed to fetch data for page {page_number} after {max_retries} attempts.") # All retries exhausted
    return False, None

if __name__ == "__main__":
    token = get_new_token()
    if token:
        print(f"Token: {token[:50]}...\n")  # Print first 50 characters of the token for verification

        print("Testing fetch_customer_data function...")
        success, data = fetch_customer_data(1, token)

        if success:
            print("Successfully fetched customer data for page 1.")
            print(f"Total pages: {data['metadata']['total_pages']}")
            print(f"Records on page 1: {len(data['data'])}")
            print(f"First customer record: {data['data'][0]}")
        else:
            print("Failed to fetch customer data for page 1.")