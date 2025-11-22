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
        "password": os.getenv("API_PASSWORD"),
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

    headers = {"Authorization": f"Bearer {token}"}

    params = {"page": page_number, "limit": 100}

    attempt = 0

    while attempt < max_retries:
        try:
            response = requests.get(
                customer_url, headers=headers, params=params, timeout=10
            )

            if response.status_code == 200:
                return True, response.json()

            elif response.status_code == 429:  # 429 is the error code for rate limiting
                wait_time = (
                    2**attempt
                ) + 1  # Exponential backoff calculation - increases wait time with each attempt
                print(f"Rate limit exceeded. Retrying in {wait_time} seconds...")
                time.sleep(
                    wait_time
                )  # Exponential backoff - pauses the script for the calculated wait time
                attempt += 1  # Increment attempt counter

            elif response.status_code in [500, 503]:  # Server errors
                wait_time = 2**attempt
                print(
                    f"Server error {response.status_code} on page {page_number}. Retry {attempt + 1}/{max_retries} in {wait_time} seconds..."
                )
                time.sleep(wait_time)
                attempt += 1

            elif (
                response.status_code == 403
            ):  # 403 is the error code for forbidden access, often due to expired tokens
                # We do not retry on token expiration, we just return the status
                print(f"Token expired on page {page_number}. Need to refresh token.")
                return False, "TOKEN_EXPIRED"

            else:
                print(f"Unexpected error: {response.status_code} on page {page_number}")

        except requests.exceptions.Timeout:  # Handling timeout exceptions
            wait_time = 2**attempt
            print(
                f"Timeout on page {page_number}. Retry {attempt + 1}/{max_retries} in {wait_time} seconds..."
            )
            time.sleep(wait_time)
            attempt += 1

        except Exception as e:  # Catch-all for any other exceptions. Fails immediately.
            print(f"An error occurred: {e} on page {page_number}")
            return False, None

    print(
        f"Failed to fetch data for page {page_number} after {max_retries} attempts."
    )  # All retries exhausted
    return False, None


def write_customers_to_csv(filename):

    token = get_new_token()
    if not token:
        print("Cannot write to CSV without a valid token.")
        return None

    # define column headers for CSV
    fieldnames = ["id", "uuid", "name", "email", "status", "signup_date", "ltv"]

    # open CSV file for writing.. closes automatically when done
    with open(filename, "w", newline="") as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)

        writer.writeheader()
        print(f"Created CSV file: {filename}")

        # Initialize counters for tracking progress
        page_requested = 0
        successful_pages = 0
        failed_pages = 0
        records_ingested = 0

        print(f"\nFetching page 1 to determine total pages...")
        success, data = fetch_customer_data(1, token)

        if not success:
            print("Failed to fetch initial page. Exiting.")
            return None

        total_pages = data["metadata"]["total_pages"]
        print(f"Total pages to fetch: {total_pages}\n")

        for record in data["data"]:
            row = {
                "id": record.get("id", ""),
                "uuid": record.get("uuid", ""),
                "name": record.get("name", ""),
                "email": record.get("email", ""),
                "status": record.get("status", ""),
                "signup_date": record.get("signup_date", ""),
                "ltv": record.get("ltv", ""),
            }
            writer.writerow(row)
            records_ingested += 1

        page_requested += 1
        successful_pages += 1
        print(f"Page 1/{total_pages} completed - {len(data['data'])} records")

        for page_number in range(2, total_pages + 1):
            page_requested += 1

            success, data = fetch_customer_data(page_number, token)

            if not success and data == "TOKEN_EXPIRED":
                print("Refreshing token...")
                token + get_new_token()

                if not token:
                    print("Failed to refresh token. Exiting.")
                    break

                success, data = fetch_customer_data(page_number, token)

            if success:
                for record in data["data"]:
                    row = {
                        "id": record.get("id", ""),
                        "uuid": record.get("uuid", ""),
                        "name": record.get("name", ""),
                        "email": record.get("email", ""),
                        "status": record.get("status", ""),
                        "signup_date": record.get("signup_date", ""),
                        "ltv": record.get("ltv", ""),
                    }
                    writer.writerow(row)
                    records_ingested += 1

                successful_pages += 1

                if page_number % 50 == 0: # Print progress every 50 pages
                    print(
                        f"Print {page_number}/{total_pages} completed - {records_ingested} total records so far"
                    )

            else:
                failed_pages += 1
                print(f"Failed to fetch page {page_number}. Moving to next page.")

        print("\nCSV writing complete!")

    return {
        "pages_requested": page_requested,
        "successful_pages": successful_pages,
        "failed_pages": failed_pages,
        "records_ingested": records_ingested,
    }

def upload_to_s3(local_filename, bucket_name, s3_key):

    try:
        profile_name = os.getenv('AWS_PROFILE')
        session = boto3.Session(profile_name=profile_name)
        s3_client = session.client('s3')

        print(f"\nUploading {local_filename} to s3://{bucket_name}/{s3_key}...")
        s3_client.upload_file(local_filename, bucket_name, s3_key)
        print("Successfully uploaded to S3.")

        return True
    
    except Exception as e:
        print(f"Failed to upload to S3: {e}")
        return False

if __name__ == "__main__":
    import time
    start_time = time.time()
    
    print("Starting customer data ingestion...\n")
    
    # Generate filename with today's date
    today = datetime.now().strftime('%Y-%m-%d')
    local_filename = "customers_raw.csv"
    
    # Write data to CSV
    summary = write_customers_to_csv(local_filename)
    
    if summary:
        # Calculate execution time
        execution_time = time.time() - start_time
        minutes = int(execution_time // 60)
        seconds = int(execution_time % 60)
        
        # Upload to S3
        bucket_name = os.getenv('AWS_BUCKET_NAME')
        s3_key = f"McKenzie/raw/customers/date={today}/customers_raw.csv"
        
        upload_success = upload_to_s3(local_filename, bucket_name, s3_key)
        
        # Print final report
        print("\n" + "="*50)
        print("EXECUTION REPORT")
        print("="*50)
        print(f"Pages Requested: {summary['pages_requested']}")
        print(f"Successful Pages: {summary['successful_pages']}")
        print(f"Failed Pages: {summary['failed_pages']}")
        print(f"Records Ingested: {summary['records_ingested']}")
        print(f"Execution Time: {minutes}m {seconds}s")
        print(f"Format Chosen: CSV (Reason: Streaming efficiency)")
        if upload_success:
            print(f"S3 Location: s3://{bucket_name}/{s3_key}")
        print("="*50)
