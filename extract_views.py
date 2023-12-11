# %%
# YOUR PYTHON CODE COMES HERE
# import libs
import datetime
import json
import os

import boto3
import requests


# %%
# create a function to used in the for loop
def upload_processed_wiki_data_to_s3(date_param="2023-10-15"):
    # %%
    # SUBJECT DATE ### TRY A FEW OF THIS IN CLASS - INSTRUCTONS WILL COME FROM THE INSTRUCTOR
    DATE_PARAM = date_param

    date = datetime.datetime.strptime(DATE_PARAM, "%Y-%m-%d")

    # Wikimedia API URL formation
    url = f"https://wikimedia.org/api/rest_v1/metrics/pageviews/top/en.wikipedia.org/all-access/{date.year}/{date.month}/{date.day}"
    print(f"Requesting REST API URL: {url}")

    # Getting response from Wikimedia API
    wiki_server_response = requests.get(url, headers={"User-Agent": "curl/7.68.0"})
    wiki_response_status = wiki_server_response.status_code
    wiki_response_body = wiki_server_response.text

    print(f"Wikipedia REST API Response body: {wiki_response_body}")
    print(f"Wikipedia REST API Response Code: {wiki_response_status}")

    # Check if response status is not OK
    if wiki_response_status != 200:
        print(
            f"Received non-OK status code from Wiki Server: {wiki_response_status}. Response body: {wiki_response_body}"
        )

    # %%
    # Save Raw Response and upload to S3
    # Get the directory of the current file
    current_directory = os.getcwd()

    # Path for the new directory
    RAW_LOCATION_BASE = f"{current_directory}/data/raw-views"

    # Create the new directory, ignore if it already exists
    if not os.path.exists(RAW_LOCATION_BASE):
        os.makedirs(RAW_LOCATION_BASE)

    # %%
    # Saving the contents of `wiki_response_body` to a file
    # The file is named in the format `raw-edits-YYYY-MM-DD.txt` and saved in the folder defined in `RAW_LOCATION_BASE`

    fp = RAW_LOCATION_BASE
    with open(f"{fp}/raw-views-{DATE_PARAM}.txt", "w") as file:
        file.write(wiki_response_body)

    # %%
    s3 = boto3.client("s3")
    S3_WIKI_BUCKET = "nguyen-viet-ceu2023"

    default_region = s3.meta.region_name
    bucket_configuration = {"LocationConstraint": default_region}
    bucket_names = [bucket["Name"] for bucket in s3.list_buckets()["Buckets"]]
    # Only create the bucket if it doesn't exist
    if S3_WIKI_BUCKET not in bucket_names:
        s3.create_bucket(
            Bucket=S3_WIKI_BUCKET, CreateBucketConfiguration=bucket_configuration
        )
    else:
        print(f"Bucket {S3_WIKI_BUCKET} exists. Won't create new bucket")

    # %%
    # Send the text file to your AWS S3 bucket
    response = s3.upload_file(f"{fp}/raw-views-{DATE_PARAM}.txt", S3_WIKI_BUCKET,
                              f"datalake/raw/raw-views-{DATE_PARAM}.txt")
    print(f"File uploaded to S3. Location: s3://{S3_WIKI_BUCKET}/raw-views-{DATE_PARAM}.txt")

    assert s3.head_object(
        Bucket=S3_WIKI_BUCKET,
        Key=f"datalake/raw/raw-views-{DATE_PARAM}.txt",
    )

    # %%
    # Parse the Wikipedia response and process the data
    wiki_response_parsed = wiki_server_response.json()
    page_views = wiki_response_parsed["items"][0]["articles"]

    # Convert server's response to JSON lines
    current_time = datetime.datetime.utcnow()  # Always use UTC!!
    json_lines = ""
    for page in page_views:
        record = {
            "article": page["article"],
            "views": page["views"],
            "rank": page["rank"],
            "date": date.strftime("%Y-%m-%d"),
            "retrieved_at": current_time.isoformat(),
        }
        json_lines += json.dumps(record) + "\n"

    # Save the Top Views JSON lines and upload them to S3
    JSON_LOCATION_DIR = f"{current_directory}/data/views"
    # Create the new directory, ignore if it already exists
    if not os.path.exists(JSON_LOCATION_DIR):
        os.makedirs(JSON_LOCATION_DIR)
    print(f"Created directory {JSON_LOCATION_DIR}")
    print(f"JSON lines:\n{json_lines}")

    json_lines_filename = f"views-{date.strftime('%Y-%m-%d')}.json"
    json_lines_file = f"{JSON_LOCATION_DIR}/{json_lines_filename}"

    with open(json_lines_file, "w") as file:
        file.write(json_lines)

    # Upload the JSON file
    s3.upload_file(json_lines_file, S3_WIKI_BUCKET, f"datalake/views/{json_lines_filename}")
    print(
        f"Uploaded JSON lines to s3://{S3_WIKI_BUCKET}/datalake/views/{json_lines_filename}"
    )

    assert s3.head_object(
        Bucket=S3_WIKI_BUCKET,
        Key=f"datalake/views/views-{DATE_PARAM}.json",
    )


# %%
if __name__ == '__main__':
    for day in range(15, 22):
        upload_processed_wiki_data_to_s3(date_param=f"2023-10-{day}")
