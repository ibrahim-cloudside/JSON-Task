import json
import pandas as pd
from google.cloud import storage
import os

os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = "/home/ibrahim_md/key.json"

# Replace these variables with your actual bucket and file names
BUCKET_NAME = 'psql-gcs'
DESTINATION_BLOB_NAME = 'JSON-output/output.csv'  # The path in the bucket where the file will be saved

# Load the main JSON file
with open('sample.json', 'r', encoding='utf-8') as file:
    data = json.load(file)

# Check if 'after' exists in the JSON data
if 'after' in data:
    # Parse the nested JSON in the 'after' field
    after_data = json.loads(data['after'])

    # Helper function to flatten nested dictionaries and replace empty values with None
    def flatten_dict(d, parent_key=''):
        items = []
        for k, v in d.items():
            new_key = f"{parent_key}.{k}" if parent_key else k
            if isinstance(v, dict):
                items.extend(flatten_dict(v, new_key).items())
            else:
                # Replace empty strings or lists with None (which will become NULL in CSV)
                items.append((new_key, v if v not in ["", []] else None))
        return dict(items)

    # Flatten the 'after' dictionary to handle nested keys
    flat_after_data = flatten_dict(after_data)

    # Convert the flattened dictionary to a DataFrame
    df = pd.DataFrame([flat_after_data])

    # Save the DataFrame as a CSV file
    csv_filename = '/tmp/output.csv'  # Temporarily save the file locally
    df.to_csv(csv_filename, index=False, na_rep='NULL')

    # Upload the CSV file to Google Cloud Storage
    storage_client = storage.Client()
    bucket = storage_client.bucket(BUCKET_NAME)
    blob = bucket.blob(DESTINATION_BLOB_NAME)

    # Upload the file to the bucket
    blob.upload_from_filename(csv_filename)

    print(f"Data has been saved to GCS bucket '{BUCKET_NAME}' at '{DESTINATION_BLOB_NAME}'.")

else:
    print("'after' key not found in the JSON file.")