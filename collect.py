from dotenv import load_dotenv
import os
import requests
import json
import time
import csv

load_dotenv()

def collect_contacts(token, properties):

    headers = {'Authorization': f'Bearer {token}', 'Content-Type': 'application/json'}
    all_records = []
    offset = 0

    while True:
        try:
            url = ("https://api.hubapi.com/crm/v3/objects/contact/search?archived=false")
            payload = json.dumps({
                "filterGroups": [
                    {
                    "filters": [
                        {
                        "value": "true",
                        "propertyName": "allowed_to_collect",
                        "operator": "EQ"
                        }
                    ]
                    }
                ],
                "properties": properties,
                "limit": 200,
                "after": offset
            })
            response = requests.post(url, headers=headers, data=payload)
            response.raise_for_status()
            data = response.json()

            all_records.extend([record["properties"] for record in data.get("results", [])])
            print(f"Contacts found with allowed_to_collect property set to true: {len(all_records)}")

            if "paging" in data and "next" in data["paging"]:
                offset = data["paging"]["next"]["after"]
            else:
                break
        except requests.exceptions.RequestException as error:
            if response.status_code == 429:
                print("Searching...")
                print(response.json().get("message", "Rate limit exceeded"))
                if i >= max_tries - 1:
                    raise Exception({"error": "Max retries reached", "details": str(error)})
                time.sleep(0.5)
            else:
                raise Exception({
                    "error": "Error searching contacts",
                    "recordType": "Contact",
                    "recordInfo": "allowed_to_collect",
                    "operator": "EQ - true",
                    "message": response.json().get("message", str(error))
                })

    return all_records

def save_to_csv(records, filename):
    if not records:
        return

    keys = records[0].keys()  # Obtener las claves como encabezados
    with open(filename, mode='w', newline='', encoding='utf-8') as file:
        writer = csv.DictWriter(file, fieldnames=keys)
        writer.writeheader()
        writer.writerows(records)

save_to_csv(collect_contacts(os.getenv("TOKEN_HUBSPOT"),['firstname', 'lastname', 'country', 'phone', 'training___create_date', 'industry', 'address',"checkbox", "raw_email_id", 'hs_object_id']), "contact_collection.csv")

