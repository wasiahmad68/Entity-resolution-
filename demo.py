import json
import os
import sys

# Ensure er_engine is in the path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__))))

from er_engine.database.session import init_db
from er_engine.api.ingestion import ingest_bulk

def generate_sample_data():
    records = [
        # Exact match (Duplicate) - Robert Smith
        {
            "DATA_SOURCE": "SYSTEM_A",
            "RECORD_ID": "100",
            "FEATURES": [
                { "RECORD_TYPE": "PERSON" },
                { "NAME_TYPE": "PRIMARY", "NAME_FIRST": "Robert", "NAME_LAST": "Smith" },
                { "DATE_OF_BIRTH": "1980-01-15" },
                { "ADDR_TYPE": "HOME", "ADDR_CITY": "Chicago", "ADDR_STATE": "IL" },
                { "PHONE_TYPE": "MOBILE", "PHONE_NUMBER": "555-0100" }
            ]
        },
        {
            "DATA_SOURCE": "SYSTEM_B",
            "RECORD_ID": "B-99",
            "FEATURES": [
                { "RECORD_TYPE": "PERSON" },
                { "NAME_TYPE": "PRIMARY", "NAME_FIRST": "Robert", "NAME_LAST": "Smith" },
                { "DATE_OF_BIRTH": "1980-01-15" },
                { "ADDR_TYPE": "HOME", "ADDR_CITY": "Chicago", "ADDR_STATE": "IL" },
                { "EMAIL_ADDRESS": "robert.smith@example.com" }
            ]
        },
        
        # Phonetic & Typo Match (Possible Match / Same Entity)
        {
            "DATA_SOURCE": "SYSTEM_A",
            "RECORD_ID": "101",
            "FEATURES": [
                { "RECORD_TYPE": "PERSON" },
                { "NAME_TYPE": "PRIMARY", "NAME_FIRST": "Jhonathan", "NAME_LAST": "Doe" },
                { "ADDR_TYPE": "HOME", "ADDR_CITY": "Seattle" }
            ]
        },
        {
            "DATA_SOURCE": "CRM",
            "RECORD_ID": "C-12",
            "FEATURES": [
                { "RECORD_TYPE": "PERSON" },
                { "NAME_TYPE": "PRIMARY", "NAME_FIRST": "Jonathan", "NAME_LAST": "Doe" },
                { "ADDR_TYPE": "HOME", "ADDR_CITY": "Seattle" }
            ]
        },
        
        # Relationship (Different Name, Same Phone/Address) -> Spouses or Roommates
        {
            "DATA_SOURCE": "HR",
            "RECORD_ID": "EMP-01",
            "FEATURES": [
                { "RECORD_TYPE": "PERSON" },
                { "NAME_TYPE": "PRIMARY", "NAME_FIRST": "Alice", "NAME_LAST": "Wonderland" },
                { "PHONE_TYPE": "MOBILE", "PHONE_NUMBER": "555-8888" },
                { "ADDR_TYPE": "HOME", "ADDR_CITY": "Austin", "ADDR_STATE": "TX" }
            ]
        },
        {
            "DATA_SOURCE": "SALES",
            "RECORD_ID": "LEAD-55",
            "FEATURES": [
                { "RECORD_TYPE": "PERSON" },
                { "NAME_TYPE": "PRIMARY", "NAME_FIRST": "Bob", "NAME_LAST": "Builder" },
                { "PHONE_TYPE": "MOBILE", "PHONE_NUMBER": "555-8888" },
                { "ADDR_TYPE": "HOME", "ADDR_CITY": "Austin", "ADDR_STATE": "TX" }
            ]
        }
    ]
    
    with open("sample_data.json", "w") as f:
        json.dump(records, f, indent=2)
        
    print("Created sample_data.json")
    return records

if __name__ == "__main__":
    print("Initializing Database...")
    init_db()
    
    print("Generating sample data...")
    data = generate_sample_data()
    
    print("Running Bulk Ingestion via API...")
    res = ingest_bulk(data, max_workers=2)
    print("Ingestion Result:", res)
    print("Demo complete! You can now start the Streamlit UI to view the graph.")
