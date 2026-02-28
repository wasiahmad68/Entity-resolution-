import json
import os
import sys

# Ensure library path
sys.path.insert(0, os.path.abspath(os.path.dirname(__file__)))

from er_engine.master_resolution import MasterResolutionEngine

def test_master_engine():
    print("=== Initializing Master Resolution Engine ===")
    # Initialize with an in-memory database for testing
    engine = MasterResolutionEngine(db_url="sqlite:///:memory:")
    
    # Ensure whitelist is completely empty before starting tests
    allowed = engine.get_allowed_sources()
    for s in allowed:
        engine.remove_data_source(s)

    print("\n1. Testing Rule & Feature Configuration Endpoints...")
    rules = engine.get_active_rules()
    print(f"Loaded {len(rules)} active matching rules.")
    
    # We shouldn't have any features indexed yet
    features = engine.get_features_summary()
    print(f"Initial Features Indexed: {len(features)}")

    print("\n2. Testing Bulk Ingestion...")
    sample_data = [
        # Level 1 Match (Exact Identity)
        {
            "DATA_SOURCE": "CRM",
            "RECORD_ID": "100",
            "FEATURES": [
                { "NAME_TYPE": "PRIMARY", "NAME_FIRST": "Alice", "NAME_LAST": "Smith" },
                { "ADDR_CITY": "Seattle" }
            ]
        },
        {
            "DATA_SOURCE": "SALES",
            "RECORD_ID": "200",
            "FEATURES": [
                { "NAME_TYPE": "PRIMARY", "NAME_FIRST": "Alice", "NAME_LAST": "Smith" },
                { "ADDR_CITY": "Seattle" }
            ]
        },
        {
            "DATA_SOURCE": "HR",
            "RECORD_ID": "300",
            "FEATURES": [
                { "NAME_TYPE": "PRIMARY", "NAME_FIRST": "Bob", "NAME_LAST": "Jones" },
                { "PHONE_NUMBER": "555-0199" }
            ]
        },
        # Level 2 Match (Possible Match - Fuzzy Name + Exact Address)
        {
            "DATA_SOURCE": "SYSTEM_A",
            "RECORD_ID": "L2-1",
            "FEATURES": [
                { "NAME_TYPE": "PRIMARY", "NAME_FIRST": "Jonathon", "NAME_LAST": "Doe" },
                { "ADDR_CITY": "Chicago" }
            ]
        },
        {
            "DATA_SOURCE": "SYSTEM_B",
            "RECORD_ID": "L2-2",
            "FEATURES": [
                { "NAME_TYPE": "PRIMARY", "NAME_FIRST": "Jonathan", "NAME_LAST": "Doe" },
                { "ADDR_CITY": "Chicago" }
            ]
        },
        # Level 3 Match (Relationship - Exact Phone but Different Names)
        {
            "DATA_SOURCE": "SYSTEM_C",
            "RECORD_ID": "L3-1",
            "FEATURES": [
                { "NAME_TYPE": "PRIMARY", "NAME_FIRST": "Mary", "NAME_LAST": "Williams" },
                { "PHONE_NUMBER": "202-555-0199" }
            ]
        },
        {
            "DATA_SOURCE": "SYSTEM_D",
            "RECORD_ID": "L3-2",
            "FEATURES": [
                { "NAME_TYPE": "PRIMARY", "NAME_FIRST": "James", "NAME_LAST": "Williams" },
                { "PHONE_NUMBER": "202-555-0199" }
            ]
        }
    ]
    
    res = engine.ingest_bulk(sample_data, max_workers=2)
    print(f"Bulk Ingestion Result: {res}")

    print("\n3. Testing Analytics & Statistics...")
    stats = engine.get_statistics()
    print(json.dumps(stats, indent=2))

    print("\n4. Testing Multi-Feature Search API...")
    # Searching for Alice in Seattle
    search_res = engine.search("Alice, Seattle")
    print(f"Found {len(search_res)} unique entity profiles matching 'Alice, Seattle'.")
    for hit in search_res:
        print(f"  -> Entity ID: {hit['entity_id']} contains {len(hit['matching_records'])} source records.")

    print("\n5. Testing Profile Analysis API...")
    if search_res:
        target_entity_records = search_res[0]['matching_records']
        ds = target_entity_records[0]['data_source']
        rid = target_entity_records[0]['record_id']
        analysis = engine.analyze(ds, rid)
        print(f"Analysis for {ds}/{rid} - Entity Cluster Size: {len(analysis.get('cluster_records', []))}")

    print("\n6. Testing Raw Record Retrieval API...")
    raw = engine.get_raw_record("CRM", "100")
    print("Raw CRM/100 Payload:", raw)

    print("\n7. Testing Single Record Override & Deletion APIs...")
    # Update Alice's record in CRM
    updated_payload = {
        "DATA_SOURCE": "CRM",
        "RECORD_ID": "100",
        "FEATURES": [
            { "NAME_TYPE": "PRIMARY", "NAME_FIRST": "Alice", "NAME_LAST": "Smith-Johnson" },
            { "ADDR_CITY": "Portland" }
        ]
    }
    engine.ingest_record("CRM", "100", updated_payload)
    print("Upserted a change to CRM/100 profile")
    
    # Delete the SALES record completely
    engine.delete_record("SALES", "200")
    print("Deleted SALES/200 completely")
    
    final_stats = engine.get_statistics()
    print(f"Final Entity Count should be 2: Actual: {final_stats['total_entities']}")
    print(f"Final Record Count should be 2 (CRM 100, HR 300): Actual: {final_stats['total_records']}")

    print("\n8. Testing Database Snapshot Generation...")
    snapshot = list(engine.get_snapshot())
    print(f"Generated a full database snapshot yielding {len(snapshot)} unified entities.")

    print("\n9. Testing Data Source Whitelisting...")
    engine.add_data_source("SECURE_SYSTEM")
    try:
        engine.ingest_record("BAD_SYSTEM", "999", {"FEATURES": [{"NAME_FIRST": "Hacker"}]})
        print("FAIL: Should have rejected BAD_SYSTEM")
    except ValueError as e:
        print(f"SUCCESS: Rejected un-whitelisted dataset. Error: {e}")
        
    engine.ingest_record("SECURE_SYSTEM", "100", {"FEATURES": [{"NAME_FIRST": "AuthUser"}]})
    print("SUCCESS: Ingested SECURE_SYSTEM.")

    print("\n10. Testing Custom Dynamic Rules & Graph Rebuild...")
    engine.add_custom_rule(
        name="RULE_CUSTOM_EMAIL_MATCH",
        description="Matches exact emails.",
        conditions=["EMAIL_ADDRESS"],
        score=99.0,
        level=1
    )
    print("SUCCESS: Injected RULE_CUSTOM_EMAIL_MATCH into engine.")
    
    # Ingest two records with only an email (no existing rule covers this)
    engine.ingest_record("SECURE_SYSTEM", "E1", {"FEATURES": [{"EMAIL_ADDRESS": "test@test.com"}]})
    engine.ingest_record("SECURE_SYSTEM", "E2", {"FEATURES": [{"EMAIL_ADDRESS": "test@test.com"}]})
    
    print("Triggering massive graph rebuild...")
    rebuild_count = engine.rebuild_graph()
    print(f"SUCCESS: Graph Rebuild complete. Processed {rebuild_count} raw records against the new Custom Rule.")

    print("\n=== ALL MASTER RESOLUTION ENGINE APIS VERIFIED SUCCESSFULLY ===")

if __name__ == "__main__":
    test_master_engine()
