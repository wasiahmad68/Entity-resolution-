import unittest
import os
import sys

# Ensure er_engine is in the path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

# Set to a unique file before importing session to prevent hitting the real SQLite DB,
# but avoiding :memory: because ThreadPoolExecutor spawns threads that can't share a bare :memory: SQLite DB.
TEST_DB_FILE = os.path.join(os.path.dirname(__file__), 'test_engine.db')
os.environ["DATABASE_URL"] = f"sqlite:///{TEST_DB_FILE}"

from er_engine.database.session import init_db
from er_engine.api.ingestion import ingest_bulk, delete_record
from er_engine.api.snapshot_and_search import get_statistics, analyze_record
from er_engine.database.schema import Record, Entity

class TestEREngine(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        # We will use a clean sqlite test database on disk
        if os.path.exists(TEST_DB_FILE):
            os.remove(TEST_DB_FILE)
        init_db()
        
    @classmethod
    def tearDownClass(cls):
        # Clean up the test database
        if os.path.exists(TEST_DB_FILE):
            # Because sessions might be tied up, we force try/catch
            try:
                os.remove(TEST_DB_FILE)
            except Exception as e:
                pass

    def test_01_ingest_exact_match(self):
        """Test that two identical identity records merge into the same exact Entity."""
        data = [
            {
                "DATA_SOURCE": "TEST_A",
                "RECORD_ID": "1",
                "FEATURES": [
                    { "NAME_TYPE": "PRIMARY", "NAME_FIRST": "Jonathan", "NAME_LAST": "Doe" },
                    { "DATE_OF_BIRTH": "1990-01-01" },
                    { "ADDR_CITY": "Seattle" }
                ]
            },
            {
                "DATA_SOURCE": "TEST_B",
                "RECORD_ID": "2",
                "FEATURES": [
                    { "NAME_TYPE": "PRIMARY", "NAME_FIRST": "Jonathan", "NAME_LAST": "Doe" },
                    { "DATE_OF_BIRTH": "1990-01-01" },
                    { "ADDR_CITY": "Seattle" }
                ]
            }
        ]
        
        res = ingest_bulk(data, max_workers=1)
        self.assertEqual(res["processed"], 2)
        
        stats = get_statistics()
        self.assertEqual(stats["total_records"], 2)
        # Should result in exactly 1 unique entity!
        self.assertEqual(stats["total_entities"], 1)

    def test_02_ingest_phonetic_match(self):
        """Test that misspellings (Phonetic hashes) trigger the rules engine successfully."""
        data = [
            {
                "DATA_SOURCE": "TEST_C",
                "RECORD_ID": "3",
                "FEATURES": [
                    { "NAME_TYPE": "PRIMARY", "NAME_FIRST": "Jhonathan", "NAME_LAST": "Doe" },
                    { "DATE_OF_BIRTH": "1990-01-01" },
                    { "ADDR_CITY": "Seattle" }
                ]
            }
        ]
        ingest_bulk(data, max_workers=1)
        stats = get_statistics()
        self.assertEqual(stats["total_records"], 3)
        # Should still be 1 entity! The rules engine catches Jonathon via Phonetic Hashing!
        self.assertEqual(stats["total_entities"], 1)

    def test_03_relationship_creation(self):
        """Test that matching on only phone number creates a relationship edge, NOT an entity merge."""
        data = [
            {
                "DATA_SOURCE": "TEST_D",
                "RECORD_ID": "4",
                "FEATURES": [
                    { "NAME_TYPE": "PRIMARY", "NAME_FIRST": "Alice", "NAME_LAST": "Smith" },
                    { "PHONE_NUMBER": "555-9999" }
                ]
            },
            {
                "DATA_SOURCE": "TEST_E",
                "RECORD_ID": "5",
                "FEATURES": [
                    { "NAME_TYPE": "PRIMARY", "NAME_FIRST": "Bob", "NAME_LAST": "Jones" },
                    { "PHONE_NUMBER": "555-9999" }
                ]
            }
        ]
        
        ingest_bulk(data, max_workers=1)
        stats = get_statistics()
        
        # Test D and E will each become their own entities (Total entities = 1 + 2 = 3)
        self.assertEqual(stats["total_entities"], 3)
        # But they will form a relationship link!
        self.assertEqual(stats["total_relationships"], 1)

    def test_04_transposition_match(self):
        """Test that inverted word orders (Wasi Ahmad vs Ahmad Wasi) natively merge using _PHONETIC_SORTED."""
        from er_engine.master_resolution import MasterResolutionEngine
        engine = MasterResolutionEngine()
        
        # Inject the specialized rule
        engine.add_custom_rule(
            "RULE_TRANSPOSED_NAME",
            "Match on sorted phonetic name",
            ["NAME_FIRST_PHONETIC_SORTED"],
            95.0,
            1
        )
        
        data = [
            {
                "DATA_SOURCE": "TEST_F",
                "RECORD_ID": "6",
                "FEATURES": [
                    { "NAME_TYPE": "PRIMARY", "NAME_FIRST": "Wasi Ahmad" }
                ]
            },
            {
                "DATA_SOURCE": "TEST_G",
                "RECORD_ID": "7",
                "FEATURES": [
                    { "NAME_TYPE": "PRIMARY", "NAME_FIRST": "Ahmad Wasi" }
                ]
            }
        ]
        
        ingest_bulk(data, max_workers=1)
        stats = get_statistics()
        
        # We had 3 entities from previous tests. 
        # TEST_F and TEST_G should merge into exactly ONE new entity because of the _PHONETIC_SORTED rule.
        # Total Entities expected: 3 + 1 = 4.
        self.assertEqual(stats["total_entities"], 4)
        
    def test_05_deletion_cascades(self):
        """Test that deleting a record cleans up entities properly."""
        success = delete_record("TEST_E", "5")
        self.assertTrue(success)
        
        stats = get_statistics()
        # Bob Jones entity was dropped because it only had 1 record.
        # Original expectation was 2. But Test 04 added 1 entity for Transposition.
        self.assertEqual(stats["total_entities"], 3)
        # Because Bob's entity is gone, the Relationship is cleanly severed.
        self.assertEqual(stats["total_relationships"], 0)

if __name__ == "__main__":
    unittest.main()
