# Entity Resolution Engine — Architecture Design

**Stack:** Python · SQLAlchemy · SQLite/PostgreSQL · Streamlit · Docker

---

## 1. System Overview

```
┌─────────────────────────────────────────────────────────────────┐
│                        BROWSER / USER                           │
└───────────────────────────┬─────────────────────────────────────┘
                            │ HTTP
┌───────────────────────────▼─────────────────────────────────────┐
│                    Streamlit UI  (ui/app.py)                     │
│  Dashboard · Ingestion · Search · Graph · Snapshot · Rules      │
└───────────────────────────┬─────────────────────────────────────┘
                            │ Python API
┌───────────────────────────▼─────────────────────────────────────┐
│            MasterResolutionEngine  (master_resolution.py)        │
│   Single facade class; routes calls to API sub-modules below    │
└────────┬───────────────────────────────────────┬────────────────┘
         │                                        │
┌────────▼────────────────┐          ┌────────────▼───────────────┐
│  Ingestion API           │          │  Search & Snapshot API     │
│  (api/ingestion.py)      │          │  (api/snapshot_and_search) │
│                          │          │                            │
│  ingest_record           │          │  get_raw_record            │
│  ingest_bulk             │          │  analyze_record            │
│  delete_record           │          │  search_records            │
│  rebuild_graph           │          │  get_snapshot              │
│  data source whitelist   │          │  get_statistics            │
└────────┬────────────────┘          └────────────┬───────────────┘
         │                                        │
┌────────▼────────────────────────────────────────▼───────────────┐
│                    Core Resolution Layer                         │
│                                                                  │
│   ┌──────────────┐   ┌─────────────────┐   ┌─────────────────┐  │
│   │  Resolver    │──▶│  Standardizer   │   │  Rules Engine   │  │
│   │ (resolver.py)│   │(standardizer.py)│   │(rules_engine.py)│  │
│   └──────────────┘   └─────────────────┘   └─────────────────┘  │
└──────────────────────────────┬──────────────────────────────────┘
                               │ SQLAlchemy ORM
┌──────────────────────────────▼──────────────────────────────────┐
│                     Database Layer                               │
│   records · entities · entity_records · features                │
│   record_features · relationships · match_rules · allowed_srcs  │
└──────────────────────────────┬──────────────────────────────────┘
                               │
                    ┌──────────▼──────────┐
                    │  SQLite / PostgreSQL │
                    └─────────────────────┘
```

---

## 2. Streamlit UI Tabs

| Tab | Purpose |
|---|---|
| **Dashboard** | Live stats: total records, entities, duplicates, relationships, per-source breakdown |
| **Batch Ingestion** | Upload CSV / JSON / JSONL with concurrent thread selection |
| **Single Ingestion** | Upsert one record by Record ID + JSON payload |
| **Fetch Raw Record** | Retrieve stored payload for one or many IDs (manual or file upload) |
| **Find Duplicate Records** | Per-ID separated cluster view + combined CSV download |
| **Delete Record** | Single or bulk delete with cascade re-evaluation |
| **Entity Graph** | Interactive node-click graph powered by `streamlit-agraph` |
| **Search** | Multi-term intersection search (comma-separated traits) |
| **Data Sources** | View/manage the ingestion whitelist |
| **Rules Manager** | Add custom rules; view all active default + custom rules |
| **Snapshot Export** | Full DB extract as CSV/JSONL, with optional PII-stripped summary |

---

## 3. Ingestion Pipeline (Step by Step)

```
Raw JSON Input
  { "DATA_SOURCE": "SRC_A",
    "RECORD_ID": "001",
    "FEATURES": [{ "NAME_FIRST": "James", "NAME_LAST": "Bond" }] }

          │
          ▼
  _map_flat_json()
    Lifts flat keys (e.g. PRIMARY_NAME_ORG) into FEATURES array

          │
          ▼
  Resolver.ingest_record()
    1. Upsert Record row (data_source + record_id unique key)
    2. Sever old feature / entity bindings if record existed

          │
          ▼
  generate_feature_hashes()   ← Standardizer
    ┌─────────────────────────────────────────────────┐
    │  COMPOUND_EXACT  →  hash of entire feature obj  │
    │  Per-key EXACT   →  SHA-256 of each attribute   │
    │  NAME_*_PHONETIC →  Soundex-like hash           │
    │  NAME_*_PHONETIC_SORTED → word-order invariant  │
    │  DOB_YEAR/MONTH/DAY → split from YYYY-MM-DD     │
    └─────────────────────────────────────────────────┘

          │
          ▼
  Persist RecordFeature rows in DB

          │
          ▼
  _find_candidates()
    SQL JOIN across record_features → features
    Returns all records sharing ≥ 1 hash  (blocking step)

          │
          ▼
  RulesEngine.evaluate_records()
    For each candidate pair:
    - Set-intersect feature hashes against each rule's conditions
    - Pick highest-scoring matching rule

          │
          ├── Level 1 (score ≥ 90)  →  Merge → same entity_id
          ├── Level 2/3 (score < 90) →  Relationship edge
          └── No match               →  New isolated Entity

          │
          ▼
  session.commit()
```

---

## 4. Standardizer Hashing

```
Input:  { "NAME_FIRST": "James", "DATE_OF_BIRTH": "1980-02-14" }

Output feature rows:
  ┌───────────────────────────┬──────────────┬──────────────┐
  │ feature_type              │ feature_value│ feature_hash │
  ├───────────────────────────┼──────────────┼──────────────┤
  │ COMPOUND_EXACT            │ NAME_FIRST:J…│ sha256(...)  │
  │ NAME_FIRST                │ James        │ sha256(JAMES)│
  │ NAME_FIRST_PHONETIC       │ James        │ sha256(J000) │
  │ NAME_FIRST_PHONETIC_SORTED│ James        │ sha256(J000) │
  │ DATE_OF_BIRTH             │ 1980-02-14   │ sha256(...)  │
  │ DOB_YEAR                  │ 1980         │ sha256(1980) │
  │ DOB_MONTH                 │ 02           │ sha256(02)   │
  │ DOB_DAY                   │ 14           │ sha256(14)   │
  └───────────────────────────┴──────────────┴──────────────┘
```

> Phonetic hashes are **auto-generated** — you never need to supply them manually.

---

## 5. Rules Engine

**Built-in Default Rules:**

```
Rule Name                  Conditions Required                    Score  Level
─────────────────────────────────────────────────────────────────────────────
RULE_EXACT_NATIONAL_ID     NATIONAL_ID_NUMBER                     100    1
RULE_EXACT_PASSPORT        PASSPORT_NUMBER + PASSPORT_COUNTRY     100    1
RULE_EXACT_NAME_DOB_ADDR   NAME_FIRST_PHONETIC + NAME_LAST_       90     1
                           PHONETIC + DOB_YEAR + ADDR_CITY
RULE_POSSIBLE_NAME_ADDR    NAME_FIRST_PHONETIC + ADDR_CITY        75     2
RULE_RELATIONSHIP_PHONE    PHONE_NUMBER                           50     3
```

**Match Levels:**
- **Level 1** — Same real-world identity → records merged under one `entity_id`
- **Level 2** — Possible duplicate → `Relationship` edge, lower confidence
- **Level 3** — Contextual link → `Relationship` edge, weakest signal

**ML Hook:** `evaluate_match_ml_hook()` is a pluggable integration point where an XGBoost/ML model can override the deterministic score with a probability float in future versions.

---

## 6. Database Schema

```
records
  id  │ data_source │ record_id │ raw_json │ created_at │ updated_at
  ────┼─────────────┼───────────┼──────────┼────────────┼───────────
  PK  │ indexed     │ indexed   │ JSON col │            │
  UNIQUE (data_source, record_id)

entities
  id  │ created_at │ updated_at
  ────┼────────────┼───────────
  PK  │            │

entity_records          (records → entities, many-to-many)
  id  │ entity_id │ record_id │ rule_fired │ score
  ────┼───────────┼───────────┼────────────┼──────
  PK  │ FK(entity)│ FK(record)│ nullable   │ float

features
  id  │ feature_type │ feature_hash │ feature_value
  ────┼──────────────┼──────────────┼──────────────
  PK  │ indexed      │ indexed      │ plaintext

record_features         (records → features, many-to-many)
  id  │ record_id │ feature_id
  ────┼───────────┼───────────
  PK  │ FK(record)│ FK(feature)

relationships           (entity ↔ entity edges)
  id  │ entity_id_1 │ entity_id_2 │ rule_fired │ score
  ────┼─────────────┼─────────────┼────────────┼──────
  PK  │ FK(entity)  │ FK(entity)  │            │ float

match_rules
  id  │ rule_name   │ rule_definition │ match_level │ score │ is_active
  ────┼─────────────┼─────────────────┼─────────────┼───────┼──────────
  PK  │ unique      │ JSON            │ 1/2/3       │ float │ 0 or 1

allowed_sources
  id  │ source_name
  ────┼────────────
  PK  │ unique/indexed
```

---

## 7. Containerization

```
docker-compose.yml
  └── streamlit-ui
        Image: built from ./Dockerfile
        Port:  8501 → host 8501
        Volume: ./data → /app/data   (SQLite persistence)

Dockerfile
  Base: python:3.11-slim
  Installs: requirements.txt
  CMD: python -m streamlit run ui/app.py --server.port=8501
```

---

## 8. Testing

The project has **two dedicated test files** covering unit behaviour and full end-to-end integration flows.

---

### 8.1 Unit Test Suite — `tests/test_engine.py`

**Framework:** Python `unittest`
**Database:** Isolated on-disk SQLite file (`tests/test_engine.db`) created fresh per test run.
**Scope:** Core resolution correctness — entity merging, phonetic matching, relationship creation, name transposition, deletion cascades.

**How to run:**
```
python -m pytest tests/test_engine.py -v
```
or
```
python -m unittest tests/test_engine.py
```

**Test Setup/Teardown:**
```
setUpClass()   →  Removes old test_engine.db, calls init_db() to create fresh schema
tearDownClass()→  Deletes test_engine.db after all tests finish
```

**Test Cases:**

```
Test ID   Method                      What is Verified
────────  ─────────────────────────── ─────────────────────────────────────────────────────
test_01   test_01_ingest_exact_match   Two records with IDENTICAL name + DOB + city from
                                       different data sources (TEST_A, TEST_B) must resolve
                                       to exactly 1 entity (Level 1 Merge).
                                       Asserts:  total_records = 2, total_entities = 1

test_02   test_02_ingest_phonetic_match A third record with MISSPELLED name "Jhonathan"
                                       (instead of "Jonathan") + same DOB + city must still
                                       merge into the SAME entity via phonetic hash matching.
                                       Asserts:  total_records = 3, total_entities = 1

test_03   test_03_relationship_creation Two totally different people (Alice Smith, Bob Jones)
                                       sharing only a phone number must NOT merge — they stay
                                       as separate entities but a Relationship edge is created.
                                       Asserts:  total_entities = 3, total_relationships = 1

test_04   test_04_transposition_match  "Wasi Ahmad" and "Ahmad Wasi" (same words, different
                                       order) must merge using NAME_FIRST_PHONETIC_SORTED.
                                       Requires injecting a custom RULE_TRANSPOSED_NAME rule
                                       via engine.add_custom_rule() first.
                                       Asserts:  total_entities = 4

test_05   test_05_deletion_cascades    Deleting record TEST_E/5 (Bob Jones) must cascade:
                                       - Bob's orphaned entity is removed
                                       - The Relationship between Alice and Bob is severed
                                       Asserts:  total_entities = 3, total_relationships = 0
```

---

### 8.2 Integration Test Suite — `test_master.py`

**Framework:** Plain Python function (print-driven integration runner)
**Database:** In-memory SQLite (`sqlite:///:memory:`) — fresh on every run, zero disk footprint.
**Scope:** Walks through ALL 10 major API flows of `MasterResolutionEngine` end-to-end.

**How to run:**
```
python test_master.py
```

**Integration Scenarios:**

```
Step   Scenario                         What is Tested
─────  ──────────────────────────────── ────────────────────────────────────────────────────────
  1    Rule & Feature Config            get_active_rules() returns default rules on boot.
                                        get_features_summary() returns empty dict on fresh DB.

  2    Bulk Ingestion                   ingest_bulk() processes 6 records across 3 match levels:
                                          Level 1: Alice Smith (CRM/100) + Alice Smith (SALES/200)
                                                   → merge into 1 entity
                                          Level 1: Jonathon Doe (SYSTEM_A) + Jonathan Doe (SYSTEM_B)
                                                   → merge via phonetic name + city matching
                                          Level 3: Mary Williams + James Williams share phone
                                                   → separate entities + Relationship edge

  3    Statistics API                   get_statistics() returns correct entity count, record
                                        count, relationship count, and per-source breakdown.

  4    Multi-Feature Search             engine.search("Alice, Seattle") performs multi-term
                                        comma-separated search and returns matching entity
                                        profiles with their associated source records.

  5    Profile Analysis API             engine.analyze(data_source, record_id) returns the full
                                        cluster: anchor record + all merged records + all
                                        relationship edges with scores and rules fired.

  6    Raw Record Retrieval             engine.get_raw_record("CRM", "100") returns the stored
                                        raw JSON exactly as ingested, with DATA_SOURCE and
                                        RECORD_ID injected into the payload.

  7    Upsert + Deletion                engine.ingest_record() with an existing ID upserts the
                                        record (re-evaluates features + re-runs matching).
                                        engine.delete_record("SALES", "200") removes the record,
                                        orphan entity cleanup, relationship re-evaluation.

  8    Snapshot Generation              engine.get_snapshot() yields the complete DB as a
                                        streaming generator of unified entity cluster dicts.

  9    Data Source Whitelisting         engine.add_data_source("SECURE_SYSTEM") enables whitelist
                                        mode. Any ingest from "BAD_SYSTEM" raises ValueError.
                                        Ingesting from "SECURE_SYSTEM" succeeds normally.

 10    Custom Rule + Graph Rebuild      engine.add_custom_rule("RULE_CUSTOM_EMAIL_MATCH", ...)
                                        injects a new email-matching rule at Level 1 score=99.
                                        engine.rebuild_graph() re-evaluates all raw records
                                        against the new rule — two records sharing an email
                                        that were previously separate now merge into one entity.
```

---

### 8.3 Test Data Sources Used

| Data Source | Record IDs | Purpose in Tests |
|---|---|---|
| TEST_A / TEST_B | 1, 2 | Exact identity merge test |
| TEST_C | 3 | Phonetic (misspelling) merge test |
| TEST_D / TEST_E | 4, 5 | Phone-only relationship + deletion cascade |
| TEST_F / TEST_G | 6, 7 | Name transposition merge test |
| CRM / SALES / HR | 100, 200, 300 | Multi-source bulk ingestion |
| SYSTEM_A–D | L2-1, L2-2, L3-1, L3-2 | Level 2/3 match integration |
| SECURE_SYSTEM | 100, E1, E2 | Whitelist + custom rule + rebuild |

---

### 8.4 Test Coverage by Layer

```
Layer                       Covered By
──────────────────────────  ──────────────────────────────────────────────
Ingestion API               test_01 → test_04, test_master Step 2,7
Search & Snapshot API       test_master Step 3,4,5,6,8
Core Resolver               test_01 → test_05 (all unit tests)
Standardizer (phonetic)     test_02, test_04, test_master Step 2 (L2 case)
Rules Engine (all levels)   test_01 (L1), test_03 (L3), test_master Step 2
Deletion + Cascade          test_05, test_master Step 7
Graph Rebuild               test_master Step 10
Data Source Whitelist       test_master Step 9
Custom Dynamic Rules        test_04, test_master Step 10
```

---

## 9. Key Design Decisions

| Decision | Rationale |
|---|---|
| Senzing-style flat `FEATURES` schema | Single normalized vertical table avoids sparse wide tables |
| SHA-256 hashing for all features | O(1) set-intersection at resolution time; no expensive string comparison at scale |
| Phonetic + sorted phonetic hashes | Catches misspellings (`Jon` ↔ `John`) and word order flips (`Bond James` ↔ `James Bond`) automatically |
| Blocking via hash intersection | Only candidates sharing ≥1 hash are evaluated — prevents O(n²) scale explosion |
| `MasterResolutionEngine` facade | Decouples UI from internals; future REST API wrapping requires zero backend changes |
| ML extensibility hook | Dedicated slot in `RulesEngine` for XGBoost/ML model override without structural refactoring |
| Per-session SQLAlchemy context managers | Prevents connection leaks under concurrent `ThreadPoolExecutor` bulk ingestion |
| Dead Letter Queue (`failed_records.jsonl`) | Failed ingestions persisted with full exception trace for replay/debugging |
