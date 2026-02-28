import os
import json
import logging
import traceback
from typing import List, Dict, Any
from concurrent.futures import ThreadPoolExecutor

from er_engine.database.session import SessionLocal
from er_engine.database.schema import Record, EntityRecord, Entity, Relationship, RecordFeature
from er_engine.core.resolver import Resolver
from er_engine.core.exceptions import BulkIngestionPartialFailure, InvalidJSONSchemaError

logger = logging.getLogger(__name__)

DEAD_LETTER_QUEUE_PATH = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..', 'data', 'failed_records.jsonl'))

def add_allowed_data_source(ds: str):
    from er_engine.database.schema import AllowedSource
    with SessionLocal() as session:
        existing = session.query(AllowedSource).filter_by(source_name=ds).first()
        if not existing:
            new_ds = AllowedSource(source_name=ds)
            session.add(new_ds)
            session.commit()

def remove_allowed_data_source(ds: str):
    from er_engine.database.schema import AllowedSource
    with SessionLocal() as session:
        session.query(AllowedSource).filter_by(source_name=ds).delete()
        session.commit()

def get_allowed_data_sources() -> List[str]:
    from er_engine.database.schema import AllowedSource
    with SessionLocal() as session:
        sources = session.query(AllowedSource).all()
        return [s.source_name for s in sources]

def _verify_data_source(ds: str):
    active_whitelist = get_allowed_data_sources()
    if active_whitelist and ds not in active_whitelist:
        raise ValueError(f"Data Source '{ds}' is rejected. Not in active whitelist.")


def _log_dead_letter(action: str, data_source: str, record_id: str, payload: Any, error_trace: str):
    """Writes failed ingestions to the DLQ file safely."""
    try:
        err_obj = {
            "action": action,
            "data_source": data_source,
            "record_id": record_id,
            "payload": payload,
            "error": error_trace
        }
        with open(DEAD_LETTER_QUEUE_PATH, "a") as f:
            f.write(json.dumps(err_obj) + "\n")
    except Exception as e:
        logger.error(f"Failed to write to Dead Letter Queue! {e}")

def delete_record(data_source: str, record_id: str) -> bool:
    """
    Safely deletes a record by external ID. 
    Wipes associated RecordFeatures. 
    If the entity drops to 0 records, it cleans up the Entity and its Relationships.
    """
    session = SessionLocal()
    try:
        record = session.query(Record).filter_by(data_source=data_source, record_id=record_id).first()
        if not record:
            return False
            
        er_link = session.query(EntityRecord).filter_by(record_id=record.id).first()
        entity_id_to_check = er_link.entity_id if er_link else None
        
        # Cascades will handle mapping deletions
        session.delete(record)
        session.commit()
        
        # Clean up orphaned Entities
        if entity_id_to_check:
            remaining = session.query(EntityRecord).filter_by(entity_id=entity_id_to_check).count()
            if remaining == 0:
                # The entity is empty!
                session.query(Relationship).filter((Relationship.entity_id_1 == entity_id_to_check) | (Relationship.entity_id_2 == entity_id_to_check)).delete()
                session.query(Entity).filter_by(id=entity_id_to_check).delete()
                session.commit()
                
        return True
    except Exception as e:
        session.rollback()
        _log_dead_letter("DELETE", data_source, record_id, None, traceback.format_exc())
        return False
    finally:
        session.close()

def delete_records_by_source(data_source: str) -> int:
    """
    Mass-deletes all records belonging to a specific data source.
    Returns the number of records successfully deleted.
    """
    session = SessionLocal()
    deleted_count = 0
    try:
        records = session.query(Record).filter_by(data_source=data_source).all()
        for rec in records:
            if delete_record(data_source, rec.record_id):
                deleted_count += 1
        return deleted_count
    except Exception as e:
        logger.error(f"Failed bulk deletion for source {data_source}: {e}")
        return deleted_count
    finally:
        session.close()

def delete_bulk_records(data_source: str, record_ids: List[str]) -> Dict[str, int]:
    """
    Deletes a specific list of records from a Data Source.
    """
    results = {"success": 0, "failed": 0}
    for rid in record_ids:
        if delete_record(data_source, rid):
            results["success"] += 1
        else:
            results["failed"] += 1
    return results

def _map_flat_json(payload: Dict[str, Any]) -> Dict[str, Any]:
    """
    Simulates Senzing's flat JSON mapper by lifting specific root-level keys
    into the engine's expected 'FEATURES' array format.
    """
    if "PRIMARY_NAME_ORG" in payload:
        if "FEATURES" not in payload:
            payload["FEATURES"] = []
        payload["FEATURES"].append({
            "NAME_TYPE": "PRIMARY",
            "NAME_ORG": payload["PRIMARY_NAME_ORG"]
        })
    return payload

def ingest_record(data_source: str, record_id: str, payload: Dict[str, Any]) -> None:
    """Wrapper for single synchronous ingestion."""
    if not data_source or not record_id or data_source == "UNKNOWN" or record_id == "UNKNOWN":
        raise ValueError("DATA_SOURCE and RECORD_ID are strictly required.")
        
    payload = _map_flat_json(payload)
        
    session = SessionLocal()
    resolver = Resolver(session)
    try:
        _verify_data_source(data_source)
        resolver.ingest_record(data_source, record_id, payload)
    except Exception as e:
        session.rollback()
        _log_dead_letter("INGEST", data_source, record_id, payload, traceback.format_exc())
        raise e
    finally:
        session.close()

def _process_batch(records: List[Dict[str, Any]]):
    """Worker function for concurrent processing, uses its own DB session per thread."""
    session = SessionLocal()
    resolver = Resolver(session)
    failures = []
    
    for rec in records:
        ds = rec.get("DATA_SOURCE", "UNKNOWN")
        rid = rec.get("RECORD_ID", "UNKNOWN")
        
        # Apply the flatten-to-features mapper to support legacy JSON forms
        rec = _map_flat_json(rec)
        
        try:
            if not ds or not rid or ds == "UNKNOWN" or rid == "UNKNOWN":
                raise ValueError("DATA_SOURCE and RECORD_ID are strictly required for ingestion.")
            
            _verify_data_source(ds)
            resolver.ingest_record(ds, rid, rec)
        except Exception as e:
            session.rollback()
            failures.append(rec)
            _log_dead_letter("INGEST", ds, rid, rec, traceback.format_exc())
            
    session.close()
    return failures

def ingest_bulk(records: List[Dict[str, Any]], max_workers: int = 4) -> Dict[str, Any]:
    """
    High-volume concurrent ingestion wrapper.
    Chunks the input sequence and distributes it across a ThreadPoolExecutor.
    """
    total = len(records)
    logger.info(f"Starting bulk ingestion of {total} records using {max_workers} threads...")
    
    # Deduplicate records by (DATA_SOURCE, RECORD_ID) to prevent SQLite IntegrityError on concurrent inserts
    unique_records_map = {}
    for rec in records:
        ds = rec.get("DATA_SOURCE", "UNKNOWN")
        rid = rec.get("RECORD_ID", "UNKNOWN")
        unique_records_map[(ds, rid)] = rec
        
    records = list(unique_records_map.values())
    total_unique = len(records)
    
    if total != total_unique:
        logger.info(f"Deduplicated {total - total_unique} records in batch. Proceeding with {total_unique} unique records.")
        total = total_unique
    
    # Split records into chunks for executor threads
    chunk_size = max(1, total // (max_workers * 2))
    chunks = [records[i:i + chunk_size] for i in range(0, total, chunk_size)]
    
    all_failures = []
    
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        results = executor.map(_process_batch, chunks)
        for fail_list in results:
            all_failures.extend(fail_list)
            
    success_count = total - len(all_failures)
    
    if all_failures:
        logger.warning(f"Bulk ingestion completed with {len(all_failures)} failures out of {total} records.")
        raise BulkIngestionPartialFailure(
            message=f"{len(all_failures)} out of {total} records failed to ingest. See Dead Letter Queue.",
            failed_records=all_failures
        )
        
    logger.info(f"Bulk ingestion successful: {success_count} records processed.")
    return {"status": "success", "processed": success_count, "failed": 0}

def rebuild_graph() -> int:
    """Wipes the current entity graph and re-ingests all stored records against current rules."""
    session = SessionLocal()
    resolver = Resolver(session)
    try:
        count = resolver.re_evaluate_database()
        return count
    except Exception as e:
        session.rollback()
        logger.error(f"Failed to rebuild graph! {e}")
        raise e
    finally:
        session.close()
