from typing import Dict, Any, List, Optional

from er_engine.database.session import init_db
from er_engine.api.ingestion import ingest_record, ingest_bulk, delete_record
from er_engine.api.snapshot_and_search import (
    get_statistics,
    analyze_record,
    search_records,
    get_snapshot,
    get_active_rules,
    get_features_summary,
    get_raw_record
)
from er_engine.api.ingestion import add_allowed_data_source, get_allowed_data_sources, rebuild_graph

class MasterResolutionEngine:
    """
    Centralized API class for the Senzing-clone Entity Resolution Engine.
    Provides a single entrypoint for all datastore initialization, ingestion,
    searching, and analytics functions.
    """
    def __init__(self, db_url: Optional[str] = None):
        """
        Initializes the database connection.
        If db_url is provided, it configures SQLAlchemy. Otherwise uses the environment default.
        """
        import os
        if db_url:
            os.environ["DATABASE_URL"] = db_url
        init_db()

    # --- INGESTION API ---
    def ingest_record(self, data_source: str, record_id: str, payload: Dict[str, Any]) -> None:
        """Upserts a single record into the engine."""
        ingest_record(data_source, record_id, payload)

    def ingest_bulk(self, records: List[Dict[str, Any]], max_workers: int = 4) -> Dict[str, Any]:
        """Concurrently ingests an array of records and resolves them into the graph."""
        return ingest_bulk(records, max_workers=max_workers)

    def delete_record(self, data_source: str, record_id: str) -> bool:
        """Deletes a record and cascades the deletion to re-evaluate the surviving entity cluster."""
        return delete_record(data_source, record_id)

    def delete_records_by_source(self, data_source: str) -> int:
        """Deletes all records matching a specific data source."""
        from er_engine.api.ingestion import delete_records_by_source
        return delete_records_by_source(data_source)

    def delete_bulk_records(self, data_source: str, record_ids: List[str]) -> Dict[str, int]:
        """Deletes a specific list of records from a Data Source."""
        from er_engine.api.ingestion import delete_bulk_records
        return delete_bulk_records(data_source, record_ids)

    def get_raw_record(self, data_source: str, record_id: str) -> Dict[str, Any]:
        """Retrieves the exact unmodified JSON payload previously ingested."""
        return get_raw_record(data_source, record_id)

    # --- INGESTION FILTERS ---
    def add_data_source(self, data_source: str):
        """Adds a Data Source to the whitelist. Once populated, only whitelisted sources are ingested."""
        from er_engine.api.ingestion import add_allowed_data_source
        add_allowed_data_source(data_source)
        
    def remove_data_source(self, data_source: str):
        """Removes a Data Source from the whitelist."""
        from er_engine.api.ingestion import remove_allowed_data_source
        remove_allowed_data_source(data_source)
        
    def get_allowed_sources(self) -> List[str]:
        """Returns the active list of accepted Data Sources."""
        from er_engine.api.ingestion import get_allowed_data_sources
        return get_allowed_data_sources()

    # --- SEARCH & ANALYSIS API ---
    def search(self, query: str) -> List[Dict[str, Any]]:
        """
        Multi-feature search. Pass comma-separated traits (e.g. 'Doe, 98101')
        Returns all entities containing ALL specified traits in their graph.
        """
        return search_records(query)

    def analyze(self, data_source: str, record_id: str) -> Dict[str, Any]:
        """
        Retrieves the exact breakdown of why a profile merged, returning
        all matched records, identical rules fired, and external relationships.
        """
        return analyze_record(data_source, record_id)

    def get_records_by_entity(self, entity_id: int) -> List[Dict[str, Any]]:
        """Retrieves raw payloads for all records connected to a specific Entity ID."""
        from er_engine.api.snapshot_and_search import get_records_by_entity
        return get_records_by_entity(entity_id)

    def get_statistics(self) -> Dict[str, Any]:
        """Returns engine health metrics, ingested volume, and exact match duplicate rates."""
        return get_statistics()

    def get_snapshot(self, summarize: bool = False):
        """Generates a point-in-time full database extract for downstream ML pipelines.
        If summarize is True, it returns a flattened mapping of Record IDs to Entity IDs and the Rule Fired."""
        from er_engine.api.snapshot_and_search import get_snapshot
        return get_snapshot(summarize=summarize)

    # --- CONFIGURATION API ---
    def get_active_rules(self) -> List[Dict[str, Any]]:
        """Returns the active scoring configurations and rule weights."""
        return get_active_rules()

    def get_features_summary(self) -> Dict[str, int]:
        """Returns a metrics breakdown of all hashed standard attributes currently indexed."""
        return get_features_summary()

    def add_custom_rule(self, name: str, description: str, conditions: List[str], score: float, level: int):
        """
        Dynamically injects a new scoring rule into the deterministic engine.
        Conditions should be a list of required feature strings (e.g., ['NAME_FIRST', 'DOB_YEAR']).
        """
        from er_engine.database.session import SessionLocal
        from er_engine.database.schema import MatchRule
        
        with SessionLocal() as session:
            existing = session.query(MatchRule).filter_by(rule_name=name).first()
            if not existing:
                rule_def = [{"feature_req": c} for c in conditions]
                new_rule = MatchRule(
                    rule_name=name,
                    rule_definition=rule_def,
                    match_level=int(level),
                    score=float(score),
                    is_active=1
                )
                session.add(new_rule)
                session.commit()
                return new_rule.rule_name
            return existing.rule_name

    def rebuild_graph(self) -> int:
        """Retroactively applies all current Match Rules to all existing historical data."""
        return rebuild_graph()

    def purge_all_data(self) -> bool:
        """Completely drops and recreates the database tables."""
        from er_engine.database.session import purge_db
        try:
            purge_db()
            return True
        except Exception:
            return False
