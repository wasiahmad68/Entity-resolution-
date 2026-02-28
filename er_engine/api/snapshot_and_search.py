import logging
from typing import Dict, Any, List
from sqlalchemy import func
from sqlalchemy.orm import joinedload
from er_engine.database.session import SessionLocal
from er_engine.database.schema import Record, EntityRecord, Entity, Relationship, Feature, RecordFeature
from er_engine.core.rules_engine import RulesEngine

logger = logging.getLogger(__name__)

def get_raw_record(data_source: str, record_id: str) -> Dict[str, Any]:
    """Retrieves the exact unmodified JSON payload previously ingested and appends its active Entity ID."""
    with SessionLocal() as session:
        rec = session.query(Record).filter_by(data_source=data_source, record_id=record_id).first()
        if not rec:
            return None
            
        payload = dict(rec.raw_json)
        if "DATA_SOURCE" not in payload: payload["DATA_SOURCE"] = rec.data_source
        if "RECORD_ID" not in payload: payload["RECORD_ID"] = rec.record_id
        
        er_link = session.query(EntityRecord).filter_by(record_id=rec.id).first()
        if er_link:
            payload["_ENTITY_ID"] = er_link.entity_id
        return payload

def get_statistics() -> Dict[str, Any]:
    """Analytics API computing real-time metrics for the ER Dashboard."""
    with SessionLocal() as session:
        total_records = session.query(Record).count()
        total_entities = session.query(Entity).count()
        total_relationships = session.query(Relationship).count()
        
        # Breakdown by Data Source
        ds_counts = session.query(Record.data_source, func.count(Record.id)).group_by(Record.data_source).all()
        data_sources = {k: v for k, v in ds_counts}
        
        # Duplicate Count (High match level groupings inside the same entity)
        duplicates = session.query(EntityRecord.record_id).count() - total_entities if total_entities > 0 else 0
        
        return {
            "total_records": total_records,
            "total_entities": total_entities,
            "total_relationships": total_relationships,
            "data_sources": data_sources,
            "exact_matches_merged": max(0, duplicates)
        }

def analyze_record(data_source: str, record_id: str) -> Dict[str, Any]:
    """
    Given a record, finds its Entity Cluster, all records within that cluster,
    the rules that fired to bring them together, and any contextual Level 3 relationships.
    Uses deep SQL JOINs to efficiently retrieve the profile grouping.
    """
    with SessionLocal() as session:
        base_record = session.query(Record).filter_by(data_source=data_source, record_id=record_id).first()
        if not base_record:
            return {"error": "Record not found"}
            
        er_link = session.query(EntityRecord).filter_by(record_id=base_record.id).first()
        if not er_link:
            return {"error": "Record found but not mapped to an Entity."}
            
        entity_id = er_link.entity_id
        
        # Get all records merged into this identity
        cluster_links = session.query(EntityRecord).join(Record).filter(EntityRecord.entity_id == entity_id).all()
        
        cluster_records = []
        seen = set()
        for link in cluster_links:
            key = (link.record.data_source, link.record.record_id)
            if key not in seen:
                seen.add(key)
                p = dict(link.record.raw_json)
                if "DATA_SOURCE" not in p: p["DATA_SOURCE"] = link.record.data_source
                if "RECORD_ID" not in p: p["RECORD_ID"] = link.record.record_id
                
                cluster_records.append({
                    "data_source": link.record.data_source,
                    "record_id": link.record.record_id,
                    "rule_fired": link.rule_fired,
                    "score": link.score,
                    "payload": p
                })
            
        # Get outside possible/relationship connections (Edges in Graph)
        rels = session.query(Relationship).filter((Relationship.entity_id_1 == entity_id) | (Relationship.entity_id_2 == entity_id)).all()
        related_entities = []
        seen_rels = set()
        for rel in rels:
            other_id = rel.entity_id_2 if rel.entity_id_1 == entity_id else rel.entity_id_1
            if other_id not in seen_rels:
                seen_rels.add(other_id)
                related_entities.append({
                    "related_entity_id": other_id,
                    "rule_fired": rel.rule_fired,
                    "score": rel.score
                })
            
        return {
            "entity_id": entity_id,
            "cluster_records": cluster_records,
            "relationships": related_entities
        }

def get_records_by_entity(entity_id: int) -> List[Dict[str, Any]]:
    """
    Returns all records connected to a specific Entity ID.
    """
    with SessionLocal() as session:
        records = session.query(Record).join(EntityRecord).filter(EntityRecord.entity_id == entity_id).all()
        results = []
        for r in records:
            payload = dict(r.raw_json)
            if "DATA_SOURCE" not in payload: payload["DATA_SOURCE"] = r.data_source
            if "RECORD_ID" not in payload: payload["RECORD_ID"] = r.record_id
            payload["_ENTITY_ID"] = entity_id
            results.append({"data_source": r.data_source, "record_id": r.record_id, "payload": payload})
        return results

def search_records(query_string: str) -> List[Dict[str, Any]]:
    """
    Looks up specific string matches safely inside the pre-computed Feature text cache.
    Splits the query into multiple terms and ensures the Entity profile has ALL terms
    somewhere in its localized feature cluster (e.g. matching Name AND City).
    Returns the distinct Entity profiles associated with the hits.
    """
    terms = [term.strip() for term in query_string.split(',') if term.strip()]
    if not terms:
        # Fallback if no comma used, just split by space for simplicity
        terms = [term.strip() for term in query_string.split(' ') if term.strip()]
        
    with SessionLocal() as session:
        # We start with a base query of all entity IDs
        base_query = session.query(EntityRecord.entity_id)
        
        # For MULTI-FEATURE matching, we intersect entities that have Feature A AND Feature B
        # Since features are normalized vertically in the `features` table, an entity
        # having Name="John" and City="Seattle" means we need to find entities whose
        # graph of features contains both strings.
        
        intersect_queries = []
        for term in terms:
            term_like = f"%{term}%"
            # Subquery: Find entity IDs that have THIS specific term anywhere in their features
            subq = session.query(EntityRecord.entity_id).join(
                RecordFeature, EntityRecord.record_id == RecordFeature.record_id
            ).join(
                Feature, RecordFeature.feature_id == Feature.id
            ).filter(Feature.feature_value.ilike(term_like)).distinct()
            intersect_queries.append(subq)
            
        if not intersect_queries:
            return []
            
        # Intersect all the subqueries so we ONLY get entities that matched EVERY word
        final_query = intersect_queries[0]
        for q in intersect_queries[1:]:
            final_query = final_query.intersect(q)
            
        hits = final_query.limit(50).all()
            
        results = []
        for (e_id,) in hits:
            records = session.query(Record).join(EntityRecord).filter(EntityRecord.entity_id == e_id).all()
            
            matching_records = []
            for r in records:
                payload = dict(r.raw_json)
                if "DATA_SOURCE" not in payload: payload["DATA_SOURCE"] = r.data_source
                if "RECORD_ID" not in payload: payload["RECORD_ID"] = r.record_id
                payload["_ENTITY_ID"] = e_id
                matching_records.append({
                    "data_source": r.data_source, 
                    "record_id": r.record_id, 
                    "payload": payload
                })
                
            results.append({
                "entity_id": e_id,
                "matching_records": matching_records
            })
            
        return results

def get_snapshot(summarize: bool = False):
    """
    Generator that streams the entire database entity by entity to prevent huge memory spikes.
    Yields lists of records grouped by their resolved Entity ID.
    If summarize is True, it strips out PII features and only returns ID mappings and the Rule Fired.
    """
    active_rules = get_active_rules()
    rule_map = {r["name"]: [c["feature_req"] for c in r["conditions"]] for r in active_rules}
    rule_level_map = {r["name"]: r.get("level", 0) for r in active_rules}

    session = SessionLocal()
    try:
        # Pre-compute connected components (Clusters) so related Entities sort together in CSV
        rels = session.query(Relationship).all()
        parent = {}
        def find(i):
            if parent.setdefault(i, i) == i: return i
            parent[i] = find(parent[i])
            return parent[i]
        def union(i, j):
            root_i = find(i)
            root_j = find(j)
            if root_i != root_j:
                parent[root_i] = min(root_i, root_j)
                
        rel_src_map = {}
        for r in rels:
            union(r.entity_id_1, r.entity_id_2)
            rel_src_map[r.entity_id_1] = r.rule_fired # The incoming entity that formed the relation
            
        # Ensure all paths are compressed
        for k in list(parent.keys()): find(k)

        query = session.query(Entity).yield_per(100)
        for entity in query:
            links = session.query(EntityRecord, Record).join(Record).filter(EntityRecord.entity_id == entity.id).all()
            
            cluster_rules = [link.rule_fired for link, rec in links if link.rule_fired]
            cluster_feature_keys = set()
            for r in cluster_rules:
                if r in rule_map:
                    cluster_feature_keys.update(rule_map[r])
                    
            # If this entire Entity was formed via a Level 2/3 Relation, fetch the rule that linked it
            entity_relation_rule = rel_src_map.get(entity.id)
            if entity_relation_rule and entity_relation_rule in rule_map:
                cluster_feature_keys.update(rule_map[entity_relation_rule])
            
            records_out = []
            for link, rec in links:
                cluster_size = len(links)
                if not link.rule_fired:
                    if entity_relation_rule:
                        # It's a Level 2/3 Duplicate
                        match_rule_str = f"Related Duplicate ({entity_relation_rule})"
                        resolve_level = rule_level_map.get(entity_relation_rule, 2)
                    elif cluster_size == 1:
                        match_rule_str = "Unique Record"
                        resolve_level = 1
                    else:
                        match_rule_str = "Duplicate (Target Identity)"
                        resolve_level = 1
                else:
                    match_rule_str = f"Duplicate ({link.rule_fired})"
                    resolve_level = rule_level_map.get(link.rule_fired, 1)

                if summarize:
                    feats = []
                    target_keys = set()
                    
                    if link.rule_fired and link.rule_fired in rule_map:
                        raw_targets = rule_map[link.rule_fired]
                    elif not link.rule_fired and entity_relation_rule and entity_relation_rule in rule_map:
                        raw_targets = rule_map[entity_relation_rule]
                    elif not link.rule_fired:
                        raw_targets = list(cluster_feature_keys)
                    else:
                        raw_targets = []
                        
                    # Clean the phonetic/standardized suffixes off the rule requirements
                    # so they match the raw JSON keys in the snapshot
                    clean_targets = set()
                    for t in raw_targets:
                        clean_targets.add(t.replace("_PHONETIC", "").replace("_STANDARDIZED", ""))
                        
                    for feature_dict in rec.raw_json.get("FEATURES", []):
                        for k, v in feature_dict.items():
                            if k in clean_targets and v:
                                feats.append(f"{k}: {v}")
                                
                    matched_features_str = " | ".join(feats) if feats else "N/A"

                    records_out.append({
                        "DATA_SOURCE": rec.data_source,
                        "RECORD_ID": rec.record_id,
                        "MATCHED_ON_RULE": match_rule_str,
                        "RESOLVE_LEVEL": resolve_level,
                        "MATCHED_FEATURES": matched_features_str
                    })
                else:
                    out_json = dict(rec.raw_json)
                    if "DATA_SOURCE" not in out_json: out_json["DATA_SOURCE"] = rec.data_source
                    if "RECORD_ID" not in out_json: out_json["RECORD_ID"] = rec.record_id
                    out_json["MATCHED_ON_RULE"] = match_rule_str
                    out_json["RESOLVE_LEVEL"] = resolve_level
                    records_out.append(out_json)
                    
            yield {
                "CLUSTER_ID": parent.get(entity.id, entity.id),
                "ENTITY_ID": entity.id,
                "RESOLVED_RECORDS": records_out
            }
    finally:
        session.close()

def get_active_rules() -> List[Dict[str, Any]]:
    """Retrieves the system rules currently configured."""
    from er_engine.core.rules_engine import DEFAULT_RULES
    from er_engine.database.schema import MatchRule
    
    with SessionLocal() as session:
        custom_rules = session.query(MatchRule).filter_by(is_active=1).all()
        formatted_custom = []
        for r in custom_rules:
            formatted_custom.append({
                "name": r.rule_name,
                "description": "Custom Rule (User Injected)",
                "conditions": r.rule_definition,
                "score": r.score,
                "level": r.match_level
            })
            
        return DEFAULT_RULES + formatted_custom

def get_features_summary() -> Dict[str, int]:
    """Scans features logically isolating frequency mapping for analytics."""
    with SessionLocal() as session:
        counts = session.query(Feature.feature_type, func.count(Feature.id)).group_by(Feature.feature_type).all()
        return {k: v for k, v in counts}
