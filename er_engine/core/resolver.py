import logging
from typing import Dict, Any, List
from sqlalchemy.orm import Session

from er_engine.database.schema import Record, Feature, RecordFeature, Entity, EntityRecord, Relationship
from er_engine.core.standardizer import generate_feature_hashes
from er_engine.core.rules_engine import RulesEngine
from er_engine.core.exceptions import InvalidJSONSchemaError

logger = logging.getLogger(__name__)

class Resolver:
    """
    The Core engine handling High-Throughput record ingestion and resolution logic.
    Executes standardizer extraction, candidate pooling, and Rule Engine evaluation.
    """
    def __init__(self, session: Session, rules_engine: RulesEngine = None):
        self.session = session
        if rules_engine:
            self.rules = rules_engine
        else:
            from er_engine.core.rules_engine import DEFAULT_RULES
            from er_engine.database.schema import MatchRule
            
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
            
            self.rules = RulesEngine(rules_config=DEFAULT_RULES + formatted_custom)
        
    def _find_candidates(self, extracted_features: List[Dict[str, str]]) -> Dict[int, List[Dict[str, str]]]:
        """
        Searches the datastore for any records that share AT LEAST ONE exact/phonetic hash.
        This provides the target candidates for deep Rule Engine permutations.
        """
        hashes = [f["feature_hash"] for f in extracted_features]
        if not hashes:
            return {}
            
        # Fast indexed DB lookup
        candidate_links = self.session.query(RecordFeature, Feature)\
            .join(Feature, RecordFeature.feature_id == Feature.id)\
            .filter(Feature.feature_hash.in_(hashes)).all()
            
        candidates = {}
        for link, feat in candidate_links:
            rec_id = link.record_id
            if rec_id not in candidates:
                candidates[rec_id] = []
            candidates[rec_id].append({"feature_type": feat.feature_type, "feature_hash": feat.feature_hash})
            
        return candidates

    def ingest_record(self, data_source: str, record_id: str, raw_json: Dict[str, Any]) -> int:
        """
        Upserts a record, extracts Senzing flattened features, finds candidates, and executes Match Rules.
        """
        features_array = raw_json.get("FEATURES", [])
        if not isinstance(features_array, list):
            raise InvalidJSONSchemaError("JSON payload must contain a flat 'FEATURES' array per the Senzing spec.")
            
        # Upsert: Full Override per specification
        existing = self.session.query(Record).filter_by(data_source=data_source, record_id=record_id).first()
        if existing:
            # 1. Sever Feature bindings
            self.session.query(RecordFeature).filter_by(record_id=existing.id).delete()
            
            # 2. Sever Entity bindings and orphan cleanup
            er = self.session.query(EntityRecord).filter_by(record_id=existing.id).first()
            if er:
                old_entity_id = er.entity_id
                self.session.delete(er)
                self.session.flush()
                
                # If no other records share this entity, destroy the entity node itself
                remaining_in_entity = self.session.query(EntityRecord).filter_by(entity_id=old_entity_id).count()
                if remaining_in_entity == 0:
                    self.session.query(Relationship).filter((Relationship.entity_id_1 == old_entity_id) | (Relationship.entity_id_2 == old_entity_id)).delete()
                    self.session.query(Entity).filter_by(id=old_entity_id).delete()
                    
            record = existing
            record.raw_json = raw_json
        else:
            record = Record(data_source=data_source, record_id=record_id, raw_json=raw_json)
            self.session.add(record)
            self.session.flush() 
            
        # Standardizer Pipeline for Feature Extraction
        new_features = []
        for feat_obj in features_array:
            if isinstance(feat_obj, dict):
                new_features.extend(generate_feature_hashes(feat_obj))
            
        # Persist features incrementally
        for feat_dict in new_features:
            db_feat = self.session.query(Feature).filter_by(feature_hash=feat_dict["feature_hash"]).first()
            if not db_feat:
                db_feat = Feature(feature_type=feat_dict["feature_type"], feature_hash=feat_dict["feature_hash"], feature_value=feat_dict["feature_value"])
                self.session.add(db_feat)
                self.session.flush()
                
            self.session.add(RecordFeature(record_id=record.id, feature_id=db_feat.id))
            
        # Resolution Match Pooling
        candidates = self._find_candidates(new_features)
        
        target_entity_id = None
        
        best_rule_fired = None
        best_rule_score = None
        
        for cand_record_id, cand_features in candidates.items():
            if cand_record_id == record.id:
                continue 
                
            # Deep match evaluation against Rules Engine and ML override
            match_result = self.rules.evaluate_records(new_features, cand_features)
            
            if match_result["matched"]:
                level = match_result["level"]
                cand_er = self.session.query(EntityRecord).filter_by(record_id=cand_record_id).first()
                if not cand_er:
                    continue 
                
                # Handling Match Levels
                if level == 1:
                    # Same identity -> Merge Entity
                    target_entity_id = cand_er.entity_id
                    best_rule_fired = match_result["rule_fired"]
                    best_rule_score = match_result["score"]
                elif level in (2, 3):
                    # Relationship / Possible Match -> Form Graph Edge
                    if not target_entity_id:
                        e = Entity()
                        self.session.add(e)
                        self.session.flush()
                        target_entity_id = e.id
                        
                    rel = Relationship(
                        entity_id_1=target_entity_id,
                        entity_id_2=cand_er.entity_id,
                        rule_fired=match_result["rule_fired"],
                        score=match_result["score"]
                    )
                    self.session.add(rel)
                    
        # No Exact Matches? Create isolated entity
        if not target_entity_id:
            e = Entity()
            self.session.add(e)
            self.session.flush()
            target_entity_id = e.id
            
        self.session.add(EntityRecord(entity_id=target_entity_id, record_id=record.id, rule_fired=best_rule_fired, score=best_rule_score))
        self.session.commit()
        
        return target_entity_id

    def re_evaluate_database(self) -> int:
        """
        Wipes all unified entities, entity_records, and relationships, 
        then safely loops over every raw record payload to re-ingest and re-evaluate 
        against the latest Rules Engine Configuration.
        """
        logger.info("Starting complete database re-evaluation sequence...")
        # Clean down entities and mappings
        self.session.query(Relationship).delete()
        self.session.query(EntityRecord).delete()
        self.session.query(Entity).delete()
        self.session.commit()
        
        all_records = self.session.query(Record).all()
        for rec in all_records:
            # Re-run purely from JSON, letting logic recalculate hashes and candidates
            self.ingest_record(rec.data_source, rec.record_id, rec.raw_json)
            
        logger.info(f"Re-evaluation complete. Processed {len(all_records)} records.")
        return len(all_records)
