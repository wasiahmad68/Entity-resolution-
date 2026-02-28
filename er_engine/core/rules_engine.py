import logging
from typing import List, Dict, Any

logger = logging.getLogger(__name__)

# Pre-populated Default Rules (Senzing Style)
DEFAULT_RULES = [
    {
        "name": "RULE_EXACT_NATIONAL_ID",
        "description": "Matches if both records share exactly the same National ID payload.",
        "conditions": [{"feature_req": "NATIONAL_ID_NUMBER"}],
        "score": 100.0,
        "level": 1
    },
    {
        "name": "RULE_EXACT_PASSPORT",
        "description": "Matches on identical Passport details.",
        "conditions": [{"feature_req": "PASSPORT_NUMBER"}, {"feature_req": "PASSPORT_COUNTRY"}],
        "score": 100.0,
        "level": 1
    },
    {
        "name": "RULE_EXACT_NAME_DOB_ADDR",
        "description": "Matches identical Name (Phonetically), DOB, and Address.",
        # Because standardizer automatically suffixed _PHONETIC, we can query it directly in rules!
        "conditions": [{"feature_req": "NAME_FIRST_PHONETIC"}, {"feature_req": "NAME_LAST_PHONETIC"}, {"feature_req": "DOB_YEAR"}, {"feature_req": "ADDR_CITY"}],
        "score": 90.0,
        "level": 1
    },
    {
        "name": "RULE_POSSIBLE_NAME_ADDR",
        "description": "Possible match on fuzzy name and exact address.",
        "conditions": [{"feature_req": "NAME_FIRST_PHONETIC"}, {"feature_req": "ADDR_CITY"}],
        "score": 75.0,
        "level": 2
    },
    {
        "name": "RULE_RELATIONSHIP_PHONE",
        "description": "Shared phone number triggers Relationship link regardless of name.",
        "conditions": [{"feature_req": "PHONE_NUMBER"}],
        "score": 50.0,
        "level": 3
    }
]

class RulesEngine:
    def __init__(self, rules_config=None):
        self.rules = rules_config if rules_config else DEFAULT_RULES
        self.version = "1.0.0"

    def _get_hash_dict(self, features: List[Dict[str, str]]) -> Dict[str, set]:
        """
        Converts a list of record features into a dictionary for O(1) intersection testing.
        e.g., {"NAME_FIRST_PHONETIC": {"hash1", "hash2"}}
        """
        hash_map = {}
        for feat in features:
            f_type = feat.get("feature_type")
            f_hash = feat.get("feature_hash")
            if f_type not in hash_map:
                hash_map[f_type] = set()
            hash_map[f_type].add(f_hash)
        return hash_map

    def evaluate_match_ml_hook(self, features_a: List[Dict[str, str]], features_b: List[Dict[str, str]], deterministic_score: float, deterministic_level: int) -> tuple:
        """
        ML Extensibility Hook. 
        In v1.0, this passes the score purely on deterministic logic.
        In later versions, this is the exact spot developers will pass features_a and features_b 
        into Xenon/XGBoost to override the rule execution score with probability floats.
        """
        # Ex: ml_score = my_xgb_model.predict(features_a, features_b)
        # if ml_score > 0.95: return 100.0, 1
        return deterministic_score, deterministic_level

    def evaluate_records(self, features_a: List[Dict[str, str]], features_b: List[Dict[str, str]]) -> Dict[str, Any]:
        """
        Extremely fast deterministic evaluation engine. 
        It compares two sets of standardized arrays using Set Intersections.
        """
        map_a = self._get_hash_dict(features_a)
        map_b = self._get_hash_dict(features_b)
        
        best_rule = None
        best_score = 0.0
        best_level = 0
        
        for rule in self.rules:
            rule_matched = True
            
            # Every condition in the rule must have a shared Hash Intersect between Record A and Record B
            for condition in rule.get("conditions", []):
                req_type = condition["feature_req"]
                
                hashes_a = map_a.get(req_type, set())
                hashes_b = map_b.get(req_type, set())
                
                # Fast set intersection
                if not hashes_a.intersection(hashes_b):
                    rule_matched = False
                    break
                    
            if rule_matched:
                rule_score = rule.get("score", 0.0)
                if rule_score > best_score:
                    best_score = rule_score
                    best_rule = rule.get("name")
                    best_level = rule.get("level", 0)
                    
        # ML Hook Layer before final return
        final_score, final_level = self.evaluate_match_ml_hook(features_a, features_b, best_score, best_level)
        
        return {
            "matched": final_level > 0,
            "rule_fired": best_rule,
            "score": final_score,
            "level": final_level
        }
