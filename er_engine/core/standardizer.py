import hashlib
import re
from typing import Dict, Any, List
# import metaphone / soundex libs down the line for heavy duty phonetic hashing

def normalize_string(val: str) -> str:
    """Removes special characters, trims whitespace, standardizes casing to upper."""
    if not isinstance(val, str):
        val = str(val)
    # Remove non-alphanumerics, keep spaces for words
    val = re.sub(r'[^a-zA-Z0-9\s]', '', val)
    return " ".join(val.split()).upper()

def parse_dob(dob_str: str) -> Dict[str, str]:
    """Extracts YEAR, MONTH, DAY from a YYYY-MM-DD string seamlessly."""
    parts = {}
    if re.match(r'^\d{4}-\d{2}-\d{2}$', dob_str):
        y, m, d = dob_str.split('-')
        parts['DOB_YEAR'] = y
        parts['DOB_MONTH'] = m
        parts['DOB_DAY'] = d
    return parts

def exact_hash(value: str) -> str:
    """Generates an exact SHA-256 hash of the normalized string."""
    normalized = normalize_string(value)
    return hashlib.sha256(normalized.encode('utf-8')).hexdigest()

def phonetic_hash(value: str, algorithm="metaphone") -> str:
    """
    Returns a phonetic hash. 
    In production this utilizes Double Metaphone, Daitch-Mokotoff, or Soundex algorithms.
    As a fallback standardizer, we strip vowels and identical consecutive consonants.
    """
    normalized = normalize_string(value)
    if not normalized: return ""
    
    # Simple Soundex-like fallback
    first_letter = normalized[0]
    # Remove vowels (except first letter) and H/W/Y
    collapsed = re.sub(r'[AEIOUHWY]', '', normalized[1:])
    # Remove consecutive duplicates
    collapsed = re.sub(r'([B-DF-HJ-NP-TV-Z])\1+', r'\1', collapsed)
    
    phonetic_val = f"{first_letter}{collapsed}"[:4].ljust(4, '0')
    
    return hashlib.sha256(f"PHONETIC_{phonetic_val}".encode('utf-8')).hexdigest()
    
def generate_feature_hashes(feature_obj: Dict[str, Any]) -> List[Dict[str, str]]:
    """
    Takes a single JSON object from the flattened FEATURES array 
    (e.g., {"PASSPORT_NUMBER": "123", "PASSPORT_COUNTRY": "US"})
    and computes the exact, sub-granular, phonetic, and compound scoped hashes.
    """
    generated_features = []
    
    # 1. Sort keys to ensure deterministic formulation of compound hashes 
    # (e.g., Passport + Country order won't break the hash match)
    sorted_keys = sorted(feature_obj.keys())
    
    # 2. Extract and create the Compound Unit Hash
    compound_val = "|".join([f"{k}:{normalize_string(feature_obj[k])}" for k in sorted_keys])
    generated_features.append({
        "feature_type": "COMPOUND_EXACT",
        "feature_value": compound_val,
        "feature_hash": exact_hash(compound_val)
    })
    
    # 3. Create independent hashes for lenient fallback rules
    for k in sorted_keys:
        val = str(feature_obj[k])
        
        # Don't natively index standalone domain types (e.g. matching two people just because they both have NAME_TYPE=PRIMARY)
        if k in ["NAME_TYPE", "ADDR_TYPE", "PHONE_TYPE", "NATIONAL_ID_TYPE", "TAX_ID_COUNTRY", "OTHER_ID_COUNTRY"]:
            continue
            
        generated_features.append({
            "feature_type": k,
            "feature_value": val,
            "feature_hash": exact_hash(val)
        })
        
        # 4. Generate granular phonetics on name characteristics
        if "NAME" in k and k != "NAME_ORG":
            generated_features.append({
                "feature_type": f"{k}_PHONETIC",
                "feature_value": val,
                "feature_hash": phonetic_hash(val)
            })
            
            # Alphabetize the name components to catch Word Transpositions 
            # (e.g. "Wasi Ahmad" and "Ahmad Wasi" both become "Ahmad Wasi")
            sorted_val = " ".join(sorted(val.split()))
            generated_features.append({
                "feature_type": f"{k}_PHONETIC_SORTED",
                "feature_value": sorted_val,
                "feature_hash": phonetic_hash(sorted_val)
            })
            
        # 5. Extract monolithic properties like dates into isolated buckets
        if k in ("DATE_OF_BIRTH", "DATE_OF_DEATH", "REGISTRATION_DATE"):
            parts = parse_dob(val)
            for pk, pv in parts.items():
                generated_features.append({
                    "feature_type": pk,
                    "feature_value": pv,
                    "feature_hash": exact_hash(pv)
                })
                
    return generated_features
