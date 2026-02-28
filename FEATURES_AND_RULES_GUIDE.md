# Senzing Features & Match Rules Guide

This engine conforms to a strict, flat array architecture for maximum determinism and scalability. The system expects a list of standardized JSON objects nested within the `FEATURES` array key.

## 1. Supported Features Exhausive Guide

When ingesting records via API or the Streamlit drag-and-drop ingestion portal, the JSON must structure data like this:

```json
{
  "DATA_SOURCE": "CRM",
  "RECORD_ID": "USER_123",
  "FEATURES": [
    { "NAME_TYPE": "PRIMARY", "NAME_FIRST": "John", "NAME_LAST": "Doe" },
    { "ADDR_TYPE": "HOME", "ADDR_CITY": "Seattle", "ADDR_POSTAL_CODE": "98101" }
  ]
}
```

Below is the exhaustive list of natively recognized and standardized identifiers you can map to.

### 1a. Names
**Fields:** `NAME_TYPE`, `NAME_FIRST`, `NAME_MIDDLE`, `NAME_LAST`, `NAME_PREFIX`, `NAME_SUFFIX`, `NAME_ORG`, `NAME_FULL`
**Behavior:** Supplying any of these fields automatically breaks them down, strips whitespace, capitalizes them, AND generates **Phonetic Hashes** for robust rule matching.
**Example:**
```json
{ "NAME_TYPE": "AKA", "NAME_FIRST": "JONATHAN", "NAME_LAST": "DOE" }
```

### 1b. Addresses
**Fields:** `ADDR_TYPE`, `ADDR_LINE1` to `ADDR_LINE6`, `ADDR_CITY`, `ADDR_STATE`, `ADDR_POSTAL_CODE`, `ADDR_COUNTRY`, `ADDR_FULL`
**Example:**
```json
{ "ADDR_TYPE": "BUSINESS", "ADDR_LINE1": "123 Tech Blvd", "ADDR_CITY": "Austin", "ADDR_STATE": "TX" }
```

### 1c. Contact Info
**Fields:** `PHONE_TYPE`, `PHONE_NUMBER`, `EMAIL_ADDRESS`
**Example:**
```json
{ "PHONE_TYPE": "MOBILE", "PHONE_NUMBER": "+1 (555) 123-4567" },
{ "EMAIL_ADDRESS": "jon.doe@example.com" }
```

### 1d. Characteristics & Demographics
**Fields:** `GENDER`, `DATE_OF_BIRTH`, `DATE_OF_DEATH`, `NATIONALITY`, `CITIZENSHIP`, `PLACE_OF_BIRTH`, `REGISTRATION_DATE`, `REGISTRATION_COUNTRY`
**Behavior:** Parsing a `DATE_OF_BIRTH` like `1990-05-15` automatically creates granular `DOB_YEAR`, `DOB_MONTH`, and `DOB_DAY` indexed hashes, allowing rules to flag "Same Year, Same Month," ignoring a typo in the exact day.
**Example:**
```json
{ "DATE_OF_BIRTH": "1990-05-15", "GENDER": "MALE", "NATIONALITY": "US" }
```

### 1e. Identifiers (Compound Scoping)
**Fields:** `PASSPORT_NUMBER/COUNTRY`, `DRIVERS_LICENSE_NUMBER/STATE`, `SSN_NUMBER`, `NATIONAL_ID_NUMBER/TYPE/COUNTRY`, `TAX_ID_NUMBER/TYPE/COUNTRY`, `OTHER_ID_NUMBER/TYPE/COUNTRY`, `ACCOUNT_NUMBER/DOMAIN`, `DUNS_NUMBER`, `NPI_NUMBER`, `LEI_NUMBER`
**Behavior:** Extremely important! Including `PASSPORT_NUMBER` and `PASSPORT_COUNTRY` in the *same* feature object links them into a single secure **COMPOUND HASH**.
**Example:**
```json
{ "PASSPORT_NUMBER": "P8712211", "PASSPORT_COUNTRY": "FR" }
```

### 1f. Web/Social Handles
**Fields:** `WEBSITE_ADDRESS`, `LINKEDIN`, `FACEBOOK`, `TWITTER`, `INSTAGRAM`, `SKYPE`, `WHATSAPP`, `SIGNAL`, `TELEGRAM`, `TANGO`, `VIBER`, `WECHAT`
**Example:**
```json
{ "LINKEDIN": "in/jondoe" }
```

---

## 2. Rule Configuration & Logic Overrides

Because the standardizer creates incredibly rich permutations behind the scenes, writing rules simply relies on referencing the specific target hashes required.

```json
{
    "name": "RULE_EXACT_NAME_DOB_ADDR",
    "description": "Matches identical Name (Phonetically), Exact DOB, and Exact Address City.",
    "conditions": [
        {"feature_req": "NAME_FIRST_PHONETIC"}, 
        {"feature_req": "NAME_LAST_PHONETIC"}, 
        {"feature_req": "DOB_YEAR"}, 
        {"feature_req": "ADDR_CITY"}
    ],
    "score": 90.0,
    "level": 1
}
```

### Rule Explanation
1. `NAME_FIRST_PHONETIC`: Asking for the `_PHONETIC` extracted hash ensures that "Jhon" and "John" will register as a perfect match at O(1) intersection speed!
2. `DOB_YEAR`: We don't ask for `DATE_OF_BIRTH` implicitly, we only request `DOB_YEAR` granularity. Thus, `1990-05-15` and `1990-06-22` will trigger a match safely contexting on Name/Address.
3. `ADDR_CITY`: Asking merely for exact city match.

Because the conditions target the subset of elements, you can easily combine these rules to output `Level 1` (Merge into Same identity), `Level 2` (Possible Match), or `Level 3` (Relationship Context - Graph drawing).

## 3. Extending/Custom Features
You can supply entirely custom labels outside of the schema natively!
```json
{ "LOYALTY_CARD_ID": "XXXX-99" }
```
The Engine will automatically hash it and index it transparently. You can immediately write a Match Rule utilizing `{"feature_req": "LOYALTY_CARD_ID"}` to leverage it natively in the scoring algorithm without editing a single line of python core!
