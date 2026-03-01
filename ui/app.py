import streamlit as st
import pandas as pd
import json
import os
import sys

# Ensure library path so modules are found globally regardless of where streamlit is called
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

try:
    from streamlit_agraph import agraph, Node, Edge, Config
except ImportError:
    Node, Edge, Config, agraph = None, None, None, None

from er_engine.master_resolution import MasterResolutionEngine

st.set_page_config(page_title="Professional ER Engine", layout="wide", initial_sidebar_state="expanded")

# Initialize Master Engine (handles DB connection seamlessly)
engine = MasterResolutionEngine()

def reorder_profile_keys(record_dict):
    """
    Reorders keys so that backend-injected metadata fields like _ENTITY_ID, 
    _MATCH_LEVEL, and _MATCH_RULE appear immediately after RECORD_ID for UI visibility.
    """
    if not isinstance(record_dict, dict):
        return record_dict
        
    ordered = {}
    
    # Core identifying fields first
    if "DATA_SOURCE" in record_dict:
        ordered["DATA_SOURCE"] = record_dict.pop("DATA_SOURCE")
    if "RECORD_ID" in record_dict:
        ordered["RECORD_ID"] = record_dict.pop("RECORD_ID")
        
    # Inject metadata immediately after
    for meta_key in ["_ENTITY_ID", "_MATCH_LEVEL", "_MATCH_RULE"]:
        if meta_key in record_dict:
            ordered[meta_key] = record_dict.pop(meta_key)
            
    # Add everything else back
    ordered.update(record_dict)
    return ordered


def ds_selector(label, key, help_text=None):
    """
    Helper: renders a locked info banner when a global DS is active,
    or a free text_input when 'ALL' is selected.
    Returns the resolved data source string.
    """
    if st.session_state["active_ds"] != "ALL":
        ds_val = st.session_state["active_ds"]
        st.info(f"🔒 **{label}** locked to: `{ds_val}` *(set via Global Active Data Source)*")
        return ds_val
    else:
        return st.text_input(label, value="", key=key, help=help_text)


def render_record_views(records):
    """
    Renders a unified UI block that allows switching between raw JSON 
    and a tabular DataFrame view of the record payloads.
    """
    if not isinstance(records, list):
        records = [records]
        
    t1, t2, t3 = st.tabs(["Raw JSON", "DataFrame Table", "YAML View"])
    
    # Reorder keys for all raw formats
    ordered_records = [reorder_profile_keys(r) for r in records]
    
    with t1:
        st.json(ordered_records)
        
    with t2:
        df_rows = []
        for rec in ordered_records:
            # Shallow clone root fields
            row = {k: v for k, v in rec.items() if k != "FEATURES"}
            
            # Extract features list into columns (taking the first value of each type if multiple exist)
            if "FEATURES" in rec and isinstance(rec["FEATURES"], list):
                for f_obj in rec["FEATURES"]:
                    for k, v in f_obj.items():
                        if k in row:
                            row[k] = f"{row[k]} | {v}"
                        else:
                            row[k] = v
            df_rows.append(row)
            
        df = pd.DataFrame(df_rows)
        st.dataframe(df, use_container_width=True)

    with t3:
        import yaml
        
        if len(ordered_records) > 1:
            num_cols = min(len(ordered_records), 4)
            cols = st.columns(num_cols)
            
            for idx, rec in enumerate(ordered_records):
                col_idx = idx % num_cols
                with cols[col_idx]:
                    st.markdown(f"**Record {idx + 1}**")
                    yaml_str = yaml.dump(rec, default_flow_style=False, sort_keys=False)
                    st.code(yaml_str, language="yaml")
        else:
            yaml_str = yaml.dump(ordered_records[0] if ordered_records else {}, default_flow_style=False, sort_keys=False)
            st.code(yaml_str, language="yaml")


def main():
    st.sidebar.title("Engine Navigation")
    st.sidebar.markdown("Advanced Entity Resolution & Linking")
    
    # Global Data Source Selector
    allowed_sources = engine.get_allowed_sources()
    ds_options = ["ALL"] + allowed_sources
    
    if "active_ds" not in st.session_state:
        st.session_state["active_ds"] = "ALL"
        
    st.sidebar.divider()
    active_ds = st.sidebar.selectbox(
        "Global Active Data Source", 
        options=ds_options, 
        index=ds_options.index(st.session_state["active_ds"]) if st.session_state["active_ds"] in ds_options else 0,
        help="Locks all target operations (Ingest, Fetch, Delete) to this specific Data Source. Select 'ALL' to bypass."
    )
    st.session_state["active_ds"] = active_ds
    st.sidebar.divider()
    
    tab_dashboard, tab_ingestion, tab_management, tab_search, tab_graph, tab_config = st.tabs([
        "Dashboard Analytics", "Data Ingestion", "Record Management", 
        "Entity Search", "Deep Analysis Graph", "Configuration"
    ])

    # ─────────────────────────────────────────────────────────────
    # DASHBOARD
    # ─────────────────────────────────────────────────────────────
    with tab_dashboard:
        st.title("ER System Health & Analytics")
        try:
            stats = engine.get_statistics()
            col1, col2, col3, col4 = st.columns(4)
            col1.metric("Total Ingested Records", stats.get("total_records", 0))
            col2.metric("Total Unique Entities", stats.get("total_entities", 0))
            col3.metric("Records Merged (Level 1)", stats.get("exact_matches_merged", 0))
            col4.metric("Relationships Found", stats.get("total_relationships", 0))
            
            st.divider()
            
            st.subheader("Records Ingested per Data Source")
            if stats.get("data_sources"):
                df = pd.DataFrame(list(stats["data_sources"].items()), columns=["Source", "Volume"]).set_index("Source")
                st.bar_chart(df)
            else:
                st.info("No records ingested yet.")
                
        except Exception as e:
            st.error(f"Failed to fetch statistics. Is the engine initialized? Error: {e}")

    # ─────────────────────────────────────────────────────────────
    # DATA INGESTION
    # ─────────────────────────────────────────────────────────────
    with tab_ingestion:
        st.title("Data Ingestion")
        st.markdown("Upload standard Senzing flat-schema JSON files. The engine will hash, standardize, and resolve entities automatically.")
        
        with st.expander("View Expected JSON Format", expanded=False):
            t1, t2 = st.tabs(["Standard JSON (Array `[]`)", "JSON Lines (Newline Delimited `{} \\n {}`)"])
            
            with t1:
                st.code('''[
  {
    "DATA_SOURCE": "SOURCE_NAME",
    "RECORD_ID": "UNIQUE_ID_123",
    "FEATURES": [
      {
        "RECORD_TYPE": "PERSON"
      },
      {
        "NAME_TYPE": "PRIMARY",
        "NAME_FIRST": "John",
        "NAME_LAST": "Doe"
      }
    ]
  },
  {
    "DATA_SOURCE": "SOURCE_NAME",
    "RECORD_ID": "UNIQUE_ID_124",
    "FEATURES": [
      {
        "NAME_FIRST": "Jane"
      }
    ]
  }
]''', language="json")
                
            with t2:
                st.code('''{"DATA_SOURCE": "SOURCE_NAME", "RECORD_ID": "UNIQUE_ID_123", "FEATURES": [{"RECORD_TYPE": "PERSON"}, {"NAME_FIRST": "John"}]}
{"DATA_SOURCE": "SOURCE_NAME", "RECORD_ID": "UNIQUE_ID_124", "FEATURES": [{"NAME_FIRST": "Jane"}]}''', language="json")

        uploaded_file = st.file_uploader("Upload Batch JSON", type=["json", "jsonl"])
        
        if uploaded_file is not None:
            try:
                content = uploaded_file.getvalue().decode('utf-8')
                try:
                    data = json.loads(content)
                    if not isinstance(data, list):
                        data = [data]
                except json.JSONDecodeError:
                    data = []
                    for line in content.splitlines():
                        line = line.strip()
                        if line:
                            data.append(json.loads(line))
                
                st.success(f"Recognized payload with {len(data)} profiles.")
                
                workers = st.slider("Concurrency Threads", min_value=1, max_value=16, value=4)
                if st.button("Execute Batch Ingestion"):
                    with st.spinner(f"Ingesting via {workers} threads..."):
                        if st.session_state["active_ds"] != "ALL":
                            for r in data:
                                if r.get("DATA_SOURCE") != st.session_state["active_ds"]:
                                    raise ValueError(f"Payload rejected. File contains Record for Source '{r.get('DATA_SOURCE')}', but UI is locked to '{st.session_state['active_ds']}'.")
                                    
                        res = engine.ingest_bulk(data, max_workers=workers)
                        st.balloons()
                        st.success(f"Success! {res['processed']} records resolved into graph.")
            except Exception as e:
                st.error(f"Batch Failed! Error logs: {e}")

    # ─────────────────────────────────────────────────────────────
    # RECORD MANAGEMENT
    # ─────────────────────────────────────────────────────────────
    with tab_management:
        st.title("Single Record Operations")
        st.markdown("Perform isolated overrides, fetches, and deletions.")
        
        tab1, tab2, tab_duplicates, tab3 = st.tabs([
            "Single Ingestion", "Fetch Raw Record", "Find Duplicate Records", "Delete Record"
        ])
        
        # ── Single Ingestion ──────────────────────────────────────
        with tab1:
            st.subheader("Upsert Single Record")
            ds = ds_selector("Data Source", key="ingest_ds")
            rid = st.text_input("Record ID", key="ingest_rid")
            payload_str = st.text_area(
                "JSON Payload (Must contain FEATURES list)",
                height=200,
                value='{"FEATURES": [{"NAME_FIRST": "John", "NAME_LAST": "Doe"}]}'
            )
            if st.button("Ingest Record"):
                try:
                    payload = json.loads(payload_str)
                    engine.ingest_record(ds, rid, payload)
                    st.success(f"Successfully ingested {ds}/{rid}")
                except Exception as e:
                    st.error(f"Ingestion failed: {e}")

        # ── Fetch Raw Record ──────────────────────────────────────
        with tab2:
            st.subheader("Fetch Raw Record Payload")
            f_ds = ds_selector("Data Source", key="fetch_ds")
            
            f_mode = st.radio(
                "Fetch Input Method",
                ["Manual Input (Comma-Separated)", "File Upload (CSV/JSON/JSONL)"],
                key="fetch_radio"
            )
            
            rids = []
            if "Manual" in f_mode:
                f_rids_str = st.text_area("Record IDs (comma-separated)", key="fetch_rid", placeholder="123, 456, 789")
                if f_rids_str:
                    rids = [r.strip() for r in f_rids_str.split(",") if r.strip()]
            else:
                f_file = st.file_uploader(
                    "Upload file with `record_id` or `RECORD_ID` column/keys",
                    type=["csv", "json", "jsonl"],
                    key="fetch_file"
                )
                if f_file:
                    try:
                        if f_file.name.endswith(".csv"):
                            df_f = pd.read_csv(f_file)
                            if "record_id" in df_f.columns:
                                rids = df_f["record_id"].astype(str).tolist()
                            elif "RECORD_ID" in df_f.columns:
                                rids = df_f["RECORD_ID"].astype(str).tolist()
                            else:
                                st.error("CSV must contain a 'record_id' or 'RECORD_ID' column.")
                        elif f_file.name.endswith(".json"):
                            f_data = json.load(f_file)
                            if isinstance(f_data, list):
                                rids = [
                                    str(item.get("record_id", item.get("RECORD_ID")))
                                    for item in f_data
                                    if item.get("record_id") or item.get("RECORD_ID")
                                ]
                        elif f_file.name.endswith(".jsonl"):
                            content = f_file.getvalue().decode("utf-8")
                            for line in content.splitlines():
                                if line.strip():
                                    item = json.loads(line)
                                    r = item.get("record_id", item.get("RECORD_ID"))
                                    if r:
                                        rids.append(str(r))
                    except Exception as e:
                        st.error(f"Failed to read file: {e}")

            if st.button("Fetch"):
                if not rids:
                    st.warning("Please provide at least one Record ID.")
                else:
                    fetched_records = []
                    not_found = []
                    for r in rids:
                        res = engine.get_raw_record(f_ds, r)
                        if res:
                            fetched_records.append(res)
                        else:
                            not_found.append(r)
                            
                    if fetched_records:
                        render_record_views(fetched_records)
                    
                    if not_found:
                        st.warning(f"Could not find the following records: {', '.join(not_found)}")
                    elif not fetched_records:
                        st.error("No records found.")

        # ── Find Duplicate Records ────────────────────────────────
        with tab_duplicates:
            st.subheader("Find Associated Duplicate Records")
            st.markdown("Search for an ID to find all direct duplicates and related records, aggregated into a single tabular view.")

            dup_ds = ds_selector("Data Source", key="dup_ds")
            
            dup_mode = st.radio(
                "Search Input Method",
                ["Manual Input (Comma-Separated)", "File Upload (CSV/JSON/JSONL)"],
                key="dup_radio"
            )
            
            dup_rids = []
            if "Manual" in dup_mode:
                dup_rids_str = st.text_area("Record IDs (comma-separated)", key="dup_rid", placeholder="123, 456, 789")
                if dup_rids_str:
                    dup_rids = [r.strip() for r in dup_rids_str.split(",") if r.strip()]
            else:
                dup_file = st.file_uploader(
                    "Upload file with `record_id` or `RECORD_ID` column/keys",
                    type=["csv", "json", "jsonl"],
                    key="dup_file"
                )
                if dup_file:
                    try:
                        if dup_file.name.endswith(".csv"):
                            df_dup = pd.read_csv(dup_file)
                            if "record_id" in df_dup.columns:
                                dup_rids = df_dup["record_id"].astype(str).tolist()
                            elif "RECORD_ID" in df_dup.columns:
                                dup_rids = df_dup["RECORD_ID"].astype(str).tolist()
                            else:
                                st.error("CSV must contain a 'record_id' or 'RECORD_ID' column.")
                        elif dup_file.name.endswith(".json"):
                            dup_data = json.load(dup_file)
                            if isinstance(dup_data, list):
                                dup_rids = [
                                    str(item.get("record_id", item.get("RECORD_ID")))
                                    for item in dup_data
                                    if item.get("record_id") or item.get("RECORD_ID")
                                ]
                        elif dup_file.name.endswith(".jsonl"):
                            content = dup_file.getvalue().decode("utf-8")
                            for line in content.splitlines():
                                if line.strip():
                                    item = json.loads(line)
                                    r = item.get("record_id", item.get("RECORD_ID"))
                                    if r:
                                        dup_rids.append(str(r))
                    except Exception as e:
                        st.error(f"Failed to read file: {e}")
            
            if st.button("Find Duplicates"):
                if not dup_ds or not dup_rids:
                    st.warning("Please provide both Data Source and at least one Record ID.")
                else:
                    all_associated_records = []
                    seen_keys = set()
                    
                    from er_engine.core.rules_engine import DEFAULT_RULES
                    rule_lvl_map = {r["name"]: r["level"] for r in DEFAULT_RULES}
                    
                    for dup_rid in dup_rids:
                        analysis = engine.analyze(dup_ds, dup_rid)
                        if "error" in analysis:
                            st.error(f"Error for Record `{dup_rid}`: {analysis['error']}")
                            continue
                            
                        st.success(f"Successfully clustered based on Entity Profile: {analysis['entity_id']} (from `{dup_rid}`)")
                        
                        associated_records = []
                        seen_keys = set()
                        
                        # Anchor record
                        anchor_rec = engine.get_raw_record(dup_ds, dup_rid)
                        if anchor_rec:
                            anchor_key = f"{dup_ds}_{dup_rid}"
                            if anchor_key not in seen_keys:
                                seen_keys.add(anchor_key)
                                anchor_rec["_MATCH_LEVEL"] = "ANCHOR (Level 0)"
                                anchor_rec["_MATCH_RULE"] = "N/A"
                                associated_records.append(anchor_rec)
                        
                        # Level 1 Matches
                        for rec in analysis.get('cluster_records', []):
                            if rec['data_source'] == dup_ds and str(rec['record_id']) == str(dup_rid):
                                continue
                                
                            rec_key = f"{rec['data_source']}_{rec['record_id']}"
                            if rec_key not in seen_keys:
                                seen_keys.add(rec_key)
                                raw_data = engine.get_raw_record(rec['data_source'], rec['record_id'])
                                if raw_data:
                                    raw_data["_MATCH_LEVEL"] = "MERGED (Level 1)"
                                    raw_data["_MATCH_RULE"] = rec.get("rule_fired", "")
                                    associated_records.append(raw_data)

                        # Level 2/3 Matches (Relationships)
                        for rel in analysis.get('relationships', []):
                            rel_recs = engine.get_records_by_entity(rel['related_entity_id'])
                            for rr in rel_recs:
                                rr_key = f"{rr['data_source']}_{rr['record_id']}"
                                if rr_key not in seen_keys:
                                    seen_keys.add(rr_key)
                                    try:
                                        raw_data = rr["payload"] if isinstance(rr["payload"], dict) else json.loads(rr["payload"])
                                        r_name = rel.get("rule_fired", "RELATED")
                                        r_level = rule_lvl_map.get(r_name, "2/3")
                                        raw_data["_MATCH_LEVEL"] = f"RELATED (Level {r_level})"
                                        raw_data["_MATCH_RULE"] = r_name
                                        associated_records.append(raw_data)
                                    except Exception:
                                        pass
                                        
                        if len(associated_records) > 0:
                            st.markdown(f"#### Results for Record ID: `{dup_rid}`")
                            render_record_views(associated_records)
                            st.divider()
                            all_associated_records.extend(associated_records)
                        else:
                            st.warning(f"No records found attached to `{dup_rid}`.")
                            
                    if len(all_associated_records) > 0:
                        st.markdown("### Combined Export")
                        df_rows = []
                        for raw_rec in all_associated_records:
                            rec = reorder_profile_keys(raw_rec)
                            row = {k: v for k, v in rec.items() if k != "FEATURES"}
                            if "FEATURES" in rec and isinstance(rec["FEATURES"], list):
                                for f_obj in rec["FEATURES"]:
                                    for k, v in f_obj.items():
                                        if k in row:
                                            row[k] = f"{row[k]} | {v}"
                                        else:
                                            row[k] = v
                            df_rows.append(row)
                        df_combined = pd.DataFrame(df_rows)
                        csv_data = df_combined.to_csv(index=False).encode('utf-8')
                        st.download_button(
                            label="Download All Combined Results (CSV)",
                            data=csv_data,
                            file_name='duplicate_results_combined.csv',
                            mime='text/csv'
                        )

        # ── Delete Records ────────────────────────────────────────
        with tab3:
            st.subheader("Delete Records")
            d_ds = ds_selector("Data Source", key="del_ds", help_text="Target Data Source for these deletions.")
            
            del_mode = st.radio(
                "Deletion Input Method",
                ["Manual Input (Comma-Separated)", "File Upload (CSV/JSON/JSONL)"]
            )
            
            record_ids_to_del = []
            if "Manual" in del_mode:
                d_rids_str = st.text_area("Record IDs (comma-separated)", key="del_rid_manual", placeholder="123, 456, 789")
                if d_rids_str:
                    record_ids_to_del = [r.strip() for r in d_rids_str.split(",") if r.strip()]
            else:
                del_file = st.file_uploader(
                    "Upload file with `record_id` or `RECORD_ID` column/keys",
                    type=["csv", "json", "jsonl"]
                )
                if del_file:
                    try:
                        if del_file.name.endswith(".csv"):
                            df_del = pd.read_csv(del_file)
                            if "record_id" in df_del.columns:
                                record_ids_to_del = df_del["record_id"].astype(str).tolist()
                            elif "RECORD_ID" in df_del.columns:
                                record_ids_to_del = df_del["RECORD_ID"].astype(str).tolist()
                            else:
                                st.error("CSV must contain a 'record_id' or 'RECORD_ID' column.")
                        elif del_file.name.endswith(".json"):
                            del_data = json.load(del_file)
                            if isinstance(del_data, list):
                                record_ids_to_del = [
                                    str(item.get("record_id", item.get("RECORD_ID")))
                                    for item in del_data
                                    if item.get("record_id") or item.get("RECORD_ID")
                                ]
                        elif del_file.name.endswith(".jsonl"):
                            content = del_file.getvalue().decode("utf-8")
                            for line in content.splitlines():
                                if line.strip():
                                    item = json.loads(line)
                                    r = item.get("record_id", item.get("RECORD_ID"))
                                    if r:
                                        record_ids_to_del.append(str(r))
                    except Exception as e:
                        st.error(f"Failed to read file: {e}")
            
            if record_ids_to_del:
                st.info(f"Ready to delete {len(record_ids_to_del)} records.")
                
            if st.button("Delete Records"):
                if not d_ds:
                    st.error("Data Source is required.")
                elif not record_ids_to_del:
                    st.warning("No valid record IDs provided.")
                else:
                    with st.spinner(f"Deleting {len(record_ids_to_del)} records and cascading entity graph cleanup..."):
                        results = engine.delete_bulk_records(d_ds, record_ids_to_del)
                        del_count = results["success"]
                        fails = results["failed"]
                        
                        if fails > 0:
                            st.warning(f"Deleted {del_count} records. ({fails} not found or failed)")
                        else:
                            st.success(f"Successfully deleted all {del_count} records and re-evaluated the entity network.")

    # ─────────────────────────────────────────────────────────────
    # ENTITY SEARCH
    # ─────────────────────────────────────────────────────────────
    with tab_search:
        st.title("Unified Search")
        st.markdown("Search across specific attributes to forcefully locate exact identities.")
        
        st.subheader("Free-text Intersection Search")
        with st.form("search_form_basic"):
            col1, col2 = st.columns(2)
            with col1:
                search_name = st.text_input("Name (e.g. John Doe)")
                search_address = st.text_input("Address / City (e.g. Seattle, WA)")
            with col2:
                search_nationality = st.text_input("Nationality (e.g. US)")
                search_other = st.text_input("Other (Phone, SSN, etc.)")
                
            submitted_basic = st.form_submit_button("Search Entity Network")
            
        if submitted_basic:
            query_parts = []
            if search_name: query_parts.append(search_name)
            if search_address: query_parts.append(search_address)
            if search_nationality: query_parts.append(search_nationality)
            if search_other: query_parts.append(search_other)
            
            final_query = ", ".join(query_parts)
            if not final_query:
                st.warning("Please enter at least one search attribute.")
            else:
                st.info(f"Querying graph for intersections: `{final_query}`")
                results = engine.search(final_query)
                if results:
                    st.success(f"Identified {len(results)} distinct Entity Profiles matching criteria:")
                    for r in results:
                        with st.expander(f"Entity Profile -> ID: {r['entity_id']} ({len(r['matching_records'])} source records)"):
                            render_record_views(r['matching_records'])
                else:
                    st.warning("No identities matched the criteria.")
        st.divider()
        
        st.subheader("Targeted Attribute Search (Up to 5)")
        st.markdown("Select an existing feature schema from the database and search exactly within that field. You can stack up to 5 explicit filters.")
        attr_summary = engine.get_features_summary()
        available_attrs = [""] + list(attr_summary.keys()) if attr_summary else []
        
        if len(available_attrs) <= 1:
            st.info("No data in database to search explicit attributes yet.")
        else:
            with st.form("search_form_explicit"):
                explicit_queries = []
                for i in range(5):
                    col_sel, col_val = st.columns([1, 2])
                    with col_sel:
                        sel = st.selectbox(f"Filter {i+1} Attribute", available_attrs, key=f"sel_{i}")
                    with col_val:
                        val = st.text_input(f"Filter {i+1} Value", key=f"val_{i}")
                    explicit_queries.append((sel, val))
                
                submitted_attr = st.form_submit_button("Search Specific Attributes")
                
            if submitted_attr:
                valid_vals = [val for sel, val in explicit_queries if sel and val]
                
                if not valid_vals:
                    st.warning("Please fully select at least one Attribute and enter its Value.")
                else:
                    final_attr_query = ", ".join(valid_vals)
                    st.info(f"Querying graph exact values: `{final_attr_query}`")
                    
                    results = engine.search(final_attr_query)
                    if results:
                        st.success(f"Identified {len(results)} distinct Entity Profiles matching criteria:")
                        for r in results:
                            with st.expander(f"Entity Profile -> ID: {r['entity_id']} ({len(r['matching_records'])} source records)"):
                                render_record_views(r['matching_records'])
                    else:
                        st.warning("No identities matched the criteria.")

    # ─────────────────────────────────────────────────────────────
    # DEEP ANALYSIS GRAPH
    # ─────────────────────────────────────────────────────────────
    with tab_graph:
        st.title("Network Graph Explorer")
        st.markdown("Visualize the connections between resolved entities and their raw incoming records.")
        
        datasource = ds_selector("Data Source (e.g. SYSTEM_A)", key="graph_ds_input")
        record_id = st.text_input("Record ID (e.g. 100)")
        
        if st.button("Generate Graph"):
            if not datasource or not record_id:
                st.warning("Please provide both Data Source and Record ID")
            else:
                st.session_state["graph_ds"] = datasource
                st.session_state["graph_rid"] = record_id
                
        if st.session_state.get("graph_ds") and st.session_state.get("graph_rid"):
            datasource = st.session_state["graph_ds"]
            record_id = st.session_state["graph_rid"]
            
            analysis = engine.analyze(datasource, record_id)
            if "error" in analysis:
                st.error(analysis["error"])
            else:
                st.success(f"Successfully resolved entity {analysis['entity_id']}")
                
                if agraph and Node and Edge:
                    state_key = f"graph_{datasource}_{record_id}"
                    
                    if state_key not in st.session_state:
                        nodes = []
                        edges = []
                        
                        from er_engine.core.rules_engine import DEFAULT_RULES
                        rule_lvl_map = {r["name"]: r["level"] for r in DEFAULT_RULES}
                        
                        node_payloads = {}

                        entity_id = analysis['entity_id']
                        nodes.append(Node(
                            id=f"ENT_{entity_id}",
                            label=f"Entity {entity_id}",
                            size=35,
                            shape="circle",
                            color="#FF6B6B",
                            title=f"Resolved Entity Profile ID: {entity_id}"
                        ))
                        
                        for rec in analysis.get('cluster_records', []):
                            record_node_id = f"REC_{rec['data_source']}_{rec['record_id']}"
                            nodes.append(Node(
                                id=record_node_id,
                                label=f"Record: {rec['data_source']}\n{rec['record_id']}",
                                size=25,
                                shape="box",
                                color="#4ECDC4",
                                title=f"Source Record\nRule: {rec.get('rule_fired','')}\nScore: {rec.get('score', 0)}"
                            ))
                            node_payloads[record_node_id] = rec.get("payload", "{}")
                            edges.append(Edge(
                                source=record_node_id,
                                target=f"ENT_{entity_id}",
                                label=f"[Lvl 1] {rec.get('rule_fired','EXACT')}",
                                color="#A8DADC",
                                length=200
                            ))
                                              
                        for rel in analysis.get('relationships', []):
                            r_name = rel.get('rule_fired', 'RELATED')
                            r_level = rule_lvl_map.get(r_name, "2/3")
                            rel_entity_id = rel['related_entity_id']
                            rel_records = engine.get_records_by_entity(rel_entity_id)
                            
                            for rr in rel_records:
                                rel_record_id = f"REC_{rr['data_source']}_{rr['record_id']}"
                                nodes.append(Node(
                                    id=rel_record_id,
                                    label=f"Related Record [{r_level}]\n{rr['data_source']}: {rr['record_id']}",
                                    size=30,
                                    shape="hexagon",
                                    color="#FFD166",
                                    title=f"Related Entity Profile ID: {rel_entity_id}"
                                ))
                                node_payloads[rel_record_id] = rr.get("payload", "{}")
                                edges.append(Edge(
                                    source=f"ENT_{entity_id}",
                                    target=rel_record_id,
                                    label=f"[Lvl {r_level}] {r_name}",
                                    color="#9D4EDD",
                                    dashes=True,
                                    length=300
                                ))
                                                  
                        st.session_state[state_key] = {"nodes": nodes, "edges": edges, "payloads": node_payloads}
                    
                    config = Config(
                        width="100%",
                        height=600,
                        directed=True,
                        physics=False,
                        hierarchical=True,
                        layout={"hierarchical": {
                            "direction": "UD",
                            "sortMethod": "directed",
                            "levelSeparation": 300,
                            "nodeSpacing": 350,
                            "treeSpacing": 350
                        }},
                        nodeHighlightBehavior=True,
                        highlightColor="#F7A072"
                    )
                    
                    @st.dialog("Record Profile Details", width="large")
                    def show_node_profile(raw_str, node_name):
                        import yaml
                        st.markdown(f"**Selected Node:** `{node_name}`")
                        try:
                            payload_dict = raw_str if isinstance(raw_str, dict) else json.loads(raw_str)
                            payload_dict = reorder_profile_keys(payload_dict)
                            yaml_str = yaml.dump(payload_dict, default_flow_style=False, sort_keys=False)
                            st.code(yaml_str, language="yaml")
                        except Exception:
                            st.write(raw_str)
                    
                    graph_data = st.session_state[state_key]
                    return_value = agraph(nodes=graph_data["nodes"], edges=graph_data["edges"], config=config)
                    
                    if return_value and return_value in graph_data["payloads"]:
                        show_node_profile(graph_data["payloads"][return_value], return_value)
                    
                else:
                    st.markdown("### Profile Merges (Level 1)")
                    for rec in analysis['cluster_records']:
                        st.info(f"Merged `{rec['data_source']}/{rec['record_id']}` due to rule: **{rec['rule_fired']}** (Score: {rec['score']})")
                    
                    st.markdown("### Discovered Relationships (Level 2/3)")
                    if not analysis['relationships']:
                        st.write("No external relationships found for this entity.")
                    for rel in analysis['relationships']:
                        st.warning(f"Linked to Entity `{rel['related_entity_id']}` due to rule: **{rel['rule_fired']}** (Score: {rel['score']})")

    # ─────────────────────────────────────────────────────────────
    # CONFIGURATION
    # ─────────────────────────────────────────────────────────────
    with tab_config:
        st.title("Engine Configuration & Rules")
        
        tab_rules, tab_sources, tab_attributes = st.tabs([
            "Match Rules", "Data Source Whitelisting", "Indexed Attributes"
        ])
        
        with tab_rules:
            st.subheader("Active Match Rules")
            st.markdown("View the active deterministic scoring logic currently running in the engine.")
            rules = engine.get_active_rules()
            if rules:
                flat_rules = []
                for r in rules:
                    conds = [c["feature_req"] for c in r.get("conditions", [])]
                    flat_rules.append({
                        "Rule Level": r.get("level"),
                        "Score": r.get("score"),
                        "Rule Name": r.get("name"),
                        "Triggers (AND)": ", ".join(conds),
                        "Description": r.get("description")
                    })
                df_rules = pd.DataFrame(flat_rules).sort_values("Score", ascending=False)
                st.dataframe(df_rules, use_container_width=True, hide_index=True)
            else:
                st.info("No active rules found.")
                
            st.divider()
            with st.expander("Inject Custom Deterministic Rule"):
                with st.form("custom_rule_form"):
                    cols = st.columns(2)
                    rule_name = cols[0].text_input("Rule Name", "RULE_CUSTOM_")
                    rule_score = cols[1].number_input("Match Score (0-100)", min_value=1.0, max_value=100.0, value=85.0)
                    rule_desc = st.text_area("Rule Description")
                    
                    st.markdown("Specify the EXACT feature types required for this rule to fire.")
                    current_schema = engine.get_features_summary()
                    existing_columns = list(current_schema.keys()) if current_schema else []
                    
                    selected_cols = st.multiselect(
                        "Select from Existing Features",
                        options=existing_columns,
                        help="These are fields that the engine has historically encountered and indexed."
                    )
                    additional_cols = st.text_input(
                        "Add Additional Features (comma-separated)",
                        placeholder="e.g. PASSPORT_NUMBER, TAX_ID",
                        help="Use this to register brand new JSON keys that haven't been ingested yet."
                    )
                    rule_level = st.selectbox(
                        "Resolution Level", [1, 2, 3],
                        help="Level 1 = Identical Entity Merge, 2/3 = Distinct Relationships"
                    )
                    
                    if st.form_submit_button("Inject Rule into Memory"):
                        final_cond_list = list(selected_cols)
                        if additional_cols:
                            manual = [c.strip() for c in additional_cols.split(",") if c.strip()]
                            final_cond_list.extend(manual)
                        final_cond_list = list(set(final_cond_list))
                        
                        if not rule_name or not final_cond_list:
                            st.warning("Rule Name and at least one Feature Trigger are required.")
                        else:
                            engine.add_custom_rule(rule_name, rule_desc, final_cond_list, rule_score, rule_level)
                            st.success(f"Rule `{rule_name}` successfully injected into system memory. It will apply to all subsequent ingestions.")
                            st.rerun()
                            
            st.divider()
            st.subheader("Retroactive Re-evaluation")
            st.markdown("Sever all current Entity mappings and retroactively rebuild the entire graph using the latest Rules configurations. **Warning: This is a heavy operation.**")
            if st.button("Rebuild Entity Resolution Graph", type="primary"):
                with st.spinner("Wiping entities and re-evaluating all raw records locally..."):
                    count = engine.rebuild_graph()
                    st.success(f"Graph fully rebuilt! Re-evaluated {count} historical records against latest rules.")
                    
            st.divider()
            st.subheader("Danger Zone: Purge Database")
            st.markdown("Completely wipe all records, entities, relationships, and configured rules from the database.")
            if st.button("Purge All Data", type="primary"):
                with st.spinner("Dropping tables and resetting schema..."):
                    success = engine.purge_all_data()
                    if success:
                        st.success("Database has been completely purged and reset.")
                    else:
                        st.error("Failed to purge the database.")

        with tab_sources:
            st.subheader("Data Source Whitelisting")
            st.markdown("Lock down the ingestion gate to carefully curated Data Sources. If empty, all sources are allowed.")
            
            allowed = engine.get_allowed_sources()
            if allowed:
                df_sources = pd.DataFrame(allowed, columns=["Secured Data Source"])
                st.dataframe(df_sources, use_container_width=True, hide_index=True)
            else:
                st.warning("No whitelisted sources configured. Current policy: ALLOW ALL.")
                
            col_add, col_del = st.columns(2)
            
            with col_add:
                with st.form("add_ds_form"):
                    new_ds = st.text_input("New Permitted Data Source(s) (comma-separated)")
                    if st.form_submit_button("Add to Whitelist"):
                        if new_ds:
                            sources = [s.strip() for s in new_ds.split(",") if s.strip()]
                            for source in sources:
                                engine.add_data_source(source)
                            st.success(f"Successfully added: {', '.join(sources)}")
                            st.rerun()
                            
            with col_del:
                if allowed:
                    with st.form("del_ds_form"):
                        del_ds = st.selectbox("Select Source to Remove", allowed)
                        if st.form_submit_button("Remove Source"):
                            engine.remove_data_source(del_ds)
                            st.success(f"Successfully removed '{del_ds}' from whitelist.")
                            st.rerun()
                            
            st.divider()
            st.subheader("Mass Data Source Deletion")
            st.markdown("Instantly delete all records and sever all entity graph linkages associated with a specific Data Source.")
            
            if allowed:
                with st.form("purge_ds_form"):
                    purge_ds = st.selectbox("Select Source to Purge", allowed)
                    confirm = st.checkbox(f"I understand this will delete ALL data for '{purge_ds}'.")
                    if st.form_submit_button("Purge Data Source", type="primary"):
                        if confirm:
                            with st.spinner(f"Purging {purge_ds} and re-evaluating entities..."):
                                count = engine.delete_records_by_source(purge_ds)
                                st.success(f"Purge complete! Deleted {count} records and updated the graph.")
                        else:
                            st.warning("Please check the confirmation box to proceed.")
            else:
                st.info("No whitelisted sources available to purge.")

            st.divider()
            st.subheader("Global Data Export")
            st.markdown("Download the fully resolved entity graph as a JSON Lines (`.jsonl`) file.")
            
            summarize_export = st.checkbox("Summarize Export (Only show mapped IDs and Rules, hide all Feature Data)")
            
            if st.button("Generate Full Graph Export"):
                with st.spinner("Streaming database snapshot..."):
                    snapshot_generator = engine.get_snapshot(summarize=summarize_export)
                    export_data = []
                    for profile in snapshot_generator:
                        export_data.append(json.dumps(profile))
                    
                    jsonl_str = "\n".join(export_data)
                    
                    st.download_button(
                        label="Download Snapshot JSONL",
                        data=jsonl_str,
                        file_name="er_snapshot_summarized.jsonl" if summarize_export else "er_snapshot.jsonl",
                        mime="application/jsonl",
                        type="primary"
                    )
                    
                    csv_rows = []
                    for profile_str in export_data:
                        profile = json.loads(profile_str)
                        ent_id = profile["ENTITY_ID"]
                        cluster_id = profile.get("CLUSTER_ID", ent_id)
                        
                        for rec in profile["RESOLVED_RECORDS"]:
                            row = {
                                "CLUSTER_ID": cluster_id,
                                "ENTITY_ID": ent_id,
                                "DATA_SOURCE": rec.get("DATA_SOURCE", ""),
                                "RECORD_ID": rec.get("RECORD_ID", ""),
                                "MATCHED_ON_RULE": rec.get("MATCHED_ON_RULE", ""),
                                "RESOLVE_LEVEL": rec.get("RESOLVE_LEVEL", 1)
                            }
                            
                            if summarize_export:
                                row["MATCHED_FEATURES"] = rec.get("MATCHED_FEATURES", "")
                            else:
                                for f in rec.get("FEATURES", []):
                                    for k, v in f.items():
                                        if k not in row:
                                            row[k] = v
                                        else:
                                            row[k] = f"{row[k]} | {v}"
                            csv_rows.append(row)
                            
                    if csv_rows:
                        df_csv = pd.DataFrame(csv_rows)
                        df_csv.sort_values(by=["CLUSTER_ID", "RESOLVE_LEVEL", "ENTITY_ID"], inplace=True)
                        csv_bytes = df_csv.to_csv(index=False).encode('utf-8')
                        st.download_button(
                            label="Download Snapshot CSV",
                            data=csv_bytes,
                            file_name="er_snapshot_summarized.csv" if summarize_export else "er_snapshot.csv",
                            mime="text/csv"
                        )

        with tab_attributes:
            st.subheader("Indexed Attributes Summary")
            st.markdown("Total distinct feature hashes cached across the entire populated database.")
            features = engine.get_features_summary()
            if features:
                df_feats = pd.DataFrame(list(features.items()), columns=["Attribute Type", "Unique Hashes Indexed"])
                df_feats = df_feats.sort_values("Unique Hashes Indexed", ascending=False)
                st.dataframe(df_feats, use_container_width=True, hide_index=True)
            else:
                st.info("No standard features indexed yet.")


if __name__ == "__main__":
    main()