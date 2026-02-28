import networkx as nx
from typing import Dict, Any, List

def generate_entity_graph(entity_id: int, cluster_records: List[Dict[str, Any]], relationships: List[Dict[str, Any]]) -> nx.Graph:
    """
    Constructs an undirected NetworkX graph for a specific entity profile.
    This provides the topology used by the Streamlit frontend to draw interactive diagrams.
    """
    G = nx.Graph()
    
    # 1. Base Core Entity Node
    e_node = f"ENTITY:{entity_id}"
    G.add_node(e_node, type="entity", label=f"Entity {entity_id}")
    
    # 2. Add Exact Matched Identity elements (Level 1)
    for idx, rec in enumerate(cluster_records):
        rec_node = f"RECORD:{rec['data_source']}_{rec['record_id']}"
        G.add_node(rec_node, type="record", label=f"{rec['data_source']}/{rec['record_id']}")
        
        # Link Record to its Identity
        G.add_edge(e_node, rec_node, relationship="MERGED_IDENTITY", score=rec.get('score', 100))
        
    # 3. Add Level 2/3 connections (e.g. shared housemates, business associates)
    for rel in relationships:
        rel_node = f"ENTITY:{rel['related_entity_id']}"
        G.add_node(rel_node, type="related_entity", label=f"Entity {rel['related_entity_id']}")
        
        # Link the two entities
        G.add_edge(e_node, rel_node, relationship=rel['rule_fired'], score=rel['score'])
        
    return G
