from sqlalchemy import Column, String, Integer, DateTime, JSON, ForeignKey, Text, Float, UniqueConstraint
from sqlalchemy.orm import declarative_base, relationship, backref
from sqlalchemy.sql import func

Base = declarative_base()

class Record(Base):
    """
    Stores the raw ingested JSON records, indexed by their origin system and unique ID.
    Per the Senzing Spec, data_source and record_id uniquely identify a record.
    """
    __tablename__ = 'records'
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    data_source = Column(String(100), nullable=False, index=True)
    record_id = Column(String(255), nullable=False, index=True)
    raw_json = Column(JSON, nullable=False)
    created_at = Column(DateTime(timezone=True), server_default=func.now())
    updated_at = Column(DateTime(timezone=True), onupdate=func.now())

    __table_args__ = (
        UniqueConstraint('data_source', 'record_id', name='_datasource_recordid_uc'),
    )

class Entity(Base):
    """
    Represents a uniquely resolved Entity. 
    Multiple Records can map to a single Entity.
    """
    __tablename__ = 'entities'
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    created_at = Column(DateTime(timezone=True), server_default=func.now())
    updated_at = Column(DateTime(timezone=True), onupdate=func.now())

class EntityRecord(Base):
    """
    Many-to-Many mapping linking a Record to an Entity.
    Tracks Match Explanation (rule_fired, score).
    """
    __tablename__ = 'entity_records'
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    entity_id = Column(Integer, ForeignKey('entities.id', ondelete="CASCADE"), nullable=False, index=True)
    record_id = Column(Integer, ForeignKey('records.id', ondelete="CASCADE"), nullable=False, index=True)
    rule_fired = Column(String(255), nullable=True) # Null if it didn't merge
    score = Column(Float, nullable=True)
    
    entity = relationship("Entity", backref="record_links")
    record = relationship("Record", backref=backref("entity_links", cascade="all, delete-orphan"))

class Feature(Base):
    """
    Stores individual extracted features (Name, Address, Dob, Phone etc) across all records.
    The feature_hash ensures extremely fast equality match lookups.
    """
    __tablename__ = 'features'
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    feature_type = Column(String(100), nullable=False, index=True) # e.g., NAME_FIRST, PASSPORT, DOB
    feature_hash = Column(String(64), nullable=False, index=True)  # SHA-256 or Phonetic hash
    feature_value = Column(Text, nullable=False) # Plain text for debugging/search/fuzzy compare

class RecordFeature(Base):
    """
    Many-to-Many mapping linking a Record to the Features extracted from its JSON.
    """
    __tablename__ = 'record_features'
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    record_id = Column(Integer, ForeignKey('records.id', ondelete="CASCADE"), nullable=False, index=True)
    feature_id = Column(Integer, ForeignKey('features.id', ondelete="CASCADE"), nullable=False, index=True)
    
    record = relationship("Record", backref=backref("features_links", cascade="all, delete-orphan"))
    feature = relationship("Feature", backref="record_links")

class Relationship(Base):
    """
    Defines Level 3 relationships between two resolved Entities based on shared features.
    """
    __tablename__ = 'relationships'
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    entity_id_1 = Column(Integer, ForeignKey('entities.id', ondelete="CASCADE"), nullable=False, index=True)
    entity_id_2 = Column(Integer, ForeignKey('entities.id', ondelete="CASCADE"), nullable=False, index=True)
    rule_fired = Column(String(255), nullable=False)
    score = Column(Float, nullable=False)

class MatchRule(Base):
    """
    Stores the active matching rules used by the engine to evaluate Record proximity.
    """
    __tablename__ = 'match_rules'
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    rule_name = Column(String(255), nullable=False, unique=True)
    rule_definition = Column(JSON, nullable=False)  # JSON structure defining the attribute requirements
    match_level = Column(Integer, nullable=False)   # 1, 2, or 3
    score = Column(Float, nullable=False)           # E.g., 90.0
    is_active = Column(Integer, default=1)

class AllowedSource(Base):
    """
    Stores globally whitelisted Data Sources. 
    If this table has rows, the ingestion gateway will reject any source not listed here.
    """
    __tablename__ = 'allowed_sources'
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    source_name = Column(String(100), nullable=False, unique=True, index=True)
