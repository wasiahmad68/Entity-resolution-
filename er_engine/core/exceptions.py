class ERError(Exception):
    """Base exception for all ER engine errors."""
    pass

class InvalidJSONSchemaError(ERError):
    """Raised when the ingested JSON does not conform to the expected Features schema."""
    pass

class DatabaseConnectionError(ERError):
    """Raised when a database transaction fails to connect or execute."""
    pass

class RuleEvaluationError(ERError):
    """Raised when a rule algorithm encounters an unparseable feature configuration or syntax."""
    pass

class BulkIngestionPartialFailure(ERError):
    """Raised when an ingestion batch has some record failures, containing failure payload structures."""
    def __init__(self, message, failed_records):
        super().__init__(message)
        self.failed_records = failed_records
