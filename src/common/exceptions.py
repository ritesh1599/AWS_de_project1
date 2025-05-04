class DataValidationError(Exception):
    """Raised when data validation fails"""
    pass

class S3OperationError(Exception):
    """Raised when S3 operations fail"""
    pass

class GlueJobError(Exception):
    """Raised when Glue job fails"""
    pass

