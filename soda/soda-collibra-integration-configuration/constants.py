"""
Constants for Soda-Collibra Integration
"""

class IntegrationConstants:
    """Constants for the integration process"""
    
    # Performance settings
    MAX_RETRIES = 3
    BATCH_SIZE = 50
    DEFAULT_PAGE_SIZE = 1000
    RATE_LIMIT_DELAY = 2
    RETRY_DELAY_MIN = 4
    RETRY_DELAY_MAX = 10
    MAX_WORKERS = 3
    
    # Naming conventions
    ASSET_NAME_SEPARATOR = "_"
    NAMING_DELIMITER_REPLACEMENT = "."
    
    # Cache settings
    CACHE_MAX_SIZE = 128
    CACHE_TTL = 300  # 5 minutes
    
    # Logging
    LOG_FORMAT_SIMPLE = '%(message)s'
    LOG_FORMAT_DEBUG = '%(asctime)s - %(levelname)s - %(message)s'
    
    # Error messages
    ERR_NO_TABLE_ASSET = "No table asset found for dataset: {}"
    ERR_MULTIPLE_TABLE_ASSETS = "Multiple table assets found for dataset: {}"
    ERR_NO_COLLIBRA_ASSET = "No Collibra asset found for check: {}"
    ERR_CONFIG_VALIDATION = "Configuration validation failed: {}"
    ERR_API_CALL_FAILED = "API call failed: {}"
    
    # Success messages
    MSG_INTEGRATION_STARTED = "🚀 Soda <-> Collibra Integration Started"
    MSG_DATASETS_FOUND = "📊 Found {} datasets"
    MSG_PROCESSING_DATASET = "Processing dataset {}/{}: {}"
    MSG_CHECKS_FOUND = "  Found {} checks"
    MSG_INTEGRATION_COMPLETE = "🎉 INTEGRATION COMPLETED SUCCESSFULLY 🎉"
    
    # Summary labels
    SUMMARY_DATASETS_PROCESSED = "📊 Datasets processed"
    SUMMARY_DATASETS_SKIPPED = "⏭️  Datasets skipped"
    SUMMARY_DATASETS_NO_TABLE_ASSET = "🚫 No tables found in Collibra"
    SUMMARY_CHECKS_CREATED = "✅ Checks created"
    SUMMARY_CHECKS_UPDATED = "🔄 Checks updated"
    SUMMARY_CHECKS_DELETED = "🗑️  Checks deleted"
    SUMMARY_ATTRIBUTES_CREATED = "📝 Attributes created"
    SUMMARY_ATTRIBUTES_UPDATED = "🔄 Attributes updated"
    SUMMARY_DIMENSION_RELATIONS = "🔗 Dimension relations created"
    SUMMARY_TABLE_RELATIONS = "📋 Table relations created"
    SUMMARY_COLUMN_RELATIONS = "📊 Column relations created"
    SUMMARY_OWNERS_SYNCED = "👥 Owners synchronized" 
    SUMMARY_OWNERSHIP_SYNC_FAILED = "❌ Ownership sync failures"
    SUMMARY_DIMENSION_SYNC_FAILED = "❌ Dimension sync failures"
    SUMMARY_ERRORS = "⚠️  Errors encountered"
    SUMMARY_TOTAL_OPERATIONS = "🎯 Total operations performed"
    
    # HTML formatting
    HTML_CLOUD_URL_TEMPLATE = '<p><a href="{}" target="_blank" rel="noopener">View check in Soda</a></p>'
    HTML_DEFINITION_TEMPLATE = '<pre><code>{}</code></pre>'
    
    # Column naming convention
    COLUMN_SUFFIX = "(column)" 