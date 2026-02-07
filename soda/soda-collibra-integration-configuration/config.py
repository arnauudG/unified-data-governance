from pydantic import BaseModel, Field, SecretStr
from typing import Dict
import yaml
import logging
import os
import re
from pathlib import Path

# Try to load environment variables from .env file
try:
    from dotenv import load_dotenv
    # Try multiple locations for .env file
    env_paths = [
        Path(__file__).parent.parent.parent / '.env',  # Project root
        Path('.env'),  # Current directory
        Path(__file__).parent / '.env',  # Config directory
    ]
    env_loaded = False
    for env_path in env_paths:
        if env_path.exists():
            load_dotenv(env_path, override=True)
            logging.debug(f"Loaded environment variables from {env_path}")
            env_loaded = True
            break
    if not env_loaded:
        # Fallback: try default load_dotenv behavior
        load_dotenv(override=True)
        logging.debug("Loaded environment variables using default dotenv behavior")
except ImportError:
    # python-dotenv not installed, skip automatic loading
    logging.debug("python-dotenv not available, skipping automatic .env loading")


class CollibraGeneral(BaseModel):
    naming_delimiter: str


class CollibraAssetTypes(BaseModel):
    table_asset_type: str
    soda_check_asset_type: str
    dimension_asset_type: str  # Asset type for data quality dimensions
    column_asset_type: str  # Asset type for table columns


class CollibraAttributeTypes(BaseModel):
    check_evaluation_status_attribute: str
    check_last_sync_date_attribute: str
    check_definition_attribute: str
    check_last_run_date_attribute: str
    check_cloud_url_attribute: str
    check_loaded_rows_attribute: str
    check_rows_failed_attribute: str
    check_rows_passed_attribute: str
    check_passing_fraction_attribute: str
    collira_row_count_attribute: str


class CollibraRelationTypes(BaseModel):
    table_column_to_check_relation_type: str
    check_to_dq_dimension_relation_type: str


class CollibraResponsibilities(BaseModel):
    owner_role_id: str


class CollibraDomains(BaseModel):
    data_quality_dimensions_domain: str
    soda_collibra_domain_mapping: str  # JSON string
    soda_collibra_default_domain: str


class CollibraConfig(BaseModel):
    base_url: str
    username: str
    password: SecretStr
    general: CollibraGeneral
    asset_types: CollibraAssetTypes
    attribute_types: CollibraAttributeTypes
    relation_types: CollibraRelationTypes
    responsibilities: CollibraResponsibilities
    domains: CollibraDomains


class SodaGeneral(BaseModel):
    filter_datasets_to_sync_to_collibra: bool
    soda_no_collibra_dataset_skip_checks: bool
    sync_monitors: bool = True  # Enable/disable syncing of monitors (items with metricType)


class SodaAttributes(BaseModel):
    soda_collibra_sync_dataset_attribute: str
    soda_collibra_domain_dataset_attribute_name: str
    soda_dimension_attribute_name: str = ""  # Optional with default empty string
    custom_attributes_mapping_soda_attribute_name_to_collibra_attribute_type_id: str = '{"description": "00000000-0000-0000-0000-000000003114"}'  # Optional with default mapping


class SodaConfig(BaseModel):
    api_key_id: str
    api_key_secret: SecretStr
    base_url: str
    general: SodaGeneral
    attributes: SodaAttributes


class AppConfig(BaseModel):
    collibra: CollibraConfig
    soda: SodaConfig


def load_config(config_path: str = None) -> AppConfig:
    """Load configuration from YAML file with environment variable overrides."""
    if config_path is None:
        # Default to config.yaml in the same directory as this script
        script_dir = Path(__file__).parent
        config_path = script_dir / "config.yaml"
    else:
        config_path = Path(config_path)
        
    if not config_path.exists():
        raise FileNotFoundError(f"Config file not found: {config_path}")

    with open(config_path) as f:
        content = f.read()
        
        # Substitute environment variables in YAML content (handle ${VAR} syntax)
        def substitute_env(match):
            var_name = match.group(1)
            return os.environ.get(var_name, match.group(0))  # Return env var or original if not found
        
        content = re.sub(r'\$\{([^}]+)\}', substitute_env, content)
        
        data = yaml.safe_load(content)
        logging.debug("Config YAML data:")
        logging.debug(f"Soda attributes: {data['soda']['attributes']}")
        
        # Override with environment variables if they exist (takes precedence over template substitution)
        env_overrides = {
            'SODA_CLOUD_API_KEY_ID': ['soda', 'api_key_id'],
            'SODA_CLOUD_API_KEY_SECRET': ['soda', 'api_key_secret'],
            'SODA_API_KEY_ID': ['soda', 'api_key_id'],  # Fallback for alternative naming
            'SODA_API_KEY_SECRET': ['soda', 'api_key_secret'],  # Fallback for alternative naming
            'SODA_CLOUD_HOST': ['soda', 'host'],
            'COLLIBRA_USERNAME': ['collibra', 'username'],
            'COLLIBRA_PASSWORD': ['collibra', 'password'],
            'COLLIBRA_BASE_URL': ['collibra', 'base_url'],
        }
        
        for env_var, config_path_list in env_overrides.items():
            env_value = os.environ.get(env_var)
            if env_value:
                logging.debug(f"Overriding config with environment variable: {env_var}")
                # Navigate to nested config and set value
                current_level = data
                for key in config_path_list[:-1]:
                    if key not in current_level:
                        current_level[key] = {}
                    current_level = current_level[key]
                current_level[config_path_list[-1]] = env_value
        
        # Debug: Check if credentials are set (without logging actual values)
        if 'soda' in data:
            has_api_key = bool(data['soda'].get('api_key_id') and data['soda'].get('api_key_id') != '${SODA_CLOUD_API_KEY_ID}')
            has_api_secret = bool(data['soda'].get('api_key_secret') and data['soda'].get('api_key_secret') != '${SODA_CLOUD_API_KEY_SECRET}')
            logging.debug(f"Soda credentials status: api_key_id={'SET' if has_api_key else 'NOT SET'}, api_key_secret={'SET' if has_api_secret else 'NOT SET'}")
        
        if 'collibra' in data:
            has_username = bool(data['collibra'].get('username') and data['collibra'].get('username') != '${COLLIBRA_USERNAME}')
            has_password = bool(data['collibra'].get('password') and data['collibra'].get('password') != '${COLLIBRA_PASSWORD}')
            has_base_url = bool(data['collibra'].get('base_url') and data['collibra'].get('base_url') != '${COLLIBRA_BASE_URL}')
            logging.debug(f"Collibra credentials status: username={'SET' if has_username else 'NOT SET'}, password={'SET' if has_password else 'NOT SET'}, base_url={'SET' if has_base_url else 'NOT SET'}")
        
        return AppConfig(**data)