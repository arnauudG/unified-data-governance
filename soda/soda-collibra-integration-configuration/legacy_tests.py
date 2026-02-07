"""
Legacy test methods for backward compatibility
"""

from config import load_config
from clients.collibra_client import CollibraClient
from clients.soda_client import SodaClient
from models.soda import UpdateDatasetRequest, DatasetOwnerUpdate
from models.collibra import AssetCreateRequest, AssetUpdateRequest, AttributeCreateRequest, AttributeUpdateRequest
import time

def soda_test_methods():
    
    print("\nGetting started...! ⌛️\n")
    
    config = load_config()
    print("✅ Config successfully loaded!\n")

    # Collibra config values
    print("📘 Collibra Config:")
    print(f"  Base URL   : {config.collibra.base_url}\n")


    # Soda config values
    print("🍹 Soda Config:")
    print(f"  Base URL   : {config.soda.base_url}\n")

    # General config values
    print("⚙️ General Config:")
    print(f"  Delimiter        : {config.collibra.general.naming_delimiter}")
    # Note: owner_role_id and attribute_ids may not exist in current config structure
    # print(f"  Owner Role ID    : {config.general.owner_role_id}")
    # print(f"  Attribute IDs    : {config.general.attribute_ids}\n")
    
    print("Setting up Soda and Collibra clients...! ⌛️\n")
    collibra = CollibraClient(config.collibra)
    soda = SodaClient(config.soda)
    
    print("Soda Client Test Connection:")
    soda_test_connection = soda.test_connection()
    print(f"  Organisation Name: {soda_test_connection.organisationName}\n")
    
    print("Soda Client Find User:")
    search_term = "hazem"  # Example search term
    print(f"  Searching for users matching '{search_term}'...")
    users = soda.find_user(search_term)
    print(f"\n  Found {len(users)} users:")
    for user in users:
        print(f"\n  👤 {user.fullName}:")
        print(f"    Email: {user.email}")
        print(f"    User ID: {user.userId}")
    
    print("\nSoda Client Get Datasets:")
    print("  Fetching all datasets (this may take a moment)...")
    datasets = soda.get_datasets()
    print(f"\n  Found {len(datasets)} datasets:")
    
    # Print dataset information and find the dataset with most checks
    dataset_with_most_checks = None
    max_checks = 0
    
    for dataset in datasets:
        print(f"\n  📊 {dataset.name}:")
        print(f"    Status: {dataset.dataQualityStatus.upper()}")
        print(f"    Health: {dataset.healthStatus}%")
        print(f"    Checks: {dataset.checks}")
        print(f"    Incidents: {dataset.incidents}")
        print(f"    Last Updated: {dataset.lastUpdated}")
        print(f"    Data Source: {dataset.datasource.type} ({dataset.datasource.name})")
        if dataset.owners:
            owner = dataset.owners[0].user
            print(f"    Owner: {owner.fullName} ({owner.email})")
            
        # Track dataset with most checks
        if dataset.checks > max_checks:
            max_checks = dataset.checks
            dataset_with_most_checks = dataset
    
    # Update the dataset with most checks as an example
    if dataset_with_most_checks:
        print(f"\nUpdating dataset '{dataset_with_most_checks.name}' (ID: {dataset_with_most_checks.id}):")
        
        # Create owner updates
        owners = [
            DatasetOwnerUpdate(userId="059f6627-ec83-4971-9c99-0b45c4e95f65"),
            DatasetOwnerUpdate(userId="04ebb2e6-c722-4278-a78b-92e6edb68ac2")
        ]
        
        update_data = UpdateDatasetRequest(
            label=f"{dataset_with_most_checks.name}_updated",
            attributes={"business_impact": "High"},
            tags=["updated", "high_impact", "analytics"],
            owners=owners
        )
        
        updated_dataset = soda.update_dataset(
            dataset_id=dataset_with_most_checks.id,
            update_data=update_data
        )
        
        print("\n  Dataset updated successfully:")
        print(f"    New label: {updated_dataset.label}")
        print(f"    New attributes: {updated_dataset.attributes}")
        print(f"    New tags: {updated_dataset.tags}")
        print(f"    New owners: {len(updated_dataset.owners)}")
        for owner in updated_dataset.owners:
            print(f"      - {owner.user.fullName} ({owner.user.email})")
        print(f"    Last Updated: {updated_dataset.lastUpdated}")
    
    print("\nSoda Client Get All Checks:")
    print("  Fetching all checks (this may take a moment)...")
    all_checks = soda.get_checks()
    print(f"\n  Found {len(all_checks)} total checks:")
    
    # Group checks by status for better visibility
    status_groups = {}
    for check in all_checks:
        status = check.evaluationStatus
        if status not in status_groups:
            status_groups[status] = []
        status_groups[status].append(check)
    
    # Print checks grouped by status
    for status, status_checks in status_groups.items():
        print(f"\n  {status.upper()} Checks ({len(status_checks)}):")
        for check in status_checks:
            print(f"    - {check.name}")
    
    # If we found a dataset with checks, show its checks specifically
    if dataset_with_most_checks and dataset_with_most_checks.checks > 0:
        print(f"\nSoda Client Get Checks for Dataset '{dataset_with_most_checks.name}':")
        print(f"  Fetching checks for dataset {dataset_with_most_checks.name} (ID: {dataset_with_most_checks.id})...")
        dataset_checks = soda.get_checks(dataset_id=dataset_with_most_checks.id)
        print(f"\n  Found {len(dataset_checks)} checks for this dataset:")
        
        # Group dataset checks by status
        dataset_status_groups = {}
        for check in dataset_checks:
            status = check.evaluationStatus
            if status not in dataset_status_groups:
                dataset_status_groups[status] = []
            dataset_status_groups[status].append(check)
        
        # Print dataset checks grouped by status
        for status, status_checks in dataset_status_groups.items():
            print(f"\n  {status.upper()} Checks ({len(status_checks)}):")
            for check in status_checks:
                print(f"    - {check.name}")

def collibra_test_methods():
    
    print("\nGetting started...! ⌛️\n")
    
    config = load_config()
    print("✅ Config successfully loaded!\n")

    # Collibra config values
    print("📘 Collibra Config:")
    print(f"  Base URL   : {config.collibra.base_url}\n")
    
    print("Setting up Collibra client...! ⌛️\n")
    collibra = CollibraClient(config.collibra)
    
    print("Collibra Client Get Application Info:")
    app_info = collibra.get_application_info()
    print(f"\n  📊 Application Information:")
    print(f"    Base URL: {app_info.baseUrl}")
    print(f"    Version: {app_info.version.fullVersion}")
    print(f"    Build Number: {app_info.buildNumber}")
    print(f"\n  🧩 Solutions ({len(app_info.solutions)}):")
    for solution in app_info.solutions:
        print(f"\n    📦 {solution.name}:")
        print(f"      Version: {solution.version.fullVersion}")
    
    print("\nCollibra Client Find Asset:")
    domain_id = "0194b6bb-7c03-7dc8-9750-95915882c6ce"  # Soda Demo domain
    asset_name = "postgres>soda_demo_data_testing>retail_orders"
    type_id = "00000000-0000-0000-0000-000000031007"  # Table type
    
    # Search in specific domain
    print(f"  Searching for asset '{asset_name}' in domain {domain_id}...")
    assets_in_domain = collibra.find_asset(
        name=asset_name,
        domain_id=domain_id,
        type_id=type_id
    )
    
    print(f"\n  Found {assets_in_domain.total} assets in domain:")
    for asset in assets_in_domain.results:
        print(f"\n  📋 {asset.displayName}:")
        print(f"    ID: {asset.id}")
        print(f"    Type: {asset.type.name}")
        print(f"    Status: {asset.status.name}")
        print(f"    Domain: {asset.domain.name}")
        print(f"    Created: {asset.createdOn}")
        print(f"    Last Modified: {asset.lastModifiedOn}")
    
    # Search across all domains
    print(f"\n  Searching for asset '{asset_name}' across all domains...")
    assets_all_domains = collibra.find_asset(
        name=asset_name,
        type_id=type_id
    )
    
    print(f"\n  Found {assets_all_domains.total} assets across all domains:")
    for asset in assets_all_domains.results:
        print(f"\n  📋 {asset.displayName}:")
        print(f"    ID: {asset.id}")
        print(f"    Type: {asset.type.name}")
        print(f"    Status: {asset.status.name}")
        print(f"    Domain: {asset.domain.name}")
        print(f"    Created: {asset.createdOn}")
        print(f"    Last Modified: {asset.lastModifiedOn}")
    
    # Create multiple assets
    print("\nCollibra Client Add Assets Bulk:")
    new_domain_id = "0197377f-e595-7434-82c7-3ce1499ac620"  # Soda DQ Checks NEW domain
    new_type_id = "00000000-0000-0000-0000-000000031107"  # Data Quality Metric type
    
    # Create list of assets to add with unique names
    timestamp = int(time.time())
    assets_to_create = [
        AssetCreateRequest(
            name=f"test_metric_1_{timestamp}",
            displayName=f"Test Metric 1 ({timestamp})",
            domainId=new_domain_id,
            typeId=new_type_id
        ),
        AssetCreateRequest(
            name=f"test_metric_2_{timestamp}",
            displayName=f"Test Metric 2 ({timestamp})",
            domainId=new_domain_id,
            typeId=new_type_id
        )
    ]
    
    print(f"  Creating {len(assets_to_create)} new assets in domain {new_domain_id}...")
    created_assets = collibra.add_assets_bulk(assets_to_create)
    
    print(f"\n  ✅ {len(created_assets)} assets created successfully:")
    for asset in created_assets:
        print(f"\n  📋 {asset.displayName}:")
        print(f"    ID: {asset.id}")
        print(f"    Name: {asset.name}")
        print(f"    Type: {asset.type.name}")
        print(f"    Domain: {asset.domain.name}")
        print(f"    Status: {asset.status.name}")
        print(f"    Created: {asset.createdOn}")
        print(f"    Last Modified: {asset.lastModifiedOn}")
    
    # Set attributes for the first created asset
    if created_assets:
        print("\nCollibra Client Set Attributes:")
        first_asset = created_assets[0]
        description_type_id = "00000000-0000-0000-0000-000000003114"  # Description type
        
        print(f"  Setting description for asset '{first_asset.displayName}' (ID: {first_asset.id})...")
        attributes_to_create = [
            AttributeCreateRequest(
                assetId=first_asset.id,
                typeId=description_type_id,
                value="This is a test description for the asset"
            )
        ]
        attributes = collibra.add_attributes_bulk(attributes_to_create)
        
        print(f"\n  ✅ {len(attributes)} attributes set successfully:")
        for attr in attributes:
            print(f"\n  📝 {attr.type.name}:")
            print(f"    ID: {attr.id}")
            print(f"    Value: {attr.value}")
            print(f"    Created: {attr.createdOn}")
            print(f"    Last Modified: {attr.lastModifiedOn}")
        
        # Set relations for the first created asset
        print("\nCollibra Client Set Relations:")
        relation_type_id = "f7e0a26b-eed6-4ba9-9152-4a1363226640"  # Example relation type
        target_asset_id = "00000000-0000-0000-0000-000000008044"  # Example target asset (Accuracy)
        
        print(f"  Setting relation for asset '{first_asset.displayName}' (ID: {first_asset.id})...")
        relations = collibra.set_relations(
            asset_id=first_asset.id,
            type_id=relation_type_id,
            related_asset_ids=[target_asset_id]
        )
        
        print(f"\n  ✅ {len(relations)} relations set successfully:")
        for relation in relations:
            print(f"\n  🔗 Relation:")
            print(f"    ID: {relation.id}")
            print(f"    Source: {relation.source.name} ({relation.source.id})")
            print(f"    Target: {relation.target.name} ({relation.target.id})")
            print(f"    Type: {relation.type.id}")
            print(f"    Created: {relation.createdOn}")
            print(f"    Last Modified: {relation.lastModifiedOn}")
    
    # Update assets in bulk
    print("\nCollibra Client Change Assets Bulk:")
    if created_assets:
        assets_to_update = [
            AssetUpdateRequest(
                id=created_assets[0].id,
                name=f"updated_{created_assets[0].name}",
                displayName=f"Updated {created_assets[0].displayName}",
                typeId=created_assets[0].type.id,
                domainId=created_assets[0].domain.id
            )
        ]
        
        print(f"  Updating {len(assets_to_update)} assets...")
        updated_assets = collibra.change_assets_bulk(assets_to_update)
        
        print(f"\n  ✅ {len(updated_assets)} assets updated successfully:")
        for asset in updated_assets:
            print(f"\n  📋 {asset.displayName}:")
            print(f"    ID: {asset.id}")
            print(f"    Name: {asset.name}")
            print(f"    Type: {asset.type.name}")
            print(f"    Domain: {asset.domain.name}")
            print(f"    Status: {asset.status.name}")
            print(f"    Last Modified: {asset.lastModifiedOn}") 