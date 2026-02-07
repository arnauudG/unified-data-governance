import requests
import logging
import json
import time
from json.decoder import JSONDecodeError
from typing import List
from urllib.parse import urljoin
from models.collibra import (
    ApplicationInfo,
    AssetSearchResponse,
    AssetCreateRequest,
    BulkAssetCreateResponse,
    Asset,
    AttributeSetRequest,
    StringAttributeResponse,
    StringAttribute,
    RelationSetRequest,
    RelationResponse,
    Relation,
    AssetUpdateRequest,
    BulkAssetUpdateResponse,
    AttributeCreateRequest,
    BulkAttributeCreateResponse,
    BooleanAttribute,
    AttributeSearchResponse,
    AttributeUpdateRequest,
    ResponsibilitySearchResponse,
    UserSearchResponse
)


class CollibraClient:
    def __init__(self, config, metrics=None):
        self.config = config
        # Normalize base URL (remove trailing slash) like the working Airflow module does
        self.config.base_url = self.config.base_url.rstrip('/')
        
        # Set session auth like the working Airflow module does
        self.session = requests.Session()
        password_value = config.password.get_secret_value()
        self.session.auth = (config.username, password_value)
        self.session.headers.update({
            'Content-Type': 'application/json',
            'Accept': 'application/json'
        })
        
        # Debug logging (don't log password)
        logging.debug(f"Collibra client initialized:")
        logging.debug(f"  Base URL: {self.config.base_url}")
        logging.debug(f"  Username: {config.username}")
        logging.debug(f"  Password: {'SET' if password_value else 'NOT SET'}")
        # Check if password looks like a placeholder (starts with ${)
        if password_value and password_value.startswith('${'):
            logging.warning(f"Password appears to be a placeholder (${...}), not a real value. Check environment variable loading.")
        
        self.metrics = metrics

    def execute_request(
        self,
        method: str,
        url: str,
        params: dict | None = None,
        body: dict | str | None = None,
        is_retry: bool = False,
        model_class: type | None = None,
    ):
        # Remove any leading slashes from the URL to avoid double slashes
        url = url.lstrip('/')
        full_url = f"{self.config.base_url}/{url}"
        logging.debug(f"> {method} {full_url}")
        if params:
            logging.debug(f"> Params: {json.dumps(params, indent=2)}")
        if body:
            logging.debug(f"> Body: {body if isinstance(body, str) else json.dumps(body, indent=2)}")

        # Session auth is already configured in __init__, so we don't need to manually create headers
        # The session will automatically add the Authorization header
        logging.debug(f"> Using session auth (username: {self.config.username})")

        # If body is a string, use data parameter, otherwise use json parameter
        if isinstance(body, str):
            response = self.session.request(
                method,
                full_url,
                params=params,
                data=body,
                allow_redirects=False  # Don't follow redirects
            )
        else:
            response = self.session.request(
                method,
                full_url,
                params=params,
                json=body,
                allow_redirects=False  # Don't follow redirects
            )
        
        # Track API call in metrics
        if self.metrics:
            self.metrics.increment_api_call(response.ok)
        
        logging.debug(f"< Status: {response.status_code}")
        
        # Handle redirects manually
        if response.status_code in (301, 302, 303, 307, 308):
            redirect_url = response.headers.get('Location')
            logging.debug(f"Following redirect to: {redirect_url}")
            
            # Convert relative redirect URLs to absolute URLs
            if redirect_url and not redirect_url.startswith(('http://', 'https://')):
                redirect_url = urljoin(self.config.base_url, redirect_url)
                logging.debug(f"Converted to absolute URL: {redirect_url}")
            
            # Check if redirect is to login page - this indicates authentication failure
            if redirect_url and '/signin' in redirect_url:
                logging.error(f"Collibra redirected to login page. This indicates authentication failure.")
                logging.error(f"Please verify your Collibra credentials (username/password) are correct.")
                raise requests.exceptions.HTTPError(
                    f"401 Unauthorized: Collibra authentication failed. Redirected to login page.",
                    response=response
                )
            
            if isinstance(body, str):
                response = self.session.request(
                    method,
                    redirect_url,
                    params=params,
                    data=body
                )
            else:
                response = self.session.request(
                    method,
                    redirect_url,
                    params=params,
                    json=body
                )
            
            # Track redirect API call in metrics
            if self.metrics:
                self.metrics.increment_api_call(response.ok)
                
            logging.debug(f"< Redirect Status: {response.status_code}")
        
        try:
            response.raise_for_status()
        except requests.exceptions.HTTPError as e:
            logging.error(f"Request failed with status {response.status_code}")
            logging.error(f"Request URL: {full_url}")
            logging.error(f"Request Headers: {json.dumps(dict(self.session.headers), indent=2)}")
            logging.error(f"Request Body: {body if isinstance(body, str) else json.dumps(body, indent=2)}")
            logging.error(f"Response Headers: {dict(response.headers)}")
            logging.error(f"Response Text: {response.text}")
            raise
        
        response_json = None
        try:
            response_json = response.json()
        except JSONDecodeError as e:
            logging.error(f"Failed to parse JSON response: {e}")
            logging.error(f"Response text: {response.text}")
            raise
        logging.debug(f"< JSON Response received")

        if response.status_code == 401 and not is_retry:
            raise RuntimeError(
                "Authentication failed. Verify if the provided credentials are still valid."
            )
        if response.status_code == 429:
            logging.warning("Rate limit applied, retrying in 10 seconds")
            time.sleep(10)
            return self.execute_request(
                method,
                url,
                params=params,
                body=body,
                is_retry=True,
                model_class=model_class,
            )
        else:
            assert (
                response.ok
            ), f"Request failed with status {response.status_code}: {json.dumps(response_json, indent=2)}"
        
        # Handle model conversion
        if model_class is None:
            return response_json
        elif isinstance(response_json, list):
            # For list responses, create a dict with 'root' key for RootModel
            return model_class(root=response_json)
        else:
            return model_class(**response_json)

    def get_application_info(self) -> ApplicationInfo:
        """
        Get Collibra application information.
        
        Returns:
            ApplicationInfo: Information about the Collibra application.
        """
        response = self.execute_request(
            method="get",
            url="rest/2.0/application/info",
            model_class=ApplicationInfo
        )
        return response

    def find_asset(
        self,
        name: str,
        type_id: str,
        domain_id: str | None = None,
        name_match_mode: str = "END",
        limit: int = 1000,
        offset: int = 0
    ) -> AssetSearchResponse:
        """
        Search for assets in Collibra.
        
        Args:
            name (str): The name to search for
            type_id (str): The asset type ID (e.g. "00000000-0000-0000-0000-000000031007" for Table type)
            domain_id (str | None): Optional domain ID to search in. If None, searches across all domains.
            name_match_mode (str): How to match the name (defaults to END)
            limit (int): Maximum number of results to return
            offset (int): Offset for pagination
            
        Returns:
            AssetSearchResponse: The search results
        """
        params = {
            "name": name,
            "typeIds": type_id,
            "nameMatchMode": name_match_mode,
            "limit": limit,
            "offset": offset,
            "countLimit": -1,
            "typeInheritance": "true",
            "excludeMeta": "true",
            "sortField": "NAME",
            "sortOrder": "ASC"
        }
        
        # Only add domainId to params if it's provided
        if domain_id is not None:
            params["domainId"] = domain_id
        
        response = self.execute_request(
            method="get",
            url="rest/2.0/assets",
            params=params,
            model_class=AssetSearchResponse
        )
        return response

    def add_assets_bulk(
        self,
        assets: List[AssetCreateRequest]
    ) -> List[Asset]:
        """
        Create multiple assets in Collibra in a single request.
        
        Args:
            assets (List[AssetCreateRequest]): List of assets to create
            
        Returns:
            List[Asset]: List of created assets with their details
        """
        # Convert list of AssetCreateRequest to list of dicts
        request_data = [asset.model_dump() for asset in assets]
        
        logging.debug(f"Creating assets with data: {json.dumps(request_data, indent=2)}")
        
        # Convert the request data to a JSON string, exactly matching the example format
        json_data = json.dumps(request_data, separators=(',', ':'))  # Compact JSON format
        
        response = self.execute_request(
            method="post",
            url="rest/2.0/assets/bulk",
            body=json_data,  # Send as JSON string
            model_class=BulkAssetCreateResponse
        )
        
        # Return the list of assets from the RootModel
        return response.root

    # def set_attributes(
    #     self,
    #     asset_id: str,
    #     type_id: str,
    #     values: List[str]
    # ) -> List[StringAttribute]:
    #     """
    #     Set attributes for an asset in Collibra.
        
    #     Args:
    #         asset_id (str): The ID of the asset to set attributes for
    #         type_id (str): The ID of the attribute type (e.g. "00000000-0000-0000-0000-000000003114" for Description)
    #         values (List[str]): List of values to set for the attribute
            
    #     Returns:
    #         List[StringAttribute]: List of created/updated attributes
    #     """
    #     request_data = AttributeSetRequest(
    #         typeId=type_id,
    #         values=values
    #     )
        
    #     logging.info(f"Setting attributes for asset {asset_id} with data: {json.dumps(request_data.model_dump(), indent=2)}")
        
    #     response = self.execute_request(
    #         method="put",
    #         url=f"assets/{asset_id}/attributes",
    #         body=request_data.model_dump(),
    #         model_class=StringAttributeResponse
    #     )
        
    #     return response.root

    def set_relations(
        self,
        asset_id: str,
        type_id: str,
        related_asset_ids: List[str],
        relation_direction: str = "TO_TARGET"
    ) -> List[Relation]:
        """
        Set relations for an asset in Collibra.
        Handles missing relation types gracefully by logging a warning and returning an empty list.
        
        Args:
            asset_id (str): The ID of the asset to set relations for
            type_id (str): The ID of the relation type (e.g. "00000000-0000-0000-0000-000000007053")
            related_asset_ids (List[str]): List of asset IDs to relate to
            relation_direction (str): Direction of the relation (defaults to "TO_TARGET")
            
        Returns:
            List[Relation]: List of created relations (empty list if relation type not found)
        """
        request_data = RelationSetRequest(
            typeId=type_id,
            relatedAssetIds=related_asset_ids,
            relationDirection=relation_direction
        )
        
        logging.debug(f"Setting relations for asset {asset_id} with data: {json.dumps(request_data.model_dump(), indent=2)}")
        
        try:
            response = self.execute_request(
                method="put",
                url=f"rest/2.0/assets/{asset_id}/relations",
                body=request_data.model_dump(),
                model_class=RelationResponse
            )
            
            return response.root
        except requests.exceptions.HTTPError as e:
            # Handle 404 errors for missing relation types
            if e.response is not None and e.response.status_code == 404:
                try:
                    error_data = e.response.json()
                    error_code = error_data.get('errorCode', '')
                    
                    # Check if this is a "relation type not found" error
                    if error_code == 'relationTypeNotFoundId' and 'properties' in error_data:
                        missing_type_id = error_data['properties'].get('id')
                        if missing_type_id:
                            logging.warning(
                                f"Relation type '{missing_type_id}' not found in Collibra. "
                                f"Skipping relation creation for asset {asset_id}."
                            )
                            return []
                except (ValueError, KeyError) as parse_error:
                    logging.debug(f"Could not parse error response: {parse_error}")
            
            # Re-raise if not a handled 404 or if parsing failed
            logging.error(f"Error setting relations for asset {asset_id}: {str(e)}")
            raise
        except Exception as e:
            logging.error(f"Error setting relations for asset {asset_id}: {str(e)}")
            raise

    def change_assets_bulk(
        self,
        assets: List[AssetUpdateRequest]
    ) -> List[Asset]:
        """
        Update multiple assets in Collibra in a single request.
        
        Args:
            assets (List[AssetUpdateRequest]): List of assets to update
            
        Returns:
            List[Asset]: List of updated assets with their details
        """
        # Convert list of AssetUpdateRequest to list of dicts
        request_data = [asset.model_dump() for asset in assets]
        
        logging.debug(f"Updating assets with data: {json.dumps(request_data, indent=2)}")
        
        # Convert the request data to a JSON string
        json_data = json.dumps(request_data, separators=(',', ':'))  # Compact JSON format
        
        response = self.execute_request(
            method="patch",
            url="rest/2.0/assets/bulk",
            body=json_data,  # Send as JSON string
            model_class=BulkAssetUpdateResponse
        )
        
        # Return the list of assets from the RootModel
        return response.root

    def delete_bulk_assets(
        self,
        asset_ids: List[str]
    ) -> dict | None:
        """
        Delete multiple assets in Collibra in a single request.
        
        Args:
            asset_ids (List[str]): List of asset IDs to delete
            
        Returns:
            dict | None: Response from the API, or None if no response body
        """
        if not asset_ids:
            logging.warning("No asset IDs provided for bulk deletion")
            return None
        
        # Convert list of asset IDs to JSON string array
        json_data = json.dumps(asset_ids, separators=(',', ':'))
        
        logging.debug(f"Deleting {len(asset_ids)} assets with IDs: {json.dumps(asset_ids, indent=2)}")
        
        # Make the request directly to handle 404 gracefully before execute_request logs errors
        url = "rest/2.0/assets/bulk"
        url = url.lstrip('/')
        full_url = f"{self.config.base_url}/{url}"
        
        # Session auth is already configured, so we don't need to manually create headers
        # Make the DELETE request
        response = self.session.request(
            method="delete",
            url=full_url,
            data=json_data,
            allow_redirects=False
        )
        
        # Track API call in metrics
        if self.metrics:
            self.metrics.increment_api_call(response.ok)
        
        # Handle 404 errors gracefully for DELETE operations (idempotent)
        # This prevents error logging and retries for already-deleted assets
        if response.status_code == 404:
            logging.info(f"Assets already deleted or not found (404). Treating as success for {len(asset_ids)} asset(s).")
            return None
        
        # Handle redirects (shouldn't happen for DELETE, but be safe)
        if response.status_code in (301, 302, 303, 307, 308):
            redirect_url = response.headers.get('Location')
            logging.debug(f"Following redirect to: {redirect_url}")
            
            # Convert relative redirect URLs to absolute URLs
            if redirect_url and not redirect_url.startswith(('http://', 'https://')):
                redirect_url = urljoin(self.config.base_url, redirect_url)
                logging.debug(f"Converted to absolute URL: {redirect_url}")
            
            response = self.session.request(
                method="delete",
                url=redirect_url,
                data=json_data
            )
            if self.metrics:
                self.metrics.increment_api_call(response.ok)
            
            # Check 404 again after redirect
            if response.status_code == 404:
                logging.info(f"Assets already deleted or not found (404). Treating as success for {len(asset_ids)} asset(s).")
                return None
        
        # Handle authentication errors
        if response.status_code == 401:
            raise RuntimeError(
                "Authentication failed. Verify if the provided credentials are still valid."
            )
        
        # Handle rate limiting
        if response.status_code == 429:
            logging.warning("Rate limit applied, retrying in 10 seconds")
            time.sleep(10)
            # Recursively retry
            return self.delete_bulk_assets(asset_ids)
        
        # For success responses, parse and return
        if response.ok:
            try:
                response_json = response.json()
                return response_json
            except (JSONDecodeError, ValueError):
                return None
        
        # For other errors, raise HTTPError (but 404 is already handled above)
        response.raise_for_status()
        return None

    def add_attributes_bulk(
        self,
        attributes: List[AttributeCreateRequest]
    ) -> List[StringAttribute | BooleanAttribute]:
        """
        Add multiple attributes in bulk.
        Handles missing attribute types gracefully by skipping them.
        
        Args:
            attributes: List of attributes to create
            
        Returns:
            List of created attributes
        """
        if not attributes:
            return []
        
        try:
            # Convert each attribute to dict and ensure value is string
            request_data = []
            for attr in attributes:
                attr_dict = attr.model_dump()
                if isinstance(attr_dict['value'], int):
                    attr_dict['value'] = str(attr_dict['value'])
                request_data.append(attr_dict)
            
            logging.debug(f"Creating attributes with data: {json.dumps(request_data, indent=2)}")
            
            # Make the API call
            response = self.execute_request(
                method="post",
                url="rest/2.0/attributes/bulk",
                body=json.dumps(request_data, separators=(',', ':')),  # Send as JSON string
                model_class=BulkAttributeCreateResponse
            )
            
            # Parse and validate the response
            parsed_response = BulkAttributeCreateResponse.model_validate(response)
            return parsed_response.root
            
        except requests.exceptions.HTTPError as e:
            # Handle 404 errors for missing attribute types
            if e.response is not None and e.response.status_code == 404:
                try:
                    error_data = e.response.json()
                    error_message = error_data.get('userMessage', '')
                    error_code = error_data.get('errorCode', '')
                    
                    # Check if this is an "attribute type not found" error
                    if error_code == 'attTypeNotFoundId' and 'properties' in error_data:
                        missing_type_id = error_data['properties'].get('id')
                        if missing_type_id:
                            logging.warning(
                                f"Attribute type '{missing_type_id}' not found in Collibra. "
                                f"Skipping attributes with this type and retrying with remaining attributes."
                            )
                            
                            # Filter out attributes with the missing type ID
                            remaining_attributes = [
                                attr for attr in attributes 
                                if attr.typeId != missing_type_id
                            ]
                            
                            if remaining_attributes:
                                # Recursively retry with remaining attributes
                                logging.debug(f"Retrying with {len(remaining_attributes)} remaining attributes")
                                return self.add_attributes_bulk(remaining_attributes)
                            else:
                                logging.warning(f"All attributes were skipped due to missing attribute type '{missing_type_id}'")
                                return []
                except (ValueError, KeyError) as parse_error:
                    logging.debug(f"Could not parse error response: {parse_error}")
            
            # Re-raise if not a handled 404 or if parsing failed
            logging.error(f"Error adding attributes in bulk: {str(e)}")
            raise
        except Exception as e:
            logging.error(f"Error adding attributes in bulk: {str(e)}")
            raise

    def find_attributes(
        self,
        asset_id: str,
        offset: int = 0,
        limit: int = 0,
        count_limit: int = -1,
        sort_order: str = "DESC",
        sort_field: str = "LAST_MODIFIED"
    ) -> AttributeSearchResponse:
        """
        Find attributes for a specific asset in Collibra.
        
        Args:
            asset_id (str): The ID of the asset to find attributes for
            offset (int): Offset for pagination (default: 0)
            limit (int): Maximum number of results to return (default: 0 for no limit)
            count_limit (int): Maximum number of results to count (default: -1 for no limit)
            sort_order (str): Sort order, either "ASC" or "DESC" (default: "DESC")
            sort_field (str): Field to sort by (default: "LAST_MODIFIED")
            
        Returns:
            AttributeSearchResponse: The search results containing attributes
        """
        params = {
            "assetId": asset_id,
            "offset": offset,
            "limit": limit,
            "countLimit": count_limit,
            "sortOrder": sort_order,
            "sortField": sort_field
        }
        
        response = self.execute_request(
            method="get",
            url="rest/2.0/attributes",
            params=params,
            model_class=AttributeSearchResponse
        )
        return response

    def change_attributes_bulk(
        self,
        attributes: List[AttributeUpdateRequest]
    ) -> List[StringAttribute | BooleanAttribute]:
        """
        Update multiple attributes in bulk.
        
        Args:
            attributes: List of attributes to update, each containing an id and new value
            
        Returns:
            List of updated attributes
        """
        try:
            # Convert each attribute to dict and ensure value is string
            request_data = []
            for attr in attributes:
                attr_dict = attr.model_dump()
                if isinstance(attr_dict['value'], int):
                    attr_dict['value'] = str(attr_dict['value'])
                request_data.append(attr_dict)
            
            logging.debug(f"Updating attributes with data: {json.dumps(request_data, indent=2)}")
            
            # Make the API call
            response = self.execute_request(
                method="patch",
                url="rest/2.0/attributes/bulk",
                body=json.dumps(request_data, separators=(',', ':')),  # Send as JSON string
                model_class=BulkAttributeCreateResponse
            )
            
            # Parse and validate the response
            parsed_response = BulkAttributeCreateResponse.model_validate(response)
            return parsed_response.root
            
        except Exception as e:
            logging.error(f"Error updating attributes in bulk: {str(e)}")
            raise

    def get_responsibilities(
        self,
        resource_id: str,
        role_id: str | None = None,
        offset: int = 0,
        limit: int = 0,
        count_limit: int = -1,
        include_inherited: bool = True,
        sort_order: str = "DESC",
        sort_field: str = "LAST_MODIFIED",
        type_param: str = "RESOURCE"
    ) -> ResponsibilitySearchResponse:
        """
        Get responsibilities for a specific resource (asset) in Collibra.
        
        Args:
            resource_id (str): The ID of the resource (asset) to find responsibilities for
            role_id (str | None): Optional role ID to filter by (e.g. "00000000-0000-0000-0000-000000005040" for Owner role)
            offset (int): Offset for pagination (default: 0)
            limit (int): Maximum number of results to return (default: 0 for no limit)
            count_limit (int): Maximum number of results to count (default: -1 for no limit)
            include_inherited (bool): Whether to include inherited responsibilities (default: True)
            sort_order (str): Sort order, either "ASC" or "DESC" (default: "DESC")
            sort_field (str): Field to sort by (default: "LAST_MODIFIED")
            type_param (str): Type of responsibility (default: "RESOURCE")
            
        Returns:
            ResponsibilitySearchResponse: The search results containing responsibilities
        """
        params = {
            "resourceIds": resource_id,
            "offset": offset,
            "limit": limit,
            "countLimit": count_limit,
            "includeInherited": include_inherited,
            "sortField": sort_field,
            "sortOrder": sort_order,
            "type": type_param
        }
        
        # Only add roleIds if provided
        if role_id is not None:
            params["roleIds"] = role_id
        
        response = self.execute_request(
            method="get",
            url="rest/2.0/responsibilities",
            params=params,
            model_class=ResponsibilitySearchResponse
        )
        return response

    def get_user_information(
        self,
        user_ids: str | List[str] | None = None,
        group_id: str | None = None,
        offset: int = 0,
        limit: int = 0,
        count_limit: int = -1,
        sort_order: str = "ASC",
        sort_field: str = "USERNAME"
    ) -> UserSearchResponse:
        """
        Get user information from Collibra.
        
        Args:
            user_ids (str | List[str] | None): Single user ID or list of user IDs to retrieve
            group_id (str | None): Optional group ID to get users from a specific group
            offset (int): Offset for pagination (default: 0)
            limit (int): Maximum number of results to return (default: 0 for no limit)
            count_limit (int): Maximum number of results to count (default: -1 for no limit)
            sort_order (str): Sort order, either "ASC" or "DESC" (default: "ASC")
            sort_field (str): Field to sort by (default: "USERNAME")
            
        Returns:
            UserSearchResponse: The search results containing user information
            
        Raises:
            ValueError: If neither user_ids nor group_id is provided
        """
        if user_ids is None and group_id is None:
            raise ValueError("Either user_ids or group_id must be provided")
        
        params = {
            "offset": offset,
            "limit": limit,
            "countLimit": count_limit,
            "sortOrder": sort_order,
            "sortField": sort_field
        }
        
        # Handle user IDs - API expects multiple userId parameters for multiple users
        if user_ids is not None:
            if isinstance(user_ids, str):
                params["userId"] = user_ids
            else:
                # For multiple user IDs, we need to pass them as multiple parameters
                # This will be handled by requests library properly
                params["userId"] = user_ids
        
        # Handle group ID
        if group_id is not None:
            params["groupId"] = group_id
        
        response = self.execute_request(
            method="get",
            url="rest/2.0/users",
            params=params,
            model_class=UserSearchResponse
        )
        return response

