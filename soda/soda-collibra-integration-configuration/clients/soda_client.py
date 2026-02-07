import requests
import logging
import json
import time
from typing import List, Dict, Any, TypeVar, Type
from json.decoder import JSONDecodeError
from models.soda import (
    SodaCheck, 
    SodaTestConnection, 
    FullDataset,
    UpdateDatasetRequest,
    UserSearchResponse,
    User
)

T = TypeVar('T')

class SodaClient:
    def __init__(self, config, metrics=None):
        self.config = config
        self.metrics = metrics
        
    def test_connection(self):
        resp = self.execute_request(method="get",url="test-login")
        return SodaTestConnection(**resp)
    
    def get_checks(self, dataset_id: str | None = None):
        """
        Fetch all checks with optional dataset filtering.
        
        Args:
            dataset_id (str | None): Optional dataset ID to filter checks for a specific dataset.
                                    If None, returns all checks.
        """
        url = "checks"
        if dataset_id:
            url = f"{url}?datasetId={dataset_id}&page=0&size=1000"
        else:
            url = f"{url}?page=0&size=1000"
        return self.execute_request(method="get", url=url, model_class=SodaCheck)
    
    def get_datasets(self):
        """Fetch all datasets with pagination support."""
        return self.execute_request(method="get", url="datasets?page=0&size=1000", model_class=FullDataset)
    
    def update_dataset(self, dataset_id: str, update_data: UpdateDatasetRequest) -> FullDataset:
        """
        Update an existing dataset.
        
        Args:
            dataset_id (str): The ID of the dataset to update.
            update_data (UpdateDatasetRequest): The data to update the dataset with.
            
        Returns:
            FullDataset: The updated dataset.
        """
        response = self.execute_request(
            method="post",
            url=f"datasets/{dataset_id}",
            body=update_data.model_dump(exclude_none=True),
            model_class=FullDataset
        )
        return response
    
    def find_user(self, search_term: str, size: int = 100) -> List[User]:
        """
        Search for users by email or name.
        
        Args:
            search_term (str): The search term to look for in user names or emails.
            size (int): Maximum number of users to return (default: 100).
            
        Returns:
            List[User]: List of users matching the search term.
        """
        response = self.execute_request(
            method="get",
            url=f"users?search={search_term}&size={size}",
            skip_pagination=True
        )
        
        if isinstance(response, dict) and "content" in response:
            return [User(**user) for user in response["content"]]
        return []
    
    def _handle_pagination(self, response: Dict[str, Any], model_class: Type[T] = None, page_size: int = 50) -> List[Any]:
        """Handle pagination for GET requests that return paginated results."""
        all_items = []
        page = 0
        
        # Extract page size from URL if present, otherwise use default
        request_url = response.get("_request_url", "")
        if "size=" in request_url:
            import re
            size_match = re.search(r'size=(\d+)', request_url)
            if size_match:
                size = int(size_match.group(1))
            else:
                size = page_size
        else:
            size = page_size
            
        total_pages = response["totalPages"]
        total_elements = response["totalElements"]
        base_url = response.get("_request_url", "").split("?")[0]  # Remove any existing query parameters
        
        logging.debug(f"Starting pagination: {total_elements} total elements across {total_pages} pages")
        
        # Process first page
        items = response["content"]
        if model_class:
            try:
                items = [model_class(**item) for item in items]
            except Exception as e:
                logging.error(f"Failed to parse first page items into {model_class.__name__} model: {e}")
                logging.error(f"Sample item data: {json.dumps(items[0] if items else {}, indent=2)[:300]}...")
                raise
        all_items.extend(items)
        logging.debug(f"Processed page 1: {len(items)} items")
        
        # Process remaining pages if any
        while page < total_pages - 1:
            page += 1
            
            # Add delay every 10 requests to avoid rate limiting
            if page % 10 == 0:
                logging.debug(f"Rate limit prevention: waiting 2 seconds after {page} requests...")
                time.sleep(2)
            
            # Construct URL with query parameters
            url = f"{base_url}?page={page}&size={size}"
            logging.debug(f"Fetching page {page + 1} of {total_pages} from {url}")
            
            next_page = self.execute_request(
                method="get",
                url=url,
                skip_pagination=True  # Prevent infinite recursion
            )
            
            if not next_page or "content" not in next_page:
                logging.error(f"Invalid response for page {page + 1}: {next_page}")
                break
                
            items = next_page["content"]
            if model_class:
                try:
                    items = [model_class(**item) for item in items]
                except Exception as e:
                    logging.error(f"Failed to parse page {page + 1} items into {model_class.__name__} model: {e}")
                    logging.error(f"Sample item data: {json.dumps(items[0] if items else {}, indent=2)[:300]}...")
                    raise
            all_items.extend(items)
            logging.debug(f"Processed page {page + 1}: {len(items)} items (total: {len(all_items)})")
            
        logging.debug(f"Pagination complete: processed {len(all_items)} items")
        return all_items
    
    def execute_request(
        self,
        method: str,
        url: str,
        params: dict | None = None,
        body: dict | None = None,
        is_retry: bool = False,
        model_class: Type[T] = None,
        skip_pagination: bool = False
    ) -> List[T] | Dict[str, Any]:
        # Add default pagination parameters for GET requests if not already present
        if method.lower() == "get" and not skip_pagination and "page=" not in url and "size=" not in url:
            separator = "&" if "?" in url else "?"
            url = f"{url}{separator}page=0&size=10"
            
        logging.debug(f"> {url}")

        response = requests.request(
            method,
            f"{self.config.base_url}/{url}",
            params=params,
            json=body,
            auth=(self.config.api_key_id,self.config.api_key_secret.get_secret_value()),
        )
        
        # Track API call in metrics
        if self.metrics:
            self.metrics.increment_api_call(response.ok)
        
        response.raise_for_status()
        response_json = None
        
        # Handle empty responses (204 No Content)
        if response.status_code == 204 or not response.text.strip():
            logging.debug(f"< {response.status_code} - Empty response")
            return None
        
        try:
            response_json = response.json()
        except JSONDecodeError as e:
            # Log more details about the failed JSON parsing
            response_preview = response.text[:500] if response.text else "(empty)"
            logging.warning(f"Not able to parse response as JSON (status {response.status_code}): {e}")
            logging.debug(f"Response preview: {response_preview}")
            # For non-empty responses that aren't JSON, this is likely an error
            # Return None to allow the caller to handle it, but log the issue
            if response.text.strip():
                logging.error(f"Non-JSON response received. This may indicate an API error.")
                logging.error(f"Full response text: {response.text}")
        
        logging.debug(f"< {response.status_code}")

        if response.status_code == 401 and not is_retry:
            raise RuntimeError(
                "Authentication failed. Verify if the provided API Key is still valid."
            )
        if response.status_code == 429:
            retry_delay = 15 if not is_retry else 30  # Longer delay on retry
            logging.warning(f"Rate limit hit, waiting {retry_delay} seconds before retry...")
            time.sleep(retry_delay)
            return self.execute_request(
                method,
                url,
                params=params,
                body=body,
                is_retry=True,
                model_class=model_class,
                skip_pagination=skip_pagination
            )
        else:
            # Only assert if we have a response_json to include in the error message
            if response_json is not None:
                assert (
                    response.ok
                ), f"Request failed with status {response.status_code}: {json.dumps(response_json, indent=2)}"
            elif not response.ok:
                assert False, f"Request failed with status {response.status_code} and non-JSON response: {response.text[:200]}"
        
        # Add request URL to response for pagination
        if isinstance(response_json, dict):
            response_json["_request_url"] = url
        
        # Handle pagination for GET requests
        if method.lower() == "get" and not skip_pagination and isinstance(response_json, dict) and "content" in response_json and "totalPages" in response_json:
            return self._handle_pagination(response_json, model_class)
        
        # Convert response to model if model_class is provided
        if model_class and response_json:
            try:
                if isinstance(response_json, list):
                    return [model_class(**item) for item in response_json]
                return model_class(**response_json)
            except Exception as e:
                logging.error(f"Failed to parse API response into {model_class.__name__} model: {e}")
                logging.error(f"Raw response data: {json.dumps(response_json, indent=2)[:500]}...")
                raise
            
        return response_json
    

