from typing import Optional, List, Dict
from models.base import BaseModelConfig
from pydantic import Field


class SodaBaseModel(BaseModelConfig):
    pass


class SodaTestConnection(SodaBaseModel):
    organisationName: str


# User model for search results
class User(SodaBaseModel):
    userId: str
    firstName: str
    lastName: str
    fullName: str
    email: str


# User search response model
class UserSearchResponse(SodaBaseModel):
    content: List[User]
    totalElements: int
    totalPages: int
    number: int
    size: int
    last: bool
    first: bool


# Dataset model for use within checks (simplified version)
class Dataset(SodaBaseModel):
    id: str
    name: str
    cloudUrl: str


# Base user model
class Owner(SodaBaseModel):
    firstName: str
    lastName: str
    fullName: str
    email: str


# New models for full dataset response
class Datasource(SodaBaseModel):
    name: str
    label: str
    type: Optional[str] = None
    prefix: Optional[str] = None


class UserGroup(SodaBaseModel):
    id: str = Field(..., alias="userGroupId")
    name: str


class DatasetOwner(SodaBaseModel):
    type: str
    user: Optional[Owner] = None
    userGroup: Optional[UserGroup] = None


class FullDataset(SodaBaseModel):
    id: str
    name: str
    label: str
    qualifiedName: str
    lastUpdated: str
    datasource: Datasource
    dataQualityStatus: str
    healthStatus: int
    checks: int
    incidents: int
    cloudUrl: str
    owners: List[DatasetOwner]
    attributes: Optional[Dict] = {}
    tags: List[str]


# Model for updating a dataset
class DatasetOwnerUpdate(SodaBaseModel):
    type: str = "user"
    userId: str


class UpdateDatasetRequest(SodaBaseModel):
    label: Optional[str] = None
    attributes: Optional[Dict] = None
    tags: Optional[List[str]] = None
    owners: Optional[List[DatasetOwnerUpdate]] = None


class Agreement(SodaBaseModel):
    name: str
    cloudUrl: str


class Incident(SodaBaseModel):
    id: str
    number: int
    name: str
    status: str
    cloudUrl: str


class CheckResultValue(SodaBaseModel):
    id: Optional[str] = None
    value: Optional[float] = None
    diagnostics: Optional[Dict] = None  # Keep as flexible Dict to handle any future diagnostic types
    dataTimestamp: Optional[str] = None
    anomalyDetectionDetails: Optional[Dict] = None


class SodaCheck(SodaBaseModel):
    id: str
    name: str
    evaluationStatus: str
    lastCheckRunTime: Optional[str] = None
    column: Optional[str] = None
    definition: Optional[str] = None
    datasets: List[Dataset]
    attributes: Dict
    owner: Owner
    agreements: List[Agreement]
    incidents: List[Incident]
    cloudUrl: str
    lastUpdated: str
    createdAt: str
    group: Dict
    lastCheckResultValue: Optional[CheckResultValue] = None
    checkType: Optional[str] = None  # Present for checks (e.g., "missing", "aggregate", "invalid")
    metricType: Optional[str] = None  # Present for monitors (e.g., "average", "maximumValue", "timeliness")
    
    