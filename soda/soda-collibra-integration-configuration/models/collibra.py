from models.base import BaseModelConfig
from typing import List, Optional
from pydantic import RootModel


class CollibraBaseModel(BaseModelConfig):
    pass


class Version(CollibraBaseModel):
    major: int
    minor: int
    fullVersion: str
    displayVersion: Optional[str] = None


class Solution(CollibraBaseModel):
    name: str
    version: Version


class ApplicationInfo(CollibraBaseModel):
    baseUrl: str
    version: Version
    buildNumber: str
    solutions: List[Solution]


class ResourceReference(CollibraBaseModel):
    id: str
    resourceType: str
    resourceDiscriminator: str
    name: Optional[str] = None


class Asset(CollibraBaseModel):
    id: str
    createdBy: str
    createdOn: int
    lastModifiedBy: str
    lastModifiedOn: int
    system: bool
    resourceType: str
    name: str
    displayName: str
    articulationScore: Optional[float] = None
    excludedFromAutoHyperlinking: bool
    domain: ResourceReference
    type: ResourceReference
    status: ResourceReference
    avgRating: float
    ratingsCount: int


class AssetSearchResponse(CollibraBaseModel):
    total: int
    offset: int
    limit: int
    results: List[Asset]


class AssetCreateRequest(CollibraBaseModel):
    name: str
    displayName: str
    domainId: str
    typeId: str


class AssetCreateResponse(CollibraBaseModel):
    id: str
    createdBy: str
    createdOn: int
    lastModifiedBy: str
    lastModifiedOn: int
    system: bool
    resourceType: str
    name: str
    displayName: str
    articulationScore: float
    excludedFromAutoHyperlinking: bool
    domain: ResourceReference
    type: ResourceReference
    status: ResourceReference
    avgRating: float
    ratingsCount: int


class StringAttribute(CollibraBaseModel):
    id: str
    createdBy: str
    createdOn: int
    lastModifiedBy: str
    lastModifiedOn: int
    system: bool
    resourceType: str
    type: ResourceReference
    asset: ResourceReference
    attributeDiscriminator: str
    value: str | int | float

    def model_dump(self, **kwargs):
        data = super().model_dump(**kwargs)
        if isinstance(data['value'], (int, float)):
            data['value'] = str(data['value'])
        return data


class AttributeSetRequest(CollibraBaseModel):
    typeId: str
    values: List[str]


class Relation(CollibraBaseModel):
    id: str
    createdBy: str
    createdOn: int
    lastModifiedBy: str
    lastModifiedOn: int
    system: bool
    resourceType: str
    source: ResourceReference
    target: ResourceReference
    type: ResourceReference

    class Config:
        extra = "allow"  # Allow extra fields in the response


class RelationSetRequest(CollibraBaseModel):
    typeId: str
    relatedAssetIds: List[str]
    relationDirection: str = "TO_TARGET"  # Default to TO_TARGET as shown in example


# Use RootModel to handle list response
BulkAssetCreateResponse = RootModel[List[Asset]]
StringAttributeResponse = RootModel[List[StringAttribute]]
RelationResponse = RootModel[List[Relation]]


class AssetUpdateRequest(CollibraBaseModel):
    id: str
    name: str
    displayName: str
    typeId: str
    domainId: str


class BulkAssetUpdateResponse(RootModel):
    root: List[Asset]


class AttributeCreateRequest(CollibraBaseModel):
    assetId: str
    typeId: str
    value: str | bool | int | float

    def model_dump(self, **kwargs):
        data = super().model_dump(**kwargs)
        # Convert value to string if it's a number
        if isinstance(data['value'], (int, float)):
            data['value'] = str(data['value'])
        return data


class BooleanAttribute(CollibraBaseModel):
    id: str
    createdBy: str
    createdOn: int
    lastModifiedBy: str
    lastModifiedOn: int
    system: bool
    resourceType: str
    type: ResourceReference
    asset: ResourceReference
    attributeDiscriminator: str
    value: bool | int | float

    def model_dump(self, **kwargs):
        data = super().model_dump(**kwargs)
        if isinstance(data['value'], (int, float)):
            data['value'] = bool(data['value'])
        return data


class BulkAttributeCreateResponse(RootModel):
    root: List[StringAttribute | BooleanAttribute]

    @classmethod
    def model_validate(cls, obj):
        # Convert any numeric values appropriately in the response
        if isinstance(obj, dict) and 'root' in obj:
            for item in obj['root']:
                if isinstance(item.get('value'), (int, float)):
                    # Check the attributeDiscriminator to determine if it should be string or boolean
                    if item.get('attributeDiscriminator') == 'BOOLEAN':
                        item['value'] = bool(item['value'])
                    else:
                        item['value'] = str(item['value'])
        return super().model_validate(obj)


class AttributeSearchResponse(CollibraBaseModel):
    total: int
    offset: int
    limit: int
    results: List[StringAttribute | BooleanAttribute]


class AttributeUpdateRequest(CollibraBaseModel):
    id: str
    value: str | bool | int | float

    def model_dump(self, **kwargs):
        data = super().model_dump(**kwargs)
        # Convert value to string if it's a number
        if isinstance(data['value'], (int, float)):
            data['value'] = str(data['value'])
        return data


class Role(CollibraBaseModel):
    id: str
    resourceType: str
    resourceDiscriminator: str
    name: str


class BaseResource(CollibraBaseModel):
    id: str
    resourceType: str
    resourceDiscriminator: str


class Owner(CollibraBaseModel):
    id: str
    resourceType: str
    resourceDiscriminator: str


class Responsibility(CollibraBaseModel):
    id: str
    createdBy: str
    createdOn: int
    lastModifiedBy: str
    lastModifiedOn: int
    system: bool
    resourceType: str
    role: Role
    baseResource: BaseResource
    owner: Owner


class ResponsibilitySearchResponse(CollibraBaseModel):
    total: int
    offset: int
    limit: int
    results: List[Responsibility]


class User(CollibraBaseModel):
    id: str
    createdBy: Optional[str] = None
    createdOn: int
    lastModifiedBy: Optional[str] = None
    lastModifiedOn: int
    system: bool
    resourceType: str
    userName: str
    firstName: str
    lastName: str
    emailAddress: str
    gender: Optional[str] = None
    language: str
    additionalEmailAddresses: List[str]
    phoneNumbers: List[str]
    instantMessagingAccounts: List[str]
    websites: List[str]
    addresses: List[str]
    activated: bool
    enabled: bool
    ldapUser: bool
    userSource: str
    guestUser: bool
    apiUser: bool
    licenseType: str


class UserSearchResponse(CollibraBaseModel):
    total: int
    offset: int
    limit: int
    results: List[User]

