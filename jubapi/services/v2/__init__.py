
import os
import uuid 
import asyncio
import datetime as DT
from option import Result,Ok,Err
from typing import List,Optional,Tuple,Dict,Any,Set
from pymongo.results import UpdateResult,DeleteResult

from jubapi.utils import Utils
import jubapi.models.v2 as M
import jubapi.repositories.v2 as R
import jubapi.dto.v2 as DTO
import jubapi.enums.v2 as ENUMS
from jubapi.querylang.v2.parser  import QueryAST,Condition,ConditionOperators,ConditionGroup,SPATIAL_VARIABLE,TEMPORAL_VARIABLE,INTEREST_VARIABLE,OBSERVABLE_VARIABLE,GROUP_VARIABLE
from jubapi.querylang.v2.translator import ASTToMongoTranslator
from jubapi.querylang.v2.service_parser import ServiceQuery
from jubapi.log.log import Log
from jubapi.storage import StorageBackend
from cachetools import TTLCache
import jubapi.errors as EX
from jubapi.db.constants import CollectionNames
from jubapi.config import JUB_SEARCH_PRODUCT_CACHE_TTL, JUB_SEARCH_OBSERVATORY_CACHE_TTL
import commonx.dto.xolo as XoloDTO
from xolo.client.client import XoloClient

L = Log(
    name = __name__,
    path = os.environ.get("JUB_LOG_PATH", "/log"),
)



class AuthenticationService:
    def __init__(self, xolo: XoloClient ):
        self.xolo = xolo
    async def login(self, dto: XoloDTO.AuthAttemptDTO) -> Result[XoloDTO.AuthenticatedDTO, EX.JubError]:
        try:
            res = self.xolo.auth(
                username    = dto.username,
                password    = dto.password,
                scope       = dto.scope,
                expiration  = dto.expiration,
                renew_token = dto.renew_token
            )
            if res.is_err:
                L.error(f"Xolo login failed: {res.unwrap_err()}")
                return Err(EX.JubError(f"Xolo login failed: {res.unwrap_err()}"))
            response = res.unwrap()
            return Ok(response)
        except Exception as e:
            L.error(f"Error during login: {e}")
            return Err(EX.JubError.from_exception(e))
        
    async def signup(self, 
        first_name: str,
        last_name: str,
        scope: str,
        username: str,
        email: str,
        password: str,
        expiration:Optional[str] = "1h",
        profile_photo: Optional[str]="",
    ) -> Result[XoloDTO.CreatedUserResponseDTO, EX.JubError]:
        try:
            L.debug({
                "msg": "Attempting to sign up user with Xolo",
                "username": username,
                "email": email,
                "scope": scope,
                "api_url": self.xolo.api_url
            })
            res = self.xolo.signup(
                username      = username,
                first_name    = first_name,
                last_name     = last_name,
                email         = email,
                password      = password,
                scope         = scope,
                expiration    = expiration,
                profile_photo = profile_photo,
            )
            if res.is_err:
                L.error(f"Xolo signup failed: {res.unwrap_err()}")
                return Err(EX.JubError(f"Xolo signup failed: {res.unwrap_err()}"))
            response = res.unwrap()

            return Ok(response)
        except Exception as e:
            L.error(f"Error during signup: {e}")
            return Err(EX.JubError.from_exception(e))
    
class GraphLinkManager:
    """
    Centralized manager for severing edges in the Jub graph.
    Relies strictly on injected Link Repositories.
    """
    def __init__(
        self,
        observatory_product_link_repository: R.ObservatoryToProductLinkRepository,
        observatory_catalog_link_repository: R.ObservatoryToCatalogLinkRepository,
        catalog_catalog_item_link_repository: R.CatalogToCatalogItemLinkRepository,
        product_catalog_item_link_repository: R.ProductToCatalogItemLinkRepository,
        catalog_item_relationship_repository: R.CatalogItemRelationshipRepository,
        catalog_item_catalog_alias_link_repository: R.CatalogItemToCatalogAliasLinkRepository,
        observatory_service_link_repository: R.ObservatoryToServiceLinkRepository,
        observatory_datasource_link_repository: R.ObservatoryToDataSourceLinkRepository,
        product_product_link_repository: R.ProductToProductLinkRepository,
    ):
        self.observatory_product_link_repository        = observatory_product_link_repository
        self.observatory_catalog_link_repository        = observatory_catalog_link_repository
        self.catalog_catalog_item_link_repository               = catalog_catalog_item_link_repository
        self.product_catalog_item_link_repository       = product_catalog_item_link_repository
        self.catalog_item_relationship_repository       = catalog_item_relationship_repository
        self.catalog_item_catalog_alias_link_repository = catalog_item_catalog_alias_link_repository
        self.observatory_service_link_repository        = observatory_service_link_repository
        self.observatory_datasource_link_repository     = observatory_datasource_link_repository
        self.product_product_link_repository            = product_product_link_repository


    async def get_alias_links_by_item_ids(self, item_ids: List[str]) -> Result[List[M.CatalogItemToCatalogAliasLink], EX.JubError]:
        try:
            cursor = self.catalog_item_catalog_alias_link_repository.collection.find(
                {"catalog_item_id": {"$in": item_ids}}
            )
            docs = await cursor.to_list(length=None)
            xs:List[M.CatalogItemToCatalogAliasLink] = []
            for doc in docs:
                del doc["_id"]  # Remove MongoDB's internal ID if not needed in the model
                xs.append(M.CatalogItemToCatalogAliasLink.model_validate(doc))
            return Ok(xs)
        except Exception as e:
            L.error(f"Error getting alias links by item IDs: {e}")
            return Err(EX.JubError.from_exception(e))   
    
    async def get_relationships_by_item_ids(self, item_ids: List[str]) -> Result[List[M.CatalogItemRelationship], EX.JubError]:
        try:
            cursor = self.catalog_item_relationship_repository.collection.find(
                {"$or": [{"parent_id": {"$in": item_ids}}, {"child_id": {"$in": item_ids}}]}
            )
            docs = await cursor.to_list(length=None)
            xs:List[M.CatalogItemRelationship] = []
            for doc in docs:
                del doc["_id"]  # Remove MongoDB's internal ID if not needed in the model
                xs.append(M.CatalogItemRelationship.model_validate(doc))
            return Ok(xs)
        except Exception as e:
            L.error(f"Error getting relationships by item IDs: {e}")
            return Err(EX.JubError.from_exception(e))
    # Get links
    async def get_products_linked_to_observatory(self, observatory_id: str) -> Result[List[str], EX.JubError]:
        try:
            cursor = self.observatory_product_link_repository.collection.find({"observatory_id": observatory_id})
            results = await cursor.to_list(length=None)
            product_ids = [doc["product_id"] for doc in results]
            return Ok(product_ids)
        except Exception as e:
            L.error(f"Error getting products linked to observatory: {e}")
            return Err(EX.JubError.from_exception(e))

    async def count_products_linked_to_observatory(self, observatory_id: str) -> Result[int, EX.JubError]:
        try:
            count = await self.observatory_product_link_repository.collection.count_documents({"observatory_id": observatory_id})
            return Ok(count)
        except Exception as e:
            L.error(f"Error counting products linked to observatory: {e}")
            return Err(EX.JubError.from_exception(e))
        
    async def exists_product_linked_to_observatory(self, observatory_id: str, product_id:str) -> Result[bool, EX.JubError]:
        try:
            count = await self.observatory_product_link_repository.collection.count_documents({"observatory_id": observatory_id, "product_id": product_id})
            return Ok(count > 0)
        except Exception as e:
            L.error(f"Error checking existence of product linked to observatory: {e}")
            return Err(EX.JubError.from_exception(e))
        
    async def exists_products_linked_to_observatory(self, observatory_id: str) -> Result[bool, EX.JubError]:
        try:
            count = await self.observatory_product_link_repository.collection.count_documents({"observatory_id": observatory_id})
            return Ok(count > 0)
        except Exception as e:
            L.error(f"Error checking existence of products linked to observatory: {e}")
            return Err(EX.JubError.from_exception(e))
    
    # _______________________
    async def get_catalogs_linked_to_observatory(self, observatory_id: str) -> Result[List[str], EX.JubError]:
        try:
            cursor = self.observatory_catalog_link_repository.collection.find({"observatory_id": observatory_id})
            results = await cursor.to_list(length=None)
            catalog_ids = [doc["catalog_id"] for doc in results]
            return Ok(catalog_ids)
        except Exception as e:
            L.error(f"Error getting catalogs linked to observatory: {e}")
            return Err(EX.JubError.from_exception(e))
    async def count_catalogs_linked_to_observatory(self, observatory_id: str) -> Result[int, EX.JubError]:
        try:
            count = await self.observatory_catalog_link_repository.collection.count_documents({"observatory_id": observatory_id})
            return Ok(count)
        except Exception as e:
            L.error(f"Error counting catalogs linked to observatory: {e}")
            return Err(EX.JubError.from_exception(e))
    async def exists_catalog_linked_to_observatory(self, observatory_id: str, catalog_id:str) -> Result[bool, EX.JubError]:
        try:
            count = await self.observatory_catalog_link_repository.collection.count_documents({"observatory_id": observatory_id, "catalog_id": catalog_id})
            return Ok(count > 0)
        except Exception as e:
            L.error(f"Error checking existence of catalog linked to observatory: {e}")
            return Err(EX.JubError.from_exception(e))
    async def exists_catalogs_linked_to_observatory(self, observatory_id: str) -> Result[bool, EX.JubError]:
        try:
            count = await self.observatory_catalog_link_repository.collection.count_documents({"observatory_id": observatory_id})
            return Ok(count > 0)
        except Exception as e:
            L.error(f"Error checking existence of catalogs linked to observatory: {e}")
            return Err(EX.JubError.from_exception(e))
    # _______________________
    async def get_catalog_items_linked_to_catalog(self, catalog_id: str) -> Result[List[str], EX.JubError]:
        try:
            cursor = self.catalog_catalog_item_link_repository.collection.find({"catalog_id": catalog_id})
            results = await cursor.to_list(length=None)
            catalog_item_ids = [doc["catalog_item_id"] for doc in results]
            return Ok(catalog_item_ids)
        except Exception as e:
            L.error(f"Error getting catalog items linked to catalog: {e}")
            return Err(EX.JubError.from_exception(e))
    async def count_catalog_items_linked_to_catalog(self, catalog_id: str) -> Result[int, EX.JubError]:
        try:
            count = await self.catalog_catalog_item_link_repository.collection.count_documents({"catalog_id": catalog_id})
            return Ok(count)
        except Exception as e:
            L.error(f"Error counting catalog items linked to catalog: {e}")
            return Err(EX.JubError.from_exception(e))
    async def exists_catalog_item_linked_to_catalog(self, catalog_id: str, catalog_item_id:str) -> Result[bool, EX.JubError]:
        try:
            count = await self.catalog_catalog_item_link_repository.collection.count_documents({"catalog_id": catalog_id, "catalog_item_id": catalog_item_id})
            return Ok(count > 0)
        except Exception as e:
            L.error(f"Error checking existence of catalog item linked to catalog: {e}")
            return Err(EX.JubError.from_exception(e))
    async def exists_catalog_items_linked_to_catalog(self, catalog_id: str) -> Result[bool, EX.JubError]:
        try:
            count = await self.catalog_catalog_item_link_repository.collection.count_documents({"catalog_id": catalog_id})
            return Ok(count > 0)
        except Exception as e:
            L.error(f"Error checking existence of catalog items linked to catalog: {e}")
            return Err(EX.JubError.from_exception(e))
    # _______________________
    async def get_catalog_items_linked_to_product(self, product_id: str) -> Result[List[str], EX.JubError]:
        try:
            cursor = self.product_catalog_item_link_repository.collection.find({"product_id": product_id})
            results = await cursor.to_list(length=None)
            catalog_item_ids = [doc["catalog_item_id"] for doc in results]
            return Ok(catalog_item_ids)
        except Exception as e:
            L.error(f"Error getting catalog items linked to product: {e}")
            return Err(EX.JubError.from_exception(e))
    async def count_catalog_items_linked_to_product(self, product_id: str) -> Result[int, EX.JubError]:
        try:
            count = await self.product_catalog_item_link_repository.collection.count_documents({"product_id": product_id})
            return Ok(count)
        except Exception as e:
            L.error(f"Error counting catalog items linked to product: {e}")
            return Err(EX.JubError.from_exception(e))
    async def exists_catalog_item_linked_to_product(self, product_id: str, catalog_item_id:str) -> Result[bool, EX.JubError]:
        try:
            count = await self.product_catalog_item_link_repository.collection.count_documents({"product_id": product_id, "catalog_item_id": catalog_item_id})
            return Ok(count > 0)
        except Exception as e:
            L.error(f"Error checking existence of catalog item linked to product: {e}")
            return Err(EX.JubError.from_exception(e))
    async def exists_catalog_items_linked_to_product(self, product_id: str) -> Result[bool, EX.JubError]:
        try:
            count = await self.product_catalog_item_link_repository.collection.count_documents({"product_id": product_id})
            return Ok(count > 0)
        except Exception as e:
            L.error(f"Error checking existence of catalog items linked to product: {e}")
            return Err(EX.JubError.from_exception(e))
    # _______________________


    async def link_observatory_to_product(self, observatory_id: str, product_id: str)->Result[UpdateResult,EX.JubError]:
        try:
            link = M.ObservatoryToProductLink(observatory_id=observatory_id, product_id=product_id)
            r = await self.observatory_product_link_repository.collection.update_one(
                {"observatory_id": observatory_id, "product_id": product_id},
                {"$set": link.model_dump()},
                upsert=True
            )
            return Ok(r)
        except Exception as e:
            L.error(f"Error linking observatory to product: {e}")
            return Err(EX.JubError.from_exception(e))
            # raise EX.JubError(f"Failed to link observatory to product: {str(e)}")

    async def link_observatory_to_catalog(self, observatory_id: str, catalog_id: str,level:int=0)->Result[UpdateResult,EX.JubError]:
        try:
            link = M.ObservatoryToCatalogLink(observatory_id=observatory_id, catalog_id=catalog_id,level=level)
            r = await self.observatory_catalog_link_repository.collection.update_one(
                {"observatory_id": observatory_id, "catalog_id": catalog_id},
                {"$set": link.model_dump()},
                upsert=True
            )
            return Ok(r)
        except Exception as e:
            L.error(f"Error linking observatory to catalog: {e}")
            return Err(EX.JubError.from_exception(e))
        

    async def link_catalog_to_item(self, catalog_id: str, catalog_item_id: str)->Result[UpdateResult,EX.JubError]:
        try:
            link = M.CatalogToCatalogItemLink(catalog_id=catalog_id, catalog_item_id=catalog_item_id)
            r = await self.catalog_catalog_item_link_repository.collection.update_one(
                {"catalog_id": catalog_id, "catalog_item_id": catalog_item_id},
                {"$set": link.model_dump()},
                upsert=True
            )
            return Ok(r)
        except Exception as e:
            L.error(f"Error linking catalog to item: {e}")
            return Err(EX.JubError.from_exception(e))
        

    async def link_product_to_catalog_item(self, product_id: str, catalog_item_id: str)->Result[UpdateResult,EX.JubError]:
        """Tags a product with a specific dimension (e.g., 'FEMALE' or 'VIC')."""
        try:
            link = M.CatalogItemToProductLink(product_id=product_id, catalog_item_id=catalog_item_id)
            r = await self.product_catalog_item_link_repository.collection.update_one(
                {"product_id": product_id, "catalog_item_id": catalog_item_id},
                {"$set": link.model_dump()},
                upsert=True
            )
            return Ok(r)
        except Exception as e:
            L.error(f"Error linking product to catalog item: {e}")
            return Err(EX.JubError.from_exception(e))
        

    async def set_item_relationship(self, parent_id: str, child_id: str)->Result[UpdateResult,EX.JubError]:
        """Builds the hierarchy (e.g., MX -> TAM)."""
        try:
            link = M.CatalogItemRelationship(parent_id=parent_id, child_id=child_id)
            r = await self.catalog_item_relationship_repository.collection.update_one(
                {"parent_id": parent_id, "child_id": child_id},
                {"$set": link.model_dump()},
                upsert=True
            )
            return Ok(r)
        except Exception as e:
            L.error(f"Error setting item relationship: {e}")
            return Err(EX.JubError.from_exception(e))

    async def link_item_to_alias(self, catalog_item_id: str, catalog_item_alias_id: str)->Result[UpdateResult,EX.JubError]:
        """Links an alias/value to the canonical item."""
        try:

            link = M.CatalogItemToCatalogAliasLink(
                catalog_item_id=catalog_item_id, 
                catalog_item_alias_id=catalog_item_alias_id
            )
            r = await self.catalog_item_catalog_alias_link_repository.collection.update_one(
                {"catalog_item_id": catalog_item_id, "catalog_item_value_id": catalog_item_alias_id},
                {"$set": link.model_dump()},
                upsert=True
            )
            return Ok(r)
        except Exception as e:
            L.error(f"Error linking item to value: {e}")
            return Err(EX.JubError.from_exception(e))

    # --------------- Targeted unlink helpers ---------------

    async def unlink_observatory_from_catalog(self, observatory_id: str, catalog_id: str) -> Result[bool, EX.JubError]:
        try:
            r = await self.observatory_catalog_link_repository.collection.delete_one(
                {"observatory_id": observatory_id, "catalog_id": catalog_id}
            )
            return Ok(r.deleted_count > 0)
        except Exception as e:
            L.error(f"Error unlinking catalog {catalog_id} from observatory {observatory_id}: {e}")
            return Err(EX.JubError.from_exception(e))

    async def unlink_observatory_from_product(self, observatory_id: str, product_id: str) -> Result[bool, EX.JubError]:
        try:
            r = await self.observatory_product_link_repository.collection.delete_one(
                {"observatory_id": observatory_id, "product_id": product_id}
            )
            return Ok(r.deleted_count > 0)
        except Exception as e:
            L.error(f"Error unlinking product {product_id} from observatory {observatory_id}: {e}")
            return Err(EX.JubError.from_exception(e))

    async def unlink_product_from_catalog_item(self, product_id: str, catalog_item_id: str) -> Result[bool, EX.JubError]:
        try:
            r = await self.product_catalog_item_link_repository.collection.delete_one(
                {"product_id": product_id, "catalog_item_id": catalog_item_id}
            )
            return Ok(r.deleted_count > 0)
        except Exception as e:
            L.error(f"Error unlinking catalog item {catalog_item_id} from product {product_id}: {e}")
            return Err(EX.JubError.from_exception(e))

    async def unlink_catalog_from_item(self, catalog_id: str, catalog_item_id: str) -> Result[bool, EX.JubError]:
        try:
            r = await self.catalog_catalog_item_link_repository.collection.delete_one(
                {"catalog_id": catalog_id, "catalog_item_id": catalog_item_id}
            )
            return Ok(r.deleted_count > 0)
        except Exception as e:
            L.error(f"Error unlinking item {catalog_item_id} from catalog {catalog_id}: {e}")
            return Err(EX.JubError.from_exception(e))

    async def unlink_item_relationship(self, parent_id: str, child_id: str) -> Result[bool, EX.JubError]:
        try:
            r = await self.catalog_item_relationship_repository.collection.delete_one(
                {"parent_id": parent_id, "child_id": child_id}
            )
            return Ok(r.deleted_count > 0)
        except Exception as e:
            L.error(f"Error removing relationship {parent_id} -> {child_id}: {e}")
            return Err(EX.JubError.from_exception(e))

    async def unlink_alias_from_item(self, catalog_item_id: str, alias_id: str) -> Result[bool, EX.JubError]:
        try:
            r = await self.catalog_item_catalog_alias_link_repository.collection.delete_one(
                {"catalog_item_id": catalog_item_id, "catalog_item_alias_id": alias_id}
            )
            return Ok(r.deleted_count > 0)
        except Exception as e:
            L.error(f"Error unlinking alias {alias_id} from item {catalog_item_id}: {e}")
            return Err(EX.JubError.from_exception(e))

    async def get_product_ids_for_catalog_item(self, catalog_item_id: str) -> Result[List[str], EX.JubError]:
        try:
            cursor = self.product_catalog_item_link_repository.collection.find(
                {"catalog_item_id": catalog_item_id}
            )
            docs = await cursor.to_list(length=None)
            return Ok([doc["product_id"] for doc in docs])
        except Exception as e:
            L.error(f"Error getting product IDs for catalog item {catalog_item_id}: {e}")
            return Err(EX.JubError.from_exception(e))

    async def link_observatory_to_service(self, observatory_id: str, service_id: str) -> Result[UpdateResult, EX.JubError]:
        try:
            link = M.ObservatoryToServiceLink(observatory_id=observatory_id, service_id=service_id)
            r = await self.observatory_service_link_repository.collection.update_one(
                {"observatory_id": observatory_id, "service_id": service_id},
                {"$set": link.model_dump()},
                upsert=True
            )
            return Ok(r)
        except Exception as e:
            L.error(f"Error linking observatory to service: {e}")
            return Err(EX.JubError.from_exception(e))

    async def unlink_observatory_from_service(self, observatory_id: str, service_id: str) -> Result[bool, EX.JubError]:
        try:
            r = await self.observatory_service_link_repository.collection.delete_one(
                {"observatory_id": observatory_id, "service_id": service_id}
            )
            return Ok(r.deleted_count > 0)
        except Exception as e:
            L.error(f"Error unlinking service {service_id} from observatory {observatory_id}: {e}")
            return Err(EX.JubError.from_exception(e))

    async def get_services_linked_to_observatory(self, observatory_id: str) -> Result[List[str], EX.JubError]:
        try:
            ids = await self.observatory_service_link_repository.get_service_ids_by_observatory_id(observatory_id)
            return Ok(ids)
        except Exception as e:
            L.error(f"Error getting services linked to observatory: {e}")
            return Err(EX.JubError.from_exception(e))

    async def link_observatory_to_datasource(self, observatory_id: str, source_id: str) -> Result[UpdateResult, EX.JubError]:
        try:
            link = M.ObservatoryToDataSourceLink(observatory_id=observatory_id, source_id=source_id)
            r = await self.observatory_datasource_link_repository.collection.update_one(
                {"observatory_id": observatory_id, "source_id": source_id},
                {"$set": link.model_dump()},
                upsert=True
            )
            return Ok(r)
        except Exception as e:
            L.error(f"Error linking observatory to datasource: {e}")
            return Err(EX.JubError.from_exception(e))

    async def unlink_observatory_from_datasource(self, observatory_id: str, source_id: str) -> Result[bool, EX.JubError]:
        try:
            r = await self.observatory_datasource_link_repository.collection.delete_one(
                {"observatory_id": observatory_id, "source_id": source_id}
            )
            return Ok(r.deleted_count > 0)
        except Exception as e:
            L.error(f"Error unlinking datasource {source_id} from observatory {observatory_id}: {e}")
            return Err(EX.JubError.from_exception(e))

    async def get_datasources_linked_to_observatory(self, observatory_id: str) -> Result[List[str], EX.JubError]:
        try:
            ids = await self.observatory_datasource_link_repository.get_source_ids_by_observatory_id(observatory_id)
            return Ok(ids)
        except Exception as e:
            L.error(f"Error getting datasources linked to observatory: {e}")
            return Err(EX.JubError.from_exception(e))

    async def remove_all_observatory_links(self, observatory_id: str) -> Result[bool, EX.JubError]:
        """Removes all catalog, product, service, and datasource links for an observatory (used on delete)."""
        try:
            await self.observatory_catalog_link_repository.collection.delete_many({"observatory_id": observatory_id})
            await self.observatory_product_link_repository.collection.delete_many({"observatory_id": observatory_id})
            await self.observatory_service_link_repository.collection.delete_many({"observatory_id": observatory_id})
            await self.observatory_datasource_link_repository.collection.delete_many({"observatory_id": observatory_id})
            return Ok(True)
        except Exception as e:
            L.error(f"Error removing all observatory links for {observatory_id}: {e}")
            return Err(EX.JubError.from_exception(e))

    #  Remove links (called by services when an entity is deleted, to maintain graph integrity)
    # ── Product ↔ Product (Related Products) ──────────────────────────────

    async def link_products(self, id_a: str, id_b: str) -> Result[UpdateResult, EX.JubError]:
        """Creates a symmetric relationship between two products. IDs are normalized so the link is unique regardless of argument order."""
        try:
            source, target = sorted([id_a, id_b])
            link = M.ProductToProductLink(source_product_id=source, target_product_id=target)
            r = await self.product_product_link_repository.collection.update_one(
                {"source_product_id": source, "target_product_id": target},
                {"$set": link.model_dump()},
                upsert=True,
            )
            return Ok(r)
        except Exception as e:
            L.error(f"Error linking products {id_a} <-> {id_b}: {e}")
            return Err(EX.JubError.from_exception(e))

    async def unlink_products(self, id_a: str, id_b: str) -> Result[bool, EX.JubError]:
        """Removes the relationship between two products."""
        try:
            source, target = sorted([id_a, id_b])
            r = await self.product_product_link_repository.collection.delete_one(
                {"source_product_id": source, "target_product_id": target}
            )
            return Ok(r.deleted_count > 0)
        except Exception as e:
            L.error(f"Error unlinking products {id_a} <-> {id_b}: {e}")
            return Err(EX.JubError.from_exception(e))

    async def get_related_product_ids(self, product_id: str) -> Result[List[str], EX.JubError]:
        """Returns all product IDs related to the given product."""
        try:
            ids = await self.product_product_link_repository.get_related_ids(product_id)
            return Ok(ids)
        except Exception as e:
            L.error(f"Error getting related product IDs for {product_id}: {e}")
            return Err(EX.JubError.from_exception(e))

    async def unlink_all_product_relationships(self, product_id: str) -> Result[int, EX.JubError]:
        """Removes all product-to-product links involving the given product. Called on product deletion."""
        try:
            count = await self.product_product_link_repository.delete_all_for_product(product_id)
            return Ok(count)
        except Exception as e:
            L.error(f"Error removing all product relationships for {product_id}: {e}")
            return Err(EX.JubError.from_exception(e))

    async def remove_all_product_links(self, product_id: str)->Result[Tuple[DeleteResult, DeleteResult],EX.JubError]:
        """Called by ProductService when a product is completely deleted."""
        try:
            r1 = await self.observatory_product_link_repository.collection.delete_many({"product_id": product_id})
            r2 = await self.product_catalog_item_link_repository.collection.delete_many({"product_id": product_id})
            return Ok((r1, r2))
        except Exception as e:
            L.error(f"Error removing all product links: {e}")
            return Err(EX.JubError.from_exception(e))


    async def remove_all_catalog_links(self, catalog_id: str)->Result[Tuple[DeleteResult, DeleteResult],EX.JubError]:
        """Called by CatalogService when a catalog is completely deleted."""
        try:
            r1 = await self.observatory_catalog_link_repository.collection.delete_many({"catalog_id": catalog_id})
            r2 = await self.catalog_catalog_item_link_repository.collection.delete_many({"catalog_id": catalog_id})
            return Ok((r1, r2))
        except Exception as e:
            L.error(f"Error removing all catalog links: {e}")
            return Err(EX.JubError.from_exception(e))

    async def remove_all_catalog_item_links(self, catalog_item_id: str)->Result[Tuple[DeleteResult, DeleteResult, DeleteResult, DeleteResult],EX.JubError]:
        """
        Called by CatalogService when an item is deleted. 
        Wipes its tags, its aliases, and its parent/child relationships.
        """
        try:
            r1 = await self.catalog_catalog_item_link_repository.collection.delete_many({"catalog_item_id": catalog_item_id})
            r2 = await self.product_catalog_item_link_repository.collection.delete_many({"catalog_item_id": catalog_item_id})
            r3 = await self.catalog_item_catalog_alias_link_repository.collection.delete_many({"catalog_item_id": catalog_item_id})
            
            # Remove it from the hierarchy tree (whether it was a parent or a child)
            r4 = await self.catalog_item_relationship_repository.collection.delete_many({"$or": [
                {"parent_id": catalog_item_id},
                {"child_id": catalog_item_id}
            ]})
            return Ok((r1, r2, r3, r4))
        except Exception as e:
            L.error(f"Error removing all catalog item links: {e}")
            return Err(EX.JubError.from_exception(e))

    
class ObservatoriesService:
    def __init__(
        self,
        observatory_repository: R.ObservatoriesRepository,
        observatory_product_link_repository: R.ObservatoryToProductLinkRepository,
        product_repository: R.ProductsRepository,
        graph_link_manager: GraphLinkManager,
        review_repository: R.ReviewRepository,
        service_repository: R.ServiceRepository,
        datasource_repository: R.DataSourceRepository,
    ):
        self.observatory_repository = observatory_repository
        self.observatory_product_link_repository = observatory_product_link_repository
        self.product_repository = product_repository
        self.graph_link_manager = graph_link_manager
        self.review_repository = review_repository
        self.service_repository = service_repository
        self.datasource_repository = datasource_repository

    # --- Create ---

    async def create_observatory(self, observatory: M.ObservatoryX) -> Result[str, EX.JubError]:
        exists = await self.observatory_repository.get_by_id(observatory.observatory_id)
        if exists.is_ok:
            return Err(EX.AlreadyExists(f"Observatory '{observatory.observatory_id}' already exists."))
        return await self.observatory_repository.insert(observatory)

    # --- Read ---

    async def get_observatories(self, query: Dict[str, Any] = {}, page_index: int = 0, limit: int = 10) -> Result[List[DTO.ObservatoryXDTO], EX.JubError]:
        try:
            cursor = self.observatory_repository.collection.find(query).skip(page_index * limit).limit(limit)
            observatories = [DTO.ObservatoryXDTO.from_model(M.ObservatoryX.from_doc(doc)) for doc in await cursor.to_list(length=None)]
            return Ok(observatories)
        except Exception as e:
            L.error(f"Error fetching observatories: {e}")
            return Err(EX.JubError.from_exception(e))

    async def get_observatory(self, observatory_id: str) -> Result[DTO.ObservatoryXDTO, EX.JubError]:
        model = await self.observatory_repository.get_by_id(observatory_id)
        if model.is_err:
            return Err(EX.NotFound(f"Observatory '{observatory_id}' not found."))
        return Ok(DTO.ObservatoryXDTO.from_model(model.unwrap()))

    async def get_observatory_detail(self, observatory_id: str, services_limit: int = 10) -> Result[DTO.ObservatoryDetailDTO, EX.JubError]:
        import time as _time
        t0 = _time.monotonic()
        base_result = await self.get_observatory(observatory_id)
        if base_result.is_err:
            L.error({"action": "service.observatory.get_detail", "error": str(base_result.unwrap_err()), "input": {"observatory_id": observatory_id}})
            return base_result  # type: ignore[return-value]
        base = base_result.unwrap()

        services_result, datasources_result, rating_stats = await asyncio.gather(
            self.list_services(observatory_id),
            self.list_datasources(observatory_id),
            self.review_repository.get_rating_stats(observatory_id),
        )

        services = []
        if services_result.is_ok:
            for s in services_result.unwrap()[:services_limit]:
                services.append(DTO.ServiceSnapshotDTO(service_id=s.service_id, name=s.name, provider=s.provider))

        data_sources = []
        if datasources_result.is_ok:
            for ds in datasources_result.unwrap():
                data_sources.append(DTO.DataSourceSnapshotDTO(source_id=ds.source_id, name=ds.name))

        avg_rating, review_count = rating_stats
        detail = DTO.ObservatoryDetailDTO(
            observatory_id = base.observatory_id,
            title          = base.title,
            description    = base.description,
            image_url      = base.image_url,
            metadata       = base.metadata,
            view_count     = base.view_count,
            created_at     = base.created_at,
            updated_at     = base.updated_at,
            services       = services,
            data_sources   = data_sources,
            avg_rating     = avg_rating,
            review_count   = review_count,
        )
        L.info({"action": "service.observatory.get_detail", "duration_ms": int((_time.monotonic()-t0)*1000), "input": {"observatory_id": observatory_id}})
        return Ok(detail)

    async def get_observatory_stats_batch(self, ids: List[str], services_limit: int = 10) -> List[DTO.ObservatoryStatsDTO]:
        import time as _time
        t0 = _time.monotonic()

        async def _stats_for(observatory_id: str) -> Optional[DTO.ObservatoryStatsDTO]:
            try:
                services_result, datasources_result, rating_stats = await asyncio.gather(
                    self.list_services(observatory_id),
                    self.list_datasources(observatory_id),
                    self.review_repository.get_rating_stats(observatory_id),
                )
                services = []
                if services_result.is_ok:
                    for s in services_result.unwrap()[:services_limit]:
                        services.append(DTO.ServiceSnapshotDTO(service_id=s.service_id, name=s.name, provider=s.provider))
                data_sources = []
                if datasources_result.is_ok:
                    for ds in datasources_result.unwrap():
                        data_sources.append(DTO.DataSourceSnapshotDTO(source_id=ds.source_id, name=ds.name))
                avg_rating, review_count = rating_stats
                return DTO.ObservatoryStatsDTO(
                    observatory_id = observatory_id,
                    avg_rating     = avg_rating,
                    review_count   = review_count,
                    services       = services,
                    data_sources   = data_sources,
                )
            except Exception as e:
                L.error({"action": "service.observatory.stats_batch", "error": str(e), "input": {"observatory_id": observatory_id}})
                return None

        results = await asyncio.gather(*[_stats_for(oid) for oid in ids])
        data = [r for r in results if r is not None]
        L.info({"action": "service.observatory.stats_batch", "duration_ms": int((_time.monotonic()-t0)*1000), "input": {"ids_count": len(ids)}, "result": {"count": len(data)}})
        return data

    async def get_all_products_in_observatory(self, observatory_id: str) -> Result[List[DTO.ProductSimpleDTO], EX.JubError]:
        try:
            pipeline = [
                {"$match": {"observatory_id": observatory_id}},
                {"$lookup": {
                    "from": self.product_repository.collection.name,
                    "localField": "product_id",
                    "foreignField": "product_id",
                    "as": "product_data",
                }},
                {"$unwind": "$product_data"},
                {"$replaceRoot": {"newRoot": "$product_data"}},
            ]
            cursor = self.observatory_product_link_repository.collection.aggregate(pipeline)
            docs = await cursor.to_list(length=None)
            return Ok([DTO.ProductSimpleDTO.from_model(M.ProductX.model_validate(d)) for d in docs])
        except Exception as e:
            L.error(f"Error fetching products in observatory {observatory_id}: {e}")
            return Err(EX.UnknownError(str(e)))

    async def get_catalogs_by_observatory_id(self, observatory_id: str) -> Result[List[M.CatalogX], EX.JubError]:
        try:
            pipeline = [
                {"$match": {"observatory_id": observatory_id}},
                {"$lookup": {
                    "from": CollectionNames.CATALOGS.value,
                    "localField": "catalog_id",
                    "foreignField": "catalog_id",
                    "as": "catalog_data",
                }},
                {"$unwind": "$catalog_data"},
                {"$replaceRoot": {"newRoot": "$catalog_data"}},
            ]
            cursor = self.graph_link_manager.observatory_catalog_link_repository.collection.aggregate(pipeline)
            documents = await cursor.to_list(length=None)
            return Ok([M.CatalogX(**doc) for doc in documents])
        except Exception as e:
            L.error({"message": "Error fetching catalogs for observatory", "error": str(e), "observatory_id": observatory_id})
            return Err(EX.JubError.from_exception(e))

    # --- Update ---

    async def update_observatory(self, observatory_id: str, data: Dict[str, Any]) -> Result[DTO.ObservatoryXDTO, EX.JubError]:
        """Partial update — only fields present in *data* are changed."""
        check = await self.observatory_repository.get_by_id(observatory_id)
        if check.is_err:
            return Err(EX.NotFound(f"Observatory '{observatory_id}' not found."))
        result = await self.observatory_repository.update(observatory_id, data)
        if result.is_err:
            return Err(result.unwrap_err())
        return Ok(DTO.ObservatoryXDTO.from_model(result.unwrap()))

    # --- Catalog link management ---

    async def add_catalog(self, observatory_id: str, catalog_id: str, level: int = 0) -> Result[bool, EX.JubError]:
        result = await self.graph_link_manager.link_observatory_to_catalog(observatory_id, catalog_id, level)
        if result.is_err:
            return Err(EX.JubError(f"Failed to link catalog '{catalog_id}' to observatory '{observatory_id}'."))
        return Ok(True)

    async def remove_catalog(self, observatory_id: str, catalog_id: str) -> Result[bool, EX.JubError]:
        return await self.graph_link_manager.unlink_observatory_from_catalog(observatory_id, catalog_id)

    # --- Product link management ---

    async def link_product(self, observatory_id: str, product_id: str) -> Result[bool, EX.JubError]:
        result = await self.graph_link_manager.link_observatory_to_product(observatory_id, product_id)
        if result.is_err:
            return Err(EX.JubError(f"Failed to link product '{product_id}' to observatory '{observatory_id}'."))
        return Ok(True)

    async def unlink_product(self, observatory_id: str, product_id: str) -> Result[bool, EX.JubError]:
        return await self.graph_link_manager.unlink_observatory_from_product(observatory_id, product_id)

    # --- Service link management ---

    async def link_service(self, observatory_id: str, service_id: str) -> Result[bool, EX.JubError]:
        svc = await self.service_repository.get_by_id(service_id)
        if svc.is_err:
            return Err(EX.NotFound(f"Service '{service_id}' not found."))
        result = await self.graph_link_manager.link_observatory_to_service(observatory_id, service_id)
        if result.is_err:
            return Err(EX.JubError(f"Failed to link service '{service_id}' to observatory '{observatory_id}'."))
        return Ok(True)

    async def unlink_service(self, observatory_id: str, service_id: str) -> Result[bool, EX.JubError]:
        return await self.graph_link_manager.unlink_observatory_from_service(observatory_id, service_id)

    async def list_services(self, observatory_id: str) -> Result[List[DTO.ServiceSimpleDTO], EX.JubError]:
        try:
            pipeline = [
                {"$match": {"observatory_id": observatory_id}},
                {"$lookup": {
                    "from": CollectionNames.SERVICES.value,
                    "localField": "service_id",
                    "foreignField": "service_id",
                    "as": "service_data",
                }},
                {"$unwind": "$service_data"},
                {"$replaceRoot": {"newRoot": "$service_data"}},
            ]
            cursor = self.graph_link_manager.observatory_service_link_repository.collection.aggregate(pipeline)
            docs = await cursor.to_list(length=None)
            return Ok([DTO.ServiceSimpleDTO.from_model(M.ServiceX.model_validate(d)) for d in docs])
        except Exception as e:
            L.error(f"Error fetching services in observatory {observatory_id}: {e}")
            return Err(EX.UnknownError(str(e)))

    # --- DataSource link management ---

    async def link_datasource(self, observatory_id: str, source_id: str) -> Result[bool, EX.JubError]:
        src = await self.datasource_repository.get_by_id(source_id)
        if src.is_err:
            return Err(EX.NotFound(f"DataSource '{source_id}' not found."))
        result = await self.graph_link_manager.link_observatory_to_datasource(observatory_id, source_id)
        if result.is_err:
            return Err(EX.JubError(f"Failed to link datasource '{source_id}' to observatory '{observatory_id}'."))
        return Ok(True)

    async def unlink_datasource(self, observatory_id: str, source_id: str) -> Result[bool, EX.JubError]:
        return await self.graph_link_manager.unlink_observatory_from_datasource(observatory_id, source_id)

    async def list_datasources(self, observatory_id: str) -> Result[List[DTO.DataSourceDTO], EX.JubError]:
        try:
            pipeline = [
                {"$match": {"observatory_id": observatory_id}},
                {"$lookup": {
                    "from": CollectionNames.DATA_SOURCES.value,
                    "localField": "source_id",
                    "foreignField": "source_id",
                    "as": "source_data",
                }},
                {"$unwind": "$source_data"},
                {"$replaceRoot": {"newRoot": "$source_data"}},
            ]
            cursor = self.graph_link_manager.observatory_datasource_link_repository.collection.aggregate(pipeline)
            docs = await cursor.to_list(length=None)
            return Ok([DTO.DataSourceDTO.from_model(M.DataSource.model_validate(d)) for d in docs])
        except Exception as e:
            L.error(f"Error fetching datasources in observatory {observatory_id}: {e}")
            return Err(EX.UnknownError(str(e)))

    # --- Views ---

    async def increment_views(self, observatory_id: str) -> Result[int, EX.JubError]:
        return await self.observatory_repository.increment_views(observatory_id)

    # --- Reviews ---

    async def get_reviews(self, observatory_id: str) -> Result[List[DTO.ReviewDTO], EX.JubError]:
        result = await self.review_repository.get_by_observatory(observatory_id)
        if result.is_err:
            return result
        return Ok([DTO.ReviewDTO.from_model(r) for r in result.unwrap()])

    async def create_review(self, observatory_id: str, user_id: str, dto: DTO.CreateReviewDTO) -> Result[DTO.ReviewDTO, EX.JubError]:
        existing = await self.review_repository.get_by_user_and_observatory(user_id, observatory_id)
        if existing.is_ok and existing.unwrap() is not None:
            return Err(EX.AlreadyExists("You have already reviewed this observatory."))
        now = DT.datetime.now(DT.timezone.utc)
        review = M.Review(
            review_id      = f"rev_{uuid.uuid4().hex[:12]}",
            observatory_id = observatory_id,
            user_id        = user_id,
            content        = dto.content,
            rating         = dto.rating,
            created_at     = now,
            updated_at     = now,
        )
        result = await self.review_repository.insert(review)
        if result.is_err:
            return result
        return Ok(DTO.ReviewDTO.from_model(review))

    async def update_review(self, review_id: str, user_id: str, dto: DTO.UpdateReviewDTO) -> Result[DTO.ReviewDTO, EX.JubError]:
        result = await self.review_repository.get_by_id(review_id)
        if result.is_err:
            return Err(EX.NotFound(f"Review '{review_id}' not found."))
        review = result.unwrap()
        if review.user_id != user_id:
            return Err(EX.AuthorizationError("You are not authorized to edit this review."))
        patch = {k: v for k, v in dto.model_dump().items() if v is not None}
        patch["updated_at"] = DT.datetime.now(DT.timezone.utc)
        updated = await self.review_repository.update(review_id, patch)
        if updated.is_err:
            return updated
        return Ok(DTO.ReviewDTO.from_model(updated.unwrap()))

    async def delete_review(self, review_id: str, user_id: str) -> Result[bool, EX.JubError]:
        result = await self.review_repository.get_by_id(review_id)
        if result.is_err:
            return Err(EX.NotFound(f"Review '{review_id}' not found."))
        if result.unwrap().user_id != user_id:
            return Err(EX.AuthorizationError("You are not authorized to delete this review."))
        return await self.review_repository.delete(review_id)

    # --- Delete ---

    async def enable_observatory(self, observatory_id: str) -> Result[bool, EX.JubError]:
        """Flips is_disabled to False — called once the setup task completes successfully."""
        result = await self.observatory_repository.update(observatory_id, {"is_disabled": False})
        if result.is_err:
            return Err(result.unwrap_err())
        return Ok(True)

    async def delete_observatory(self, observatory_id: str) -> Result[bool, EX.JubError]:
        """Deletes the observatory and all its catalog/product links."""
        check = await self.observatory_repository.get_by_id(observatory_id)
        if check.is_err:
            return Err(EX.NotFound(f"Observatory '{observatory_id}' not found."))
        await self.graph_link_manager.remove_all_observatory_links(observatory_id)
        return await self.observatory_repository.delete(observatory_id)

class CatalogService:
    def __init__(
        self, 
        catalog_repository: R.CatalogsRepository, 
        catalog_items_repository: R.CatalogItemsRepository,
        catalog_item_alias_repository: R.CatalogItemAliasesRepository, # The alias/value repo
        link_manager: GraphLinkManager
    ):
        self.catalog_repository            = catalog_repository
        self.catalog_item_repository       = catalog_items_repository
        self.catalog_item_alias_repository = catalog_item_alias_repository
        self.link_manager                  = link_manager

    # --- Create Operations ---
    async def create_catalog_bulk(self, dto: DTO.CatalogCreateDTO) -> Result[str, EX.JubError]:
        try:
            # 1. Create the Root Catalog
            catalog_id = f"cat_{uuid.uuid4().hex[:8]}" if dto.catalog_id is None else dto.catalog_id

            exists = await self.catalog_repository.get_by_id(catalog_id)
            if exists.is_ok:
                return Err(EX.AlreadyExists(f"Catalog with ID '{catalog_id}' already exists. Please choose a different ID or omit it to auto-generate."))

            catalog_model = M.CatalogX(
                catalog_id=catalog_id,
                name=dto.name,
                value=dto.value,
                catalog_type=dto.catalog_type,
                description=dto.description
            )
            await self.catalog_repository.insert(catalog_model)

            # 2. Process all root items
            for item_dto in dto.items:
                await self._process_item_recursive(
                    item_dto       = item_dto,
                    catalog_id     = catalog_id,
                    parent_item_id = None,             # It's a root item
                    catalog_type   = dto.catalog_type
                )
            
            return Ok(catalog_id)
            
        except Exception as e:
            L.error(f"Error during bulk catalog creation: {e}")
            return Err(EX.UnknownError(detail=f"Bulk ingestion failed: {str(e)}", status_code=500))

    async def _process_item_recursive(self, item_dto: DTO.CatalogItemCreateDTO, catalog_id: str, parent_item_id: Optional[str], catalog_type: ENUMS.CatalogType):
        # A. Create the Item
        item_id = item_dto.catalog_item_id or f"itm_{uuid.uuid4().hex[:8]}"
        exists = await self.catalog_item_repository.get_by_id(item_id)
        if exists.is_ok:
            L.warning(f"Catalog item with ID '{item_id}' already exists. Skipping creation and using existing item.")
            item_id = f"itm_{uuid.uuid4().hex[:8]}"

            # return Err(EX.AlreadyExists(f"Catalog item with ID '{item_id}' already exists. Please choose a different ID or omit it to auto-generate."))
        item_model = M.CatalogItemX(
            catalog_item_id = item_id,
            name            = item_dto.name,
            value           = item_dto.value,
            code            = item_dto.code,
            value_type      = item_dto.value_type,
            catalog_type    = catalog_type,
            temporal_value  = item_dto.temporal_value,
            description     = item_dto.description
        )
        await self.catalog_item_repository.insert(item_model)

        # B. Link Item to Catalog
        # cat_link = M.CatalogToCatalogItemLink(catalog_id=catalog_id, catalog_item_id=item_id)
        await self.link_manager.link_catalog_to_item(catalog_id, item_id)

        # C. If it has a parent, create the Hierarchy Relationship
        if parent_item_id:
            # hierarchy_link = M.CatalogItemRelationship(parent_id=parent_item_id, child_id=item_id)
            await self.link_manager.set_item_relationship(parent_item_id, item_id)

        # D. Process Aliases
        for alias_dto in item_dto.aliases:
            alias_id = alias_dto.alias_id or f"alias_{uuid.uuid4().hex[:8]}"
            exists_alias = await self.catalog_item_alias_repository.get_by_id(alias_id)
            if exists_alias.is_ok:
                L.warning(f"Catalog item alias with ID '{alias_id}' already exists. Skipping creation and using existing alias.")
                alias_id = f"alias_{uuid.uuid4().hex[:8]}"

            alias_model = M.CatalogItemAlias(
                catalog_item_alias_id = alias_id,
                value                 = alias_dto.value,
                value_type            = alias_dto.value_type,
                catalog_type          = catalog_type,
                description           = alias_dto.description
            )
            await self.catalog_item_alias_repository.insert(alias_model)
            
            # Link Alias to Item
            # alias_link = M.CatalogItemToCatalogAliasLink(catalog_item_id=item_id, catalog_item_alias_id=alias_id)
            await self.link_manager.link_item_to_alias(item_id, alias_id)

        # E. Process Children Recursively (This handles N-levels deep)
        for child_dto in item_dto.children:
            await self._process_item_recursive(
                item_dto=child_dto, 
                catalog_id=catalog_id, 
                parent_item_id=item_id, # This item becomes the parent
                catalog_type=catalog_type
            )

    async def list_catalogs(self) -> Result[List[DTO.CatalogSummaryDTO], EX.JubError]:
        """Returns a lightweight list of all catalogs."""
        try:
            catalogs_result = await self.catalog_repository.find({})
            if catalogs_result.is_err:
                L.error(f"Error fetching catalogs: {catalogs_result.unwrap_err()}")
                return Err(EX.JubError(f"Error fetching catalogs: {catalogs_result.unwrap_err()}"))
            catalogs = catalogs_result.unwrap()
            dtos = [
                DTO.CatalogSummaryDTO(
                    catalog_id=c.catalog_id, name=c.name, 
                    value=c.value, catalog_type=c.catalog_type
                ) for c in catalogs
            ]
            return Ok(dtos)
        except Exception as e:
            return Err(EX.UnknownError(detail=str(e), status_code=500))

    async def get_catalog_items(self, catalog_id: str) -> Result[List[DTO.CatalogItemXResponseDTO], EX.JubError]:
        """Returns a flat list of all items linked to a catalog."""
        try:
            ids_result = await self.link_manager.get_catalog_items_linked_to_catalog(catalog_id)
            if ids_result.is_err:
                return Err(ids_result.unwrap_err())
            item_ids = ids_result.unwrap()
            if not item_ids:
                return Ok([])
            items_result = await self.catalog_item_repository.find_by_ids(item_ids)
            if items_result.is_err:
                return Err(items_result.unwrap_err())
            return Ok([DTO.CatalogItemXResponseDTO.from_model(i) for i in items_result.unwrap()])
        except Exception as e:
            return Err(EX.UnknownError(detail=str(e), status_code=500))

    async def get_catalog_details(self, catalog_id: str) -> Result[DTO.CatalogResponseDTO, EX.JubError]:
        """Fetches a catalog and fully hydrates its items, aliases, and hierarchy."""
        try:
            # 1. Fetch the root catalog
            catalog_result = await self.catalog_repository.get_by_id(catalog_id)
            if catalog_result.is_err:
                return Err(EX.NotFound(detail="Catalog not found"))
            catalog = catalog_result.unwrap()

            # 2. Fetch all item links for this catalog
            item_links_result = await self.link_manager.get_catalog_items_linked_to_catalog(catalog_id)
            if item_links_result.is_err:
                L.error(f"Error fetching item links for catalog {catalog_id}: {item_links_result.unwrap_err()}")
                return Err(EX.JubError(f"Error fetching item links for catalog {catalog_id}: {item_links_result.unwrap_err()}"))
            items_ids = item_links_result.unwrap()

            if len(items_ids)==0:
                # Return empty catalog if it has no items
                return Ok(DTO.CatalogResponseDTO(**catalog.model_dump(), items=[]))
            

            # item_ids = [link for link in item_links]

            # 3. Fetch all items, aliases, and hierarchy links CONCURRENTLY
            # import asyncio
            items_result, aliases_links_result, hierarchy_links_result = await asyncio.gather(
                self.catalog_item_repository.find_by_ids(items_ids),
                self.link_manager.get_alias_links_by_item_ids(items_ids),
                self.link_manager.get_relationships_by_item_ids(items_ids)
            )

            if items_result.is_err:
                L.error(f"Error fetching items for catalog {catalog_id}: {items_result.unwrap_err()}")
                return Err(EX.JubError(f"Error fetching items for catalog {catalog_id}: {items_result.unwrap_err()}"))
            items = items_result.unwrap()

            if aliases_links_result.is_err:
                L.error(f"Error fetching alias links for catalog {catalog_id}: {aliases_links_result.unwrap_err()}")
                return Err(EX.JubError(f"Error fetching alias links for catalog {catalog_id}: {aliases_links_result.unwrap_err()}"))
            aliases_links = aliases_links_result.unwrap()

            if hierarchy_links_result.is_err:
                L.error(f"Error fetching hierarchy links for catalog {catalog_id}: {hierarchy_links_result.unwrap_err()}")
                return Err(EX.JubError(f"Error fetching hierarchy links for catalog {catalog_id}: {hierarchy_links_result.unwrap_err()}"))
            hierarchy_links = hierarchy_links_result.unwrap()

            # 4. Fetch the actual alias models based on the links
            alias_ids = [link.catalog_item_alias_id for link in aliases_links]
            aliases_result = await self.catalog_item_alias_repository.find_by_ids(alias_ids)
            if aliases_result.is_err:
                L.error(f"Error fetching alias models for catalog {catalog_id}: {aliases_result.unwrap_err()}")
                return Err(EX.JubError(f"Error fetching alias models for catalog {catalog_id}: {aliases_result.unwrap_err()}"))
    
            # --- HYDRATION PROCESS ---
            
            # Map aliases to their items: item_id -> List[AliasDTO]
            aliases = aliases_result.unwrap()
            alias_map: Dict[str, List[DTO.CatalogItemAliasResponseDTO]] = {i_id: [] for i_id in items_ids}
            alias_model_dict = {a.catalog_item_alias_id: a for a in aliases}
            
            for link in aliases_links:
                alias_model = alias_model_dict.get(link.catalog_item_alias_id)
                if alias_model:
                    alias_map[link.catalog_item_id].append(
                        DTO.CatalogItemAliasResponseDTO(**alias_model.model_dump())
                    )

            # Build the base Item DTOs
            item_dtos: Dict[str, DTO.CatalogItemResponseDTO] = {}
            for item in items:
                dto = DTO.CatalogItemResponseDTO(**item.model_dump())
                dto.aliases = alias_map.get(item.catalog_item_id, [])
                item_dtos[item.catalog_item_id] = dto

            # Wire up the Hierarchy (Parent -> Children)
            child_ids = set()
            for rel in hierarchy_links:
                parent_dto = item_dtos.get(rel.parent_id)
                child_dto = item_dtos.get(rel.child_id)
                if parent_dto and child_dto:
                    parent_dto.children.append(child_dto)
                    child_ids.add(rel.child_id)

            # Filter out the children from the root items list
            root_items = [dto for i_id, dto in item_dtos.items() if i_id not in child_ids]

            # 5. Assemble final response
            response_dto = DTO.CatalogResponseDTO(
                catalog_id=catalog.catalog_id,
                name=catalog.name,
                value=catalog.value,
                catalog_type=catalog.catalog_type,
                description=catalog.description,
                items=root_items
            )

            return Ok(response_dto)

        except Exception as e:
            return Err(EX.UnknownError(detail=f"Error hydrating catalog: {str(e)}", status_code=500))


    async def create_catalog(self, catalog: M.CatalogX) -> Result[str,EX.JubError]:
        exists_result = await self.catalog_repository.get_by_id(catalog.catalog_id)
        if exists_result.is_ok:
            return Err(EX.AlreadyExists(f"Catalog with ID {catalog.catalog_id} already exists"))
        return await self.catalog_repository.insert(catalog)

    async def add_item_to_catalog(self, catalog_id: str, item: M.CatalogItemX, parent_id: Optional[str] = None) -> Result[str,EX.JubError]:
        """Saves a new item, links it to its catalog, and builds the hierarchy if requested."""
        insert_rest = await self.catalog_item_repository.insert(item)

        if insert_rest.is_err:
            L.error({
                "message": "Failed to insert catalog item",
                "error": insert_rest.unwrap_err(),
                "catalog_id": catalog_id,
            })
            return Err(EX.JubError(f"Failed to insert catalog item: {insert_rest.unwrap_err()}"))
        
        item_id = insert_rest.unwrap()
        
        # Link to the main catalog (e.g., SPATIAL)
        result = await self.link_manager.link_catalog_to_item(catalog_id, item_id)
        if result.is_err:
            # Rollback item insertion if linking fails
            delete_catalog_item_result = await self.catalog_item_repository.delete(item_id)

            if delete_catalog_item_result.is_err:
                L.error(f"Failed to rollback catalog item after link failure: {delete_catalog_item_result.unwrap_err()}")
                return Err(EX.JubError(f"Failed to rollback catalog item after link failure: {delete_catalog_item_result.unwrap_err()}"))

            return Err(EX.JubError(f"Failed to link item to catalog: {result.unwrap_err()}"))
        
        # Link to parent if it exists (e.g., TAM -> VIC)
        if parent_id:
            await self.link_manager.set_item_relationship(parent_id, item_id)
            
        return Ok(item_id)

    async def add_alias_to_catalog_item(self, catalog_item_id: str, value: M.CatalogItemAlias) -> Result[str,EX.JubError]:
        """Saves an alias (e.g., '1' or 'CDVALLES') and links it to the canonical item."""
        try: 
            val_id_result = await self.catalog_item_alias_repository.insert(value)
            if val_id_result.is_err:
                L.error(f"Failed to insert catalog item alias: {val_id_result.unwrap_err()}")
                return Err(EX.JubError(f"Failed to insert catalog item alias: {val_id_result.unwrap_err()}"))
            
            val_id = val_id_result.unwrap()
            
            res = await self.link_manager.link_item_to_alias(catalog_item_id, val_id)
            if res.is_err:
                # Rollback alias insertion if linking fails
                delete_alias_result = await self.catalog_item_alias_repository.delete(val_id)

                if delete_alias_result.is_err:
                    L.error(f"Failed to rollback catalog item alias after link failure: {delete_alias_result.unwrap_err()}")
                    return Err(EX.JubError(f"Failed to rollback catalog item alias after link failure: {delete_alias_result.unwrap_err()}"))

                return Err(EX.JubError(f"Failed to link alias to catalog item: {res.unwrap_err()}"))
            return Ok(val_id)
        except Exception as e:
            L.error(f"Error adding value to item: {e}")
            return Err(EX.JubError.from_exception(e))
    
    async def get_catalog_hierarchy_levels(self, root_catalog_id: str) -> Result[List[M.CatalogX], EX.JubError]:
        """
        Gets the structural hierarchy for a SPECIFIC catalog family.
        Example: Pass "cat_cie10", get [Capítulo, Bloque, Categoría] in exact order.
        Example: Pass "cat_spatial_mx", get [País, Estado, Municipio] in exact order.
        """
        try:
            # We instantly grab the whole family and sort by level (0, 1, 2, 3...)
            cursor = self.catalog_repository.collection.find(
                {"root_catalog_id": root_catalog_id}
            ).sort("level", 1)
            
            docs = await cursor.to_list(length=None)
            return Ok([M.CatalogX(**doc) for doc in docs])
            
        except Exception as e:
            return Err(EX.JubError.from_exception(e))

    async def get_items_by_parent(self, parent_item_id: str) -> Result[List[M.CatalogItemX], EX.JubError]:
        """
        Gets the specific sub-level items.
        Example: Pass "MX" (Mexico), get all States linked to it.
        """
        try:
            # 1. Find all children edges in the relationship collection
            rel_cursor = self.link_manager.catalog_item_relationship_repository.collection.find({"parent_id": parent_item_id})
            rel_docs = await rel_cursor.to_list(length=None)
            
            if not rel_docs:
                return Ok([])
                
            child_ids = [doc["child_id"] for doc in rel_docs]
            
            # 2. Fetch the actual CatalogItem metadata for those children
            item_cursor = self.catalog_item_repository.collection.find({"catalog_item_id": {"$in": child_ids}})
            item_docs = await item_cursor.to_list(length=None)
            
            items = [M.CatalogItemX(**doc) for doc in item_docs]
            items.sort(key=lambda x: x.name) # Alphabetical for the UI
            
            return Ok(items)
        except Exception as e:
            return Err(EX.JubError.from_exception(e))
    # --- Delete Operations (Cascading) ---

    async def delete_catalog_item(self, catalog_item_id: str) -> Result[bool, EX.JubError]:
        """Deletes an item and securely wipes its tags, aliases, and hierarchy edges."""
        try:
            success = await self.catalog_item_repository.delete(catalog_item_id)
            if success.is_ok:
                await self.link_manager.remove_all_catalog_item_links(catalog_item_id)
            return success
        except Exception as e:
            return Err(EX.JubError.from_exception(e))

    async def update_catalog(self, catalog_id: str, data: dict) -> Result[M.CatalogX, EX.JubError]:
        exists = await self.catalog_repository.get_by_id(catalog_id)
        if exists.is_err:
            return Err(EX.NotFound(f"Catalog '{catalog_id}' not found."))
        return await self.catalog_repository.update(catalog_id, data)

    async def delete_catalog(self, catalog_id: str) -> Result[bool, EX.JubError]:
        """Deletes a catalog, all its linked CatalogItems (with full cascade), and link rows."""
        try:
            item_ids_result = await self.link_manager.get_catalog_items_linked_to_catalog(catalog_id)
            if item_ids_result.is_ok:
                for item_id in item_ids_result.unwrap():
                    await self.link_manager.remove_all_catalog_item_links(item_id)
                    await self.catalog_item_repository.delete(item_id)
            await self.link_manager.remove_all_catalog_links(catalog_id)
            return await self.catalog_repository.delete(catalog_id)
        except Exception as e:
            return Err(EX.JubError.from_exception(e))

    # --- Catalog Item CRUD ---

    async def get_catalog_item(self, catalog_item_id: str) -> Result[M.CatalogItemX, EX.JubError]:
        return await self.catalog_item_repository.get_by_id(catalog_item_id)

    async def list_catalog_items(self, limit: int = 100) -> Result[List[M.CatalogItemX], EX.JubError]:
        return await self.catalog_item_repository.find({}, limit=limit)

    async def update_catalog_item(self, catalog_item_id: str, update_data: Dict) -> Result[M.CatalogItemX, EX.JubError]:
        return await self.catalog_item_repository.update(catalog_item_id, update_data)

    # --- Alias management ---

    async def get_aliases_for_item(self, catalog_item_id: str) -> Result[List[M.CatalogItemAlias], EX.JubError]:
        try:
            alias_links_result = await self.link_manager.get_alias_links_by_item_ids([catalog_item_id])
            if alias_links_result.is_err:
                return Err(alias_links_result.unwrap_err())
            alias_ids = [lnk.catalog_item_alias_id for lnk in alias_links_result.unwrap()]
            if not alias_ids:
                return Ok([])
            return await self.catalog_item_alias_repository.find_by_ids(alias_ids)
        except Exception as e:
            return Err(EX.JubError.from_exception(e))

    async def delete_alias(self, catalog_item_id: str, alias_id: str) -> Result[bool, EX.JubError]:
        try:
            await self.link_manager.unlink_alias_from_item(catalog_item_id, alias_id)
            result = await self.catalog_item_alias_repository.delete(alias_id)
            return result
        except Exception as e:
            return Err(EX.JubError.from_exception(e))

    # --- Cross-entity lookups ---

    async def get_catalogs_for_item(self, catalog_item_id: str) -> Result[List[M.CatalogX], EX.JubError]:
        try:
            links_result = await self.link_manager.catalog_catalog_item_link_repository.get_by_catalog_item_id(catalog_item_id)
            if links_result.is_err:
                return Err(links_result.unwrap_err())
            catalog_ids = [lnk.catalog_id for lnk in links_result.unwrap()]
            if not catalog_ids:
                return Ok([])
            return await self.catalog_repository.find_by_ids(catalog_ids)
        except Exception as e:
            return Err(EX.JubError.from_exception(e))

    async def get_product_ids_for_item(self, catalog_item_id: str) -> Result[List[str], EX.JubError]:
        return await self.link_manager.get_product_ids_for_catalog_item(catalog_item_id)

    # --- Explicit link/unlink for catalog ↔ item ---

    async def link_item_to_catalog(self, catalog_id: str, catalog_item_id: str) -> Result[bool, EX.JubError]:
        result = await self.link_manager.link_catalog_to_item(catalog_id, catalog_item_id)
        if result.is_err:
            return Err(result.unwrap_err())
        return Ok(True)

    async def unlink_item_from_catalog(self, catalog_id: str, catalog_item_id: str) -> Result[bool, EX.JubError]:
        return await self.link_manager.unlink_catalog_from_item(catalog_id, catalog_item_id)

    # --- Item hierarchy (parent ↔ child relationships) ---

    async def add_item_relationship(self, parent_id: str, child_id: str) -> Result[bool, EX.JubError]:
        result = await self.link_manager.set_item_relationship(parent_id, child_id)
        if result.is_err:
            return Err(result.unwrap_err())
        return Ok(True)

    async def remove_item_relationship(self, parent_id: str, child_id: str) -> Result[bool, EX.JubError]:
        return await self.link_manager.unlink_item_relationship(parent_id, child_id)


class ProductService:
    def __init__(
        self,
        product_repository: R.ProductsRepository,
        link_manager: GraphLinkManager,
        storage: Optional[StorageBackend] = None,
    ):
        self.product_repository = product_repository
        self.link_manager = link_manager
        self.storage = storage

    
    async def get_product_by_id(self, product_id: str) -> Result[M.ProductX, EX.JubError]:
        """Fetches a product by its ID, including all its tags."""
        try:
            product_res = await self.product_repository.get_by_id(product_id)
            if product_res.is_err:
                L.error({
                    "message": f"Failed to fetch product {product_id}",
                    "error": str(product_res.unwrap_err())
                })
                return product_res
            
            product = product_res.unwrap()
            
            # # Fetch tags
            # tags_res = await self.link_manager.get_catalog_items_linked_to_product(product_id)
            # if tags_res.is_err:
            #     log.warning({
            #         "message": f"Failed to fetch tags for product {product_id}",
            #         "error": tags_res.unwrap_err()
            #     })
            #     product.tags = []
            # else:
            #     product.tags = tags_res.unwrap()
            
            return Ok(product)
        except Exception as e:
            L.error({
                "message": f"Error fetching product by ID: {product_id}",
                "error": str(e)
            })
            return Err(EX.JubError.from_exception(e))

    async def insert_product(
        self, 
        product: M.ProductX, 
        observatory_id: str, 
        catalog_item_ids: List[str] = None
    ) -> Result[str, EX.JubError]:
        """Inserts a dataset, assigns it to an observatory, and applies all its tags."""
        
        # 1. Save the product
        prod_res = await self.product_repository.insert(product)
        if prod_res.is_err:
            return prod_res

        # 2. Assign to the observatory
        obs_link_res = await self.link_manager.link_observatory_to_product(observatory_id, product.product_id)
        if obs_link_res.is_err:
            return obs_link_res

        # 3. Apply the tags
        if catalog_item_ids:
            for item_id in catalog_item_ids:
                tag_res = await self.link_manager.link_product_to_catalog_item(product.product_id, item_id)
                if tag_res.is_err:
                    L.warning({
                        "message": f"Failed to tag product {product.product_id} with {item_id}",
                        "error": tag_res.unwrap_err()
                    })
                    # Depending on your strictness, you could return an Err here, 
                    # but usually, you want to keep going even if one tag fails.

        return Ok(product.product_id)

    async def get_product_observatory(self, product_id: str) -> Result[str, EX.JubError]:
        """Returns the observatory_id this product is linked to (first match)."""
        try:
            doc = await self.link_manager.observatory_product_link_repository.collection.find_one(
                {"product_id": product_id}
            )
            if not doc:
                return Err(EX.NotFound(f"No observatory link found for product {product_id}"))
            return Ok(doc["observatory_id"])
        except Exception as e:
            return Err(EX.JubError.from_exception(e))

    async def list_products(self, limit: int = 100) -> Result[List[DTO.ProductSimpleDTO], EX.JubError]:
        result = await self.product_repository.find({}, limit=limit)
        if result.is_err:
            return result
        return Ok([DTO.ProductSimpleDTO.from_model(p) for p in result.unwrap()])

    async def update_product(self, product_id: str, data: Dict[str, Any]) -> Result[DTO.ProductSimpleDTO, EX.JubError]:
        check = await self.product_repository.get_by_id(product_id)
        if check.is_err:
            return Err(EX.NotFound(f"Product '{product_id}' not found."))
        result = await self.product_repository.update(product_id, data)
        if result.is_err:
            return Err(result.unwrap_err())
        return Ok(DTO.ProductSimpleDTO.from_model(result.unwrap()))

    async def get_product_tags(self, product_id: str) -> Result[List[str], EX.JubError]:
        """Returns the list of catalog_item_ids linked to this product."""
        return await self.link_manager.get_catalog_items_linked_to_product(product_id)

    async def tag_product(self, product_id: str, catalog_item_ids: List[str]) -> Result[int, EX.JubError]:
        """Adds catalog-item tags to a product. Returns the number of tags added."""
        check = await self.product_repository.get_by_id(product_id)
        if check.is_err:
            return Err(EX.NotFound(f"Product '{product_id}' not found."))
        added = 0
        for item_id in catalog_item_ids:
            res = await self.link_manager.link_product_to_catalog_item(product_id, item_id)
            if res.is_ok:
                added += 1
        return Ok(added)

    async def untag_product(self, product_id: str, catalog_item_id: str) -> Result[bool, EX.JubError]:
        """Removes a single catalog-item tag from a product."""
        return await self.link_manager.unlink_product_from_catalog_item(product_id, catalog_item_id)

    async def tag_product_from_catalog(self, product_id: str, catalog_id: str) -> Result[int, EX.JubError]:
        """Links a product to every item currently in the given catalog (wildcard assign)."""
        check = await self.product_repository.get_by_id(product_id)
        if check.is_err:
            return Err(EX.NotFound(f"Product '{product_id}' not found."))
        items_result = await self.link_manager.get_catalog_items_linked_to_catalog(catalog_id)
        if items_result.is_err:
            return items_result
        item_ids = items_result.unwrap()
        results = await asyncio.gather(
            *[self.link_manager.link_product_to_catalog_item(product_id, iid) for iid in item_ids]
        )
        return Ok(sum(1 for r in results if r.is_ok))

    # ── Related products ──────────────────────────────────────────────────

    async def relate_products(self, product_id: str, related_product_id: str) -> Result[bool, EX.JubError]:
        """Creates a symmetric relationship between two products."""
        check_a = await self.product_repository.get_by_id(product_id)
        if check_a.is_err:
            return Err(EX.NotFound(f"Product '{product_id}' not found."))
        check_b = await self.product_repository.get_by_id(related_product_id)
        if check_b.is_err:
            return Err(EX.NotFound(f"Product '{related_product_id}' not found."))
        res = await self.link_manager.link_products(product_id, related_product_id)
        if res.is_err:
            return Err(res.unwrap_err())
        return Ok(True)

    async def unrelate_products(self, product_id: str, related_product_id: str) -> Result[bool, EX.JubError]:
        """Removes the relationship between two products."""
        return await self.link_manager.unlink_products(product_id, related_product_id)

    async def get_related_products(self, product_id: str) -> Result[List[DTO.ProductSimpleDTO], EX.JubError]:
        """Returns all products related to the given product."""
        ids_res = await self.link_manager.get_related_product_ids(product_id)
        if ids_res.is_err:
            return Err(ids_res.unwrap_err())
        ids = ids_res.unwrap()
        if not ids:
            return Ok([])
        models_res = await self.product_repository.find_by_ids(ids)
        if models_res.is_err:
            return Err(models_res.unwrap_err())
        return Ok([DTO.ProductSimpleDTO.from_model(m) for m in models_res.unwrap()])

    # ── Metadata filter ───────────────────────────────────────────────────

    async def filter_products_by_metadata(self, filters: Dict[str, str], limit: int = 100) -> Result[List[DTO.ProductSimpleDTO], EX.JubError]:
        """Filters products by metadata key-value pairs. All supplied pairs must match (AND)."""
        query = {f"metadata.{k}": v for k, v in filters.items()}
        res = await self.product_repository.find(query, limit=limit)
        if res.is_err:
            return Err(res.unwrap_err())
        return Ok([DTO.ProductSimpleDTO.from_model(m) for m in res.unwrap()])

    async def delete_product(self, product_id: str) -> Result[bool, EX.JubError]:
        """Deletes the product and securely wipes its observatory assignment and tags."""
        check = await self.product_repository.get_by_id(product_id)
        if check.is_err:
            return Err(EX.NotFound(f"Product '{product_id}' not found."))

        del_res = await self.product_repository.delete(product_id)
        if del_res.is_err:
            return del_res

        wipe_res = await self.link_manager.remove_all_product_links(product_id)
        if wipe_res.is_err:
            return wipe_res

        await self.link_manager.unlink_all_product_relationships(product_id)

        if self.storage is not None:
            try:
                prefix = self.storage.prefix_for(product_id)
                deleted_count = await self.storage.delete_prefix(prefix)
                if deleted_count > 0:
                    L.info({"action": "service.product.delete_files", "result": {"product_id": product_id, "files_deleted": deleted_count, "prefix": prefix}})
                else:
                    L.info({"action": "service.product.delete_files", "result": {"product_id": product_id, "files_deleted": 0, "prefix": prefix, "note": "no files found"}})
            except Exception as e:
                L.error({"action": "service.product.delete_files", "error": str(e), "input": {"product_id": product_id}})

        return Ok(True)


class SearchService:

    
    def __init__(
        self, 
        observatory_product_link_repository:R.ObservatoryToProductLinkRepository,
        product_catalog_item_link_repository: R.ProductToCatalogItemLinkRepository,
        catalog_item_relationship_repository: R.CatalogItemRelationshipRepository,
        catalog_item_repository: R.CatalogItemsRepository,
        product_repository: R.ProductsRepository,
        catalog_alias_repository: R.CatalogItemAliasesRepository,
        catalog_item_catalog_alias_link_repository: R.CatalogItemToCatalogAliasLinkRepository,
        observatory_catalog_link_repository: R.ObservatoryToCatalogLinkRepository,
        catalog_catalog_item_link_repository: R.CatalogToCatalogItemLinkRepository,
        observatory_repository: R.ObservatoriesRepository,
        catalog_repository: R.CatalogsRepository,
        data_records_repository: R.DataRecordsRepository
    ):
        self.observatory_product_link_repository        = observatory_product_link_repository
        self.product_catalog_item_link_repository       = product_catalog_item_link_repository
        self.catalog_item_relationship_repository       = catalog_item_relationship_repository
        self.catalog_item_repository                    = catalog_item_repository
        self.product_repository                         = product_repository
        self.catalog_alias_repository                   = catalog_alias_repository
        self.catalog_item_catalog_alias_link_repository = catalog_item_catalog_alias_link_repository
        self.observatory_catalog_link_repository        = observatory_catalog_link_repository
        self.catalog_catalog_item_link_repository       = catalog_catalog_item_link_repository
        self.observatory_repository                     = observatory_repository
        self.catalog_repository                         = catalog_repository
        self.data_records_repository                    = data_records_repository
        self.suggestion_repository: Optional[R.ObservatorySearchSuggestionRepository] = None
        self._product_cache     = TTLCache(maxsize=512, ttl=JUB_SEARCH_PRODUCT_CACHE_TTL)
        self._observatory_cache = TTLCache(maxsize=256, ttl=JUB_SEARCH_OBSERVATORY_CACHE_TTL)

    async def __resolve_item_ids_for_value(self, leaf_value: str) -> set:
        """
        Returns catalog_item_ids that match leaf_value directly or through an alias.
        """
        item_ids: set = set()

        # Direct match on catalog items
        items = await self.catalog_item_repository.find_by_value(leaf_value)
        item_ids.update(item.catalog_item_id for item in items)

        # Alias match: find aliases with this value, then resolve to item_ids
        aliases = await self.catalog_alias_repository.find_by_value(leaf_value)
        for alias in aliases:
            item_id = await self.catalog_item_catalog_alias_link_repository.get_catalog_item_id_by_alias_id(
                alias.catalog_item_alias_id
            )
            if item_id:
                item_ids.add(item_id)

        return item_ids

    async def __product_ids_for_item_ids(self, item_ids: set) -> set:
        """Returns the union of product_ids tagged with any of the given catalog_item_ids."""
        product_ids: set = set()
        for item_id in item_ids:
            pids = await self.product_catalog_item_link_repository.get_product_ids_by_catalog_item_id(item_id)
            product_ids.update(pids)
        return product_ids

    def __combine_product_sets(self, sets: List[Optional[set]], logic: str) -> Optional[set]:
        """
        Combines per-condition product sets according to group logic.
        None means wildcard (matches everything).
        - OR:  union — a wildcard in any condition makes the whole block a wildcard
        - AND: intersect — wildcards are transparent (ignored in AND)
        """
        if logic == "OR":
            if any(s is None for s in sets):
                return None  # at least one wildcard → whole OR block matches everything
            result: set = set()
            for s in sets:
                result.update(s)
            return result
        else:  # AND / SINGLE
            specific = [s for s in sets if s is not None]
            if not specific:
                return None  # all wildcards → transparent
            result = specific[0]
            for s in specific[1:]:
                result = result & s
            return result

    async def search_observatories(self, query: str, user_id: str, strict: bool = True, skip: int = 0, limit: int = 100, no_cache: bool = False) -> Result[List[DTO.ObservatoryXDTO], EX.JubError]:
        """
        Finds observatories by walking: DSL condition → catalog items (+ aliases)
        → products → observatories. Only observatories with at least one matching
        product are returned.
        """
        cache_key = (user_id, query, strict, skip, limit)
        if not no_cache and cache_key in self._observatory_cache:
            L.info({
                "message": "Observatory search cache hit",
                "cache_key": cache_key,
            })
            return Ok(self._observatory_cache[cache_key])

        try:
            ast = QueryAST.parse(query)

            mongo_op_map = {">": "$gt", ">=": "$gte", "<": "$lt", "<=": "$lte", "=": "$eq"}

            # One entry per VS/VT/VI block. None = wildcard (no filter).
            # Blocks are AND-ed together; conditions within a block follow the group logic (OR/AND).
            per_block_product_sets: List[Optional[set]] = []

            for catalog_query in ast.queries:
                # Resolve each condition in the group to a product set (or None for wildcard)
                condition_product_sets: List[Optional[set]] = []

                for condition in catalog_query.group.conditions:
                    L.debug({
                        "message": "Processing condition",
                        "catalog_value": condition.catalog_value,
                        "operator": condition.operator,
                        "item_path": condition.item_path,
                    })

                    # Pure wildcard (VS(*), VT(*), VI(*), VI(CAT.*)) → no filter for this condition
                    if condition.operator == "WILDCARD" and not condition.item_path:
                        condition_product_sets.append(None)
                        continue

                    item_ids: set = set()

                    if condition.catalog_value == "TEMPORAL":
                        mongo_op = mongo_op_map.get(condition.operator, "$eq")
                        target_date = condition.item_path[-1] if isinstance(condition.item_path, list) else condition.item_path
                        items = await self.catalog_item_repository.find_by_temporal_operator(
                            mongo_op=mongo_op, target_date=target_date
                        )
                        item_ids = {item.catalog_item_id for item in items}
                    else:
                        leaf_value = (
                            condition.item_path[-1]
                            if isinstance(condition.item_path, list) and condition.item_path
                            else condition.item_path or condition.catalog_value
                        )
                        item_ids = await self.__resolve_item_ids_for_value(leaf_value)

                    L.debug(f"Condition matched {len(item_ids)} item(s)")

                    if not item_ids:
                        condition_product_sets.append(set())
                        continue

                    product_ids = await self.__product_ids_for_item_ids(item_ids)
                    L.debug(f"Condition {condition} → {len(product_ids)} product(s)")
                    condition_product_sets.append(product_ids)

                # Combine condition results according to group logic
                block_result = self.__combine_product_sets(condition_product_sets, catalog_query.group.logic)
                L.debug(f"Block {catalog_query.catalog_prefix} ({catalog_query.group.logic}) → {len(block_result) if block_result is not None else 'wildcard'}")
                per_block_product_sets.append(block_result)

            if not per_block_product_sets:
                if not no_cache:
                    self._observatory_cache[cache_key] = []
                return Ok([])

            if strict:
                # Short-circuit: any block with an empty (not None) set means no results
                if any(s is not None and len(s) == 0 for s in per_block_product_sets):
                    if not no_cache:
                        self._observatory_cache[cache_key] = []
                    return Ok([])
            else:
                # Lenient: drop blocks that resolved to nothing, keep wildcards and non-empty sets
                per_block_product_sets = [s for s in per_block_product_sets if s is None or len(s) > 0]

            specific_sets = [s for s in per_block_product_sets if s is not None]

            if not specific_sets:
                # All blocks were wildcards → any observatory with at least one product
                obs_ids = await self.observatory_product_link_repository.get_all_observatory_ids_with_products()
            else:
                # Convert each per-block product set to an observatory set, then intersect at
                # the observatory level. This allows an observatory to satisfy VS(MX) via one
                # product and VT(2015) via a different product — both correct.
                per_block_obs_sets: List[Set[str]] = []
                for product_set in specific_sets:
                    block_obs: Set[str] = set()
                    for product_id in product_set:
                        ids = await self.observatory_product_link_repository.get_observatory_ids_by_product_id(product_id)
                        block_obs.update(ids)
                    per_block_obs_sets.append(block_obs)

                obs_ids: Set[str] = per_block_obs_sets[0]
                for s in per_block_obs_sets[1:]:
                    obs_ids = obs_ids & s
                    if not obs_ids:
                        if not no_cache:
                            self._observatory_cache[cache_key] = []
                        return Ok([])

            if not obs_ids:
                if not no_cache:
                    self._observatory_cache[cache_key] = []
                return Ok([])

            observatory_tasks = [self.observatory_repository.get_by_id(obs_id) for obs_id in obs_ids]
            # observatory_tasks = [self.observatory_repository.find({"observatory_id": obs_id,"is_disabled":False}) for obs_id in obs_ids]
            # print(observatory_tasks)
            raw_observatories = await asyncio.gather(*observatory_tasks)
            # dtos = [DTO.ObservatoryXDTO.from_model(obs.unwrap()) for obs in raw_observatories if obs.is_ok]
            dtos = []
            for obs in raw_observatories:
                if obs.is_ok:
                    model = obs.unwrap()
                    if not model.is_disabled:
                        dtos.append(DTO.ObservatoryXDTO.from_model(model))
                else:
                    L.warning(f"Failed to fetch observatory details for one of the results: {obs.unwrap_err()}")
            result = dtos[skip: skip + limit]
            
            if not no_cache:
                L.info({
                    "action": "observatory_search_cache_store",
                    "cache_key": cache_key,
                    "result_count": len(result),
                })
                self._observatory_cache[cache_key] = result
            
            if result and self.suggestion_repository:
                asyncio.create_task(
                    self.suggestion_repository.record_hit("__observatories__", query)
                )
            return Ok(result)
        except Exception as e:
            L.error({"message": "Error during observatory search", "error": str(e), "query": query})
            return Err(EX.JubError.from_exception(e))


    async def generate_plot(self, query_str: str, source_id: Optional[str] = None, chart_type: str = "bar", strict: bool = True, **_):
        try:
            ast = QueryAST.parse(query_str)

            # 1. $match — scope by source_id, resolve VS/VT/VI to catalog_item_ids
            match_filter: Dict[str, Any] = {}
            if source_id:
                match_filter["source_id"] = source_id

            for q in ast.queries:
                prefix = q.catalog_prefix
                group  = q.group
                if prefix == SPATIAL_VARIABLE:
                    f = await self._build_spatial_filter(group)
                    if f or strict:
                        match_filter.update(f)
                elif prefix == TEMPORAL_VARIABLE:
                    f = ASTToMongoTranslator._build_temporal(group)
                    if f or strict:
                        match_filter.update(f)
                elif prefix == INTEREST_VARIABLE:
                    f = await self._build_interest_filter(group)
                    if f or strict:
                        match_filter.update(f)

            # 2. $group — BY() resolved via catalog membership, VO() stays pure
            group_id, by_item_ids = await self._build_group_id(ast)
            metric                = self._extract_metric(ast)

            # Prevent None-group records: require at least one BY catalog item in interest_ids.
            # Use $and so we never clobber an existing VI filter (works for all VI logic variants).
            if by_item_ids:
                by_interest_filter = {"interest_ids": {"$in": by_item_ids}}
                vi_interest        = match_filter.pop("interest_ids", None)
                if vi_interest is not None:
                    match_filter.setdefault("$and", [])
                    match_filter["$and"] += [{"interest_ids": vi_interest}, by_interest_filter]
                else:
                    match_filter["interest_ids"] = {"$in": by_item_ids}

            group_stage: Dict[str, Any] = {"_id": group_id}
            group_stage.update(metric)

            # 3. Build and run pipeline
            pipeline: List[Dict] = []
            if match_filter:
                pipeline.append({"$match": match_filter})
            pipeline.append({"$group": group_stage})
            if group_id is not None:
                pipeline.append({"$sort": {"_id.x_axis": 1}})

            L.debug({"message": "generate_plot pipeline", "pipeline": str(pipeline)})

            cursor     = self.data_records_repository.collection.aggregate(pipeline)
            raw_results = await cursor.to_list(length=None)

            # 4. Label catalog_item_ids with human-readable names; drop None-axis rows
            labeled = await self._label_aggregation_results(raw_results)

            L.debug({"message": "generate_plot results", "n": len(labeled)})

            return Ok(Utils.format_for_echarts(labeled, chart_type=chart_type))

        except ValueError as ve:
            return Err(EX.UnknownError(str(ve)))
        except Exception as e:
            return Err(EX.UnknownError(f"Plot error: {str(e)}"))

    def _extract_metric(self, ast: QueryAST) -> dict:
        """Returns the VO metric dict for the $group stage (pure, no DB needed)."""
        for q in ast.queries:
            if q.catalog_prefix == OBSERVABLE_VARIABLE:
                return ASTToMongoTranslator._build_observable(q.group)
        return {"metric_value": {"$sum": 1}}  # default: COUNT

    async def _build_group_id(self, ast: QueryAST):
        """
        Returns (group_id_expression, by_item_ids).
        group_id_expression is the $group._id dict (or None for global aggregate).
        by_item_ids is the flat list of catalog_item_ids used for BY filtering.
        """
        for q in ast.queries:
            if q.catalog_prefix == GROUP_VARIABLE:
                return await self._resolve_by_grouping(q.group)
        return None, []

    async def _resolve_by_grouping(self, group: ConditionGroup):
        """
        Builds the $group._id expression from BY() conditions using catalog membership.
        Returns (group_id_dict, all_by_item_ids).
        """
        grouping: Dict[str, Any] = {}
        all_item_ids: List[str]  = []

        for i, cond in enumerate(group.conditions):
            target = cond.catalog_value

            if target == "TEMPORAL":
                db_field: Any = "$temporal_id"
            elif target == "SPATIAL":
                db_field = "$spatial_id"
            else:
                catalog_doc = await self.catalog_repository.collection.find_one(
                    {"$or": [{"value": target.upper()}, {"catalog_id": target}]}
                )
                if catalog_doc:
                    link_cursor = self.catalog_catalog_item_link_repository.collection.find(
                        {"catalog_id": catalog_doc["catalog_id"]}
                    )
                    link_docs = await link_cursor.to_list(length=None)
                    item_ids  = [doc["catalog_item_id"] for doc in link_docs]
                    all_item_ids.extend(item_ids)

                    if item_ids:
                        db_field = {
                            "$arrayElemAt": [
                                {"$filter": {
                                    "input": "$interest_ids",
                                    "as": "item",
                                    "cond": {"$in": ["$$item", item_ids]}
                                }},
                                0
                            ]
                        }
                    else:
                        db_field = self._interest_prefix_field(target)
                else:
                    db_field = self._interest_prefix_field(target)

            key = "x_axis" if i == 0 else "hue"
            grouping[key] = db_field

        return grouping, all_item_ids

    async def _label_aggregation_results(self, raw_results: list) -> list:
        """
        Replaces catalog_item_ids in aggregation _id fields with human-readable names.
        Filters out results where x_axis resolved to None (no BY match).
        """
        # Collect every ID that appears in x_axis or hue
        ids_to_lookup: Set[str] = set()
        for r in raw_results:
            _id = r.get("_id") or {}
            if isinstance(_id, dict):
                for key in ("x_axis", "hue"):
                    v = _id.get(key)
                    if v is not None:
                        ids_to_lookup.add(str(v))

        # Batch-fetch catalog item labels
        label_map: Dict[str, str] = {}
        if ids_to_lookup:
            cursor = self.catalog_item_repository.collection.find(
                {"catalog_item_id": {"$in": list(ids_to_lookup)}},
                {"catalog_item_id": 1, "name": 1, "value": 1}
            )
            docs = await cursor.to_list(length=None)
            for doc in docs:
                cid   = doc["catalog_item_id"]
                label = doc.get("name") or doc.get("value") or cid
                label_map[cid] = label

        # Rewrite results, drop None-axis rows
        labeled = []
        for r in raw_results:
            _id = r.get("_id")
            if isinstance(_id, dict):
                if _id.get("x_axis") is None:
                    continue  # skip records that didn't match any BY item
                _id["x_axis"] = label_map.get(str(_id["x_axis"]), str(_id["x_axis"]))
                if "hue" in _id and _id["hue"] is not None:
                    _id["hue"] = label_map.get(str(_id["hue"]), str(_id["hue"]))
            labeled.append(r)
        return labeled

    def _interest_prefix_field(self, prefix: str) -> dict:
        return {
            "$arrayElemAt": [
                {"$filter": {
                    "input": "$interest_ids",
                    "as": "item",
                    "cond": {"$regexMatch": {"input": "$$item", "regex": f"^{prefix}_"}}
                }},
                0
            ]
        }   
    
    
    
    async def search(self, query: str, user_id: str, observatory_id: Optional[str] = None, skip: int = 0, limit: int = 10, no_cache: bool = False) -> Result[List[DTO.ProductXDTO], EX.JubError]:
        """
        Takes raw ProductX models, resolves their graph relationships to get
        catalog item names, and returns fully hydrated ProductXDTOs.
        """
        cache_key = (user_id, query, observatory_id, skip, limit)
        if not no_cache and cache_key in self._product_cache:
            return Ok(self._product_cache[cache_key])

        products_result = await self.execute_query(query=query, observatory_id=observatory_id, skip=skip, limit=limit)
        
        if products_result.is_err:
            return Err(products_result.unwrap_err())
        

        products = products_result.unwrap()
        if not products:
            if not no_cache:
                self._product_cache[cache_key] = []
            return Ok([])

        # 1. Extract all product IDs
        product_ids = [p.product_id for p in products]

        # 2. Batch-fetch all catalog-item links for these products in one query
        link_docs = await self.product_catalog_item_link_repository.collection.find(
            {"product_id": {"$in": product_ids}},
            {"product_id": 1, "catalog_item_id": 1, "_id": 0},
        ).to_list(length=None)

        # 3. Map product_id -> list of catalog_item_ids and gather unique item IDs
        product_to_item_ids: Dict[str, List[str]] = {}
        unique_item_ids: Set[str] = set()
        for doc in link_docs:
            pid    = doc["product_id"]
            iid    = doc["catalog_item_id"]
            product_to_item_ids.setdefault(pid, []).append(iid)
            unique_item_ids.add(iid)

        # 4. Batch-fetch all catalog items in one query
        item_docs = await self.catalog_item_repository.collection.find(
            {"catalog_item_id": {"$in": list(unique_item_ids)}}
        ).to_list(length=None)

        # 5. Build lookup and metadata maps
        item_lookup: Dict[str, str] = {}
        item_metadata: Dict[str, List[DTO.VariableMetadataDTO]] = {}
        for doc in item_docs:
            item_model = M.CatalogItemX(**doc)
            item_lookup[item_model.catalog_item_id] = item_model.name
            if item_model.catalog_type is None:
                continue
            item_metadata.setdefault(item_model.catalog_type.value, []).append(
                DTO.VariableMetadataDTO(
                    code=item_model.code,
                    name=item_model.name,
                    value=item_model.value,
                    description=item_model.description,
                )
            )

        # 6. Batch-fetch observatory memberships for all returned products
        obs_link_docs = await self.observatory_product_link_repository.collection.find(
            {"product_id": {"$in": product_ids}},
            {"product_id": 1, "observatory_id": 1, "_id": 0},
        ).to_list(length=None)
        product_to_obs_ids: Dict[str, List[str]] = {}
        for doc in obs_link_docs:
            product_to_obs_ids.setdefault(doc["product_id"], []).append(doc["observatory_id"])

        # 7. Hydrate and build the DTOs
        response_dtos:List[DTO.ProductXDTO] = []
        for p in products:
            # Get the raw item IDs linked to this specific product
            p_item_ids = product_to_item_ids.get(p.product_id, [])
            
            # Translate IDs to names
            human_readable_attributes = [
                item_lookup.get(i_id, i_id) # Fallback to the raw ID if the name is missing
                for i_id in p_item_ids
            ]

            # Build the DTO. We can use the raw IDs as 'tags' and the names as 'attributes'!
            dto = DTO.ProductXDTO.from_model(
                model=p,
                # attributes_names=human_readable_attributes,
                # tags=p_item_ids 
            )
            dto.tags = p_item_ids
            dto.attributes = human_readable_attributes
            dto.observatory_ids = product_to_obs_ids.get(p.product_id, [])
            default_spatial_var =   item_metadata.get("SPATIAL", [])
            dto.spatial_variable =  default_spatial_var[0] if default_spatial_var else DTO.VariableMetadataDTO()
            default_temporal_var =   item_metadata.get("TEMPORAL", [])
            dto.temporal_variable =  default_temporal_var[0] if default_temporal_var else DTO.VariableMetadataDTO()
            dto.interest_variable = item_metadata.get("INTEREST", [])
            response_dtos.append(dto)

        if not no_cache:
            self._product_cache[cache_key] = response_dtos
        if self.suggestion_repository:
            tracking_key = observatory_id if observatory_id else "__global__"
            asyncio.create_task(self.suggestion_repository.record_hit(tracking_key, query))
        return Ok(response_dtos)

    async def get_search_suggestions(self, observatory_id: str, limit: int = 5) -> Result[List[DTO.SearchSuggestionItemDTO], EX.JubError]:
        try:
            if not self.suggestion_repository:
                return Ok([])
            suggestions = await self.suggestion_repository.get_top(observatory_id, limit)
            return Ok([DTO.SearchSuggestionItemDTO(query=s.query, hit_count=s.hit_count) for s in suggestions])
        except Exception as e:
            return Err(EX.JubError.from_exception(e))

    async def get_observatory_search_suggestions(self, limit: int = 5) -> Result[List[DTO.SearchSuggestionItemDTO], EX.JubError]:
        try:
            if not self.suggestion_repository:
                return Ok([])
            suggestions = await self.suggestion_repository.get_top("__observatories__", limit)
            return Ok([DTO.SearchSuggestionItemDTO(query=s.query, hit_count=s.hit_count) for s in suggestions])
        except Exception as e:
            return Err(EX.JubError.from_exception(e))

    async def search_data_records(self, query_str: str, observatory_id: Optional[str] = None, skip: int = 0, limit: int = 100, strict: bool = True):
        try:
            ast = QueryAST.parse(query_str)
            # DataRecord has no observatory_id field — do NOT add it to the match filter
            match_filter: Dict[str, Any] = {}

            for query in ast.queries:
                prefix = query.catalog_prefix
                group  = query.group

                if prefix == SPATIAL_VARIABLE:
                    if not strict:
                        # Lenient: _build_spatial_filter always returns a non-empty dict
                        # (falls back to the raw string when the identifier is unknown).
                        # Check resolution explicitly so that VS(NOWHERE) is dropped instead
                        # of producing a filter that matches zero records.
                        cond = group.conditions[0]
                        leaf = (cond.item_path[-1]
                                if isinstance(cond.item_path, list) and cond.item_path
                                else cond.item_path or cond.catalog_value)
                        if leaf and leaf != "*":
                            resolved_ids = await self.__resolve_item_ids_for_value(leaf)
                            if not resolved_ids:
                                continue  # identifier unknown — skip this clause
                    f = await self._build_spatial_filter(group)
                    match_filter.update(f)
                elif prefix == TEMPORAL_VARIABLE:
                    f = ASTToMongoTranslator._build_temporal(group)
                    if f or strict:
                        match_filter.update(f)
                elif prefix == INTEREST_VARIABLE:
                    f = await self._build_interest_filter(group)
                    if f or strict:
                        match_filter.update(f)
                # VO / BY are aggregation-only; skip for raw record fetch

            cursor = self.data_records_repository.collection.find(match_filter, {"_id": 0}).skip(skip).limit(limit)
            records = await cursor.to_list(length=limit)
            return Ok(records)

        except ValueError as ve:
            return Err(EX.UnknownError(str(ve)))
        except Exception as e:
            return Err(EX.UnknownError(f"Error fetching data records: {str(e)}"))

    async def _resolve_identifier(self, raw_value: str) -> str:
        """
        Given any string (catalog_item_id, value, or numeric code), returns the
        canonical catalog_item_id stored in the records.  Falls back to raw_value
        if nothing matches so existing queries are never broken.
        """
        try:
            # Single query covering all three catalog-item fields at once
            or_conds: List[dict] = [
                {"catalog_item_id": raw_value},
                {"value": raw_value.upper()},
            ]
            if raw_value.lstrip("-").isdigit():
                or_conds.append({"code": int(raw_value)})

            doc = await self.catalog_item_repository.collection.find_one({"$or": or_conds})
            if doc:
                resolved = doc["catalog_item_id"]
                L.debug(f"resolve_identifier: '{raw_value}' → '{resolved}' (catalog_item)")
                return resolved

            # Alias lookup (value, code, or alias id)
            alias_or: List[dict] = [
                {"value": raw_value},
                {"catalog_item_alias_id": raw_value},
            ]
            if raw_value.lstrip("-").isdigit():
                alias_or.append({"code": int(raw_value)})

            alias_doc = await self.catalog_alias_repository.collection.find_one({"$or": alias_or})
            if alias_doc:
                link = await self.catalog_item_catalog_alias_link_repository.collection.find_one(
                    {"catalog_item_alias_id": alias_doc["catalog_item_alias_id"]}
                )
                if link:
                    resolved = link["catalog_item_id"]
                    L.debug(f"resolve_identifier: '{raw_value}' → '{resolved}' (alias)")
                    return resolved

            L.debug(f"resolve_identifier: '{raw_value}' not found in catalog — used as-is")
            return raw_value
        except Exception as e:
            L.error(f"resolve_identifier error for '{raw_value}': {e}")
            return raw_value

    async def _build_spatial_filter(self, group: ConditionGroup) -> dict:
        """Builds a resolved $match filter for VS(...) conditions."""
        if group.logic == "AND":
            raise ValueError("Logical AND is not allowed in VS(). A record has one location — did you mean OR?")

        async def _resolve_single(cond: Condition):
            raw = cond.item_path[0] if isinstance(cond.item_path, list) and cond.item_path else cond.catalog_value
            return await self._resolve_identifier(raw)

        if group.logic == "SINGLE":
            cond = group.conditions[0]
            if cond.operator == "WILDCARD":
                if not cond.item_path:
                    return {}
                parent_id = await self._resolve_identifier(cond.item_path[0])
                children_res = await self.catalog_item_relationship_repository.get_all_children_nodes(parent_id)
                all_ids = [parent_id] + (children_res.unwrap() if not children_res.is_err else [])
                return {"spatial_id": {"$in": all_ids}}
            resolved = await _resolve_single(cond)
            return {"spatial_id": resolved}

        # OR logic
        all_ids: List[str] = []
        for cond in group.conditions:
            if cond.operator == "WILDCARD":
                if not cond.item_path:
                    return {}  # global wildcard cancels all filters
                parent_id = await self._resolve_identifier(cond.item_path[0])
                children_res = await self.catalog_item_relationship_repository.get_all_children_nodes(parent_id)
                all_ids.append(parent_id)
                if not children_res.is_err:
                    all_ids.extend(children_res.unwrap())
            else:
                all_ids.append(await _resolve_single(cond))

        unique_ids = list(dict.fromkeys(all_ids))  # preserve order, deduplicate
        if len(unique_ids) == 1:
            return {"spatial_id": unique_ids[0]}
        return {"spatial_id": {"$in": unique_ids}}

    async def _build_interest_filter(self, group: ConditionGroup) -> dict:
        """Builds a resolved $match filter for VI(...) conditions."""
        # VI(*) — global wildcard, no filter
        if group.logic == "SINGLE" and group.conditions[0].operator == "WILDCARD" and group.conditions[0].catalog_value == "*":
            return {}

        resolved_ids: List[str] = []
        for cond in group.conditions:
            raw_combined = ASTToMongoTranslator._format_id(cond.catalog_value, cond.item_path)
            resolved_ids.append(await self._resolve_identifier(raw_combined))

        if group.logic == "SINGLE":
            return {"interest_ids": resolved_ids[0]}
        elif group.logic == "AND":
            return {"interest_ids": {"$all": resolved_ids}}
        elif group.logic == "OR":
            return {"interest_ids": {"$in": resolved_ids}}
        return {}

    async def _get_canonical_id(self, raw_target: str) -> Result[str, EX.JubError]:
        """Thin wrapper kept for backward-compatibility with _resolve_condition."""
        try:
            return Ok(await self._resolve_identifier(raw_target))
        except Exception as e:
            L.error(f"Error resolving alias for {raw_target}: {e}")
            return Err(EX.JubError.from_exception(e))


    def __is_global_wildcard(self, condition: Condition) -> bool:
            """
            Returns True only for a bare * wildcard (VS(*), VI(*), VT(*)).
            EXACT conditions with an empty item_path — e.g. VI(FEMALE) which the parser
            produces as (operator=EXACT, catalog_value="FEMALE", item_path=[]) — are NOT
            wildcards and must not be skipped.
            """
            if condition.operator != "WILDCARD":
                return False
            path = condition.item_path
            L.debug(f"Checking if condition {condition} is a global wildcard. Path: {path}")
            if isinstance(path, list):
                return len(path) == 0 or (len(path) == 1 and path[0] == "*")
            return path == "*" or path == ""
    
    async def execute_query(self, query: str, observatory_id: Optional[str] = None,skip:int= 0,limit:int=10) -> Result[List[M.ProductX], EX.JubError]:
        """
        The main entry point for the Jub search bar.
        """
        try:
            # 1. Parse string to AST
            ast = QueryAST.parse(query)
            # print("AST",ast)
            # 2. Resolve AST conditions into required sets of Catalog Item IDs
            required_sets_res = await self._build_required_sets(ast)
            # print("Required Sets Result:", required_sets_res)

            # print("_"*20)
            if required_sets_res.is_err:
                return required_sets_res
            
            required_sets = required_sets_res.unwrap()
            
            # 3. Build and execute the aggregation pipeline
            pipeline = self._build_mongo_pipeline(observatory_id, required_sets,skip,limit)
            
            cursor = self.observatory_product_link_repository.collection.aggregate(pipeline)
            documents = await cursor.to_list(length=None)
            
            # 4. Return formatted products
            products = [M.ProductX(**doc) for doc in documents]
            return Ok(products)
            
        except Exception as e:
            L.error(f"Execution failed for query '{query}': {e}")
            return Err(EX.JubError.from_exception(e))

    async def _build_required_sets(self, ast: QueryAST) -> Result[List[List[str]], EX.JubError]:
        """
        Converts the AST into a list of required tag lists.
        A product MUST have at least one matching tag from EVERY list in required_sets.
        """
        try: 
            required_sets: List[List[str]] = []
            for query in ast.queries:
                # If OR/SINGLE logic: All conditions pool together into ONE requirement set.
                if query.group.logic in ["OR", "SINGLE"]:
                    # We combine all conditions into one big set. The product needs at least one tag from this combined set to satisfy the OR logic.
                    combined_set = set([])
                    # skip_group is a flag to identify if we have a global wildcard in the group. If we do, we can skip processing the rest of the conditions because the wildcard already allows any tag to match.
                    skip_group = False
                    for cond in query.group.conditions:
                        is_global_wildcard = self.__is_global_wildcard(cond)
                        print("Processing condition for OR group:", cond,is_global_wildcard)
                        
                        if is_global_wildcard:
                            skip_group = True
                            break
                        # If it's not a global wildcard, we resolve the condition as normal and add its valid tags to the combined set.
                        res = await self._resolve_condition(cond)
                        print("Condition resolution result for", cond, "is", res)
                        if res.is_err: 
                            L.error(f"Failed to resolve condition {cond}: {res.unwrap_err()}")
                            return res
                        # combined_set.extend(res.unwrap())
                        combined_set.update(res.unwrap()) # Using a set to avoid duplicates

                    if not skip_group:
                        required_sets.append(list(combined_set))
                    
                elif query.group.logic == "AND":
                    # Temporal AND (e.g. VT(>= 2021 AND <= 2024)): intersect the resolved
                    # ID sets so only items in the full range survive.
                    # Cross-catalog AND (e.g. VI(SEX AND AGE)): separate required_sets so
                    # the pipeline checks the product has at least one tag from EACH set.
                    is_temporal = query.catalog_prefix == TEMPORAL_VARIABLE
                    intersected: Optional[set] = None

                    for cond in query.group.conditions:
                        if self.__is_global_wildcard(cond):
                            continue

                        res = await self._resolve_condition(cond)
                        if res.is_err:
                            L.error(f"Failed to resolve condition {cond}: {res.unwrap_err()}")
                            return res
                        conds_ids = res.unwrap()

                        if is_temporal:
                            conds_set = set(conds_ids)
                            intersected = conds_set if intersected is None else intersected & conds_set
                        else:
                            if conds_ids:
                                required_sets.append(conds_ids)

                    if is_temporal and intersected is not None:
                        required_sets.append(list(intersected))

            # print("REQUIRED",required_sets)
            return Ok(required_sets)
        except Exception as e:
            L.error({
                "message": "Error building required sets from AST",
                "error": str(e),
            })
            return Err(EX.JubError.from_exception(e))

    async def _resolve_condition(self, condition: Condition) -> Result[List[str], EX.JubError]:
        """
        Translates a single AST condition into an exact list of catalog_item_ids.
        """
        try:
            L.debug({
                "event":"CONDITION_RESOLUTION",
                "message": "Resolving condition",
                "condition": condition.model_dump()
            })
            path       = condition.item_path
            is_list    = isinstance(path, list)
            path_val   = path[-1] if is_list and len(path) > 0 else path


            # ==== Path 1: Handle temporal conditions  ======
            if condition.catalog_value == "TEMPORAL" and condition.operator not in ["WILDCARD"]:
                mongo_op_map = {
                    ">": "$gt", ">=": "$gte", "<": "$lt", "<=": "$lte", "=": "$eq",
                    "EXACT": "$eq" 
                }
                mongo_op     = mongo_op_map.get(condition.operator)
                if not mongo_op:
                    return Err(EX.UnknownError(f"Unsupported operator for temporal condition: {condition.operator}"))

                path_val = condition.item_path[-1] if isinstance(condition.item_path, list) and len(condition.item_path) > 0 else condition.item_path
                
                try:
                    dt_val = DT.datetime.fromisoformat(path_val.replace("Z", "+00:00"))  # Convert ISO string to datetime object
                except ValueError as ve:
                    L.error(f"Invalid datetime format for temporal condition: {path_val}")
                    return Err(EX.JubError(f"Invalid datetime format for temporal condition: {path_val}"))


                # Query the catalog items to find which IDs fall in this date range
                cursor = self.catalog_item_repository.collection.find(
                    # Assuming temporal values are stored as ISO strings in 'value'
                    {"value_type": "DATETIME", "temporal_value": {mongo_op: dt_val}}
                )
                docs = await cursor.to_list(length=None)
                L.debug({
                    "event": "TEMPORAL_CONDITION_RESOLUTION",
                    "message": "Resolved temporal condition",
                    "path_val": path_val,
                    "mongo_op": mongo_op,
                    "dt_val": dt_val.isoformat(),
                    "condition": condition.model_dump(),
                    "resolved_ids": [doc["catalog_item_id"] for doc in docs]
                })
                return Ok([doc["catalog_item_id"] for doc in docs])

            # Path 2: NUMERICAL MATH (Querying by 'code')
            elif condition.operator in [">", ">=", "<", "<=", "="]:
                # e.g., AGE >= 20. catalog_value="AGE", path_val="20"
                mongo_op_map = {">": "$gt", ">=": "$gte", "<": "$lt", "<=": "$lte", "=": "$eq"}
                mongo_op     = mongo_op_map.get(condition.operator)
                
                try:
                    num_val = float(path_val) # Convert the string "20" to a number
                except ValueError:
                    return Err(EX.ValidationError(f"Expected a number for condition {condition.catalog_value}, got {path_val}"))

                cat_val = condition.catalog_value
                
                # Find all items that belong to this catalog (e.g., AGE or AGE_1) 
                # AND whose 'code' matches the mathematical condition
                cursor = self.catalog_item_repository.collection.find({
                    "catalog_item_id": {"$regex": f"^{cat_val}$|^{cat_val}_"},
                    "code": {mongo_op: num_val}
                })
                docs = await cursor.to_list(length=None)
                return Ok([doc["catalog_item_id"] for doc in docs])

            # ==========================================
            # PATH 3: PREFIX / ROOT MATCH (e.g., VI(AGE))
            # ==========================================
            elif condition.operator == "EXACT" and (not path or len(path) == 0):
                cat_val = condition.catalog_value
                L.debug(f"Handling PREFIX match for root catalog: {cat_val}")
                
                # Fetch the root item itself AND all its numbered buckets/sub-items
                cursor = self.catalog_item_repository.collection.find({
                    "catalog_item_id": {"$regex": f"^{cat_val}$|^{cat_val}_"}
                })
                docs = await cursor.to_list(length=None)
                return Ok([doc["catalog_item_id"] for doc in docs])
            path       = condition.item_path
            is_list    = isinstance(path, list)
            raw_target = ""
            # Extract target ID from the path (e.g., "CIE10.C50" -> "C50")
            # target_id = condition.item_path[-1] if isinstance(condition.item_path, list) else condition.item_path


            # Handle EXACT
            L.debug({
                "event": "CONDITION_RESOLUTION",
                "message": "Handling exact condition",
                "operator": condition.operator,
                "item_path": condition.item_path
            })
            if condition.operator == "WILDCARD":
                path_len = len(path)
                if is_list:
                    # Scenario A: The parser left the '*' in the list (e.g., ['MX', 'TAMPS', '*'])
                    if path_len > 1 and path[-1] == "*":
                        raw_target = path[-2]
                    # Scenario B: The parser stripped the '*' (e.g., ['MX', 'TAMPS'])
                    elif path_len > 0 and path[-1] != "*":
                        raw_target = path[-1]
                    # Scenario C: Reverse wildcard (e.g., ['*', 'MX'])
                    elif path_len > 0 and path[0] == "*":
                        raw_target = path[-1]
                    else:
                        return Err(EX.JubError(f"Invalid wildcard list format: {path}"))
                else:
                    # Handle raw strings
                    if path.endswith(".*"):
                        raw_target = path[:-2]
                    elif path != "*":
                        raw_target = path  # The parser just sent "TAMPS" with a WILDCARD operator
                    else:
                        raw_target = path # Global wildcard handling

                # return Err(EX.JubError(f"Invalid wildcard format in condition: {condition}"))
            elif condition.operator == "EXACT":
                # print("Handling EXACT condition with path:", path)
                raw_target = path[-1] if is_list else path
            else:
                return Err(EX.UnknownError(f"Unsupported operator in condition: {condition.operator}"))



            # Alias resolution: If the user typed an alias (e.g., '28'), we need to find the canonical ID (e.g., 'TAM') 
            canonical_res = await self._get_canonical_id(raw_target)
            print("Canonical resolution result:", canonical_res)
            if canonical_res.is_err:
                L.error(f"Failed to resolve canonical ID for {raw_target}: {canonical_res.unwrap_err()}")
                return Err(EX.JubError(f"Failed to resolve canonical ID for {raw_target}: {canonical_res.unwrap_err()}"))
            target_id = canonical_res.unwrap()

            if condition.operator == ConditionOperators.WILDCARD.value:
                children_res = await self.catalog_item_relationship_repository.get_all_children_nodes(target_id)
                if children_res.is_err:
                    L.error(f"Failed to fetch children for wildcard condition {condition}: {children_res.unwrap_err()}")
                    return Err(EX.JubError(f"Failed to fetch children for wildcard condition {condition}: {children_res.unwrap_err()}"))
                valid_ids = [target_id] + children_res.unwrap()  # Include the parent ID itself
                return Ok(valid_ids)
            
            elif condition.operator == ConditionOperators.EXACT.value:
                # Multiple observatories can have different catalog_item_ids for the
                # same logical value (e.g., "CAM" exists in obs-1, obs-2, obs-3 with
                # different IDs). Return ALL matching IDs so the pipeline intersection
                # works regardless of which observatory the product belongs to.
                cursor = self.catalog_item_repository.collection.find({
                    "$or": [
                        {"value": raw_target.upper()},
                        {"catalog_item_id": raw_target},
                    ]
                })
                docs = await cursor.to_list(length=None)
                all_ids = [doc["catalog_item_id"] for doc in docs]
                return Ok(all_ids if all_ids else [target_id])
            
            else:
                L.error(f"Unsupported operator in condition: {condition.operator}")
                return Err(EX.UnknownError(f"Unsupported operator in condition: {condition.operator}"))
           

        except Exception as e:
            L.error(f"Error resolving condition {condition}: {e}")
            return Err(EX.JubError.from_exception(e))

    def _build_mongo_pipeline(self,
        observatory_id: Optional[str],
        required_sets: List[List[str]],
        skip:int = 0,
        limit:int = 10
    ) -> List[dict]:
        """
        Translates the required_sets into a high-performance MongoDB intersection pipeline.
        If observatory_id is None, it searches across all observatories.
        """
        pipeline = []

        # 1. Scope the search strictly to this observatory (IF provided)
        if observatory_id:
            pipeline.append({"$match": {"observatory_id": observatory_id}})
            
        # 1.5. DEDUPLICATION: If searching all observatories, a product might appear multiple times 
        # if it belongs to multiple observatories. We group by product_id to ensure unique results.
        pipeline.append({
            "$group": {
                "_id": "$product_id",
                "product_id": {"$first": "$product_id"}
            }
        })

        # 2. Join the product's tags from the linking table
        pipeline.append({
            "$lookup": {
                "from": self.product_catalog_item_link_repository.collection.name,
                "localField": "product_id",
                "foreignField": "product_id",
                "as": "raw_tags"
            }
        })

        # 3. Transform the array of link objects into a simple array of strings (IDs)
        pipeline.append({
            "$project": {
                "product_id": 1,
                "matched_tags": {
                    "$map": {
                        "input": "$raw_tags",
                        "as": "tag_doc",
                        "in": "$$tag_doc.catalog_item_id"
                    }
                }
            }
        })

        # 4. Apply the logical intersections parsed from the AST (ONLY if there are requirements)
        if required_sets:
            intersection_conditions = [
                {"$gt": [{"$size": {"$setIntersection": ["$matched_tags", req_set]}}, 0]}
                for req_set in required_sets
            ]
            
            pipeline.append({
                "$match": {
                    "$expr": {
                        "$and": intersection_conditions
                    }
                }
            })

        if skip > 0:
            pipeline.append({"$skip": skip})
            
        if limit > 0:
            pipeline.append({"$limit": limit})
        # 5. Fetch the actual product metadata for the surviving IDs
        pipeline.extend([
            {"$lookup": {
                "from": self.product_repository.collection.name,
                "localField": "product_id",
                "foreignField": "product_id",
                "as": "product_data"
            }},
            {"$unwind": "$product_data"},
            {"$replaceRoot": {"newRoot": "$product_data"}}
        ])

        return pipeline


class NotificationService:
    def __init__(self, repository: R.NotificationsRepository):
        self.notification_repo = repository

    async def trigger_notification(self, dto: DTO.CreateNotificationDTO) -> Result[str, EX.JubError]:
        """
        Creates a new notification. This will be called internally by other services 
        (e.g., after an Observatory is created or a CSV finishes processing).
        """
        print("Triggering notification with DTO:", dto)
        new_notification = M.Notification(
            notification_id = f"notif_{uuid.uuid4().hex[:12]}",
            user_id         = dto.user_id,
            status          = dto.status,
            operation       = dto.operation,
            entity          = dto.entity_type,
            entity_id       = dto.entity_id,
            title           = dto.title,
            message         = dto.message,
            is_read         = False
            # created_at is automatically handled by the model's default_factory
        )
        
        return await self.notification_repo.insert(new_notification)

    async def get_user_notifications(self, user_id: str, unread_only: bool = False, limit: int = 50) -> Result[List[M.Notification], EX.JubError]:
        """Fetches notifications for the user's UI."""
        if unread_only:
            return await self.notification_repo.get_unread_by_user(user_id, limit)
        return await self.notification_repo.get_all_by_user(user_id, limit)

    async def is_my_notification(self, notification_id: str, user_id: str) -> Result[bool, EX.JubError]:
        """Checks if a notification belongs to the user (for authorization)."""
        return await self.notification_repo.check_ownership(notification_id, user_id)
    async def mark_as_read(self, notification_id: str, user_id: str) -> Result[M.Notification, EX.JubError]:
        """Marks a single notification as read."""
        res = await self.notification_repo.get_by_id_and_user(notification_id,user_id=user_id)
        if res.is_err:
            L.error(f"Failed to fetch notification {notification_id} for user {user_id}: {res.unwrap_err()}")
            return Err(EX.JubError(f"Failed to fetch notification: {res.unwrap_err()}"))
        return await self.notification_repo.mark_as_read(notification_id)

    async def mark_all_as_read(self, user_id: str) -> Result[int, EX.JubError]:
        """Marks all unread notifications for a user as read."""
        return await self.notification_repo.mark_all_as_read(user_id)

    async def clear_read_notifications(self, user_id: str) -> Result[int, EX.JubError]:
        """Deletes all notifications the user has already read."""
        return await self.notification_repo.delete_read_by_user(user_id)

class UsersProfileXService:
    def __init__(self, 
        user_profile_repository: R.UserProfileXRepository,
        auth_service: AuthenticationService,
        notification_service: NotificationService
    ):
        self.user_profile_repository = user_profile_repository
        self.auth_service            = auth_service
        self.notification_service    = notification_service


    async def get_user_preferences(self, user_id: str) -> Result[M.UserPreferences, EX.JubError]:
        try:
            user_result = await self.user_profile_repository.get_by_id(user_id)
            if user_result.is_err:
                return Err(EX.JubError(f"User with ID {user_id} not found: {user_result.unwrap_err()}"))
            
            user = user_result.unwrap()
            return Ok(user.settings)
        except Exception as e:
            L.error(f"Error fetching user profile for {user_id}: {e}")
            return Err(EX.JubError.from_exception(e))
        
    async def update_user_preferences(self, user_id: str, new_settings: DTO.UserPreferencesDTO) -> Result[M.UserProfileX, EX.JubError]:
        try:
            
            preference_model = new_settings.to_model()
            update_result = await self.user_profile_repository.update_settings(user_id, preference_model)
            if update_result.is_err:
                L.error(f"Failed to update user profile for {user_id}: {update_result.unwrap_err()}")
                return Err(EX.JubError(f"Failed to update user profile: {update_result.unwrap_err()}"))
            res = await self.notification_service.trigger_notification(
                DTO.CreateNotificationDTO(
                    user_id     = user_id,
                    status      = ENUMS.NotificationStatusEnum.INFO,
                    entity_id   = user_id,
                    entity_type = ENUMS.NotificationEntityEnum.USER_PROFILE,
                    operation   = ENUMS.NotificationOperationEnum.UPDATE,
                    message     = "Tus preferencias han sido actualizadas exitosamente.",
                    title       = "Preferencias actualizadas",
                )
            )
            if res.is_err:
                L.error(f"Failed to trigger notification after updating preferences for {user_id}: {res.unwrap_err()}")
            return Ok(update_result.unwrap())
        except Exception as e:
            L.error(f"Error updating user profile for {user_id}: {e}")
            return Err(EX.JubError.from_exception(e))
        

    async def login(self,dto:XoloDTO.AuthAttemptDTO)->Result[DTO.AutenticationResponsetDTO,EX.JubError]:
        try:
            res = await self.auth_service.login(
                dto = dto
            )
            if res.is_err:
                L.error(f"Login failed for {dto.username}: {res.unwrap_err()}")
                return Err(EX.AuthorizationError(f"Login failed: {res.unwrap_err()}"))
            L.info({
                "event": "USER_LOGIN",
                "message": f"User {dto.username} logged in successfully."
            })
            result         = res.unwrap()
            profile_result = await self.get_user_profile_by_username(dto.username)
            if profile_result.is_err:
                e = profile_result.unwrap_err()
                if e.status_code == 404:
                    L.warning(f"User profile not found for {dto.username} after successful login. This might be a new user who hasn't completed their profile setup yet.")
                    create_up = await self.create_user_profile(
                        dto= DTO.UserProfileDTO(
                            user_id     = result.user_id,
                            username    = dto.username,
                            email       = result.email,
                            first_name  = result.first_name,
                            last_name   = result.last_name,
                            fullname    = f"{result.first_name} {result.last_name}",
                            settings    = DTO.UserPreferencesDTO.default(),
                            created_at  = DT.datetime.now(DT.timezone.utc).isoformat(),
                            updated_at  = DT.datetime.now(DT.timezone.utc).isoformat(),
                            is_disabled = False
                        )
                    )  # Fire-and-forget profile creation for new users
                    if create_up.is_err:
                        L.error(f"Failed to create user profile for {dto.username}: {create_up.unwrap_err()}")
                        return Err(EX.JubError(f"Login succeeded but failed to create user profile: {create_up.unwrap_err()}"))
                    
                    return Ok(DTO.AutenticationResponsetDTO(
                        access_token        = result.access_token,
                        temporal_secret_key = result.temporal_secret,
                        user_profile        =  DTO.UserProfileDTO.from_model(create_up.unwrap())
                    ))
                L.error(f"Failed to fetch user profile for {dto.username} after successful login: {e}")
                return Err(EX.JubError(f"Login succeeded but failed to fetch user profile: {e}"))
            profile = profile_result.unwrap()
            # result.emai
            return Ok(DTO.AutenticationResponsetDTO(
                access_token        = result.access_token,
                temporal_secret_key = result.temporal_secret,
                user_profile        = DTO.UserProfileDTO.from_model(profile),
            ))
        except Exception as e:
            L.error(f"Error during user login: {e}")
            return Err(EX.JubError.from_exception(e))
        
    async def signup(self,dto:XoloDTO.SignUpDTO)->Result[DTO.UserProfileDTO,EX.JubError]:
        try:
            default_settings = M.UserPreferences.default()
            res = await self.auth_service.signup(
                email         = dto.email,
                username      = dto.username,
                password      = dto.password,
                first_name    = dto.first_name,
                last_name     = dto.last_name,
                expiration    = dto.expiration,
                profile_photo = dto.profile_photo,
                scope         = dto.scope
            )
            if res.is_err:
                L.error(f"Signup failed for {dto.email}: {res.unwrap_err()}")
                return Err(EX.JubError(f"Signup failed: {res.unwrap_err()}"))
            result = res.unwrap()
            user_id = result.key
            default_profile  = M.UserProfileX(
                email      = dto.email,
                username   = dto.username,
                first_name = dto.first_name,
                last_name  = dto.last_name,
                fullname   = f"{dto.first_name} {dto.last_name}",
                settings   = default_settings,
                user_id    = user_id
            )
            result = await self.user_profile_repository.insert(default_profile)
            if result.is_err:
                L.error(f"Failed to create default profile for {dto.email}: {result.unwrap_err()}")
                return Err(EX.JubError(f"Failed to create default profile: {result.unwrap_err()}"))
            return Ok(default_profile)
        except Exception as e:
            L.error(f"Error during user signup: {e}")
            return Err(EX.JubError.from_exception(e))
    

    async def update_user_profile(self, user_id: str, new_settings: M.UserPreferences) -> Result[M.UserProfileX, EX.JubError]:
        try: 
            update_result = await self.user_profile_repository.update_settings(user_id, new_settings)
            if update_result.is_err:
                return Err(EX.JubError(f"Failed to update user profile: {update_result.unwrap_err()}"))
            
            # Fetch the updated profile to return
            return await self.get_user_profile(user_id)
        except Exception as e:
            L.error(f"Error updating user profile for {user_id}: {e}")
            return Err(EX.JubError.from_exception(e))
    async def create_user_profile(self, dto:DTO.UserProfileDTO) -> Result[M.UserProfileX, EX.JubError]:
        try:
            new_profile = M.UserProfileX(
                user_id    = dto.user_id,
                username   = dto.username,
                email      = dto.email,
                first_name = dto.first_name,
                last_name  = dto.last_name,
                fullname   = f"{dto.first_name} {dto.last_name}",
                settings   = M.UserPreferences.default()
            )
            create_result = await self.user_profile_repository.insert(new_profile)
            if create_result.is_err:
                return Err(EX.JubError(f"Failed to create user profile: {create_result.unwrap_err()}"))
            
            return Ok(new_profile)
        except Exception as e:
            L.error(f"Error creating user profile for {dto.user_id}: {e}")
            return Err(EX.JubError.from_exception(e))
        
    
    async def get_user_profile(self, user_id: str) -> Result[M.UserProfileX, EX.JubError]:
        try:
            user_result = await self.user_profile_repository.get_by_id(user_id)
            if user_result.is_err:
                return Err(EX.JubError(f"User with ID {user_id} not found: {user_result.unwrap_err()}"))
            
            user = user_result.unwrap()
            return Ok(M.UserProfileX.from_doc(user.model_dump()))
        except Exception as e:
            L.error(f"Error fetching user profile for {user_id}: {e}")
            return Err(EX.JubError.from_exception(e))

    async def get_user_profile_by_username(self, username: str) -> Result[M.UserProfileX, EX.JubError]:
        try:
            user_result = await self.user_profile_repository.get_by_username(username)
            if user_result.is_err:
                return Err(EX.NotFound(f"User with username {username} not found: {user_result.unwrap_err()}"))
            
            user = user_result.unwrap()
            return Ok(M.UserProfileX.from_doc(user.model_dump()))
        except Exception as e:
            L.error(f"Error fetching user profile for username {username}: {e}")
            return Err(EX.JubError.from_exception(e))
        
  
        



class TasksService:
    def __init__(
        self, 
        repository: R.TaskRepository,
        notification_service: NotificationService
    ):
        self.task_repo = repository
        self.notification_service = notification_service

    async def create_task(self, dto: DTO.CreateTaskDTO) -> Result[str, EX.JubError]:
        """
        Creates a new task with its initial attempt history.
        """
        now = DT.datetime.now(DT.timezone.utc)
        
        # Create the very first attempt
        initial_attempt = M.TaskAttempt(
            attempt_number = 1,
            status         = ENUMS.TaskStatusEnum.PENDING,
            start_time     = now
        )
        
        new_task = M.TaskX(
            task_id          = dto.task_id if dto.task_id is not None else f"tsk_{uuid.uuid4().hex[:12]}",
            user_id          = dto.user_id,
            observatory_id   = dto.observatory_id,
            title            = dto.title,
            description      = dto.description,
            operation        = dto.operation,
            current_status   = ENUMS.TaskStatusEnum.PENDING,
            progress_message = "En cola...",
            attempts         = [initial_attempt],
            created_at       = now,
            updated_at       = now
        )
        
        return await self.task_repo.insert(new_task)

    async def get_user_tasks(self, user_id: str, skip: int = 0, limit: int = 50) -> Result[List[DTO.TaskXDTO], EX.JubError]:
        """
        Fetches the recent tasks to populate the UI list.
        """
        result = await self.task_repo.get_tasks_by_user(user_id, skip, limit)
        if result.is_err:
            L.error(f"Failed to fetch tasks for user {user_id}: {result.unwrap_err()}")
            return Err(EX.JubError(f"Failed to fetch tasks: {result.unwrap_err()}"))
        tasks = result.unwrap()
        return Ok([DTO.TaskXDTO.from_model(task) for task in tasks])

    async def get_task_details(self, task_id: str, user_id: str) -> Result[DTO.TaskXDTO, EX.JubError]:
        """
        Fetches the full details of a single task, including its attempt history, for the task details view.
        """
        
        task_result = await self.task_repo.get_by_id(task_id)
        if task_result.is_err:
            e = task_result.unwrap_err()
            L.error(f"Failed to fetch task {task_id}: {e}")
            return Err(e)
        
        task = task_result.unwrap()
        if task.user_id != user_id:
            L.warning(f"Unauthorized access attempt to task {task_id} by user {user_id}")
            return Err(EX.AuthorizationError("You are not authorized to view this task."))
        
        return Ok(DTO.TaskXDTO.from_model(task))

    async def get_stats(self, user_id: str) -> Result[DTO.TasksStatsDTO, EX.JubError]:
        """
        Fetches the aggregate counters for the UI (In Progress, Completed, Failed).
        """
        return await self.task_repo.get_task_statistics(user_id)

    async def update_live_progress(
        self, 
        task_id: str, 
        percentage: int, 
        message: str, 
        status: ENUMS.TaskStatusEnum = ENUMS.TaskStatusEnum.RUNNING
    ) -> Result[bool, EX.JubError]:
        """
        Updates the progress bar in the UI. 
        Intended for high-frequency calls by background workers.
        """
        return await self.task_repo.update_progress(task_id, percentage, message, status)

    async def complete_task(self, task_id: str, success: bool, error_msg: str = None) -> Result[M.TaskX, EX.JubError]:
        """
        Finalizes a task attempt. Synchronizes the root status and the attempt history.
        Called by the background worker when the job succeeds or crashes.
        """
        task_result = await self.task_repo.get_by_id(task_id)
        if task_result.is_err:
            return task_result
            
        task = task_result.unwrap()
        final_status = ENUMS.TaskStatusEnum.SUCCESS if success else ENUMS.TaskStatusEnum.FAILED
        now = DT.datetime.now(DT.timezone.utc)
        
        # 1. Sync Root Level (UI)
        task.current_status = final_status
        task.progress_percentage = 100 if success else task.progress_percentage
        task.progress_message = "Completado" if success else f"Error: {error_msg}"
        task.updated_at = now
        
        # 2. Sync History Level (Audit Trail)
        if task.attempts:
            task.attempts[-1].status = final_status
            task.attempts[-1].end_time = now
            if error_msg:
                task.attempts[-1].error_message = error_msg

        update_data = task.model_dump(exclude={"task_id"}, mode="python") 
        return await self.task_repo.update(task_id, update_data)

    async def retry_task(self, task_id: str,user_id: str) -> Result[bool, EX.JubError]:
        """
        Handles the user clicking 'Reintentar' on the UI.
        Appends a new attempt and resets the root UI state to pending.
        """
        task_result = await self.task_repo.get_by_id(task_id)
        if task_result.is_err:
            return Err(EX.NotFound(f"Task {task_id} not found."))
            
        task = task_result.unwrap()
        if task.user_id != user_id:
            return Err(EX.AuthorizationError("You are not authorized to retry this task."))
        
        # Optional safeguard: Prevent retrying tasks that are currently running or already succeeded
        if task.current_status in [ENUMS.TaskStatusEnum.RUNNING, ENUMS.TaskStatusEnum.SUCCESS]:
            return Err(EX.JubError(f"Cannot retry task in {task.current_status} state."))

        now                 = DT.datetime.now(DT.timezone.utc)
        next_attempt_number = len(task.attempts) + 1
        
        new_attempt = M.TaskAttempt(
            attempt_number = next_attempt_number,
            status         = ENUMS.TaskStatusEnum.PENDING,
            start_time     = now
        )
        
        return await self.task_repo.add_retry_attempt(task_id, new_attempt)



# ══════════════════════════════════════════════════════════════════════════════
# SERVICE / WORKFLOW DOMAIN SERVICES
# ══════════════════════════════════════════════════════════════════════════════

class BuildingBlockService:
    def __init__(self, repo: R.BuildingBlockRepository):
        self.repo = repo

    async def create(self, dto: DTO.BuildingBlockCreateDTO) -> Result[M.BuildingBlock, EX.JubError]:
        model = M.BuildingBlock(
            building_block_id = f"bb_{uuid.uuid4().hex[:12]}",
            name              = dto.name,
            command           = dto.command,
            image             = dto.image,
            description       = dto.description or "",
        )
        res = await self.repo.insert(model)
        if res.is_err:
            return res
        return Ok(model)

    async def get(self, building_block_id: str) -> Result[M.BuildingBlock, EX.JubError]:
        return await self.repo.get_by_id(building_block_id)

    async def list(self, skip: int = 0, limit: int = 100) -> Result[List[M.BuildingBlock], EX.JubError]:
        return await self.repo.find({}, skip=skip, limit=limit)

    async def update(self, building_block_id: str, dto: DTO.BuildingBlockUpdateDTO) -> Result[M.BuildingBlock, EX.JubError]:
        existing = await self.repo.get_by_id(building_block_id)
        if existing.is_err:
            return existing
        model = existing.unwrap()
        if dto.name        is not None: model.name        = dto.name
        if dto.command     is not None: model.command     = dto.command
        if dto.image       is not None: model.image       = dto.image
        if dto.description is not None: model.description = dto.description
        model.updated_at = DT.datetime.now(DT.timezone.utc)
        res = await self.repo.update(building_block_id, model.model_dump())
        if res.is_err:
            return Err(res.unwrap_err())
        return Ok(model)

    async def delete(self, building_block_id: str) -> Result[bool, EX.JubError]:
        existing = await self.repo.get_by_id(building_block_id)
        if existing.is_err:
            return Err(EX.NotFound(f"BuildingBlock '{building_block_id}' not found."))
        return await self.repo.delete(building_block_id)


class PatternService:
    def __init__(self, repo: R.PatternRepository):
        self.repo = repo

    async def create(self, dto: DTO.PatternCreateDTO) -> Result[M.PatternX, EX.JubError]:
        model = M.PatternX(
            pattern_id        = f"pat_{uuid.uuid4().hex[:12]}",
            name              = dto.name,
            task              = dto.task,
            pattern           = dto.pattern,
            description       = dto.description or "",
            workers           = dto.workers,
            loadbalancer      = dto.loadbalancer,
            building_block_id = dto.building_block_id,
        )
        res = await self.repo.insert(model)
        if res.is_err:
            return res
        return Ok(model)

    async def get(self, pattern_id: str) -> Result[M.PatternX, EX.JubError]:
        return await self.repo.get_by_id(pattern_id)

    async def list(self, skip: int = 0, limit: int = 100) -> Result[List[M.PatternX], EX.JubError]:
        return await self.repo.find({}, skip=skip, limit=limit)

    async def update(self, pattern_id: str, dto: DTO.PatternUpdateDTO) -> Result[M.PatternX, EX.JubError]:
        existing = await self.repo.get_by_id(pattern_id)
        if existing.is_err:
            return existing
        model = existing.unwrap()
        if dto.name             is not None: model.name             = dto.name
        if dto.task             is not None: model.task             = dto.task
        if dto.pattern          is not None: model.pattern          = dto.pattern
        if dto.description      is not None: model.description      = dto.description
        if dto.workers          is not None: model.workers          = dto.workers
        if dto.loadbalancer     is not None: model.loadbalancer     = dto.loadbalancer
        if dto.building_block_id is not None: model.building_block_id = dto.building_block_id
        model.updated_at = DT.datetime.now(DT.timezone.utc)
        res = await self.repo.update(pattern_id, model.model_dump())
        if res.is_err:
            return Err(res.unwrap_err())
        return Ok(model)

    async def delete(self, pattern_id: str) -> Result[bool, EX.JubError]:
        existing = await self.repo.get_by_id(pattern_id)
        if existing.is_err:
            return Err(EX.NotFound(f"Pattern '{pattern_id}' not found."))
        return await self.repo.delete(pattern_id)


class StageService:
    def __init__(self, repo: R.StageRepository):
        self.repo = repo

    async def create(self, dto: DTO.StageCreateDTO) -> Result[M.StageX, EX.JubError]:
        model = M.StageX(
            stage_id          = f"stg_{uuid.uuid4().hex[:12]}",
            name              = dto.name,
            source            = dto.source,
            sink              = dto.sink,
            endpoint          = dto.endpoint,
            transformation_id = dto.transformation_id,
        )
        res = await self.repo.insert(model)
        if res.is_err:
            return res
        return Ok(model)

    async def get(self, stage_id: str) -> Result[M.StageX, EX.JubError]:
        return await self.repo.get_by_id(stage_id)

    async def list(self, skip: int = 0, limit: int = 100) -> Result[List[M.StageX], EX.JubError]:
        return await self.repo.find({}, skip=skip, limit=limit)

    async def update(self, stage_id: str, dto: DTO.StageUpdateDTO) -> Result[M.StageX, EX.JubError]:
        existing = await self.repo.get_by_id(stage_id)
        if existing.is_err:
            return existing
        model = existing.unwrap()
        if dto.name             is not None: model.name             = dto.name
        if dto.source           is not None: model.source           = dto.source
        if dto.sink             is not None: model.sink             = dto.sink
        if dto.endpoint         is not None: model.endpoint         = dto.endpoint
        if dto.transformation_id is not None: model.transformation_id = dto.transformation_id
        model.updated_at = DT.datetime.now(DT.timezone.utc)
        res = await self.repo.update(stage_id, model.model_dump())
        if res.is_err:
            return Err(res.unwrap_err())
        return Ok(model)

    async def delete(self, stage_id: str) -> Result[bool, EX.JubError]:
        existing = await self.repo.get_by_id(stage_id)
        if existing.is_err:
            return Err(EX.NotFound(f"Stage '{stage_id}' not found."))
        return await self.repo.delete(stage_id)


class WorkflowService:
    def __init__(self, repo: R.WorkflowRepository, stage_repo: R.StageRepository):
        self.repo       = repo
        self.stage_repo = stage_repo

    async def create(self, dto: DTO.WorkflowCreateDTO) -> Result[M.WorkflowX, EX.JubError]:
        model = M.WorkflowX(
            workflow_id = f"wf_{uuid.uuid4().hex[:12]}",
            name        = dto.name,
            stage_ids   = dto.stage_ids,
        )
        res = await self.repo.insert(model)
        if res.is_err:
            return res
        return Ok(model)

    async def get(self, workflow_id: str) -> Result[M.WorkflowX, EX.JubError]:
        return await self.repo.get_by_id(workflow_id)

    async def list(self, skip: int = 0, limit: int = 100) -> Result[List[M.WorkflowX], EX.JubError]:
        return await self.repo.find({}, skip=skip, limit=limit)

    async def update(self, workflow_id: str, dto: DTO.WorkflowUpdateDTO) -> Result[M.WorkflowX, EX.JubError]:
        existing = await self.repo.get_by_id(workflow_id)
        if existing.is_err:
            return existing
        model = existing.unwrap()
        if dto.name      is not None: model.name      = dto.name
        if dto.stage_ids is not None: model.stage_ids = dto.stage_ids
        model.updated_at = DT.datetime.now(DT.timezone.utc)
        res = await self.repo.update(workflow_id, model.model_dump())
        if res.is_err:
            return Err(res.unwrap_err())
        return Ok(model)

    async def delete(self, workflow_id: str, cascade: bool = False) -> Result[dict, EX.JubError]:
        existing = await self.repo.get_by_id(workflow_id)
        if existing.is_err:
            return Err(EX.NotFound(f"Workflow '{workflow_id}' not found."))
        workflow     = existing.unwrap()
        stages_count = 0
        if cascade and workflow.stage_ids:
            del_res = await self.stage_repo.delete_many_by_ids(workflow.stage_ids)
            if del_res.is_ok:
                stages_count = del_res.unwrap()
        res = await self.repo.delete(workflow_id)
        if res.is_err:
            return Err(res.unwrap_err())
        return Ok({"deleted": True, "stages": stages_count})


class ServiceXService:
    def __init__(
        self,
        repo:          R.ServiceRepository,
        workflow_repo: R.WorkflowRepository,
        stage_repo:    R.StageRepository,
        pattern_repo:  R.PatternRepository,
        bb_repo:       R.BuildingBlockRepository,
        link_manager:  GraphLinkManager,
    ):
        self.repo          = repo
        self.workflow_repo = workflow_repo
        self.stage_repo    = stage_repo
        self.pattern_repo  = pattern_repo
        self.bb_repo       = bb_repo
        self.link_manager  = link_manager

    async def create(self, dto: DTO.ServiceCreateDTO) -> Result[M.ServiceX, EX.JubError]:
        model = M.ServiceX(
            service_id  = f"svc_{uuid.uuid4().hex[:12]}",
            name        = dto.name,
            description = dto.description or "",
            owner_id    = dto.owner_id,
            public      = dto.public,
            workflow_id = dto.workflow_id,
            provider    = dto.provider or ENUMS.ServiceProviderEnum.OTHER
        )
        res = await self.repo.insert(model)
        if res.is_err:
            return res
        return Ok(model)

    async def get(self, service_id: str) -> Result[M.ServiceX, EX.JubError]:
        return await self.repo.get_by_id(service_id)

    async def list(self, skip: int = 0, limit: int = 100) -> Result[List[M.ServiceX], EX.JubError]:
        return await self.repo.find({}, skip=skip, limit=limit)

    async def update(self, service_id: str, dto: DTO.ServiceUpdateDTO) -> Result[M.ServiceX, EX.JubError]:
        existing = await self.repo.get_by_id(service_id)
        if existing.is_err:
            return existing
        model = existing.unwrap()
        if dto.name        is not None: model.name        = dto.name
        if dto.description is not None: model.description = dto.description
        if dto.public      is not None: model.public      = dto.public
        if dto.workflow_id is not None: model.workflow_id = dto.workflow_id
        model.updated_at = DT.datetime.now(DT.timezone.utc)
        res = await self.repo.update(service_id, model.model_dump())
        if res.is_err:
            return Err(res.unwrap_err())
        return Ok(model)

    async def delete(self, service_id: str) -> Result[DTO.ServiceDeleteResponseDTO, EX.JubError]:
        existing = await self.repo.get_by_id(service_id)
        if existing.is_err:
            return Err(EX.NotFound(f"Service '{service_id}' not found."))
        res = await self.repo.delete(service_id)
        if res.is_err:
            return Err(res.unwrap_err())
        await self.link_manager.observatory_service_link_repository.collection.delete_many({"service_id": service_id})
        return Ok(DTO.ServiceDeleteResponseDTO(deleted=True, service_id=service_id))

    async def query_services(self, dsl: str, skip: int = 0, limit: int = 100) -> Result[List[M.ServiceX], EX.JubError]:
        try:
            L.debug({
                "event": "QUERY_SERVICES",
                "dsl": dsl,
                "skip": skip,
                "limit": limit
            })
            mongo_filter = ServiceQuery.parse(dsl).to_mongo_filter()
            L.debug({
                "event": "PARSED_SERVICE_QUERY",
                "mongo_filter": mongo_filter         
            })
            return await self.repo.search(mongo_filter, skip=skip, limit=limit)
        except ValueError as e:
            return Err(EX.ValidationError(str(e)))
        except Exception as e:
            return Err(EX.JubError.from_exception(e))

    async def query_services_hydrated(self, dsl: str, skip: int = 0, limit: int = 100) -> Result[List[DTO.ServiceDetailDTO], EX.JubError]:
        services_result = await self.query_services(dsl, skip=skip, limit=limit)
        if services_result.is_err:
            return services_result

        hydrated: List[DTO.ServiceDetailDTO] = []

        for service in services_result.unwrap():
            workflow_detail: Optional[DTO.WorkflowDetailDTO] = None

            if service.workflow_id:
                wf_res = await self.workflow_repo.get_by_id(service.workflow_id)
                if wf_res.is_ok:
                    wf = wf_res.unwrap()
                    stage_details: List[DTO.StageDetailDTO] = []

                    for stage_id in wf.stage_ids:
                        stage_res = await self.stage_repo.get_by_id(stage_id)
                        if stage_res.is_err:
                            continue
                        stage = stage_res.unwrap()

                        pattern_detail: Optional[DTO.PatternDetailDTO] = None
                        if stage.transformation_id:
                            pat_res = await self.pattern_repo.get_by_id(stage.transformation_id)
                            if pat_res.is_ok:
                                pat = pat_res.unwrap()
                                bb_detail: Optional[DTO.BuildingBlockDTO] = None
                                if pat.building_block_id:
                                    bb_res = await self.bb_repo.get_by_id(pat.building_block_id)
                                    if bb_res.is_ok:
                                        bb_detail = DTO.BuildingBlockDTO.from_model(bb_res.unwrap())
                                pattern_detail = DTO.PatternDetailDTO.from_model(pat, bb_detail)

                        stage_details.append(DTO.StageDetailDTO.from_model(stage, pattern_detail))

                    workflow_detail = DTO.WorkflowDetailDTO.from_model(wf, stage_details)

            hydrated.append(DTO.ServiceDetailDTO.from_model(service, workflow_detail))
        # print(hydrated)
        return Ok(hydrated)

    async def create_full(self, dto: DTO.ServiceIndexDTO) -> Result[DTO.ServiceIndexResponseDTO, EX.JubError]:
        """One-shot create: Service → Workflow → Stages → Patterns → BuildingBlocks."""
        bb_ids      : List[str] = []
        pattern_ids : List[str] = []
        stage_ids   : List[str] = []
        workflow_id : Optional[str] = dto.workflow_id

        if dto.workflow:
            for stage_inline in dto.workflow.stages:
                transformation_id: Optional[str] = stage_inline.transformation_id

                if stage_inline.transformation:
                    t = stage_inline.transformation
                    bb_id: Optional[str] = t.building_block_id

                    if t.building_block:
                        bb = t.building_block
                        bb_model = M.BuildingBlock(
                            building_block_id = f"bb_{uuid.uuid4().hex[:12]}",
                            name              = bb.name,
                            command           = bb.command,
                            image             = bb.image,
                            description       = bb.description or "",
                        )
                        res = await self.bb_repo.insert(bb_model)
                        if res.is_err:
                            return Err(res.unwrap_err())
                        bb_id = bb_model.building_block_id
                        bb_ids.append(bb_id)

                    pat_model = M.PatternX(
                        pattern_id        = f"pat_{uuid.uuid4().hex[:12]}",
                        name              = t.name,
                        task              = t.task,
                        pattern           = t.pattern,
                        description       = t.description or "",
                        workers           = t.workers,
                        loadbalancer      = t.loadbalancer,
                        building_block_id = bb_id,
                    )
                    res = await self.pattern_repo.insert(pat_model)
                    if res.is_err:
                        return Err(res.unwrap_err())
                    transformation_id = pat_model.pattern_id
                    pattern_ids.append(transformation_id)

                stg_model = M.StageX(
                    stage_id          = f"stg_{uuid.uuid4().hex[:12]}",
                    name              = stage_inline.name,
                    source            = stage_inline.source,
                    sink              = stage_inline.sink,
                    endpoint          = stage_inline.endpoint,
                    transformation_id = transformation_id,
                )
                res = await self.stage_repo.insert(stg_model)
                if res.is_err:
                    return Err(res.unwrap_err())
                stage_ids.append(stg_model.stage_id)

            wf_model = M.WorkflowX(
                workflow_id = f"wf_{uuid.uuid4().hex[:12]}",
                name        = dto.workflow.name,
                stage_ids   = stage_ids,
            )
            res = await self.workflow_repo.insert(wf_model)
            if res.is_err:
                return Err(res.unwrap_err())
            workflow_id = wf_model.workflow_id

        svc_model = M.ServiceX(
            service_id  = f"svc_{uuid.uuid4().hex[:12]}",
            name        = dto.name,
            description = dto.description or "",
            owner_id    = dto.owner_id,
            public      = dto.public,
            workflow_id = workflow_id,
            provider    = dto.provider
        )
        res = await self.repo.insert(svc_model)
        if res.is_err:
            return Err(res.unwrap_err())

        return Ok(DTO.ServiceIndexResponseDTO(
            service_id        = svc_model.service_id,
            workflow_id       = workflow_id,
            stage_ids         = stage_ids,
            pattern_ids       = pattern_ids,
            building_block_ids= bb_ids,
        ))


class DataIngestionService:
    def __init__(self, source_repo: R.DataSourceRepository, record_repo: R.DataRecordsRepository, link_manager: GraphLinkManager):
        self.source_repo = source_repo
        self.record_repo = record_repo
        self.link_manager = link_manager

    async def register_data_source(
        self, 
        name: str, 
        description: str,
        source_id: Optional[str] = None,
        bucket_id: str = "",
        ball_id: str = ""
    ) -> Result[M.DataSource, EX.JubError]:
        """
        Registers the CSV file metadata in the database.
        """
        # exists = self.source_repo.get_by_id()
        
        source_id   = source_id or f"src_{uuid.uuid4().hex[:12]}"

        exists = await self.source_repo.get_by_id(source_id)
        if exists.is_ok:
            return Err(EX.Conflict(f"Data source with ID '{source_id}' already exists."))

        new_source = M.DataSource(
            source_id   = source_id,
            name        = name,
            description = description,
            format      = ENUMS.DataSourceFormatEnum.CSV,
            bucket_id   = bucket_id,
            ball_id     = ball_id
        )
        
        insert_result = await self.source_repo.insert(new_source)
        if insert_result.is_err:
            return insert_result
            
        return Ok(new_source)

    async def ingest_parsed_records(self, source_id: str, records: List[M.DataRecord]) -> Result[int, EX.JubError]:
        """
        Takes a list of normalized DataRecord objects and saves them to the database.
        It processes them in chunks to prevent memory overload.
        """
        # First, ensure the data source actually exists
        source_check = await self.source_repo.get_by_id(source_id)
        if source_check.is_err:
            return Err(EX.NotFound(f"Data source {source_id} does not exist."))

        # Define a chunk size (e.g., insert 5,000 rows at a time)
        CHUNK_SIZE = 5000
        total_inserted = 0
        
        for i in range(0, len(records), CHUNK_SIZE):
            chunk = records[i:i + CHUNK_SIZE]
            result = await self.record_repo.insert_many(chunk)
            
            if result.is_err:
                # If a chunk fails, you might want to rollback or return the error
                return result 
                
            total_inserted += result.unwrap()
            
        return Ok(total_inserted)

    async def update_data_source(self, source_id: str, data: dict) -> Result[M.DataSource, EX.JubError]:
        exists = await self.source_repo.get_by_id(source_id)
        if exists.is_err:
            return Err(EX.NotFound(f"Data source '{source_id}' not found."))
        return await self.source_repo.update(source_id, data)

    async def delete_data_source(self, source_id: str) -> Result[bool, EX.JubError]:
        """
        Removes the data source, all its records, and cleans up observatory links.
        """
        # Guard: ensure the source exists before attempting deletion
        exists = await self.source_repo.get_by_id(source_id)
        if exists.is_err:
            return Err(EX.NotFound(f"Data source '{source_id}' not found."))

        # 1. Delete all associated records first
        delete_records_result = await self.record_repo.delete_by_source(source_id)
        if delete_records_result.is_err:
            return delete_records_result

        # 2. Delete the source metadata
        delete_source_result = await self.source_repo.delete(source_id)

        # 3. Clean up observatory links
        await self.link_manager.observatory_datasource_link_repository.collection.delete_many({"source_id": source_id})
        return delete_source_result
    


class DataQueryService:
    def __init__(
        self,
        record_repo: R.DataRecordsRepository,
        catalog_item_repo: R.CatalogItemsRepository,
        catalog_alias_repo: R.CatalogItemAliasesRepository,
        catalog_item_alias_link_repo: R.CatalogItemToCatalogAliasLinkRepository,
    ):
        self.record_repo               = record_repo
        self.catalog_item_repo         = catalog_item_repo
        self.catalog_alias_repo        = catalog_alias_repo
        self.catalog_item_alias_link_repo = catalog_item_alias_link_repo

    async def _resolve_identifier(self, raw_value: str) -> str:
        """
        Given any string (catalog_item_id, value, or numeric code), returns the
        canonical catalog_item_id.  Falls back to raw_value if nothing matches.
        """
        try:
            or_conds: List[dict] = [
                {"catalog_item_id": raw_value},
                {"value": raw_value.upper()},
            ]
            if raw_value.lstrip("-").isdigit():
                or_conds.append({"code": int(raw_value)})

            doc = await self.catalog_item_repo.collection.find_one({"$or": or_conds})
            if doc:
                L.debug(f"resolve_identifier: '{raw_value}' → '{doc['catalog_item_id']}' (catalog_item)")
                return doc["catalog_item_id"]

            alias_or: List[dict] = [
                {"value": raw_value},
                {"catalog_item_alias_id": raw_value},
            ]
            if raw_value.lstrip("-").isdigit():
                alias_or.append({"code": int(raw_value)})

            alias_doc = await self.catalog_alias_repo.collection.find_one({"$or": alias_or})
            if alias_doc:
                link = await self.catalog_item_alias_link_repo.collection.find_one(
                    {"catalog_item_alias_id": alias_doc["catalog_item_alias_id"]}
                )
                if link:
                    L.debug(f"resolve_identifier: '{raw_value}' → '{link['catalog_item_id']}' (alias)")
                    return link["catalog_item_id"]

            L.debug(f"resolve_identifier: '{raw_value}' not found — used as-is")
            return raw_value
        except Exception as e:
            L.error(f"resolve_identifier error for '{raw_value}': {e}")
            return raw_value

    async def _build_spatial_filter(self, group: ConditionGroup) -> dict:
        if group.logic == "AND":
            raise ValueError("AND is not allowed in VS() — a record has one location.")

        async def _resolve_raw(cond: Condition) -> str:
            raw = cond.item_path[0] if isinstance(cond.item_path, list) and cond.item_path else cond.catalog_value
            return await self._resolve_identifier(raw)

        if group.logic == "SINGLE":
            cond = group.conditions[0]
            if cond.operator == "WILDCARD":
                if not cond.item_path:
                    return {}
                # Wildcard: fetch parent + all children by regex (no graph lookup available here)
                parent_id = await self._resolve_identifier(cond.item_path[0])
                return {"spatial_id": {"$regex": f"^{parent_id}$|^{parent_id}_"}}
            return {"spatial_id": await _resolve_raw(cond)}

        # OR
        all_ids: List[str] = []
        for cond in group.conditions:
            if cond.operator == "WILDCARD":
                if not cond.item_path:
                    return {}
                parent_id = await self._resolve_identifier(cond.item_path[0])
                # Collect via regex inline — expand later if graph lookup is needed
                all_ids.append(parent_id)
            else:
                all_ids.append(await _resolve_raw(cond))

        unique = list(dict.fromkeys(all_ids))
        return {"spatial_id": unique[0]} if len(unique) == 1 else {"spatial_id": {"$in": unique}}

    async def _build_interest_filter(self, group: ConditionGroup) -> dict:
        # VI(*) — global wildcard, no filter
        if group.logic == "SINGLE" and group.conditions[0].operator == "WILDCARD" and group.conditions[0].catalog_value == "*":
            return {}

        resolved: List[str] = []
        for cond in group.conditions:
            raw = ASTToMongoTranslator._format_id(cond.catalog_value, cond.item_path)
            resolved.append(await self._resolve_identifier(raw))

        if group.logic == "SINGLE":
            return {"interest_ids": resolved[0]}
        elif group.logic == "AND":
            return {"interest_ids": {"$all": resolved}}
        elif group.logic == "OR":
            return {"interest_ids": {"$in": resolved}}
        return {}

    async def query(self, source_id: str, raw_dsl_string: str) -> Result[List[M.DataRecord], EX.JubError]:
        try:
            ast = QueryAST.parse(raw_dsl_string)

            match_filter: Dict[str, Any] = {"source_id": source_id}

            for q in ast.queries:
                prefix = q.catalog_prefix
                group  = q.group
                if prefix == SPATIAL_VARIABLE:
                    match_filter.update(await self._build_spatial_filter(group))
                elif prefix == TEMPORAL_VARIABLE:
                    match_filter.update(ASTToMongoTranslator._build_temporal(group))
                elif prefix == INTEREST_VARIABLE:
                    match_filter.update(await self._build_interest_filter(group))

            L.debug({
                "event": "DSL_TRANSLATION_SUCCESS",
                "message": "Successfully translated AST to MongoDB query",
                "match_filter": Utils.from_string_any_to_string_to_string_dict(match_filter)
            })

            result = await self.record_repo.find(query=match_filter, limit=10000)
            return result

        except ValueError as ve:
            L.warning(f"DSL Parsing/Translation Error: {ve}")
            return Err(EX.ValidationError(f"Invalid query syntax or logic: {str(ve)}"))
        except Exception as e:
            L.error(f"Unexpected error executing query '{raw_dsl_string}': {e}")
            return Err(EX.UnknownError(f"Failed to execute query: {str(e)}"))

