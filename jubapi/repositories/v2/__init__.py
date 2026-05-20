
import os
from jubapi.repositories.v2.base import BaseRepository
from motor.motor_asyncio import AsyncIOMotorCollection as Collection
import datetime as DT
import jubapi.models.v2 as M
from typing import List, Dict, Optional, Set, Tuple
from option import Result,Err,Ok
import jubapi.errors as EX
from jubapi.log.log import Log
import jubapi.enums.v2 as ENUMS
import jubapi.dto.v2 as DTO

L = Log(
    name = __name__,
    path = os.environ.get("JUB_LOG_PATH", "/log")
)


class ObservatoriesRepository(BaseRepository[M.ObservatoryX]):
    def __init__(self, collection: Collection):
        super().__init__(collection, M.ObservatoryX, "observatory_id")

    async def increment_views(self, observatory_id: str) -> Result[int, EX.JubError]:
        try:
            result = await self.collection.find_one_and_update(
                {"observatory_id": observatory_id},
                {"$inc": {"view_count": 1}},
                return_document=True,
            )
            if result is None:
                return Err(EX.NotFound(f"Observatory '{observatory_id}' not found."))
            return Ok(result.get("view_count", 0))
        except Exception as e:
            return Err(EX.UnknownError(str(e)))

class ProductsRepository(BaseRepository[M.ProductX]):
    def __init__(self, collection: Collection):
        super().__init__(collection, M.ProductX, "product_id")

class CatalogsRepository(BaseRepository[M.CatalogX]):
    def __init__(self, collection: Collection):
        super().__init__(collection, M.CatalogX, "catalog_id")
    async def get_catalog_by_catalog_type(self, catalog_type:str)->Result[List[M.CatalogX],EX.JubError]:
        try: 
            result = self.collection.find({"catalog_type": catalog_type})
            items = await result.to_list(length=None)
            models:List[M.CatalogX] = []
            for item in items:
                m = M.CatalogX(
                    catalog_id        = item.get('catalog_id',""),
                    catalog_type      = item.get('catalog_type',""),
                    description       = item.get('description',""),
                    level             = item.get('level',0),
                    name              = item.get('name',""),
                    value             = item.get('value',""),
                    parent_catalog_id = item.get('parent_catalog_id',None),
                    root_group_id     = item.get('root_group_id',None),
                    created_at        = item.get('created_at',DT.datetime.now(DT.timezone.utc) ),
                    updated_at        = item.get('updated_at',DT.datetime.now(DT.timezone.utc) )
                )
                models.append(m)
                
                # log.info(f"Found Catalog: {item}")
            return Ok(models)
        except Exception as e:
            return Err(EX.JubError.from_exception(e))


class CatalogItemsRepository(BaseRepository[M.CatalogItemX]):
    def __init__(self, collection: Collection):
        super().__init__(collection, M.CatalogItemX, "catalog_item_id")

    async def find_by_value(self, search_value: str) -> List[M.CatalogItemX]:
            """
            Finds catalog items that exactly match the given string value.
            """
            try:
                # Query the collection for an exact match on the 'value' field
                cursor = self.collection.find({"value": search_value})
                
                # Fetch all matching documents and map them to models
                docs = await cursor.to_list(length=None)
                return [M.CatalogItemX.from_doc(doc) for doc in docs]
                
            except Exception as e:
                L.error(f"Error querying catalog items by value '{search_value}': {e}")
                return []

    async def find_by_temporal_operator(self, mongo_op: str, target_date: str) -> List[M.CatalogItemX]:
            """
            Finds catalog items based on a temporal operator and date.
            """
            try:
                # 1. Convert the standardized ISO string to a Python datetime object
                # MongoDB requires native datetime objects to run operators like $gte and $lte correctly.
                if isinstance(target_date, str):
                    dt_val = DT.datetime.fromisoformat(target_date.replace("Z", "+00:00"))
                else:
                    dt_val = target_date
                # 2. Query the collection
                cursor = self.collection.find({
                    "value_type": "DATETIME", # Ensure this matches your Enum if you use one (e.g., M.CatalogItemValueType.DATETIME)
                    "temporal_value": {mongo_op: dt_val}
                })
                
                # 3. Fetch and map to models
                docs = await cursor.to_list(length=None)
                return [M.CatalogItemX.from_doc(doc) for doc in docs]
                
            except Exception as e:
                # Depending on how your repo handles errors, you might want to log this or raise a custom error.
                L.error(f"Error querying temporal operator {mongo_op} with date {target_date}: {e}")
                return []

class CatalogItemAliasesRepository(BaseRepository[M.CatalogItemAlias]):
    def __init__(self, collection: Collection):
        super().__init__(collection, M.CatalogItemAlias, "catalog_item_alias_id")

    async def find_by_value(self, value: str) -> List[M.CatalogItemAlias]:
        cursor = self.collection.find({"value": value})
        docs = await cursor.to_list(length=None)
        return [M.CatalogItemAlias.from_doc(doc) for doc in docs]


# 1. Observatory <-> Product
class ReviewRepository(BaseRepository[M.Review]):
    def __init__(self, collection: Collection):
        super().__init__(collection, M.Review, "review_id")

    async def get_by_observatory(self, observatory_id: str) -> Result[List[M.Review], EX.JubError]:
        return await self.find({"observatory_id": observatory_id})

    async def get_by_user_and_observatory(self, user_id: str, observatory_id: str) -> Result[Optional[M.Review], EX.JubError]:
        try:
            doc = await self.collection.find_one({"user_id": user_id, "observatory_id": observatory_id})
            return Ok(M.Review.model_validate(doc) if doc else None)
        except Exception as e:
            return Err(EX.UnknownError(str(e)))

    async def get_rating_stats(self, observatory_id: str) -> Tuple[float, int]:
        import time as _time
        t0 = _time.monotonic()
        pipeline = [
            {"$match": {"observatory_id": observatory_id}},
            {"$group": {"_id": None, "avg": {"$avg": "$rating"}, "count": {"$sum": 1}}},
        ]
        try:
            cursor = self.collection.aggregate(pipeline)
            docs = await cursor.to_list(length=1)
            if not docs:
                return (0.0, 0)
            doc = docs[0]
            result = (round(float(doc["avg"]), 2), int(doc["count"]))
            L.info({"action": "repository.review.get_rating_stats", "duration_ms": int((_time.monotonic()-t0)*1000), "input": {"observatory_id": observatory_id}, "result": {"avg_rating": result[0], "review_count": result[1]}})
            return result
        except Exception as e:
            L.error({"action": "repository.review.get_rating_stats", "error": str(e), "input": {"observatory_id": observatory_id}})
            return (0.0, 0)


class ObservatoryToProductLinkRepository(BaseRepository[M.ObservatoryToProductLink]):
    def __init__(self, collection: Collection):
        super().__init__(collection, M.ObservatoryToProductLink, "observatory_id")

    async def get_observatory_ids_by_product_id(self, product_id: str) -> List[str]:
        cursor = self.collection.find({"product_id": product_id}, {"observatory_id": 1, "_id": 0})
        docs = await cursor.to_list(length=None)
        return [d["observatory_id"] for d in docs if d.get("observatory_id")]

    async def get_all_observatory_ids_with_products(self) -> Set[str]:
        cursor = self.collection.find({}, {"observatory_id": 1, "_id": 0})
        docs = await cursor.to_list(length=None)
        return {d["observatory_id"] for d in docs if d.get("observatory_id")}

# 2. Observatory <-> Catalog
class ObservatoryToCatalogLinkRepository(BaseRepository[M.ObservatoryToCatalogLink]):
    def __init__(self, collection: Collection):
        super().__init__(collection, M.ObservatoryToCatalogLink, "observatory_id")
    async def get_by_catalog_id(self, catalog_id:str)->Result[List[M.ObservatoryToCatalogLink],EX.JubError]:
        try: 
            result = self.collection.find({"catalog_id": catalog_id})
            items = await result.to_list(length=None)
            models:List[M.ObservatoryToCatalogLink] = []
            for item in items:
                m = M.ObservatoryToCatalogLink(
                    observatory_id = item.get('observatory_id',""),
                    catalog_id     = item.get('catalog_id',"")
                )
                models.append(m)
                
                # log.info(f"Found ObservatoryToCatalogLink: {item}")
            return Ok(models)
        except Exception as e:
            return Err(EX.JubError.from_exception(e))

# 3. Observatory -> Service
class ObservatoryToServiceLinkRepository(BaseRepository[M.ObservatoryToServiceLink]):
    def __init__(self, collection: Collection):
        super().__init__(collection, M.ObservatoryToServiceLink, "observatory_id")

    async def get_service_ids_by_observatory_id(self, observatory_id: str) -> List[str]:
        cursor = self.collection.find({"observatory_id": observatory_id}, {"service_id": 1, "_id": 0})
        docs = await cursor.to_list(length=None)
        return [d["service_id"] for d in docs if d.get("service_id")]

# 4. Observatory -> DataSource
class ObservatoryToDataSourceLinkRepository(BaseRepository[M.ObservatoryToDataSourceLink]):
    def __init__(self, collection: Collection):
        super().__init__(collection, M.ObservatoryToDataSourceLink, "observatory_id")

    async def get_source_ids_by_observatory_id(self, observatory_id: str) -> List[str]:
        cursor = self.collection.find({"observatory_id": observatory_id}, {"source_id": 1, "_id": 0})
        docs = await cursor.to_list(length=None)
        return [d["source_id"] for d in docs if d.get("source_id")]

# 5. Catalog -> Catalog Item
class CatalogToCatalogItemLinkRepository(BaseRepository[M.CatalogToCatalogItemLink]):
    def __init__(self, collection: Collection):
        super().__init__(collection, M.CatalogToCatalogItemLink, "catalog_id")
    async def get_by_catalog_item_id(self, catalog_item_id:str)->Result[List[M.CatalogToCatalogItemLink],EX.JubError]:
        try: 
            result = self.collection.find({"catalog_item_id": catalog_item_id})
            items = await result.to_list(length=None)
            models:List[M.CatalogToCatalogItemLink] = []
            for item in items:
                m = M.CatalogToCatalogItemLink(
                    catalog_id      = item.get('catalog_id',""),
                    catalog_item_id = item.get('catalog_item_id',"")
                )
                models.append(m)
                
                # log.info(f"Found CatalogToCatalogItemLink: {item}")
            return Ok(models)
        except Exception as e:
            return Err(EX.JubError.from_exception(e))
    async def get_catalog_id_by_catalog_item_id(self, catalog_item_id:str)->Result[str,EX.JubError]:
        try: 
            result = await self.collection.find_one({"catalog_item_id": catalog_item_id})
            if not result:
                return Err(EX.NotFound(f"No catalog link found for item {catalog_item_id}"))
            catalog_id = result.get('catalog_id',"")
            return Ok(catalog_id)
        except Exception as e:
            return Err(EX.JubError.from_exception(e))

# 4. Product -> Catalog Item 
class ProductToCatalogItemLinkRepository(BaseRepository[M.CatalogItemToProductLink]):
    def __init__(self, collection: Collection):
        super().__init__(collection, M.CatalogItemToProductLink, "product_id")
    
    async def get_product_ids_by_catalog_item_id(self, catalog_item_id: str) -> List[str]:
        cursor = self.collection.find({"catalog_item_id": catalog_item_id}, {"product_id": 1, "_id": 0})
        docs = await cursor.to_list(length=None)
        return [d["product_id"] for d in docs if d.get("product_id")]

    async def get_by_product_id(self,product_id:str)->Result[List[M.CatalogItemToProductLink],EX.JubError]:
        try: 
            result = self.collection.find({"product_id": product_id})
            items = await result.to_list(length=None)
            models:List[M.CatalogItemToProductLink] = []
            for item in items:
                m = M.CatalogItemToProductLink(
                    product_id      = item.get('product_id',""),
                    catalog_item_id = item.get('catalog_item_id',"")
                )
                models.append(m)
                
                # log.info(f"Found CatalogItemToProductLink: {item}")
            return Ok(models)
        except Exception as e:
            return Err(EX.JubError.from_exception(e))





# 5. Product -> Product (Related Products)
class ProductToProductLinkRepository(BaseRepository[M.ProductToProductLink]):
    def __init__(self, collection: Collection):
        super().__init__(collection, M.ProductToProductLink, "source_product_id")

    async def get_related_ids(self, product_id: str) -> List[str]:
        """Returns all product IDs related to product_id, regardless of link direction."""
        cursor = self.collection.find(
            {"$or": [{"source_product_id": product_id}, {"target_product_id": product_id}]}
        )
        docs = await cursor.to_list(length=None)
        related = []
        for doc in docs:
            if doc.get("source_product_id") == product_id:
                related.append(doc["target_product_id"])
            else:
                related.append(doc["source_product_id"])
        return related

    async def delete_all_for_product(self, product_id: str) -> int:
        """Removes all links involving product_id. Returns the count deleted."""
        r = await self.collection.delete_many(
            {"$or": [{"source_product_id": product_id}, {"target_product_id": product_id}]}
        )
        return r.deleted_count


# Catalog Item Value -> Catalog Item (The Alias Engine)
class CatalogItemToCatalogAliasLinkRepository(BaseRepository[M.CatalogItemToCatalogAliasLink]):
    def __init__(self, collection: Collection):
        super().__init__(collection, M.CatalogItemToCatalogAliasLink, "catalog_item_id")

    async def get_catalog_item_id_by_alias_id(self, alias_id: str) -> Optional[str]:
        doc = await self.collection.find_one({"catalog_item_alias_id": alias_id})
        return doc.get("catalog_item_id") if doc else None

# 5. Catalog Item -> Catalog Item (The Hierarchy Engine)
class CatalogItemRelationshipRepository(BaseRepository[M.CatalogItemRelationship]):
    def __init__(self, collection: Collection):
        super().__init__(collection, M.CatalogItemRelationship, "parent_id")

    
    async def get_all_children_nodes(self, root_parent_id: str,length:int = None) -> Result[List[str], EX.JubError]:
        """
        This hides the MongoDB $graphLookup logic.
        It resolves wildcards like MX.* by fetching the ID of every child node.
        """
        try: 
            pipeline = [
                {"$match": {"parent_id": root_parent_id}},
                {"$graphLookup": {
                    "from": self.collection.name,
                    "startWith": "$child_id",
                    "connectFromField": "child_id",
                    "connectToField": "parent_id",
                    "as": "descendants"
                }}
            ]
            
            cursor = self.collection.aggregate(pipeline) 
            results = await cursor.to_list(length=length)
            
            # Parse the results to return a simple list of child IDs
            descendant_ids = set()
            for doc in results:
                descendant_ids.add(doc["child_id"])
                for desc in doc.get("descendants", []):
                    descendant_ids.add(desc["child_id"])
                    
            return Ok(list(descendant_ids))
        except Exception as e:
            L.error(f"Error in get_all_children_nodes: {e}")
            return Err(EX.JubError.from_exception(e))


class UserProfileXRepository(BaseRepository[M.UserProfileX]):
    def __init__(self, collection: Collection):
        super().__init__(collection, M.UserProfileX, "user_id")
    
    async def update_settings(self, user_id: str, new_settings: M.UserPreferences) -> Result[M.UserProfileX, EX.JubError]:
        try:
            update_result = await self.collection.update_one(
                {"user_id": user_id},
                {"$set": {"settings": new_settings.model_dump(), "updated_at": DT.datetime.now(DT.timezone.utc)}}
            )
            if update_result.modified_count == 0:
                return Err(EX.NotFound(f"User with ID '{user_id}' not found or settings are the same"))
            
            # Fetch the updated document
            updated_doc = await self.collection.find_one({"user_id": user_id})
            if not updated_doc:
                return Err(EX.NotFound(f"User with ID '{user_id}' not found after update"))
            
            return Ok(M.UserProfileX.from_doc(updated_doc))
        except Exception as e:
            L.error(f"Error updating user preferences for {user_id}: {e}")
            return Err(EX.JubError.from_exception(e))
    async def get_by_username(self, username:str)->Result[M.UserProfileX,EX.JubError]:
        try:
            doc = await self.collection.find_one({"username": username})
            if not doc:
                return Err(EX.NotFound(f"User with username '{username}' not found"))
            return Ok(M.UserProfileX.from_doc(doc))
        except Exception as e:
            return Err(EX.JubError.from_exception(e))
        


class NotificationsRepository(BaseRepository[M.Notification]):
    def __init__(self, collection):
        """
        Initializes the repository with the specific Notification model and ID field.
        """
        super().__init__(collection, M.Notification, "notification_id")

    async def check_ownership(self, notification_id: str, user_id: str) -> Result[bool, EX.JubError]:
        """
        Checks if a notification belongs to the user (for authorization).
        """
        try:
            doc = await self.collection.find_one({"notification_id": notification_id})
            if not doc:
                return Err(EX.NotFound(f"Notification with ID '{notification_id}' not found"))
            if '_id' in doc:
                del doc['_id']  # Remove MongoDB's internal ID if present

            model = M.Notification.model_validate(doc)

            is_owner = model.user_id == user_id
            return Ok(is_owner)
        except Exception as e:
            L.error(f"Error checking ownership for notification {notification_id} and user {user_id}: {e}")
            return Err(EX.JubError.from_exception(e))
    async def get_by_id_and_user(self, notification_id: str, user_id: str) -> Result[M.Notification, EX.JubError]:
        """
        Fetches a notification by its ID and checks if it belongs to the user.
        This can be used for authorization before allowing updates or deletions.
        """
        try:
            doc = await self.collection.find_one({"notification_id": notification_id, "user_id": user_id})
            if not doc:
                return Err(EX.NotFound(f"Notification with ID '{notification_id}' not found for user '{user_id}'"))
            if '_id' in doc:
                del doc['_id']  # Remove MongoDB's internal ID if present

            model = M.Notification.model_validate(doc)
            return Ok(model)
        except Exception as e:
            L.error(f"Error fetching notification {notification_id} for user {user_id}: {e}")
            return Err(EX.JubError.from_exception(e))
    async def get_unread_by_user(self, user_id: str, limit: int = 50) -> Result[List[M.Notification], EX.JubError]:
        """
        Fetches all unread notifications for a specific user, sorted by newest first.
        """
        try:
            # We don't use self.find_many here because we want to sort them by date descending
            cursor = self.collection.find(
                {"user_id": user_id, "is_read": False}
            ).sort("created_at", -1).limit(limit)
            
            notifications = []
            for doc in await cursor.to_list(length=limit):
                if '_id' in doc:
                    del doc['_id']  # Remove MongoDB's internal ID if present
                notifications.append(self.model_class.model_validate(doc))
            # [self.model_class.model_validate(doc) async for doc in cursor]
            return Ok(notifications)
        except Exception as e:
            L.error(f"Error fetching unread notifications for user {user_id}: {e}")
            return Err(EX.UnknownError(str(e)))

    async def get_all_by_user(self, user_id: str, limit: int = 50) -> Result[List[M.Notification], EX.JubError]:
        """
        Fetches all notifications (read and unread) for a specific user, sorted by newest first.
        """
        try:
            sort_criteria = [
                ("is_read", 1),  # Unread first
                ("created_at", -1)  # Newest first
            ]
            cursor = self.collection.find(
                {"user_id": user_id}
            ).sort(sort_criteria).limit(limit)
            
            notifications = []
            for doc in await cursor.to_list(length=limit):
                if '_id' in doc:
                    del doc['_id']  # Remove MongoDB's internal ID if present
                notifications.append(self.model_class.model_validate(doc))

            # notifications = [self.model_class.model_validate(doc) async for doc in cursor]
            return Ok(notifications)
        except Exception as e:
            L.error(f"Error fetching all notifications for user {user_id}: {e}")
            return Err(EX.UnknownError(str(e)))

    async def mark_as_read(self, notification_id: str) -> Result[M.Notification, EX.JubError]:
        """
        Marks a single notification as read using the base class update method.
        """
        return await self.update(notification_id, {"is_read": True})

    async def mark_all_as_read(self, user_id: str) -> Result[int, EX.JubError]:
        """
        Marks all unread notifications for a user as read.
        Returns the number of notifications modified.
        """
        try:
            result = await self.collection.update_many(
                {"user_id": user_id, "is_read": False},
                {"$set": {"is_read": True}}
            )
            return Ok(result.modified_count)
        except Exception as e:
            L.error(f"Error marking all notifications as read for user {user_id}: {e}")
            return Err(EX.UnknownError(str(e)))

    async def delete_read_by_user(self, user_id: str) -> Result[int, EX.JubError]:
        """
        Cleans up the database by deleting all notifications that the user has already read.
        Returns the number of deleted notifications.
        """
        try:
            result = await self.collection.delete_many(
                {"user_id": user_id, "is_read": True}
            )
            return Ok(result.deleted_count)
        except Exception as e:
            L.error(f"Error deleting read notifications for user {user_id}: {e}")
            return Err(EX.UnknownError(str(e)))



class TaskRepository(BaseRepository[M.TaskX]):
    def __init__(self, collection:Collection):
        """
        Initializes the repository with the TaskX model and its specific ID field.
        """
        super().__init__(collection, M.TaskX, "task_id")

    async def get_tasks_by_user(self, user_id: str, skip: int = 0, limit: int = 50) -> Result[List[M.TaskX], EX.JubError]:
        """
        Fetches the recent tasks for a user, sorted by newest first.
        This feeds the main list in your UI.
        """
        try:
            cursor = self.collection.find({"user_id": user_id}).sort("updated_at", -1).skip(skip).limit(limit)
            tasks = []
            for doc in await cursor.to_list(length=limit):
                if '_id' in doc:
                    del doc['_id']  # Remove MongoDB's internal ID if present
                tasks.append(self.model_class.model_validate(doc))
            return Ok(tasks)
        except Exception as e:
            L.error(f"Error fetching tasks for user {user_id}: {e}")
            return Err(EX.UnknownError(str(e)))

    async def get_task_statistics(self, user_id: str) -> Result[DTO.TasksStatsDTO, EX.JubError]:
        """
        Aggregates task counts by their current status.
        This provides the exact numbers needed for the top UI cards (In Progress, Completed, Failed).
        """
        try:
            pipeline = [
                {"$match": {"user_id": user_id}},
                {"$group": {"_id": "$current_status", "count": {"$sum": 1}}}
            ]
            
            cursor = self.collection.aggregate(pipeline)
            
            # Initialize with 0 so the frontend always has the keys even if empty
            stats = {
                ENUMS.TaskStatusEnum.PENDING.value: 0,
                ENUMS.TaskStatusEnum.RUNNING.value: 0,
                ENUMS.TaskStatusEnum.SUCCESS.value: 0,
                ENUMS.TaskStatusEnum.FAILED.value: 0,
            }
            
            async for doc in cursor:
                status_key = doc["_id"]
                if status_key in stats:
                    stats[status_key] = doc["count"]
                    
            return Ok(DTO.TasksStatsDTO.model_validate(stats))
        except Exception as e:
            L.error(f"Error aggregating task stats for user {user_id}: {e}")
            return Err(EX.UnknownError(str(e)))

    async def update_progress(
        self, 
        task_id: str, 
        percentage: int, 
        message: str, 
        status: ENUMS.TaskStatusEnum = None
    ) -> Result[bool, EX.JubError]:
        """
        Updates the live progress of a task. 
        Designed to be called frequently by background workers generating the products.
        """
        try:
            update_data = {
                "progress_percentage": percentage,
                "progress_message": message,
                "updated_at": DT.datetime.now(DT.timezone.utc)
            }
            if status:
                update_data["current_status"] = status

            result = await self.collection.update_one(
                {"task_id": task_id},
                {"$set": update_data}
            )
            
            if result.matched_count == 0:
                return Err(EX.NotFound(f"Task {task_id} not found."))
                
            return Ok(True)
        except Exception as e:
            L.error(f"Error updating progress for task {task_id}: {e}")
            return Err(EX.UnknownError(str(e)))

    async def add_retry_attempt(self, task_id: str, new_attempt: M.TaskAttempt) -> Result[bool, EX.JubError]:
        """
        Pushes a new attempt into the task history and resets the progress indicators.
        Called when a user clicks 'Reintentar' on the UI.
        """
        try:
            # We use $push to atomically add the attempt to the array without overwriting history
            result = await self.collection.update_one(
                {"task_id": task_id},
                {
                    "$push": {"attempts": new_attempt.model_dump(mode="json")},
                    "$set": {
                        "current_status": ENUMS.TaskStatusEnum.PENDING,
                        "progress_percentage": 0,
                        "progress_message": "En cola para reintento...",
                        "updated_at": DT.datetime.now(DT.timezone.utc)
                    }
                }
            )
            
            if result.matched_count == 0:
                return Err(EX.NotFound(f"Task {task_id} not found."))
                
            return Ok(True)
        except Exception as e:
            L.error(f"Error adding retry attempt for task {task_id}: {e}")
            return Err(EX.UnknownError(str(e)))

class DataSourceRepository(BaseRepository[M.DataSource]):
    def __init__(self, collection:Collection):
        super().__init__(collection, M.DataSource, "source_id")

class DataRecordsRepository(BaseRepository[M.DataRecord]):
    def __init__(self, collection:Collection):
        super().__init__(collection, M.DataRecord, "record_id")

    async def find_by_query(self, source_id: str, query: dict, limit: int = 100) -> Result[List[M.DataRecord], EX.JubError]:
        """
        Finds records based on a MongoDB query dict, filtered by source_id.
        This is the main method used by your Jub DSL engine to fetch data subsets.
        """
        try:
            # Ensure the query is always scoped to the specific data source
            full_query = {"source_id": source_id, **query}
            result = await self.find(query=full_query,limit=limit)
            if result.is_err:
                return Err(result.unwrap_err())
            records = result.unwrap()
            return Ok(records)
        except Exception as e:
            L.error(f"Error executing find with query {query} for source {source_id}: {e}")
            return Err(EX.UnknownError(str(e)))
    async def insert_many(self, records: List[M.DataRecord]) -> Result[int, EX.JubError]:
        """
        Inserts a large batch of records at once. 
        Crucial for CSV ingestion to avoid database timeouts.
        """
        if not records:
            return Ok(0)
            
        try:
            # Convert all Pydantic models to dictionaries
            documents = [record.model_dump() for record in records]
            result = await self.collection.insert_many(documents)
            return Ok(len(result.inserted_ids))
        except Exception as e:
            L.error(f"Error bulk inserting records: {e}")
            return Err(EX.UnknownError(str(e)))

    async def delete_by_source(self, source_id: str) -> Result[int, EX.JubError]:
        """
        Deletes all records associated with a specific data source.
        Useful if a user deletes a CSV or needs to re-upload it.
        """
        try:
            result = await self.collection.delete_many({"source_id": source_id})
            return Ok(result.deleted_count)
        except Exception as e:
            L.error(f"Error deleting records for source {source_id}: {e}")
            return Err(EX.UnknownError(str(e)))


# ── Service / Workflow domain ──────────────────────────────────────────────────

class BuildingBlockRepository(BaseRepository[M.BuildingBlock]):
    def __init__(self, collection: Collection):
        super().__init__(collection, M.BuildingBlock, "building_block_id")


class PatternRepository(BaseRepository[M.PatternX]):
    def __init__(self, collection: Collection):
        super().__init__(collection, M.PatternX, "pattern_id")


class StageRepository(BaseRepository[M.StageX]):
    def __init__(self, collection: Collection):
        super().__init__(collection, M.StageX, "stage_id")

    async def delete_many_by_ids(self, stage_ids: List[str]) -> Result[int, EX.JubError]:
        try:
            result = await self.collection.delete_many({"stage_id": {"$in": stage_ids}})
            return Ok(result.deleted_count)
        except Exception as e:
            return Err(EX.UnknownError(str(e)))


class WorkflowRepository(BaseRepository[M.WorkflowX]):
    def __init__(self, collection: Collection):
        super().__init__(collection, M.WorkflowX, "workflow_id")


class ServiceRepository(BaseRepository[M.ServiceX]):
    def __init__(self, collection: Collection):
        super().__init__(collection, M.ServiceX, "service_id")

    async def search(self, mongo_filter: dict, skip: int = 0, limit: int = 100) -> Result[List[M.ServiceX], EX.JubError]:
        try:
            # x = await self.find(query=mongo_filter, skip=skip, limit=limit)
            # print(x)
            cursor = self.collection.find(mongo_filter,skip = skip, limit = limit)
            docs   = await cursor.to_list(length=limit)
            L.debug({
                "event":"SERVICE_REPOSITORY.SEARCH",
                "message": "Service search executed",
                "filter": mongo_filter,
                "skip": skip,
                "limit": limit,
                "result_count": len(docs)
            })
            return Ok([M.ServiceX.model_validate(d) for d in docs])
        except Exception as e:
            return Err(EX.UnknownError(str(e)))


class ObservatorySearchSuggestionRepository:
    def __init__(self, collection: Collection):
        self.collection = collection

    async def record_hit(self, observatory_id: str, query: str) -> None:
        try:
            now = DT.datetime.now(DT.timezone.utc)
            await self.collection.update_one(
                {"observatory_id": observatory_id, "query": query},
                {
                    "$inc": {"hit_count": 1},
                    "$set": {"last_used": now, "observatory_id": observatory_id, "query": query},
                },
                upsert=True,
            )
        except Exception as e:
            L.error(f"Failed to record search suggestion hit: {e}")

    async def get_top(self, observatory_id: str, limit: int = 5) -> List[M.ObservatorySearchSuggestion]:
        try:
            cursor = self.collection.find(
                {"observatory_id": observatory_id},
                {"_id": 0},
            ).sort([("hit_count", -1), ("last_used", -1)]).limit(limit)
            docs = await cursor.to_list(length=limit)
            return [M.ObservatorySearchSuggestion(**d) for d in docs]
        except Exception as e:
            L.error(f"Failed to fetch search suggestions: {e}")
            return []